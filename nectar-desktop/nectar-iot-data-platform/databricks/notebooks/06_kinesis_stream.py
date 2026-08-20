# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time path - Kinesis
# MAGIC
# MAGIC The AWS-native ingress alternative to Kafka. Same downstream logic, same
# MAGIC rules; only the source differs.
# MAGIC
# MAGIC **The trade-off to state honestly:** Kinesis retention is 24 hours by
# MAGIC default and 7 days at most, against whatever you configure on Kafka. Replay
# MAGIC is the primary recovery mechanism in this architecture, so a shorter
# MAGIC retention window directly shortens the recovery story. It is a real cost of
# MAGIC choosing the managed service, not a detail.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.text("catalog", "nectar")
dbutils.widgets.text("stream_name", "iot-telemetry-raw")
dbutils.widgets.text("region", "ap-south-1")

CATALOG = dbutils.widgets.get("catalog")
STREAM = dbutils.widgets.get("stream_name")
REGION = dbutils.widgets.get("region")
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/landing/_checkpoints/kinesis"

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in [
    "timestamp", "site_id", "building_id", "asset_id", "sensor_id",
    "temperature", "humidity", "pressure", "vibration",
    "power_consumption", "operating_mode",
]])

# COMMAND ----------

raw = (
    spark.readStream.format("kinesis")
    .option("streamName", STREAM)
    .option("region", REGION)
    .option("initialPosition", "TRIM_HORIZON")
    # Bounds each micro-batch so a backlog after downtime does not produce one
    # unschedulable catch-up batch.
    .option("maxRecordsPerFetch", "10000")
    .load()
)

parsed = (
    raw.select(
        F.col("data").cast("string").alias("_raw_value"),
        F.col("partitionKey").alias("_partition_key"),
        F.col("approximateArrivalTimestamp").alias("_arrival_ts"),
    )
    .withColumn("_parsed", F.from_json("_raw_value", RAW_SCHEMA))
    # A payload that is not valid JSON cannot be evaluated by a rule - there is
    # no record to evaluate. It goes to the DLQ so one bad producer never blocks
    # the stream.
    .withColumn("_parse_failed", F.col("_parsed").isNull())
)

(parsed.filter("_parse_failed")
 .withColumn("_dlq_reason", F.lit("payload_not_parseable"))
 .withColumn("_dlq_at", F.current_timestamp())
 .writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{CHECKPOINT}/dlq")
 .trigger(processingTime="10 seconds")
 .toTable(f"{CATALOG}.quarantine.dlq_telemetry"))

# COMMAND ----------

typed = parsed.filter("NOT _parse_failed").select("_arrival_ts", "_partition_key", "_parsed.*")
typed = typed.withColumn("timestamp", F.coalesce(
    F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss['Z']")),
    F.try_to_timestamp("timestamp")))
for c in ["temperature", "humidity", "pressure", "vibration", "power_consumption"]:
    typed = typed.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))

clean = (
    typed
    # Event time, not arrival time. Using arrival would misattribute a batch of
    # readings buffered through a network outage to the moment they landed.
    .withWatermark("timestamp", "15 minutes")
    .dropDuplicates(["asset_id", "sensor_id", "timestamp"])
    .withColumn("event_date", F.to_date("timestamp"))
)

(clean.writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{CHECKPOINT}/silver")
 .trigger(processingTime="10 seconds")
 .toTable(f"{CATALOG}.silver.telemetry_stream"))

# COMMAND ----------
# MAGIC %md ## Live 5-minute rollup for the operations dashboard

rollup = (
    clean.groupBy(F.window("timestamp", "5 minutes").alias("w"),
                  "site_id", "building_id", "asset_id")
    .agg(F.count(F.lit(1)).alias("readings"),
         F.avg("power_consumption").alias("avg_power_kw"),
         F.max("power_consumption").alias("peak_power_kw"),
         F.max("vibration").alias("max_vibration"))
    .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
            "site_id", "building_id", "asset_id", "readings",
            "avg_power_kw", "peak_power_kw", "max_vibration")
    # Fast and approximate. The batch layer restates it exactly - that
    # reconciliation is the design, not an incident.
    .withColumn("approx_energy_kwh", F.col("avg_power_kw") / F.lit(12.0))
)

(rollup.writeStream.format("delta").outputMode("append")
 .option("checkpointLocation", f"{CHECKPOINT}/rollup")
 .trigger(processingTime="10 seconds")
 .toTable(f"{CATALOG}.gold.stream_asset_5min"))
