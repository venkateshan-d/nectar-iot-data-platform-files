# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Bronze — read Kafka, save everything
# MAGIC
# MAGIC This is the consumer. It reads the topic and writes to a Delta table.
# MAGIC
# MAGIC **Rule: change nothing.** Every field stays as text. If a machine sent
# MAGIC `"temperature": "hot"`, we keep `"hot"`. If we cleaned it here we could
# MAGIC never prove what the machine actually sent, and we could never replay it
# MAGIC after the machine is fixed.
# MAGIC
# MAGIC A message that is not proper JSON cannot be checked by any rule — there is
# MAGIC no record to check. Those go to a separate table, the **dead letter queue**,
# MAGIC so one bad machine can never stop the whole stream.

# COMMAND ----------

dbutils.widgets.text("bootstrap", "", "Confluent bootstrap server")
dbutils.widgets.text("api_key", "", "API key")
dbutils.widgets.text("api_secret", "", "API secret")
dbutils.widgets.text("topic", "iot-telemetry-raw", "Topic")

BOOTSTRAP = dbutils.widgets.get("bootstrap")
API_KEY = dbutils.widgets.get("api_key")
API_SECRET = dbutils.widgets.get("api_secret")
TOPIC = dbutils.widgets.get("topic")
CKPT = "/Volumes/nectar/bronze/checkpoints"

spark.sql("CREATE VOLUME IF NOT EXISTS nectar.bronze.checkpoints")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

JAAS = (f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{API_KEY}" password="{API_SECRET}";')

# Every field is text on purpose. See the note at the top.
RAW = StructType([StructField(c, StringType(), True) for c in [
    "timestamp", "site_id", "building_id", "asset_id", "sensor_id",
    "temperature", "humidity", "pressure", "vibration",
    "power_consumption", "operating_mode"]])

stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    # Limit each small batch. After downtime there could be a huge backlog, and
    # one giant batch would be slow and might not fit in memory.
    .option("maxOffsetsPerTrigger", 50000)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_time"),
    )
    .withColumn("parsed", F.from_json("raw_value", RAW))
    .withColumn("bad_json", F.col("parsed").isNull())
)

# COMMAND ----------
# MAGIC %md ## Good messages → bronze table

# COMMAND ----------

bronze = (
    parsed.filter("NOT bad_json")
    .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_time", "parsed.*")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("ingest_date", F.to_date(F.current_timestamp()))
)

q_bronze = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze_telemetry")
    # The checkpoint remembers which messages we already read. If this notebook
    # crashes and restarts, it carries on from there - no gaps, no repeats.
    .trigger(processingTime="10 seconds")
    .toTable("nectar.bronze.telemetry")
)

# COMMAND ----------
# MAGIC %md ## Broken messages → dead letter queue

# COMMAND ----------

q_dlq = (
    parsed.filter("bad_json")
    .select("raw_value", "kafka_partition", "kafka_offset", "kafka_time")
    .withColumn("reason", F.lit("message is not valid JSON"))
    .withColumn("arrived_at", F.current_timestamp())
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/dlq")
    .trigger(processingTime="10 seconds")
    .toTable("nectar.bronze.dead_letters")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Watch it fill up
# MAGIC
# MAGIC Run this cell again and again while the producer is running. The number
# MAGIC should climb. **This is the demo for your video.**

# COMMAND ----------

import time
for i in range(10):
    good = spark.table("nectar.bronze.telemetry").count()
    bad = spark.table("nectar.bronze.dead_letters").count()
    print(f"{i*15:>4}s   good rows: {good:>8,}   dead letters: {bad:>5,}")
    time.sleep(15)

# COMMAND ----------
# MAGIC %md ## Stop the streams when you are done

# COMMAND ----------

# for q in spark.streams.active:
#     print("stopping", q.name or q.id)
#     q.stop()
