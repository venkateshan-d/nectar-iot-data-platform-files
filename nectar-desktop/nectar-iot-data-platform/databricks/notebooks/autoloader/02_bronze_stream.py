# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Bronze — read the folder, save everything
# MAGIC
# MAGIC This is the **consumer**. Auto Loader watches the landing folder. Every time
# MAGIC notebook 1 drops a new file, this stream picks it up on its own. You never
# MAGIC tell it which files are new — it keeps that list itself, in the checkpoint.
# MAGIC
# MAGIC | Kafka word | Here |
# MAGIC |---|---|
# MAGIC | `subscribe` topic | `cloudFiles` on a folder |
# MAGIC | consumer group offset | checkpoint |
# MAGIC | `maxOffsetsPerTrigger` | `cloudFiles.maxFilesPerTrigger` |
# MAGIC | exactly once | checkpoint + Delta commit |
# MAGIC
# MAGIC **Rule: change nothing here.** Every field stays as text. If a machine sent
# MAGIC `"temperature": "hot"`, we keep `"hot"`. If we cleaned it here we could never
# MAGIC prove what the machine actually sent, and we could never replay it after the
# MAGIC machine is fixed.
# MAGIC
# MAGIC A line that is not proper JSON cannot be checked by any rule — there is no
# MAGIC record to check. Those go to a separate table, the **dead letter queue**, so
# MAGIC one bad machine can never stop the whole stream.

# COMMAND ----------

CATALOG = "nectar"
LANDING = f"/Volumes/{CATALOG}/bronze/landing/telemetry"
CKPT = f"/Volumes/{CATALOG}/bronze/checkpoints"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
print("watching:", LANDING)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read the folder as a stream
# MAGIC
# MAGIC We read the files as **plain text**, one line per row, and parse the JSON
# MAGIC ourselves. Reading them as JSON directly would be shorter, but a broken line
# MAGIC would then be silently dropped or would fail the batch. Reading as text means
# MAGIC every line arrives, and we decide what to do with the broken ones.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Every field is text on purpose. See the note at the top.
RAW = StructType([StructField(c, StringType(), True) for c in [
    "timestamp", "site_id", "building_id", "asset_id", "sensor_id",
    "temperature", "humidity", "pressure", "vibration",
    "power_consumption", "operating_mode"]])

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", f"{CKPT}/bronze_schema")
    # Limit each small batch. After downtime there could be a big backlog, and one
    # giant batch would be slow and might not fit in memory.
    .option("cloudFiles.maxFilesPerTrigger", 20)
    # Files whose name starts with _ are half-written. The producer writes to
    # _tmp_*.json first and renames, so this line stops us reading a partial file.
    .option("pathGlobFilter", "batch_*.json")
    .load(LANDING)
)

parsed = (
    stream.select(
        F.col("value").alias("raw_value"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("file_time"),
    )
    .filter(F.length(F.trim(F.col("raw_value"))) > 0)
    .withColumn("parsed", F.from_json("raw_value", RAW))
    .withColumn("bad_json", F.col("parsed").isNull())
)

# COMMAND ----------
# MAGIC %md ## Good lines → bronze table

# COMMAND ----------

bronze = (
    parsed.filter("NOT bad_json")
    .select("source_file", "file_time", "parsed.*")
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("ingest_date", F.to_date(F.current_timestamp()))
)

q_bronze = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze_telemetry")
    # The checkpoint remembers which files we already read. If this notebook
    # crashes and restarts, it carries on from there - no gaps, no repeats.
    .trigger(processingTime="10 seconds")
    .toTable(f"{CATALOG}.bronze.telemetry")
)

# COMMAND ----------
# MAGIC %md ## Broken lines → dead letter queue

# COMMAND ----------

q_dlq = (
    parsed.filter("bad_json")
    .select("raw_value", "source_file", "file_time")
    .withColumn("reason", F.lit("line is not valid JSON"))
    .withColumn("arrived_at", F.current_timestamp())
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/dlq")
    .trigger(processingTime="10 seconds")
    .toTable(f"{CATALOG}.bronze.dead_letters")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Watch it fill up
# MAGIC
# MAGIC Run this cell again and again while notebook 1 is running. The numbers should
# MAGIC climb. **This is the demo for your video.**

# COMMAND ----------

import time
for i in range(10):
    good = spark.table(f"{CATALOG}.bronze.telemetry").count()
    bad = spark.table(f"{CATALOG}.bronze.dead_letters").count()
    print(f"{i*15:>4}s   good rows: {good:>8,}   dead letters: {bad:>5,}")
    time.sleep(15)

# COMMAND ----------
# MAGIC %md
# MAGIC ## How far behind are we?
# MAGIC
# MAGIC This is the lag number. `numFilesOutstanding` is how many files are waiting.
# MAGIC If it keeps growing, the machines are producing faster than we can read.

# COMMAND ----------

import json
p = q_bronze.lastProgress
if p:
    print("batch id      :", p.get("batchId"))
    print("rows this batch:", p.get("numInputRows"))
    print("rows / second :", round(p.get("processedRowsPerSecond") or 0, 1))
    print("backlog       :", json.dumps(p.get("sources", [{}])[0].get("metrics", {}), indent=2))
else:
    print("no batch finished yet - wait 15 seconds and run again")

# COMMAND ----------
# MAGIC %md ## Stop the streams when you are done

# COMMAND ----------

# for q in spark.streams.active:
#     print("stopping", q.name or q.id)
#     q.stop()
