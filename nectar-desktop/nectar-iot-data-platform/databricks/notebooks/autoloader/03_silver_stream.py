# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Silver — clean it
# MAGIC
# MAGIC Reads the bronze table **as a stream**. Bronze is the topic now.
# MAGIC
# MAGIC Every row is checked against a list of rules. Good rows go to silver.
# MAGIC Bad rows go to a bin — **not deleted** — with a label saying which rules
# MAGIC they broke. That label is what lets an engineer find the faulty machine.
# MAGIC
# MAGIC Two levels:
# MAGIC
# MAGIC * **STOP** — the row cannot be used at all. No machine id means we do not
# MAGIC   know whose reading it is. Bin it.
# MAGIC * **WARN** — one value is missing but the rest is fine. A missing
# MAGIC   temperature does not spoil the power reading. Keep it, flag it.
# MAGIC
# MAGIC That difference is a judgement about **usefulness**, not about how bad the
# MAGIC row looks.

# COMMAND ----------

CKPT = "/Volumes/nectar/bronze/checkpoints"
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md ## The rules

# COMMAND ----------

# Each rule: a name, and a test the row must pass.
STOP_RULES = {
    "missing.timestamp":      "ts IS NOT NULL",
    "missing.machine_id":     "asset_id IS NOT NULL AND asset_id <> ''",
    "missing.site_id":        "site_id IS NOT NULL AND site_id <> ''",
    "bad.date_unreadable":    "raw_timestamp IS NULL OR ts IS NOT NULL",
    # A dead clock reports 1970. A broken one reports next century. Both read as
    # valid dates, so checking the format alone would let them through.
    "bad.date_impossible":    "ts IS NULL OR (ts >= '2000-01-01' AND ts <= current_timestamp() + INTERVAL 1 DAY)",
    "unknown.machine":        "machine_known",
    # Physically impossible readings. A stuck sensor sends -999 or 9999.
    "impossible.temperature": "temperature IS NULL OR temperature BETWEEN -40 AND 120",
    "impossible.humidity":    "humidity IS NULL OR humidity BETWEEN 0 AND 100",
    "impossible.pressure":    "pressure IS NULL OR pressure BETWEEN 0 AND 1200",
    "impossible.power":       "power_consumption IS NULL OR power_consumption BETWEEN 0 AND 5000",
}

WARN_RULES = {
    "gap.temperature": "temperature IS NOT NULL",
    "gap.humidity":    "humidity IS NOT NULL",
    "gap.power":       "power_consumption IS NOT NULL",
}

def rules_broken(rules):
    """A list of the rule names this row failed. Empty list means the row is fine."""
    return F.array_compact(F.array(*[
        F.when(~F.expr(test), F.lit(name)) for name, test in rules.items()]))

# COMMAND ----------
# MAGIC %md ## Read bronze as a stream, clean each row

# COMMAND ----------

# The machine list. Small, so Spark sends a copy to every worker instead of
# shuffling the big table around.
machines = F.broadcast(
    spark.table("nectar.bronze.assets")
         .select(F.upper(F.trim("asset_id")).alias("known_id"))
         .dropDuplicates(["known_id"])
)

stream = spark.readStream.table("nectar.bronze.telemetry")

cleaned = (
    stream
    # Keep the original text. Then we can tell "the machine sent nothing" apart
    # from "the machine sent something we could not read".
    .withColumn("raw_timestamp", F.col("timestamp"))
    .withColumn("ts", F.coalesce(
        F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")),
        F.try_to_timestamp("timestamp")))
    .withColumn("asset_id", F.upper(F.trim("asset_id")))
    .withColumn("site_id", F.upper(F.trim("site_id")))
    .withColumn("building_id", F.upper(F.trim("building_id")))
    .withColumn("operating_mode", F.upper(F.trim("operating_mode")))
)
# try_cast turns junk text into NULL instead of killing the whole job.
for c in ["temperature", "humidity", "pressure", "vibration", "power_consumption"]:
    cleaned = cleaned.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))

cleaned = (
    cleaned.join(machines, cleaned.asset_id == F.col("known_id"), "left")
    .withColumn("machine_known", F.col("known_id").isNotNull())
    .drop("known_id")
    .withColumn("rules_failed", rules_broken(STOP_RULES))
    .withColumn("warnings", rules_broken(WARN_RULES))
    .withColumn("event_date", F.to_date("ts"))
)

# COMMAND ----------
# MAGIC %md ## Good rows → silver

# COMMAND ----------

good = (
    cleaned.filter(F.size("rules_failed") == 0)
    # The producer may send the same reading twice. The watermark keeps only
    # 15 minutes of memory, so this never grows forever.
    .withWatermark("ts", "15 minutes")
    .dropDuplicates(["asset_id", "sensor_id", "ts"])
    .drop("rules_failed", "raw_timestamp", "machine_known")
)

q_silver = (
    good.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CKPT}/silver_telemetry")
    .trigger(processingTime="10 seconds")
    .toTable("nectar.silver.telemetry")
)

# COMMAND ----------
# MAGIC %md ## Bad rows → the bin, with reasons

# COMMAND ----------

q_bin = (
    cleaned.filter(F.size("rules_failed") > 0)
    .withColumn("binned_at", F.current_timestamp())
    .writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CKPT}/quarantine")
    .trigger(processingTime="10 seconds")
    .toTable("nectar.silver.quarantine")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Live 5-minute summary
# MAGIC
# MAGIC Grouped by the machine's own clock, not by when it reached us. A batch of
# MAGIC readings held back by a bad connection belongs to the time it happened.

# COMMAND ----------

live = (
    cleaned.filter(F.size("rules_failed") == 0)
    .withWatermark("ts", "15 minutes")
    .groupBy(F.window("ts", "5 minutes").alias("w"), "site_id", "building_id", "asset_id")
    .agg(F.count(F.lit(1)).alias("readings"),
         F.avg("power_consumption").alias("avg_power_kw"),
         F.max("power_consumption").alias("peak_power_kw"),
         F.avg("temperature").alias("avg_temperature"),
         F.max("vibration").alias("max_vibration"))
    .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
            "site_id", "building_id", "asset_id", "readings",
            "avg_power_kw", "peak_power_kw", "avg_temperature", "max_vibration")
)

q_live = (
    live.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{CKPT}/live_5min")
    .trigger(processingTime="10 seconds")
    .toTable("nectar.gold.live_asset_5min")
)

# COMMAND ----------
# MAGIC %md ## Watch it work

# COMMAND ----------

import time
for i in range(10):
    print(f"{i*15:>4}s  silver: {spark.table('nectar.silver.telemetry').count():>8,}"
          f"   bin: {spark.table('nectar.silver.quarantine').count():>6,}"
          f"   5-min windows: {spark.table('nectar.gold.live_asset_5min').count():>5,}")
    time.sleep(15)

# COMMAND ----------
# MAGIC %md ## Why each row was binned

# COMMAND ----------

display(spark.sql("""
    SELECT reason, count(*) AS rows_binned
    FROM (SELECT explode(rules_failed) AS reason FROM nectar.silver.quarantine)
    GROUP BY reason ORDER BY rows_binned DESC
"""))
