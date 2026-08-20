# Databricks notebook source
# MAGIC %md
# MAGIC # Data quality report + freshness
# MAGIC
# MAGIC Lakeflow already records expectation pass/fail in the pipeline event log.
# MAGIC This notebook does the two things the event log cannot:
# MAGIC
# MAGIC 1. flattens those metrics into a queryable `quality.dq_results` table, so
# MAGIC    "which site's data degraded this week" is a SQL question rather than a
# MAGIC    UI click;
# MAGIC 2. computes **freshness**, which no expectation can - a row-level rule
# MAGIC    cannot validate a record that never arrived.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "nectar")
dbutils.widgets.text("pipeline_id", "")
CATALOG = dbutils.widgets.get("catalog")
PIPELINE_ID = dbutils.widgets.get("pipeline_id")

# COMMAND ----------
# MAGIC %md ## Expectation metrics out of the pipeline event log

events = spark.read.table(f"event_log(TABLE({CATALOG}.silver.silver_telemetry))") \
    if not PIPELINE_ID else spark.read.table(f"event_log('{PIPELINE_ID}')")

dq = (
    events.filter("event_type = 'flow_progress'")
    .select("timestamp", "origin.flow_name",
            F.explode("details:flow_progress.data_quality.expectations").alias("e"))
    .select(
        F.col("timestamp").alias("evaluated_at"),
        F.col("flow_name").alias("table_name"),
        F.col("e.name").alias("rule_id"),
        F.col("e.dataset").alias("dataset"),
        F.col("e.passed_records").cast("long").alias("rows_passed"),
        F.col("e.failed_records").cast("long").alias("rows_failed"),
    )
    .withColumn("rows_evaluated", F.col("rows_passed") + F.col("rows_failed"))
    .withColumn("failure_rate", F.when(F.col("rows_evaluated") > 0,
                                       F.col("rows_failed") / F.col("rows_evaluated")).otherwise(0.0))
    .withColumn("dimension", F.split(F.col("rule_id"), "\\.").getItem(1))
)
dq.write.mode("append").saveAsTable(f"{CATALOG}.quality.dq_results")
display(dq.filter("rows_failed > 0").orderBy(F.col("failure_rate").desc()))

# COMMAND ----------
# MAGIC %md ## Freshness - how absence is detected

freshness = (
    spark.table(f"{CATALOG}.silver.silver_telemetry")
    .groupBy("site_id", "building_id", "asset_id")
    .agg(F.max("timestamp").alias("last_seen_at"), F.count(F.lit(1)).alias("readings"))
    .withColumn("lag_minutes",
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_seen_at")) / 60.0)
    .withColumn("is_stale", F.col("lag_minutes") > 60)
    .withColumn("evaluated_at", F.current_timestamp())
)
freshness.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.quality.asset_freshness")

stale = freshness.filter("is_stale")
print(f"stale assets: {stale.count()}")
display(stale.orderBy(F.col("lag_minutes").desc()))

# COMMAND ----------
# MAGIC %md ## Gate - fail the job when a blocking rule breaches its threshold

THRESHOLD = 0.05
breaches = [r.asDict() for r in dq.filter(F.col("failure_rate") > THRESHOLD).collect()]
quarantined = spark.table(f"{CATALOG}.silver.quarantine_telemetry").count()

summary = {"breaches": len(breaches), "rows_quarantined": quarantined,
           "verdict": "FAIL" if breaches else "PASS"}
print(summary)

if breaches:
    # Fails the task, which stops every downstream task in the job. Stale
    # dashboards are recoverable; wrong ones are not.
    raise Exception(f"Data quality gate failed: {[b['rule_id'] for b in breaches]}")

dbutils.jobs.taskValues.set(key="quality_summary", value=summary)
