# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - typed, validated, deduplicated
# MAGIC
# MAGIC ## The one place this deviates from idiomatic Lakeflow
# MAGIC
# MAGIC `expect_all_or_drop` discards failing rows. That loses the evidence needed
# MAGIC to go back to whoever owns the device, and "how many rows did we reject
# MAGIC last night, and why" stops being answerable from the data itself.
# MAGIC
# MAGIC So the rules are defined **once**, as data, and used **twice**:
# MAGIC
# MAGIC 1. as Lakeflow expectations on the clean table, so the pass/fail metrics
# MAGIC    land in the pipeline event log and appear in the pipeline UI;
# MAGIC 2. as the predicate for a **quarantine table**, which keeps every rejected
# MAGIC    row annotated with the rule ids it broke.
# MAGIC
# MAGIC Expectations give observability. The quarantine table gives recoverability.
# MAGIC Neither substitutes for the other.

# COMMAND ----------

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

from pyspark.sql import functions as F

CATALOG = spark.conf.get("nectar.catalog", "nectar")
WATERMARK_HOURS = float(spark.conf.get("nectar.late_arrival_watermark_hours", "24"))

VALID_MODES = ["RUNNING", "IDLE", "STANDBY", "BOOST", "MAINTENANCE", "OFF", "FAULT"]
VALID_EVENT_TYPES = ["Alarm", "Warning", "Fault", "Info"]
VALID_SEVERITIES = ["Low", "Medium", "High"]

RANGES = {
    "temperature": (-40.0, 120.0),
    "humidity": (0.0, 100.0),
    "pressure": (0.0, 1200.0),
    "vibration": (0.0, 100.0),
    "power_consumption": (0.0, 5000.0),
}

# ---------------------------------------------------------------------------
# Rules as data. Expressed as SQL strings because Lakeflow expectations take
# SQL, and because the same string can then drive the quarantine filter.
# Each entry is: rule_id -> condition that must be TRUE for the row to pass.
# ---------------------------------------------------------------------------
TELEMETRY_RULES = {
    "tel.completeness.timestamp_not_null": "timestamp IS NOT NULL",
    "tel.completeness.asset_id_not_null": "asset_id IS NOT NULL AND asset_id <> ''",
    "tel.completeness.site_id_not_null": "site_id IS NOT NULL AND site_id <> ''",
    "tel.completeness.building_id_not_null": "building_id IS NOT NULL AND building_id <> ''",
    "tel.validity.timestamp_parseable": "_raw_timestamp IS NULL OR timestamp IS NOT NULL",
    "tel.validity.timestamp_plausible":
        "timestamp IS NULL OR (timestamp >= '2000-01-01' "
        "AND timestamp <= current_timestamp() + INTERVAL 1 DAY)",
    "tel.consistency.asset_registered": "_asset_known",
    **{
        f"tel.accuracy.{col}_in_range":
            f"{col} IS NULL OR ({col} BETWEEN {lo} AND {hi})"
        for col, (lo, hi) in RANGES.items()
    },
}

# WARN-severity rules. Recorded and visible in the event log, but a failing row
# is still usable - a null temperature does not invalidate the power reading.
TELEMETRY_WARN_RULES = {
    "tel.validity.operating_mode_enum":
        f"operating_mode IS NULL OR operating_mode IN {tuple(VALID_MODES)}",
    "tel.completeness.power_consumption_present": "power_consumption IS NOT NULL",
    "tel.completeness.temperature_present": "temperature IS NOT NULL",
}

EVENT_RULES = {
    "evt.completeness.event_id_not_null": "event_id IS NOT NULL AND event_id <> ''",
    "evt.completeness.timestamp_not_null": "timestamp IS NOT NULL",
    "evt.completeness.asset_id_not_null": "asset_id IS NOT NULL AND asset_id <> ''",
    "evt.validity.timestamp_parseable": "_raw_timestamp IS NULL OR timestamp IS NOT NULL",
    "evt.validity.event_type_enum":
        f"event_type IS NULL OR event_type IN {tuple(VALID_EVENT_TYPES)}",
    "evt.validity.severity_enum":
        f"severity IS NULL OR severity IN {tuple(VALID_SEVERITIES)}",
    "evt.consistency.asset_registered": "_asset_known",
}


def _failed_rules_column(rules: dict):
    """Array of the rule ids this row broke - the annotation on quarantined rows."""
    return F.array_compact(F.array(*[
        F.when(~F.expr(cond), F.lit(rule_id)) for rule_id, cond in rules.items()
    ]))


# ---------------------------------------------------------------------------
# Prepared views: cast, enrich, flag. Temporary - not published to the catalog.
# ---------------------------------------------------------------------------
@dp.temporary_view(name="telemetry_prepared")
def telemetry_prepared():
    register = (
        dp.read("bronze_assets")
        .select(
            F.upper(F.trim("asset_id")).alias("_reg_asset_id"),
            F.upper(F.trim("site_id")).alias("_register_site_id"),
        )
        .dropDuplicates(["_reg_asset_id"])
    )

    df = dp.read_stream("bronze_telemetry")
    df = (
        df.withColumn("_raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp", F.coalesce(
            F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss['Z']")),
            F.try_to_timestamp("timestamp")))
    )
    for c in ["site_id", "building_id", "asset_id", "sensor_id", "operating_mode"]:
        df = df.withColumn(c, F.upper(F.trim(F.col(c))))
    for c in RANGES:
        df = df.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))

    df = (
        df.join(F.broadcast(register), df.asset_id == F.col("_reg_asset_id"), "left")
        .withColumn("_asset_known", F.col("_reg_asset_id").isNotNull())
        .drop("_reg_asset_id")
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("_lateness_seconds",
                    F.datediff(F.col("ingest_date"), F.to_date("timestamp")).cast("long") * 86400)
        .withColumn("_is_late",
                    F.coalesce(F.col("_lateness_seconds") >= F.lit(WATERMARK_HOURS * 3600),
                               F.lit(False)))
        .withColumn("_failed_rules", _failed_rules_column(TELEMETRY_RULES))
        .withColumn("_warnings", _failed_rules_column(TELEMETRY_WARN_RULES))
    )
    return df


@dp.temporary_view(name="events_prepared")
def events_prepared():
    register = (
        dp.read("bronze_assets")
        .select(
            F.upper(F.trim("asset_id")).alias("_reg_asset_id"),
            F.upper(F.trim("site_id")).alias("_register_site_id"),
            F.upper(F.trim("building_id")).alias("_register_building_id"),
        )
        .dropDuplicates(["_reg_asset_id"])
    )
    df = dp.read_stream("bronze_events")
    df = (
        df.withColumn("_raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp", F.coalesce(
            F.try_to_timestamp("timestamp", F.lit("yyyy-MM-dd'T'HH:mm:ss['Z']")),
            F.try_to_timestamp("timestamp")))
        .withColumn("event_id", F.upper(F.trim("event_id")))
        .withColumn("asset_id", F.upper(F.trim("asset_id")))
        .withColumn("event_type", F.initcap(F.trim("event_type")))
        .withColumn("severity", F.initcap(F.trim("severity")))
        .join(F.broadcast(register), F.col("asset_id") == F.col("_reg_asset_id"), "left")
        .withColumn("_asset_known", F.col("_reg_asset_id").isNotNull())
        .withColumn("site_id", F.col("_register_site_id"))
        .withColumn("building_id", F.col("_register_building_id"))
        .drop("_reg_asset_id")
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("_failed_rules", _failed_rules_column(EVENT_RULES))
    )
    return df


# ---------------------------------------------------------------------------
# Clean tables. Expectations here are what surfaces in the pipeline event log.
# ---------------------------------------------------------------------------
@dp.table(
    name="silver_telemetry",
    comment="Typed, validated, deduplicated telemetry. One row per (asset, sensor, timestamp).",
    table_properties={"quality": "silver", "clusteringColumns": "asset_id,timestamp"},
)
@dp.expect_all_or_drop(TELEMETRY_RULES)
@dp.expect_all(TELEMETRY_WARN_RULES)
def silver_telemetry():
    return (
        dp.read_stream("telemetry_prepared")
        # Bounded state: the gateway delivers at-least-once, and duplicates
        # arrive close together. Beyond the watermark the batch layer owns it.
        .withWatermark("timestamp", "15 minutes")
        .dropDuplicates(["asset_id", "sensor_id", "timestamp"])
        .drop("_failed_rules", "_raw_timestamp", "_asset_known", "_register_site_id")
    )


@dp.table(
    name="silver_events",
    comment="Typed, validated operational events. One row per event_id.",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(EVENT_RULES)
def silver_events():
    return (
        dp.read_stream("events_prepared")
        .withWatermark("timestamp", "15 minutes")
        .dropDuplicates(["event_id"])
        .drop("_failed_rules", "_raw_timestamp", "_asset_known",
              "_register_site_id", "_register_building_id")
    )


# ---------------------------------------------------------------------------
# Quarantine. The complement of the clean tables, with the reasons attached.
# ---------------------------------------------------------------------------
@dp.table(
    name="quarantine_telemetry",
    comment="Rows rejected by a blocking rule, annotated with the rule ids they broke. "
            "Replayable after the upstream fix.",
    table_properties={"quality": "quarantine"},
)
def quarantine_telemetry():
    return (
        dp.read_stream("telemetry_prepared")
        .filter(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )


@dp.table(
    name="quarantine_events",
    comment="Rejected events with their failure reasons.",
    table_properties={"quality": "quarantine"},
)
def quarantine_events():
    return (
        dp.read_stream("events_prepared")
        .filter(F.size("_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )
