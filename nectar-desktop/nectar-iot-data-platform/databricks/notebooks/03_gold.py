# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dimensional model, curated marts, roll-ups
# MAGIC
# MAGIC Materialized views rather than streaming tables: these are aggregates over
# MAGIC the full history, and Lakeflow recomputes them incrementally where it can.
# MAGIC
# MAGIC The two calculations that are easy to get silently wrong are unchanged
# MAGIC from the portable pipeline, and for the same reasons:
# MAGIC
# MAGIC * **Energy** - devices report instantaneous kW, once per sensor, for an
# MAGIC   asset-level quantity. Readings are collapsed to asset grain first, then
# MAGIC   weighted by the gap to the next reading, capped at 2x the sampling
# MAGIC   interval so an outage is not billed at the last known load.
# MAGIC * **Utilisation** - productive hours over *observed* hours, not over 24,
# MAGIC   so "idle" and "not reporting" stay distinguishable.

# COMMAND ----------

try:
    from pyspark import pipelines as dp
except ImportError:
    import dlt as dp

from pyspark.sql import Window
from pyspark.sql import functions as F

INTERVAL_MIN = float(spark.conf.get("nectar.telemetry_interval_minutes", "5"))
MAX_GAP_SEC = INTERVAL_MIN * 60 * 2
PRODUCTIVE = ["RUNNING", "BOOST"]
DOWN = ["OFF", "FAULT", "MAINTENANCE"]
MEASURES = ["temperature", "humidity", "pressure", "vibration", "power_consumption"]

# COMMAND ----------
# MAGIC %md ## Dimensions

@dp.materialized_view(name="dim_site", comment="Site dimension.")
def dim_site():
    return dp.read("bronze_sites").select(
        F.xxhash64("site_id").alias("site_key"),
        "site_id", "site_name", "city", "country", "timezone", "customer_id",
    ).dropDuplicates(["site_id"])


@dp.materialized_view(name="dim_building", comment="Building dimension with size band.")
def dim_building():
    return (
        dp.read("bronze_buildings").dropDuplicates(["building_id"])
        .select("building_id", "building_name", "site_id", "floor_area_sqm", "building_type")
        .withColumn("building_key", F.xxhash64("building_id"))
        .withColumn("site_key", F.xxhash64("site_id"))
        .withColumn("size_band", F.when(F.col("floor_area_sqm") < 5000, "Small")
                    .when(F.col("floor_area_sqm") < 15000, "Medium").otherwise("Large"))
    )


# SCD Type 2 on the asset dimension. Assets get relocated between buildings and
# re-rated after retrofits; overwriting those attributes would silently restate
# last quarter's per-building energy. AUTO CDC (formerly APPLY CHANGES) handles
# the versioning, so no hand-written merge.
dp.create_streaming_table(
    name="dim_asset",
    comment="Asset dimension, SCD Type 2. valid_from/valid_to track attribute history.",
    table_properties={"quality": "gold"},
)


@dp.temporary_view(name="asset_changes")
def asset_changes():
    return (
        dp.read_stream("bronze_assets")
        .withColumn("asset_id", F.upper(F.trim("asset_id")))
        .withColumn("parent_asset_id", F.upper(F.trim("parent_asset_id")))
        .withColumn("_change_ts", F.col("_ingested_at"))
    )


dp.apply_changes(
    target="dim_asset",
    source="asset_changes",
    keys=["asset_id"],
    sequence_by=F.col("_change_ts"),
    stored_as_scd_type=2,
    # Only these attributes open a new version. An ingestion timestamp changing
    # is not a business change and must not create a row.
    track_history_except_column_list=["_ingested_at", "_change_ts", "_source_file"],
)

# COMMAND ----------
# MAGIC %md ## Asset-level readings with duration weights

@dp.materialized_view(
    name="asset_readings",
    comment="Telemetry collapsed to asset grain with a duration weight per reading. "
            "Averaging across sensors first is what stops a two-sensor chiller "
            "reporting double its real energy.",
)
def asset_readings():
    per_asset = (
        dp.read("silver_telemetry")
        .filter(F.col("timestamp").isNotNull())
        .groupBy("site_id", "building_id", "asset_id", "timestamp")
        .agg(*[F.avg(m).alias(m) for m in MEASURES],
             F.first("operating_mode", ignorenulls=True).alias("operating_mode"),
             F.count(F.lit(1)).alias("sensor_readings"))
    )
    w = Window.partitionBy("asset_id").orderBy(F.col("timestamp").asc())
    return (
        per_asset
        .withColumn("_next_ts", F.lead("timestamp").over(w))
        .withColumn("duration_hours", F.least(
            F.coalesce((F.unix_timestamp("_next_ts") - F.unix_timestamp("timestamp")).cast("double"),
                       F.lit(INTERVAL_MIN * 60)),
            F.lit(MAX_GAP_SEC)) / 3600.0)
        .drop("_next_ts")
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("energy_kwh", F.col("power_consumption") * F.col("duration_hours"))
    )

# COMMAND ----------
# MAGIC %md ## Facts

@dp.materialized_view(
    name="fact_energy_hourly",
    comment="Energy fact at asset x hour. energy_kwh is the only fully additive "
            "measure in the model - safe to sum across assets, buildings and time.",
    table_properties={"quality": "gold", "clusteringColumns": "asset_id"},
)
@dp.expect("energy_non_negative", "energy_kwh >= 0")
@dp.expect_or_drop("hour_present", "event_hour IS NOT NULL")
def fact_energy_hourly():
    return (
        dp.read("asset_readings")
        .groupBy("site_id", "building_id", "asset_id", "event_hour")
        .agg(F.sum("energy_kwh").alias("energy_kwh"),
             F.avg("power_consumption").alias("avg_power_kw"),
             F.max("power_consumption").alias("peak_power_kw"),
             F.sum("duration_hours").alias("covered_hours"),
             F.count(F.lit(1)).alias("reading_count"))
        .withColumn("event_date", F.to_date("event_hour"))
        .withColumn("hour_of_day", F.hour("event_hour"))
        # < 1.0 means readings were lost in that hour. Consumers decide whether
        # to trust the number; it is never silently hidden.
        .withColumn("data_coverage_ratio", F.round(F.col("covered_hours"), 4))
    )


@dp.materialized_view(
    name="fact_event",
    comment="One row per operational event.",
    table_properties={"quality": "gold"},
)
def fact_event():
    return (
        dp.read("silver_events")
        .withColumn("is_fault", F.col("event_type") == F.lit("Fault"))
        .withColumn("severity_rank", F.when(F.col("severity") == "High", 3)
                    .when(F.col("severity") == "Medium", 2).otherwise(1))
    )

# COMMAND ----------
# MAGIC %md ## Curated marts

@dp.materialized_view(
    name="curated_daily_asset_utilization",
    comment="Utilisation over OBSERVED hours, not calendar hours - otherwise "
            "'idle' and 'stopped reporting' collapse into one number.",
)
def curated_daily_asset_utilization():
    return (
        dp.read("asset_readings")
        .groupBy("site_id", "building_id", "asset_id", "event_date")
        .agg(
            F.sum(F.when(F.col("operating_mode").isin(PRODUCTIVE), F.col("duration_hours"))
                  .otherwise(F.lit(0.0))).alias("productive_hours"),
            F.sum(F.when(F.col("operating_mode").isin(DOWN), F.col("duration_hours"))
                  .otherwise(F.lit(0.0))).alias("downtime_hours"),
            F.sum("duration_hours").alias("observed_hours"),
            F.sum("energy_kwh").alias("energy_kwh"),
            F.avg("power_consumption").alias("avg_power_kw"),
            F.max("power_consumption").alias("peak_power_kw"),
        )
        .withColumn("utilization_pct", F.round(
            F.when(F.col("observed_hours") > 0,
                   F.col("productive_hours") / F.col("observed_hours") * 100), 2))
        .withColumn("availability_pct", F.round(
            F.when(F.col("observed_hours") > 0,
                   (1 - F.col("downtime_hours") / F.col("observed_hours")) * 100), 2))
        .withColumn("data_coverage_pct",
                    F.round(F.least(F.col("observed_hours") / 24.0, F.lit(1.0)) * 100, 2))
    )


@dp.materialized_view(
    name="curated_fault_statistics",
    comment="Per-asset reliability: counts, MTBF, health score, risk band.",
)
def curated_fault_statistics():
    events = dp.read("silver_events")
    faults = (
        events.filter(F.col("event_type") == "Fault")
        .groupBy("asset_id")
        .agg(F.min("timestamp").alias("first_fault_at"),
             F.max("timestamp").alias("last_fault_at"),
             F.count(F.lit(1)).alias("fault_count"))
        # A single fault cannot yield a mean time between failures. NULL is the
        # honest answer; a fabricated number would be worse.
        .withColumn("mtbf_hours", F.when(F.col("fault_count") > 1, F.round(
            (F.unix_timestamp("last_fault_at") - F.unix_timestamp("first_fault_at"))
            / 3600.0 / (F.col("fault_count") - 1), 2)))
    )
    return (
        events.groupBy("site_id", "building_id", "asset_id")
        .agg(F.count(F.lit(1)).alias("total_events"),
             F.sum(F.when(F.col("event_type") == "Fault", 1).otherwise(0)).alias("faults"),
             F.sum(F.when(F.col("event_type") == "Alarm", 1).otherwise(0)).alias("alarms"),
             F.sum(F.when(F.col("event_type") == "Warning", 1).otherwise(0)).alias("warnings"),
             F.sum(F.when(F.col("severity") == "High", 1).otherwise(0)).alias("high_severity_events"))
        .join(faults, "asset_id", "left")
        .withColumn("health_score", F.greatest(F.lit(0.0), F.round(
            100 - (F.col("faults") * 5 + F.col("alarms") + F.col("warnings") * 0.25), 2)))
        .withColumn("risk_band", F.when(F.col("health_score") >= 80, "Low")
                    .when(F.col("health_score") >= 50, "Medium").otherwise("High"))
    )

# COMMAND ----------
# MAGIC %md ## Roll-ups

@dp.materialized_view(name="agg_building_daily", comment="Building x day roll-up with energy intensity.")
def agg_building_daily():
    return (
        dp.read("curated_daily_asset_utilization")
        .groupBy("site_id", "building_id", "event_date")
        .agg(F.countDistinct("asset_id").alias("active_assets"),
             F.round(F.sum("energy_kwh"), 3).alias("energy_kwh"),
             F.round(F.avg("utilization_pct"), 2).alias("avg_utilization_pct"),
             F.round(F.avg("availability_pct"), 2).alias("avg_availability_pct"),
             F.round(F.avg("data_coverage_pct"), 2).alias("data_coverage_pct"),
             F.round(F.max("peak_power_kw"), 3).alias("peak_power_kw"))
        .join(dp.read("dim_building").select("building_id", "building_name",
                                             "floor_area_sqm", "building_type"),
              "building_id", "left")
        # Energy intensity, not raw kWh - otherwise the ranking is just building size.
        .withColumn("energy_intensity_kwh_per_sqm",
                    F.when(F.col("floor_area_sqm") > 0,
                           F.round(F.col("energy_kwh") / F.col("floor_area_sqm"), 5)))
    )


@dp.materialized_view(
    name="agg_site_daily",
    comment="Site x day roll-up with a trailing-baseline z-score for anomaly detection.",
)
def agg_site_daily():
    daily = (
        dp.read("agg_building_daily")
        .groupBy("site_id", "event_date")
        .agg(F.countDistinct("building_id").alias("buildings"),
             F.sum("active_assets").alias("active_assets"),
             F.round(F.sum("energy_kwh"), 3).alias("energy_kwh"),
             F.round(F.avg("avg_utilization_pct"), 2).alias("avg_utilization_pct"),
             F.round(F.avg("data_coverage_pct"), 2).alias("data_coverage_pct"),
             F.round(F.max("peak_power_kw"), 3).alias("peak_power_kw"))
    )
    # Baseline excludes today, so an anomaly cannot raise its own threshold.
    w = (Window.partitionBy("site_id")
         .orderBy(F.col("event_date").cast("timestamp").cast("long"))
         .rangeBetween(-7 * 86400, -86400))
    return (
        daily
        .withColumn("baseline_energy_kwh", F.round(F.avg("energy_kwh").over(w), 3))
        .withColumn("baseline_stddev", F.stddev_samp("energy_kwh").over(w))
        .withColumn("energy_zscore", F.when(F.col("baseline_stddev") > 0, F.round(
            (F.col("energy_kwh") - F.col("baseline_energy_kwh")) / F.col("baseline_stddev"), 3)))
        .withColumn("pct_vs_baseline", F.when(F.col("baseline_energy_kwh") > 0, F.round(
            100.0 * (F.col("energy_kwh") - F.col("baseline_energy_kwh"))
            / F.col("baseline_energy_kwh"), 1)))
        # A z-score alone fires on a site so stable that a 0.5% move is extreme.
        # Pairing it with a minimum effect size is what keeps the alert actionable.
        .withColumn("is_energy_anomaly", F.coalesce(
            (F.col("energy_zscore") >= 2.0) & (F.col("pct_vs_baseline") >= 10.0), F.lit(False)))
        .join(dp.read("dim_site").select("site_id", "site_name", "city", "country"),
              "site_id", "left")
    )
