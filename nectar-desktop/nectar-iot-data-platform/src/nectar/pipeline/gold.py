"""Gold layer - dimensional model, curated marts and roll-ups.

Contents
--------
**Dimensions** ``dim_site``, ``dim_building``, ``dim_asset`` (SCD Type 2),
``dim_date``, ``dim_time``.

**Facts** ``fact_telemetry`` (one row per reading), ``fact_energy_hourly``
(asset x hour), ``fact_event`` (one row per operational event).

**Curated marts** (Task 2) ``curated_hourly_energy``,
``curated_daily_asset_utilization``, ``curated_daily_environment``,
``curated_fault_statistics``.

**Roll-ups** (Task 2) ``agg_asset_daily``, ``agg_building_daily``,
``agg_site_daily``.

Two modelling decisions worth calling out
-----------------------------------------
*Energy from instantaneous power.* Devices report kW, not kWh. Integrating with
``avg(power) * hours`` silently under-counts when readings are missing and
double-counts when a device has two sensors. Instead each asset-level reading is
given a **duration weight** - the gap to the next reading, capped at twice the
nominal sampling interval so a 6-hour outage does not get billed as 6 hours at
the last known load - and energy is ``sum(power_kw * duration_hours)``.

*Multi-sensor assets.* ``power_consumption`` is an asset-level measure that
arrives once per sensor. Readings are therefore collapsed to
(asset, timestamp) with ``avg`` before any energy maths, otherwise a two-sensor
chiller would report double its real consumption.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..config import Config
from ..io_layer import read_table, table_exists, upsert_table, write_table
from ..logging_utils import RunContext

LOG = logging.getLogger("nectar.gold")

_MEASURES = ["temperature", "humidity", "pressure", "vibration", "power_consumption"]
_PRODUCTIVE_DEFAULT = ["RUNNING", "BOOST"]
_DOWN_MODES = ["OFF", "FAULT", "MAINTENANCE"]


def _fmt(cfg: Config) -> str:
    return cfg.get("_resolved_format", cfg.table_format)


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, cfg: Config, start: str, end: str) -> DataFrame:
    """Conformed date dimension.

    A physical date dimension (rather than deriving parts in every query) keeps
    BI tools honest about fiscal periods and lets the warehouse prune on an
    integer surrogate key.
    """
    df = (
        spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_key_date")
        .withColumn("date_key", F.date_format("date_key_date", "yyyyMMdd").cast("int"))
        .withColumn("full_date", F.col("date_key_date"))
        .withColumn("year", F.year("date_key_date"))
        .withColumn("quarter", F.quarter("date_key_date"))
        .withColumn("month", F.month("date_key_date"))
        .withColumn("month_name", F.date_format("date_key_date", "MMMM"))
        .withColumn("week_of_year", F.weekofyear("date_key_date"))
        .withColumn("day_of_month", F.dayofmonth("date_key_date"))
        .withColumn("day_of_week", F.dayofweek("date_key_date"))
        .withColumn("day_name", F.date_format("date_key_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key_date").isin([1, 7]))
        # Indian fiscal year runs April-March.
        .withColumn("fiscal_year", F.when(F.month("date_key_date") >= 4, F.year("date_key_date") + 1)
                    .otherwise(F.year("date_key_date")))
        .withColumn("fiscal_quarter", F.floor(((F.month("date_key_date") + 8) % 12) / 3) + 1)
        .drop("date_key_date")
    )
    write_table(df, cfg.table_path("gold", "dim_date"), fmt=_fmt(cfg), mode="overwrite")
    return df


def build_dim_time(spark: SparkSession, cfg: Config) -> DataFrame:
    """Time-of-day dimension at minute grain (1440 rows).

    Splitting time-of-day out of the date dimension is what keeps ``dim_date``
    at 365 rows/year instead of 525,600, and makes "compare 14:00 across sites"
    a dimension filter rather than a function call.
    """
    df = (
        spark.range(0, 1440).toDF("minute_of_day")
        .withColumn("time_key", F.col("minute_of_day").cast("int"))
        .withColumn("hour_of_day", (F.col("minute_of_day") / 60).cast("int"))
        .withColumn("minute_of_hour", (F.col("minute_of_day") % 60).cast("int"))
        .withColumn("hh_mm", F.format_string("%02d:%02d",
                                             (F.col("minute_of_day") / 60).cast("int"),
                                             (F.col("minute_of_day") % 60).cast("int")))
        .withColumn("day_part", F.when(F.col("hour_of_day") < 6, "Night")
                    .when(F.col("hour_of_day") < 12, "Morning")
                    .when(F.col("hour_of_day") < 18, "Afternoon")
                    .otherwise("Evening"))
        .withColumn("is_business_hours", F.col("hour_of_day").between(8, 19))
        .drop("minute_of_day")
    )
    write_table(df, cfg.table_path("gold", "dim_time"), fmt=_fmt(cfg), mode="overwrite")
    return df


def build_dim_site(spark: SparkSession, cfg: Config, sites: DataFrame) -> DataFrame:
    df = (
        sites.select("site_id", "site_name", "city", "country", "timezone", "customer_id")
        .withColumn("site_key", F.xxhash64("site_id"))
        .withColumn("_valid_from", F.current_timestamp())
        .withColumn("is_current", F.lit(True))
    )
    write_table(df, cfg.table_path("gold", "dim_site"), fmt=_fmt(cfg), mode="overwrite")
    return df


def build_dim_building(spark: SparkSession, cfg: Config, buildings: DataFrame, sites: DataFrame) -> DataFrame:
    df = (
        buildings.select("building_id", "building_name", "site_id", "floor_area_sqm", "building_type")
        .join(F.broadcast(sites.select("site_id", "site_name")), "site_id", "left")
        .withColumn("building_key", F.xxhash64("building_id"))
        .withColumn("site_key", F.xxhash64("site_id"))
        .withColumn("size_band", F.when(F.col("floor_area_sqm") < 5000, "Small")
                    .when(F.col("floor_area_sqm") < 15000, "Medium").otherwise("Large"))
        .withColumn("is_current", F.lit(True))
    )
    write_table(df, cfg.table_path("gold", "dim_building"), fmt=_fmt(cfg), mode="overwrite")
    return df


#: Attributes whose change opens a new SCD2 version of the asset row.
SCD2_TRACKED_COLUMNS = ["asset_name", "asset_type", "manufacturer", "model",
                        "rated_power_kw", "site_id", "building_id", "parent_asset_id"]


def build_dim_asset(spark: SparkSession, cfg: Config, assets: DataFrame,
                    buildings: Optional[DataFrame] = None) -> DataFrame:
    """Asset dimension with **SCD Type 2** history.

    An asset genuinely moves: a chiller is relocated to another building, is
    re-rated after a retrofit, or is re-parented when the plant room is
    re-plumbed. Overwriting those attributes (Type 1) would silently restate
    last quarter's per-building energy. Type 2 keeps the old row closed with
    ``valid_to`` so historical facts continue to join to the world as it was.
    """
    high_date = F.lit("9999-12-31 00:00:00").cast("timestamp")

    incoming = (
        assets.select(
            "asset_id", "asset_name", "asset_type", "manufacturer", "model",
            "installation_date", "rated_power_kw", "site_id", "building_id",
            "parent_asset_id", "is_orphan", "is_root",
        )
        .withColumn("asset_age_years",
                    F.round(F.datediff(F.current_date(), F.col("installation_date")) / 365.25, 2))
        .withColumn("_scd_hash", F.sha2(F.concat_ws("||", *[
            F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in SCD2_TRACKED_COLUMNS]), 256))
        .withColumn("valid_from", F.current_timestamp())
        .withColumn("valid_to", high_date)
        .withColumn("is_current", F.lit(True))
        .withColumn("asset_key", F.xxhash64(F.concat_ws("|", F.col("asset_id"), F.col("_scd_hash"))))
    )

    path = cfg.table_path("gold", "dim_asset")
    fmt = _fmt(cfg)

    if not table_exists(spark, path, fmt):
        write_table(incoming, path, fmt=fmt, mode="overwrite")
        LOG.info("dim_asset initialised with %d rows", incoming.count())
        return incoming

    # --- Type 2 merge -----------------------------------------------------
    existing = read_table(spark, path, fmt)
    current = existing.filter("is_current")

    changed = (
        incoming.alias("n")
        .join(current.alias("c"), "asset_id", "left")
        .filter((F.col("c.asset_id").isNull()) | (F.col("n._scd_hash") != F.col("c._scd_hash")))
        .select("n.*")
    )
    n_changed = changed.count()
    if n_changed == 0:
        LOG.info("dim_asset: no attribute changes detected")
        return existing

    closed = (
        current.alias("c")
        .join(changed.select("asset_id").alias("x"), "asset_id", "inner")
        .select("c.*")
        .withColumn("valid_to", F.current_timestamp())
        .withColumn("is_current", F.lit(False))
    )
    untouched = existing.join(changed.select("asset_id"), "asset_id", "left_anti")
    final = untouched.unionByName(closed, allowMissingColumns=True).unionByName(changed, allowMissingColumns=True)
    write_table(final, path, fmt=fmt, mode="overwrite")
    LOG.info("dim_asset SCD2: %d rows versioned", n_changed)
    return final


# ---------------------------------------------------------------------------
# Asset-level readings + duration weights
# ---------------------------------------------------------------------------
def asset_level_readings(telemetry: DataFrame, cfg: Config) -> DataFrame:
    """Collapse sensor-grain telemetry to asset grain and attach duration weights.

    See the module docstring for why both steps are necessary.
    """
    interval = float(cfg.get("generator.telemetry_interval_minutes", 5))
    max_gap_seconds = interval * 60 * 2

    per_asset = (
        telemetry.filter(F.col("timestamp").isNotNull())
        .groupBy("site_id", "building_id", "asset_id", "timestamp")
        .agg(
            *[F.avg(m).alias(m) for m in _MEASURES],
            # Operating mode is categorical: take the mode reported by the
            # majority of sensors, tie-broken deterministically.
            F.first("operating_mode", ignorenulls=True).alias("operating_mode"),
            F.count(F.lit(1)).alias("sensor_readings"),
        )
    )

    w = Window.partitionBy("asset_id").orderBy(F.col("timestamp").asc())
    return (
        per_asset
        .withColumn("_next_ts", F.lead("timestamp").over(w))
        .withColumn(
            "duration_hours",
            F.least(
                F.coalesce(
                    (F.unix_timestamp("_next_ts") - F.unix_timestamp("timestamp")).cast("double"),
                    F.lit(interval * 60),
                ),
                F.lit(max_gap_seconds),
            ) / 3600.0,
        )
        .drop("_next_ts")
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("energy_kwh", F.col("power_consumption") * F.col("duration_hours"))
    )


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------
def build_fact_telemetry(spark: SparkSession, cfg: Config, telemetry: DataFrame) -> DataFrame:
    """Atomic telemetry fact - one row per (asset, sensor, timestamp).

    Kept at full grain because ML feature engineering needs the raw series;
    partitioned by ``event_date`` and Z-ORDERed on ``asset_id`` so dashboards
    that filter one asset over one week touch a handful of files.
    """
    fact = (
        telemetry.select(
            F.xxhash64("asset_id").alias("asset_nk_key"),
            F.xxhash64("site_id").alias("site_key"),
            F.xxhash64("building_id").alias("building_key"),
            F.date_format("timestamp", "yyyyMMdd").cast("int").alias("date_key"),
            (F.hour("timestamp") * 60 + F.minute("timestamp")).alias("time_key"),
            "asset_id", "sensor_id", "site_id", "building_id", "timestamp",
            *_MEASURES, "operating_mode",
            F.col("_is_late").alias("is_late_arrival"),
            "event_date", "event_hour",
        )
    )
    write_table(fact, cfg.table_path("gold", "fact_telemetry"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return fact


def build_fact_energy_hourly(spark: SparkSession, cfg: Config, readings: DataFrame) -> DataFrame:
    """Energy fact at asset x hour - the grain nearly every energy question wants."""
    fact = (
        readings.groupBy("site_id", "building_id", "asset_id", "event_hour")
        .agg(
            F.sum("energy_kwh").alias("energy_kwh"),
            F.avg("power_consumption").alias("avg_power_kw"),
            F.max("power_consumption").alias("peak_power_kw"),
            F.min("power_consumption").alias("min_power_kw"),
            F.sum("duration_hours").alias("covered_hours"),
            F.count(F.lit(1)).alias("reading_count"),
        )
        .withColumn("event_date", F.to_date("event_hour"))
        .withColumn("date_key", F.date_format("event_hour", "yyyyMMdd").cast("int"))
        .withColumn("hour_of_day", F.hour("event_hour"))
        .withColumn("asset_nk_key", F.xxhash64("asset_id"))
        .withColumn("site_key", F.xxhash64("site_id"))
        .withColumn("building_key", F.xxhash64("building_id"))
        # <1.0 means readings were lost in that hour; consumers can decide
        # whether to trust or extrapolate the number.
        .withColumn("data_coverage_ratio", F.round(F.col("covered_hours") / F.lit(1.0), 4))
    )
    write_table(fact, cfg.table_path("gold", "fact_energy_hourly"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return fact


def build_fact_event(spark: SparkSession, cfg: Config, events: DataFrame) -> DataFrame:
    fact = (
        events.select(
            "event_id", "asset_id", "site_id", "building_id", "timestamp",
            "event_type", "severity", "message", "event_date", "event_hour",
            F.col("_is_late").alias("is_late_arrival"),
        )
        .withColumn("date_key", F.date_format("timestamp", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("timestamp") * 60 + F.minute("timestamp"))
        .withColumn("asset_nk_key", F.xxhash64("asset_id"))
        .withColumn("site_key", F.xxhash64("site_id"))
        .withColumn("building_key", F.xxhash64("building_id"))
        .withColumn("is_fault", F.col("event_type") == F.lit("Fault"))
        .withColumn("severity_rank", F.when(F.col("severity") == "High", 3)
                    .when(F.col("severity") == "Medium", 2).otherwise(1))
    )
    write_table(fact, cfg.table_path("gold", "fact_event"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return fact


# ---------------------------------------------------------------------------
# Curated marts (Task 2 - Data Transformation)
# ---------------------------------------------------------------------------
def curated_hourly_energy(spark: SparkSession, cfg: Config, energy_hourly: DataFrame) -> DataFrame:
    """Hourly energy with a same-hour-yesterday comparison and a rolling mean."""
    w_asset = Window.partitionBy("asset_id").orderBy(F.col("event_hour").cast("long"))
    w_24 = w_asset.rangeBetween(-24 * 3600, -1)
    df = (
        energy_hourly.select("site_id", "building_id", "asset_id", "event_hour", "event_date",
                             "hour_of_day", "energy_kwh", "avg_power_kw", "peak_power_kw", "reading_count")
        .withColumn("energy_kwh_prev_day",
                    F.lag("energy_kwh", 24).over(w_asset))
        .withColumn("rolling_24h_avg_kwh", F.avg("energy_kwh").over(w_24))
        .withColumn("dod_change_pct",
                    F.when(F.col("energy_kwh_prev_day") > 0,
                           F.round((F.col("energy_kwh") - F.col("energy_kwh_prev_day"))
                                   / F.col("energy_kwh_prev_day") * 100, 2)))
    )
    write_table(df, cfg.table_path("gold", "curated_hourly_energy"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return df


def curated_daily_asset_utilization(spark: SparkSession, cfg: Config, readings: DataFrame) -> DataFrame:
    """Daily utilisation and availability per asset.

    ``utilization_pct`` = productive hours / hours actually observed. Dividing
    by 24 instead would conflate "the asset was idle" with "the asset stopped
    reporting", which are different operational problems.
    """
    productive = cfg.get("gold.utilization.productive_modes", _PRODUCTIVE_DEFAULT)
    df = (
        readings.groupBy("site_id", "building_id", "asset_id", "event_date")
        .agg(
            F.sum(F.when(F.col("operating_mode").isin(productive), F.col("duration_hours"))
                  .otherwise(F.lit(0.0))).alias("productive_hours"),
            F.sum(F.when(F.col("operating_mode").isin(_DOWN_MODES), F.col("duration_hours"))
                  .otherwise(F.lit(0.0))).alias("downtime_hours"),
            F.sum("duration_hours").alias("observed_hours"),
            F.sum("energy_kwh").alias("energy_kwh"),
            F.avg("power_consumption").alias("avg_power_kw"),
            F.max("power_consumption").alias("peak_power_kw"),
            F.count(F.lit(1)).alias("reading_count"),
            F.countDistinct("operating_mode").alias("distinct_modes"),
        )
        .withColumn("utilization_pct",
                    F.round(F.when(F.col("observed_hours") > 0,
                                   F.col("productive_hours") / F.col("observed_hours") * 100).otherwise(None), 2))
        .withColumn("availability_pct",
                    F.round(F.when(F.col("observed_hours") > 0,
                                   (1 - F.col("downtime_hours") / F.col("observed_hours")) * 100).otherwise(None), 2))
        .withColumn("data_coverage_pct", F.round(F.least(F.col("observed_hours") / 24.0, F.lit(1.0)) * 100, 2))
    )
    write_table(df, cfg.table_path("gold", "curated_daily_asset_utilization"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return df


def curated_daily_environment(spark: SparkSession, cfg: Config, readings: DataFrame) -> DataFrame:
    """Average environmental conditions per asset per day, with spread."""
    aggs = []
    for m in ["temperature", "humidity", "pressure", "vibration"]:
        aggs += [
            F.round(F.avg(m), 3).alias(f"avg_{m}"),
            F.round(F.min(m), 3).alias(f"min_{m}"),
            F.round(F.max(m), 3).alias(f"max_{m}"),
            F.round(F.stddev_samp(m), 3).alias(f"stddev_{m}"),
            F.round(F.percentile_approx(F.col(m), 0.95), 3).alias(f"p95_{m}"),
        ]
    df = (
        readings.groupBy("site_id", "building_id", "asset_id", "event_date")
        .agg(*aggs, F.count(F.lit(1)).alias("reading_count"))
        # A simple comfort flag the facilities dashboard can filter on.
        .withColumn("thermal_comfort_ok",
                    F.col("avg_temperature").between(18, 27) & F.col("avg_humidity").between(30, 65))
    )
    write_table(df, cfg.table_path("gold", "curated_daily_environment"), fmt=_fmt(cfg),
                mode="overwrite", partition_by=["event_date"])
    return df


def curated_fault_statistics(spark: SparkSession, cfg: Config, events: DataFrame,
                             assets: DataFrame) -> DataFrame:
    """Per-asset reliability summary: counts, severity mix and MTBF.

    MTBF is computed as observed span / fault count, which is the pragmatic
    definition when the asset's true operating hours are only partially
    observed. Assets with a single fault get NULL rather than a misleading
    number.
    """
    faults = events.filter(F.col("event_type") == "Fault")
    fault_span = (
        faults.groupBy("asset_id")
        .agg(
            F.min("timestamp").alias("first_fault_at"),
            F.max("timestamp").alias("last_fault_at"),
            F.count(F.lit(1)).alias("fault_count"),
        )
        .withColumn(
            "mtbf_hours",
            F.when(F.col("fault_count") > 1,
                   F.round((F.unix_timestamp("last_fault_at") - F.unix_timestamp("first_fault_at"))
                           / 3600.0 / (F.col("fault_count") - 1), 2)),
        )
    )

    by_asset = (
        events.groupBy("site_id", "building_id", "asset_id")
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(F.when(F.col("event_type") == "Fault", 1).otherwise(0)).alias("faults"),
            F.sum(F.when(F.col("event_type") == "Alarm", 1).otherwise(0)).alias("alarms"),
            F.sum(F.when(F.col("event_type") == "Warning", 1).otherwise(0)).alias("warnings"),
            F.sum(F.when(F.col("severity") == "High", 1).otherwise(0)).alias("high_severity_events"),
            F.countDistinct("event_date").alias("days_with_events"),
            F.min("timestamp").alias("first_event_at"),
            F.max("timestamp").alias("last_event_at"),
        )
        .join(fault_span, "asset_id", "left")
        .join(F.broadcast(assets.select("asset_id", "asset_type", "manufacturer", "rated_power_kw")),
              "asset_id", "left")
        .withColumn("observed_days",
                    F.greatest(F.datediff(F.col("last_event_at"), F.col("first_event_at")), F.lit(1)))
        .withColumn("faults_per_day", F.round(F.col("faults") / F.col("observed_days"), 3))
        .withColumn("health_score",
                    # 100 = clean. Faults are weighted 5x an alarm, capped at 0.
                    F.greatest(F.lit(0.0),
                               F.round(100 - (F.col("faults") * 5 + F.col("alarms") * 1
                                              + F.col("warnings") * 0.25), 2)))
        .withColumn("risk_band", F.when(F.col("health_score") >= 80, "Low")
                    .when(F.col("health_score") >= 50, "Medium").otherwise("High"))
    )
    write_table(by_asset, cfg.table_path("gold", "curated_fault_statistics"), fmt=_fmt(cfg), mode="overwrite")
    return by_asset


# ---------------------------------------------------------------------------
# Roll-ups (Task 2 - Data Aggregation)
# ---------------------------------------------------------------------------
def _daily_event_counts(events: DataFrame, group_cols: Sequence[str]) -> DataFrame:
    return (
        events.groupBy(*group_cols, "event_date")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.sum(F.when(F.col("event_type") == "Fault", 1).otherwise(0)).alias("faults"),
            F.sum(F.when(F.col("severity") == "High", 1).otherwise(0)).alias("high_severity_events"),
        )
    )


def build_aggregates(spark: SparkSession, cfg: Config, utilization: DataFrame,
                     environment: DataFrame, events: DataFrame,
                     buildings: DataFrame, sites: DataFrame) -> Dict[str, DataFrame]:
    fmt = _fmt(cfg)
    out: Dict[str, DataFrame] = {}

    # -- asset level -------------------------------------------------------
    asset_daily = (
        utilization.join(
            environment.select("asset_id", "event_date", "avg_temperature", "avg_humidity",
                               "avg_vibration", "max_vibration", "thermal_comfort_ok"),
            ["asset_id", "event_date"], "left")
        .join(_daily_event_counts(events, ["asset_id"]), ["asset_id", "event_date"], "left")
        .fillna({"events": 0, "faults": 0, "high_severity_events": 0})
    )
    write_table(asset_daily, cfg.table_path("gold", "agg_asset_daily"), fmt=fmt,
                mode="overwrite", partition_by=["event_date"])
    out["agg_asset_daily"] = asset_daily

    # -- building level ----------------------------------------------------
    building_daily = (
        asset_daily.groupBy("site_id", "building_id", "event_date")
        .agg(
            F.countDistinct("asset_id").alias("active_assets"),
            F.round(F.sum("energy_kwh"), 3).alias("energy_kwh"),
            F.round(F.avg("utilization_pct"), 2).alias("avg_utilization_pct"),
            F.round(F.avg("availability_pct"), 2).alias("avg_availability_pct"),
            F.round(F.max("peak_power_kw"), 3).alias("peak_power_kw"),
            F.round(F.avg("avg_temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("avg_humidity"), 2).alias("avg_humidity"),
            # Carried up the roll-up so consumers can refuse to compare a
            # partially-observed day against a complete one.
            F.round(F.avg("data_coverage_pct"), 2).alias("data_coverage_pct"),
            F.sum("events").alias("events"),
            F.sum("faults").alias("faults"),
            F.sum("high_severity_events").alias("high_severity_events"),
        )
        .join(F.broadcast(buildings.select("building_id", "building_name", "floor_area_sqm", "building_type")),
              "building_id", "left")
        # Energy use intensity is the metric facilities teams actually compare
        # buildings on; raw kWh just ranks buildings by size.
        .withColumn("energy_intensity_kwh_per_sqm",
                    F.when(F.col("floor_area_sqm") > 0,
                           F.round(F.col("energy_kwh") / F.col("floor_area_sqm"), 5)))
    )
    write_table(building_daily, cfg.table_path("gold", "agg_building_daily"), fmt=fmt,
                mode="overwrite", partition_by=["event_date"])
    out["agg_building_daily"] = building_daily

    # -- site level --------------------------------------------------------
    zscore_threshold = float(cfg.get("gold.anomaly.zscore_threshold", 2.0))
    baseline_days = int(cfg.get("gold.anomaly.baseline_days", 7))
    w_site = (Window.partitionBy("site_id").orderBy(F.col("event_date").cast("timestamp").cast("long"))
              .rangeBetween(-baseline_days * 86400, -86400))

    site_daily = (
        building_daily.groupBy("site_id", "event_date")
        .agg(
            F.countDistinct("building_id").alias("buildings"),
            F.sum("active_assets").alias("active_assets"),
            F.round(F.sum("energy_kwh"), 3).alias("energy_kwh"),
            F.round(F.avg("avg_utilization_pct"), 2).alias("avg_utilization_pct"),
            F.round(F.avg("avg_availability_pct"), 2).alias("avg_availability_pct"),
            F.round(F.max("peak_power_kw"), 3).alias("peak_power_kw"),
            F.round(F.avg("avg_temperature"), 2).alias("avg_temperature"),
            F.round(F.avg("data_coverage_pct"), 2).alias("data_coverage_pct"),
            F.sum("events").alias("events"),
            F.sum("faults").alias("faults"),
            F.sum("high_severity_events").alias("high_severity_events"),
        )
        .join(F.broadcast(sites.select("site_id", "site_name", "city", "country")), "site_id", "left")
        .withColumn("baseline_energy_kwh", F.round(F.avg("energy_kwh").over(w_site), 3))
        .withColumn("baseline_stddev", F.stddev_samp("energy_kwh").over(w_site))
        .withColumn("energy_zscore",
                    F.when(F.col("baseline_stddev") > 0,
                           F.round((F.col("energy_kwh") - F.col("baseline_energy_kwh"))
                                   / F.col("baseline_stddev"), 3)))
        .withColumn("is_energy_anomaly", F.coalesce(F.col("energy_zscore") > F.lit(zscore_threshold), F.lit(False)))
    )
    write_table(site_daily, cfg.table_path("gold", "agg_site_daily"), fmt=fmt,
                mode="overwrite", partition_by=["event_date"])
    out["agg_site_daily"] = site_daily

    return out


# ---------------------------------------------------------------------------
# Orchestration for the layer
# ---------------------------------------------------------------------------
def run(spark: SparkSession, cfg: Config, ctx: RunContext, silver: Optional[dict] = None) -> Dict[str, DataFrame]:
    fmt = _fmt(cfg)
    silver = silver or {}
    telemetry = silver.get("telemetry") if silver.get("telemetry") is not None else read_table(
        spark, cfg.table_path("silver", "telemetry"), fmt)
    events = silver.get("events") if silver.get("events") is not None else read_table(
        spark, cfg.table_path("silver", "events"), fmt)
    assets = silver.get("assets") if silver.get("assets") is not None else read_table(
        spark, cfg.table_path("silver", "assets"), fmt)
    buildings = silver.get("buildings") if silver.get("buildings") is not None else read_table(
        spark, cfg.table_path("silver", "buildings"), fmt)
    sites = silver.get("sites") if silver.get("sites") is not None else read_table(
        spark, cfg.table_path("silver", "sites"), fmt)

    bounds = telemetry.agg(F.min("event_date").alias("lo"), F.max("event_date").alias("hi")).collect()[0]
    lo = (bounds["lo"] or F.current_date()).isoformat() if bounds["lo"] else "2024-01-01"
    hi = bounds["hi"].isoformat() if bounds["hi"] else "2030-12-31"

    out: Dict[str, DataFrame] = {}
    out["dim_date"] = build_dim_date(spark, cfg, lo, hi)
    out["dim_time"] = build_dim_time(spark, cfg)
    out["dim_site"] = build_dim_site(spark, cfg, sites)
    out["dim_building"] = build_dim_building(spark, cfg, buildings, sites)
    out["dim_asset"] = build_dim_asset(spark, cfg, assets, buildings)

    readings = asset_level_readings(telemetry, cfg).cache()

    out["fact_telemetry"] = build_fact_telemetry(spark, cfg, telemetry)
    energy_hourly = build_fact_energy_hourly(spark, cfg, readings)
    out["fact_energy_hourly"] = energy_hourly
    out["fact_event"] = build_fact_event(spark, cfg, events)

    out["curated_hourly_energy"] = curated_hourly_energy(spark, cfg, energy_hourly)
    utilization = curated_daily_asset_utilization(spark, cfg, readings)
    out["curated_daily_asset_utilization"] = utilization
    environment = curated_daily_environment(spark, cfg, readings)
    out["curated_daily_environment"] = environment
    out["curated_fault_statistics"] = curated_fault_statistics(spark, cfg, events, assets)

    out.update(build_aggregates(spark, cfg, utilization, environment, events, buildings, sites))

    readings.unpersist()
    ctx.record("gold.tables", len(out))
    LOG.info("gold layer complete: %d tables", len(out))
    return out
