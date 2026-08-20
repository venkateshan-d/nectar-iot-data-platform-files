"""Silver layer - typed, validated, deduplicated, conformed.

This is where raw strings become a trustworthy dataset:

1. **cast** every column to its contract type, keeping the original string in
   ``_raw_timestamp`` so a cast failure is distinguishable from a missing value;
2. **enrich** with the asset register so referential integrity can be checked
   and so events (which only carry ``asset_id``) gain their site/building;
3. **evaluate** the Task 5 rule set - one pass, row-level flags;
4. **split** into clean vs quarantine on BLOCKING severity;
5. **MERGE** the clean rows into the silver table on the business key, which
   makes the whole step idempotent under orchestrator retries and correctly absorbs
   late-arriving records into the day they belong to.

Dedupe strategy
---------------
The gateway delivers at-least-once, so the same reading can land twice (or in
two different files after a replay). ``row_number()`` over
``(asset_id, sensor_id, timestamp)`` ordered by ingestion time keeps the first
copy. The duplicate is not discarded silently - it is quarantined with reason
``tel.uniqueness.business_key`` so the volume of gateway retries is measurable.
"""

from __future__ import annotations

import logging
from typing import Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config
from ..io_layer import append_table, read_table, upsert_table, write_table
from ..logging_utils import RunContext
from ..quality.engine import QualityEngine, QualityOutcome
from ..quality.rules import duplicate_rank_column, get_rules
from ..schemas import EVENT_BUSINESS_KEY, TELEMETRY_BUSINESS_KEY

LOG = logging.getLogger("nectar.silver")

#: Devices report ISO-8601 UTC. A second pattern is tried because one gateway
#: firmware revision emits a space separator instead of 'T'.
TIMESTAMP_FORMATS = ["yyyy-MM-dd'T'HH:mm:ss['Z']", "yyyy-MM-dd HH:mm:ss"]

_MEASURES = ["temperature", "humidity", "pressure", "vibration", "power_consumption"]


# ---------------------------------------------------------------------------
# casting helpers
# ---------------------------------------------------------------------------
def _parse_timestamp(col: str = "timestamp"):
    """Try each accepted format; ``try_to_timestamp`` yields null instead of
    throwing, which is what lets a bad row be quarantined rather than killing
    the whole batch."""
    expr = F.try_to_timestamp(F.col(col), F.lit(TIMESTAMP_FORMATS[0]))
    for fmt in TIMESTAMP_FORMATS[1:]:
        expr = F.coalesce(expr, F.try_to_timestamp(F.col(col), F.lit(fmt)))
    return F.coalesce(expr, F.try_to_timestamp(F.col(col)))


def _clean_string(col: str):
    """Trim and fold the empty string to NULL - '' and NULL mean the same thing
    coming off a device and should not be two different states downstream."""
    return F.when(F.trim(F.col(col)) == "", None).otherwise(F.trim(F.col(col)))


def cast_telemetry(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("_raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp", _parse_timestamp("timestamp"))
    )
    for col in ["site_id", "building_id", "asset_id", "sensor_id"]:
        out = out.withColumn(col, F.upper(_clean_string(col)))
    out = out.withColumn("operating_mode", F.upper(_clean_string("operating_mode")))
    for col in _MEASURES:
        # try_cast keeps a junk numeric string as NULL instead of failing the job
        out = out.withColumn(col, F.expr(f"try_cast({col} AS DOUBLE)"))
    return out


def cast_events(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("_raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp", _parse_timestamp("timestamp"))
        .withColumn("event_id", F.upper(_clean_string("event_id")))
        .withColumn("asset_id", F.upper(_clean_string("asset_id")))
        .withColumn("event_type", F.initcap(_clean_string("event_type")))
        .withColumn("severity", F.initcap(_clean_string("severity")))
        .withColumn("message", _clean_string("message"))
    )
    return out


# ---------------------------------------------------------------------------
# enrichment + helper columns the rules depend on
# ---------------------------------------------------------------------------
def _asset_register(spark: SparkSession, cfg: Config) -> DataFrame:
    fmt = cfg.get("_resolved_format", cfg.table_format)
    return (
        read_table(spark, cfg.table_path("bronze", "assets"), fmt)
        .select(
            F.upper(F.trim("asset_id")).alias("_reg_asset_id"),
            F.upper(F.trim("site_id")).alias("_register_site_id"),
            F.upper(F.trim("building_id")).alias("_register_building_id"),
            F.col("asset_type").alias("_register_asset_type"),
        )
        .dropDuplicates(["_reg_asset_id"])
    )


def prepare_telemetry(spark: SparkSession, cfg: Config, bronze: DataFrame) -> DataFrame:
    """Cast, enrich and add every helper column the telemetry rules require."""
    df = cast_telemetry(bronze)

    register = F.broadcast(_asset_register(spark, cfg))
    df = (
        df.join(register, df.asset_id == F.col("_reg_asset_id"), "left")
        .withColumn("_asset_known", F.col("_reg_asset_id").isNotNull())
        .drop("_reg_asset_id")
    )

    # Landing lag. The raw zone records the *day* a record landed, so lateness
    # is measured in whole days against the event date - enough to decide which
    # downstream aggregate windows have to be recomputed.
    df = df.withColumn(
        "_lateness_seconds",
        (F.datediff(F.col("ingest_date"), F.to_date("timestamp")).cast("long") * F.lit(86400)),
    ).withColumn(
        "_is_late",
        # >= : a record whose landing day is later than its event day arrived
        # after that day's aggregate window had already closed.
        F.coalesce(F.col("_lateness_seconds") >= F.lit(
            float(cfg.get("quality.late_arrival_watermark_hours", 24)) * 3600), F.lit(False)),
    )

    df = duplicate_rank_column(df, TELEMETRY_BUSINESS_KEY, order_by="_ingested_at")
    return df


def prepare_events(spark: SparkSession, cfg: Config, bronze: DataFrame) -> DataFrame:
    df = cast_events(bronze)

    register = F.broadcast(_asset_register(spark, cfg))
    df = (
        df.join(register, df.asset_id == F.col("_reg_asset_id"), "left")
        .withColumn("_asset_known", F.col("_reg_asset_id").isNotNull())
        # Events arrive with only asset_id; site/building come from the register
        # so that event and telemetry facts share the same grain keys.
        .withColumn("site_id", F.col("_register_site_id"))
        .withColumn("building_id", F.col("_register_building_id"))
        .drop("_reg_asset_id")
    )
    df = df.withColumn(
        "_lateness_seconds",
        (F.datediff(F.col("ingest_date"), F.to_date("timestamp")).cast("long") * F.lit(86400)),
    ).withColumn(
        "_is_late",
        # >= : a record whose landing day is later than its event day arrived
        # after that day's aggregate window had already closed.
        F.coalesce(F.col("_lateness_seconds") >= F.lit(
            float(cfg.get("quality.late_arrival_watermark_hours", 24)) * 3600), F.lit(False)),
    )
    df = duplicate_rank_column(df, EVENT_BUSINESS_KEY, order_by="_ingested_at")
    return df


# ---------------------------------------------------------------------------
# finalisation
# ---------------------------------------------------------------------------
_HELPER_COLUMNS = ["_dup_rank", "_asset_known", "_register_site_id", "_register_building_id",
                   "_register_asset_type", "_raw_timestamp", "_corrupt_record"]


def _finalise(df: DataFrame, grain_columns: Sequence[str]) -> DataFrame:
    """Add silver audit columns and drop the scaffolding.

    ``_record_hash`` fingerprints the record's *grain*, giving downstream jobs a
    single column to join or deduplicate on instead of a composite key.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in grain_columns if c in df.columns]
    out = (
        df.withColumn("_processed_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .withColumn("_record_hash", F.sha2(F.concat_ws("||", *parts), 256))
    )
    return out.drop(*[c for c in _HELPER_COLUMNS if c in out.columns])


def _write_quality(spark: SparkSession, cfg: Config, outcome: QualityOutcome, table: str) -> None:
    fmt = cfg.get("_resolved_format", cfg.table_format)
    append_table(outcome.results, cfg.table_path("quality", "dq_results"), fmt=fmt,
                 partition_by=["layer", "table_name"])
    quarantined = outcome.quarantine
    if quarantined.take(1):
        # Quarantine rows keep every original column plus the reasons array, so
        # a replay is a straight re-read of this table after the fix.
        keep = [c for c in quarantined.columns if not c.startswith("_dqf_")]
        append_table(quarantined.select(*keep).withColumn("_q_ts", F.col("_quarantined_at").cast("string")),
                     cfg.table_path("quarantine", table), fmt=fmt, partition_by=["_batch_id"])


# ---------------------------------------------------------------------------
# entry points
# ---------------------------------------------------------------------------
def build_telemetry(spark: SparkSession, cfg: Config, ctx: RunContext,
                    bronze: Optional[DataFrame] = None) -> Tuple[DataFrame, QualityOutcome]:
    fmt = cfg.get("_resolved_format", cfg.table_format)
    bronze = bronze if bronze is not None else read_table(spark, cfg.table_path("bronze", "telemetry"), fmt)

    prepared = prepare_telemetry(spark, cfg, bronze)
    engine = QualityEngine(spark, cfg, get_rules("telemetry", cfg), "silver", "telemetry", ctx.batch_id)
    outcome = engine.evaluate(prepared)
    _write_quality(spark, cfg, outcome, "telemetry")
    engine.enforce(outcome)

    silver = _finalise(outcome.clean, TELEMETRY_BUSINESS_KEY)
    target = cfg.table_path("silver", "telemetry")
    upsert_table(spark, silver, target, keys=TELEMETRY_BUSINESS_KEY, fmt=fmt, partition_by=["event_date"])
    ctx.record("silver.telemetry.rows", silver.count())
    LOG.info("silver.telemetry written to %s", target)
    return silver, outcome


def build_events(spark: SparkSession, cfg: Config, ctx: RunContext,
                 bronze: Optional[DataFrame] = None) -> Tuple[DataFrame, QualityOutcome]:
    fmt = cfg.get("_resolved_format", cfg.table_format)
    bronze = bronze if bronze is not None else read_table(spark, cfg.table_path("bronze", "events"), fmt)

    prepared = prepare_events(spark, cfg, bronze)
    engine = QualityEngine(spark, cfg, get_rules("events", cfg), "silver", "events", ctx.batch_id)
    outcome = engine.evaluate(prepared)
    _write_quality(spark, cfg, outcome, "events")
    engine.enforce(outcome)

    silver = _finalise(outcome.clean, EVENT_BUSINESS_KEY)
    target = cfg.table_path("silver", "events")
    upsert_table(spark, silver, target, keys=EVENT_BUSINESS_KEY, fmt=fmt, partition_by=["event_date"])
    ctx.record("silver.events.rows", silver.count())
    LOG.info("silver.events written to %s", target)
    return silver, outcome


def build_reference(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict:
    """Conform the reference tables.

    The asset register is deduplicated on ``asset_id`` and its hierarchy pointer
    is cleaned: a ``parent_asset_id`` that does not resolve is set to NULL and
    the asset is marked as an orphan rather than silently disappearing from
    hierarchy queries.
    """
    fmt = cfg.get("_resolved_format", cfg.table_format)
    out = {}

    sites = read_table(spark, cfg.table_path("bronze", "sites"), fmt).dropDuplicates(["site_id"])
    sites = sites.withColumn("site_id", F.upper(F.trim("site_id")))
    write_table(sites, cfg.table_path("silver", "sites"), fmt=fmt, mode="overwrite")
    out["sites"] = sites

    buildings = read_table(spark, cfg.table_path("bronze", "buildings"), fmt).dropDuplicates(["building_id"])
    buildings = (buildings.withColumn("building_id", F.upper(F.trim("building_id")))
                          .withColumn("site_id", F.upper(F.trim("site_id"))))
    write_table(buildings, cfg.table_path("silver", "buildings"), fmt=fmt, mode="overwrite")
    out["buildings"] = buildings

    assets = read_table(spark, cfg.table_path("bronze", "assets"), fmt).dropDuplicates(["asset_id"])
    assets = (
        assets.withColumn("asset_id", F.upper(F.trim("asset_id")))
        .withColumn("site_id", F.upper(F.trim("site_id")))
        .withColumn("building_id", F.upper(F.trim("building_id")))
        .withColumn("parent_asset_id", F.upper(_clean_string("parent_asset_id")))
    )
    valid_parents = assets.select(F.col("asset_id").alias("_p_id"))
    assets = (
        assets.join(F.broadcast(valid_parents), assets.parent_asset_id == F.col("_p_id"), "left")
        .withColumn("is_orphan", F.col("parent_asset_id").isNotNull() & F.col("_p_id").isNull())
        .withColumn("is_root", F.col("parent_asset_id").isNull())
        .withColumn("parent_asset_id",
                    F.when(F.col("_p_id").isNull(), None).otherwise(F.col("parent_asset_id")))
        .drop("_p_id")
    )
    write_table(assets, cfg.table_path("silver", "assets"), fmt=fmt, mode="overwrite")
    out["assets"] = assets

    orphans = assets.filter("is_orphan").count()
    LOG.info("silver reference: %d sites, %d buildings, %d assets (%d orphaned)",
             sites.count(), buildings.count(), assets.count(), orphans)
    ctx.record("silver.assets.orphans", orphans)
    return out


def run(spark: SparkSession, cfg: Config, ctx: RunContext, bronze: Optional[dict] = None) -> dict:
    bronze = bronze or {}
    reference = build_reference(spark, cfg, ctx)
    telemetry, tel_dq = build_telemetry(spark, cfg, ctx, bronze.get("telemetry"))
    events, evt_dq = build_events(spark, cfg, ctx, bronze.get("events"))
    return {
        "telemetry": telemetry,
        "events": events,
        "quality": {"telemetry": tel_dq, "events": evt_dq},
        **reference,
    }
