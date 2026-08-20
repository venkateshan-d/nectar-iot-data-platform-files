"""Bronze layer - land raw data exactly as received, plus lineage.

Principles
----------
* **No transformation.** Every source column stays a string. If the gateway
  sends ``"temperature": "not-a-number"`` we want that in bronze verbatim, so
  the failure is reproducible and the row is replayable after a fix. Casting is
  a silver concern.
* **Explicit schema.** Inference is banned (see ``schemas.py``); a column that
  disappears upstream must fail loudly, not silently become null.
* **Lineage on every row.** ``_ingest_id``/``_source_file``/``_ingested_at``
  answer "which run produced this?" without a separate catalog.
* **Idempotent.** Re-running the same ingest_date replaces exactly that
  partition, so an orchestrator retry cannot duplicate data.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config
from ..io_layer import write_table
from ..logging_utils import RunContext
from ..schemas import (
    ASSET_METADATA_SCHEMA,
    BUILDING_SCHEMA,
    EVENT_RAW_SCHEMA,
    SITE_SCHEMA,
    TELEMETRY_RAW_SCHEMA,
)

LOG = logging.getLogger("nectar.bronze")


def _with_audit(df: DataFrame, ctx: RunContext, source_system: str, hash_cols: Sequence[str]) -> DataFrame:
    """Stamp lineage columns onto a raw DataFrame.

    ``_payload_hash`` is a content fingerprint of the business columns. It makes
    "is this an exact replay or a genuine correction?" answerable: two rows with
    the same business key but different hashes mean the device restated a value.
    """
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_id", F.lit(ctx.batch_id))
        .withColumn("_source_file", F.col("_metadata.file_path") if "_metadata" in df.columns else F.input_file_name())
        .withColumn("_source_system", F.lit(source_system))
        # NULL is folded to a sentinel so that ("a", NULL) and ("a", "") hash
        # differently - otherwise a dropped field would look like an empty one.
        .withColumn("_payload_hash", F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~NULL~")) for c in hash_cols]), 256))
    )


def _read_jsonl(spark: SparkSession, path: str, schema, ingest_dates: Optional[Sequence[str]] = None) -> DataFrame:
    """Read the partitioned JSONL landing zone.

    ``basePath`` keeps the ``ingest_date=`` directory as a real column even when
    only a subset of partitions is selected, which is what makes incremental
    (per-day) runs possible without rescanning history.
    """
    reader = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("basePath", path)
    )
    if ingest_dates:
        paths = [f"{path}/ingest_date={d}" for d in ingest_dates]
        df = reader.json(paths)
        # Selected paths lose the partition column; re-derive it from the file path.
        df = df.withColumn(
            "ingest_date",
            F.to_date(F.regexp_extract(F.input_file_name(), r"ingest_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)),
        )
    else:
        df = reader.json(path)
        if "ingest_date" in df.columns:
            df = df.withColumn("ingest_date", F.col("ingest_date").cast("date"))
    return df


# ---------------------------------------------------------------------------
# fact sources
# ---------------------------------------------------------------------------
def ingest_telemetry(spark: SparkSession, cfg: Config, ctx: RunContext,
                     ingest_dates: Optional[Sequence[str]] = None) -> DataFrame:
    src = str(cfg.layer_path("raw") / "telemetry")
    df = _read_jsonl(spark, src, TELEMETRY_RAW_SCHEMA, ingest_dates)
    business_cols = [f.name for f in TELEMETRY_RAW_SCHEMA.fields]
    out = _with_audit(df, ctx, "edge-gateway/telemetry", business_cols)

    fmt = cfg.get("_resolved_format", cfg.table_format)
    target = cfg.table_path("bronze", "telemetry")
    write_table(out, target, fmt=fmt, mode="overwrite", partition_by=["ingest_date"])
    LOG.info("bronze.telemetry <- %s", src)
    return out


def ingest_events(spark: SparkSession, cfg: Config, ctx: RunContext,
                  ingest_dates: Optional[Sequence[str]] = None) -> DataFrame:
    src = str(cfg.layer_path("raw") / "events")
    df = _read_jsonl(spark, src, EVENT_RAW_SCHEMA, ingest_dates)
    business_cols = [f.name for f in EVENT_RAW_SCHEMA.fields]
    out = _with_audit(df, ctx, "edge-gateway/events", business_cols)

    fmt = cfg.get("_resolved_format", cfg.table_format)
    target = cfg.table_path("bronze", "events")
    write_table(out, target, fmt=fmt, mode="overwrite", partition_by=["ingest_date"])
    LOG.info("bronze.events <- %s", src)
    return out


# ---------------------------------------------------------------------------
# reference sources
# ---------------------------------------------------------------------------
def _read_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


def ingest_reference(spark: SparkSession, cfg: Config, ctx: RunContext) -> dict:
    """Sites, buildings and the asset register.

    These are small, slowly changing and read on every join, so they are written
    unpartitioned and later broadcast.
    """
    raw = cfg.layer_path("raw")
    fmt = cfg.get("_resolved_format", cfg.table_format)
    out = {}
    for name, schema, subdir in [
        ("sites", SITE_SCHEMA, "sites"),
        ("buildings", BUILDING_SCHEMA, "buildings"),
        ("assets", ASSET_METADATA_SCHEMA, "assets"),
    ]:
        df = _read_csv(spark, str(raw / subdir), schema)
        df = _with_audit(df, ctx, f"asset-registry/{name}", [f.name for f in schema.fields])
        write_table(df, cfg.table_path("bronze", name), fmt=fmt, mode="overwrite")
        out[name] = df
        LOG.info("bronze.%s <- %s (%d rows)", name, subdir, df.count())
    return out


def run(spark: SparkSession, cfg: Config, ctx: RunContext,
        ingest_dates: Optional[Sequence[str]] = None) -> dict:
    """Ingest everything. Returns the bronze DataFrames by name."""
    reference = ingest_reference(spark, cfg, ctx)
    telemetry = ingest_telemetry(spark, cfg, ctx, ingest_dates)
    events = ingest_events(spark, cfg, ctx, ingest_dates)
    return {"telemetry": telemetry, "events": events, **reference}
