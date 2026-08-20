"""Storage abstraction over the lakehouse tables.

Why this module exists
----------------------
The platform targets **Delta Lake**: we want ACID commits, ``MERGE`` for
idempotent upserts, schema enforcement, time travel and ``OPTIMIZE``/``VACUUM``
maintenance. Delta is therefore the default and every production path uses it.

But the Delta jars are resolved from Maven Central at session start, and some
environments (air-gapped CI runners, offline demo machines) cannot reach it.
Rather than leaving the project unrunnable there, the storage calls go through
this thin layer, which falls back to plain Parquet when
``storage.table_format: parquet``. The pipeline code above it is identical; only
the guarantees change, and the fallback is explicitly documented as dev-only:

======================  ===========================  ==========================
Capability              delta (production)           parquet (offline fallback)
======================  ===========================  ==========================
Atomic commit           yes (transaction log)        no (directory rename)
Idempotent upsert       ``MERGE INTO``               dynamic partition overwrite
Schema enforcement      on write                     manual (``schemas.py``)
Time travel             ``versionAsOf``              none
Compaction              ``OPTIMIZE`` / Z-ORDER       manual repartition
Concurrent writers      OCC, safe                    unsafe
======================  ===========================  ==========================
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Iterable, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

LOG = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# capability probe
# --------------------------------------------------------------------------
def delta_available(spark: SparkSession) -> bool:
    """True when the Delta jars are actually loaded in this JVM."""
    try:
        spark._jvm.io.delta.tables.DeltaTable  # noqa: B018 - attribute probe
        return True
    except Exception:  # pragma: no cover - depends on classpath
        return False


def resolve_format(spark: SparkSession, requested: str) -> str:
    """Return the format we can really use, warning loudly on a downgrade."""
    requested = (requested or "delta").lower()
    if requested == "delta" and not delta_available(spark):
        LOG.warning(
            "table_format=delta requested but Delta jars are not on the classpath; "
            "falling back to parquet. Transactional guarantees are DISABLED."
        )
        return "parquet"
    return requested


# --------------------------------------------------------------------------
# read / write
# --------------------------------------------------------------------------
def table_exists(spark: SparkSession, path: str, fmt: str = "delta") -> bool:
    p = Path(path)
    if not p.exists():
        return False
    if fmt == "delta":
        return (p / "_delta_log").exists()
    return any(p.glob("**/*.parquet"))


def read_table(spark: SparkSession, path: str, fmt: str = "delta", version: Optional[int] = None) -> DataFrame:
    """Read a lakehouse table; ``version`` performs Delta time travel."""
    reader = spark.read.format(fmt)
    if version is not None:
        if fmt != "delta":
            raise ValueError("Time travel requires table_format=delta")
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def write_table(
    df: DataFrame,
    path: str,
    fmt: str = "delta",
    mode: str = "overwrite",
    partition_by: Optional[Sequence[str]] = None,
    merge_schema: bool = False,
) -> None:
    """Write a DataFrame to a lakehouse table."""
    writer = df.write.format(fmt).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if fmt == "delta":
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if mode == "overwrite" and partition_by:
            # Replace only the partitions present in this batch. Without this a
            # backfill of one day would wipe the whole table.
            writer = writer.option("partitionOverwriteMode", "dynamic")
    writer.save(path)
    LOG.info("wrote %s rows=%s mode=%s fmt=%s", path, "n/a", mode, fmt)


def upsert_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    keys: Iterable[str],
    fmt: str = "delta",
    partition_by: Optional[Sequence[str]] = None,
    partition_pruning_predicate: Optional[str] = None,
) -> None:
    """Idempotent upsert on *keys*.

    This is what makes a re-run of the same batch a no-op instead of a
    duplication event - the single most important property of a pipeline that
    The orchestrator will retry automatically.

    Delta  : ``MERGE INTO ... WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT``.
    Parquet: dynamic partition overwrite, which is idempotent per partition but
             requires the batch to contain whole partitions.
    """
    keys = list(keys)
    if not table_exists(spark, path, fmt):
        write_table(df, path, fmt=fmt, mode="overwrite", partition_by=partition_by)
        return

    if fmt == "delta":
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(spark, path)
        condition = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        if partition_pruning_predicate:
            # Pruning the target scan to the touched partitions turns a full
            # table rewrite into a partition-local one.
            condition = f"({partition_pruning_predicate}) AND {condition}"
        (
            target.alias("t")
            .merge(df.alias("s"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        LOG.info("MERGE into %s on %s", path, keys)
    else:
        write_table(df, path, fmt=fmt, mode="overwrite", partition_by=partition_by)


def append_table(df: DataFrame, path: str, fmt: str = "delta", partition_by: Optional[Sequence[str]] = None) -> None:
    write_table(df, path, fmt=fmt, mode="append", partition_by=partition_by)


# --------------------------------------------------------------------------
# maintenance
# --------------------------------------------------------------------------
def optimize_table(spark: SparkSession, path: str, fmt: str = "delta", zorder_by: Optional[Sequence[str]] = None) -> None:
    """Compact small files and (optionally) Z-ORDER for data skipping.

    IoT ingestion produces many small files (one micro-batch every few
    seconds). Left alone this destroys read performance, so the nightly
    maintenance DAG calls this on the hot tables.
    """
    if fmt != "delta":
        LOG.info("optimize skipped for fmt=%s (no-op)", fmt)
        return
    cols = f" ZORDER BY ({', '.join(zorder_by)})" if zorder_by else ""
    spark.sql(f"OPTIMIZE delta.`{path}`{cols}")
    LOG.info("OPTIMIZE %s%s", path, cols)


def vacuum_table(spark: SparkSession, path: str, fmt: str = "delta", retain_hours: int = 168) -> None:
    if fmt != "delta":
        return
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")


def drop_table(path: str) -> None:
    """Remove a table directory - used by ``make clean`` and by tests."""
    shutil.rmtree(path, ignore_errors=True)
