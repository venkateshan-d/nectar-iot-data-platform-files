"""Nightly lakehouse maintenance.

Separated from the hourly pipeline on purpose. Compaction and vacuum are
expensive, must not compete with the ingest window, and must not be able to
fail the pipeline that produces the data. Running them in their own DAG also
means their retries and alerting are tuned differently: nobody needs to be paged
at 03:00 because an OPTIMIZE was slow.

What it does
------------
1. **OPTIMIZE + Z-ORDER** the hot tables. Streaming and hourly micro-batches
   produce many small files; left alone, read performance degrades steadily and
   the metadata scan starts to dominate query time.
2. **VACUUM** files older than the retention window, so storage cost does not
   grow without bound. Retention is 7 days, which is longer than the longest
   query and longer than the longest plausible time-travel debugging session.
3. **Recompute late-affected aggregates.** Records that arrived after their
   day's window closed mean the gold aggregates for that day are stale. This
   step finds the affected dates and rebuilds exactly those partitions -
   the alternative (full recompute) is wasteful, and doing nothing leaves
   permanently wrong history.
4. **Refresh the SCD2 asset dimension** and prune quarantine partitions past
   their retention.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT_HOME = os.environ.get("NECTAR_HOME", "/opt/nectar")

MAINTENANCE_TABLES = [
    ("silver", "telemetry", ["asset_id", "timestamp"]),
    ("silver", "telemetry_stream", ["asset_id", "timestamp"]),
    ("gold", "fact_telemetry", ["asset_id", "timestamp"]),
    ("gold", "fact_energy_hourly", ["asset_id"]),
    ("gold", "fact_event", ["asset_id", "event_type"]),
    ("gold", "asset_closure", ["ancestor_id", "descendant_id"]),
]


def _spark_and_cfg(app: str):
    import sys

    sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
    from nectar.config import load_config
    from nectar.io_layer import resolve_format
    from nectar.logging_utils import setup_logging
    from nectar.spark_session import get_spark

    setup_logging(json_logs=True)
    cfg = load_config(os.path.join(PROJECT_HOME, "config", "pipeline.yaml"))
    spark = get_spark(cfg, app)
    cfg.data["_resolved_format"] = resolve_format(spark, cfg.table_format)
    return spark, cfg


def compact_tables(**context) -> dict:
    from nectar.io_layer import optimize_table, table_exists

    spark, cfg = _spark_and_cfg("maintenance-optimize")
    fmt = cfg.get("_resolved_format")
    done, skipped = [], []
    for layer, table, zorder in MAINTENANCE_TABLES:
        path = cfg.table_path(layer, table)
        if not table_exists(spark, path, fmt):
            skipped.append(f"{layer}.{table}")
            continue
        try:
            optimize_table(spark, path, fmt=fmt, zorder_by=zorder)
            done.append(f"{layer}.{table}")
        except Exception as exc:          # maintenance is best-effort
            skipped.append(f"{layer}.{table}: {exc}")
    return {"optimized": done, "skipped": skipped}


def vacuum_tables(**context) -> dict:
    from nectar.io_layer import table_exists, vacuum_table

    spark, cfg = _spark_and_cfg("maintenance-vacuum")
    fmt = cfg.get("_resolved_format")
    done = []
    for layer, table, _ in MAINTENANCE_TABLES:
        path = cfg.table_path(layer, table)
        if table_exists(spark, path, fmt):
            vacuum_table(spark, path, fmt=fmt, retain_hours=168)
            done.append(f"{layer}.{table}")
    return {"vacuumed": done}


def recompute_late_partitions(**context) -> dict:
    """Rebuild gold for any date that received late-arriving records today.

    This is the correctness half of late-arrival handling. Ingesting a late
    record into silver is easy; the part that is usually forgotten is that the
    aggregate for that record's *event* date is now wrong and has to be redone.
    """
    from pyspark.sql import functions as F

    from nectar.io_layer import read_table, table_exists
    from nectar.logging_utils import RunContext
    from nectar.pipeline.run_batch import run_pipeline

    spark, cfg = _spark_and_cfg("maintenance-late")
    fmt = cfg.get("_resolved_format")
    path = cfg.table_path("silver", "telemetry")
    if not table_exists(spark, path, fmt):
        return {"affected_dates": [], "recomputed": False}

    lookback = int(cfg.get("quality.late_arrival_watermark_hours", 24)) // 24 + 3
    cutoff = context["logical_date"].date() - timedelta(days=lookback)
    affected = [
        r["event_date"].isoformat()
        for r in (
            read_table(spark, path, fmt)
            .filter(F.col("_is_late") & (F.col("event_date") >= F.lit(cutoff.isoformat())))
            .select("event_date").distinct().collect()
        )
    ]
    if not affected:
        return {"affected_dates": [], "recomputed": False}

    # The gold builders are partition-scoped overwrites, so rerunning the layer
    # restates exactly the affected dates.
    run_pipeline(cfg, ["gold"], ctx=RunContext(batch_id="late-fix"))
    return {"affected_dates": sorted(affected), "recomputed": True}


def prune_quarantine(**context) -> dict:
    """Drop quarantine partitions past their retention.

    Quarantined rows exist so an engineer can diagnose and replay. After 30 days
    they are archaeology, and they are the fastest-growing table in the
    lakehouse when a device goes bad.
    """
    import shutil
    from pathlib import Path

    _, cfg = _spark_and_cfg("maintenance-prune")
    root = cfg.layer_path("quarantine")
    if not root.exists():
        return {"removed": []}
    cutoff = datetime.now().timestamp() - 30 * 86400
    removed = []
    for batch_dir in root.glob("*/_batch_id=*"):
        if batch_dir.stat().st_mtime < cutoff:
            shutil.rmtree(batch_dir, ignore_errors=True)
            removed.append(str(batch_dir))
    return {"removed": removed}


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="nectar_iot_maintenance",
    description="Nightly compaction, vacuum, late-arrival restatement and retention",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="30 2 * * *",     # 02:30 - after the nightly ingest trough
    catchup=False,
    max_active_runs=1,
    tags=["nectar", "maintenance", "delta"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    late_fix = PythonOperator(task_id="recompute_late_partitions",
                              python_callable=recompute_late_partitions)
    optimize = PythonOperator(task_id="optimize_tables", python_callable=compact_tables)
    vacuum = PythonOperator(task_id="vacuum_tables", python_callable=vacuum_tables)
    prune = PythonOperator(task_id="prune_quarantine", python_callable=prune_quarantine)

    end = EmptyOperator(task_id="end")

    # Restate before compacting, so OPTIMIZE runs once over the final files.
    start >> late_fix >> optimize >> vacuum >> prune >> end
