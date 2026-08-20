"""Batch pipeline entry point.

    python -m nectar.pipeline.run_batch --layers bronze,silver,gold,hierarchy,report

Each layer is independently runnable so the orchestrator can schedule them as separate
tasks with their own retries, and so a failed gold build can be re-run without
re-ingesting.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import List, Optional

from ..config import Config, load_config
from ..io_layer import optimize_table, resolve_format
from ..logging_utils import RunContext, setup_logging, stage
from ..spark_session import get_spark

LOG = logging.getLogger("nectar.run")

ALL_LAYERS = ["bronze", "silver", "gold", "hierarchy", "report"]

#: Tables worth compacting + Z-ORDERing after a batch. Z-ORDER columns are the
#: ones dashboards filter on hardest.
_MAINTENANCE = [
    ("silver", "telemetry", ["asset_id", "timestamp"]),
    ("gold", "fact_telemetry", ["asset_id", "timestamp"]),
    ("gold", "fact_energy_hourly", ["asset_id"]),
    ("gold", "fact_event", ["asset_id", "event_type"]),
]


def run_pipeline(cfg: Config, layers: List[str], ingest_dates: Optional[List[str]] = None,
                 optimize: bool = False, ctx: Optional[RunContext] = None) -> dict:
    from . import bronze as bronze_mod
    from . import gold as gold_mod
    from . import silver as silver_mod

    ctx = ctx or RunContext.from_env()
    spark = get_spark(cfg)

    # Record the format we can actually honour so every module agrees.
    resolved = resolve_format(spark, cfg.table_format)
    cfg.data["_resolved_format"] = resolved
    LOG.info("batch %s starting | layers=%s | format=%s", ctx.batch_id, layers, resolved)

    results: dict = {}
    bronze_out = silver_out = None

    if "bronze" in layers:
        with stage("bronze", ctx):
            bronze_out = bronze_mod.run(spark, cfg, ctx, ingest_dates)
            results["bronze"] = {k: v for k, v in bronze_out.items()}

    if "silver" in layers:
        with stage("silver", ctx):
            silver_out = silver_mod.run(spark, cfg, ctx, bronze_out)
            results["silver"] = silver_out

    if "gold" in layers:
        with stage("gold", ctx):
            results["gold"] = gold_mod.run(spark, cfg, ctx, silver_out)

    if "hierarchy" in layers:
        with stage("hierarchy", ctx):
            from ..hierarchy.closure_table import build_hierarchy_tables

            results["hierarchy"] = build_hierarchy_tables(spark, cfg, ctx)

    if "report" in layers:
        with stage("quality_report", ctx):
            from ..quality.report import generate_reports

            results["report"] = generate_reports(spark, cfg, ctx)

    if optimize:
        with stage("optimize", ctx):
            for layer, table, zorder in _MAINTENANCE:
                try:
                    optimize_table(spark, cfg.table_path(layer, table), fmt=resolved, zorder_by=zorder)
                except Exception as exc:  # OPTIMIZE is best-effort maintenance
                    LOG.warning("OPTIMIZE %s.%s skipped: %s", layer, table, exc)

    ctx.record("elapsed_seconds", round(ctx.elapsed, 2))
    LOG.info("batch %s finished in %.1fs", ctx.batch_id, ctx.elapsed)
    return results


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Nectar IoT batch pipeline")
    parser.add_argument("--layers", default=",".join(ALL_LAYERS),
                        help=f"comma separated subset of {ALL_LAYERS}")
    parser.add_argument("--ingest-dates", default=None,
                        help="comma separated YYYY-MM-DD list for an incremental run")
    parser.add_argument("--config", default=None)
    parser.add_argument("--format", default=None, choices=["delta", "parquet"],
                        help="override storage.table_format")
    parser.add_argument("--optimize", action="store_true", help="run OPTIMIZE/Z-ORDER afterwards")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    setup_logging(args.log_level)
    cfg = load_config(args.config)
    if args.format:
        cfg.data.setdefault("storage", {})["table_format"] = args.format

    layers = [l.strip() for l in args.layers.split(",") if l.strip()]
    unknown = set(layers) - set(ALL_LAYERS)
    if unknown:
        parser.error(f"unknown layers: {sorted(unknown)}")

    dates = [d.strip() for d in args.ingest_dates.split(",")] if args.ingest_dates else None

    ctx = RunContext.from_env()
    try:
        run_pipeline(cfg, layers, dates, optimize=args.optimize, ctx=ctx)
    except Exception as exc:
        LOG.exception("pipeline failed: %s", exc)
        print(json.dumps({"batch_id": ctx.batch_id, "status": "FAILED", "error": str(exc)}, indent=2))
        return 1

    print(json.dumps({"batch_id": ctx.batch_id, "status": "SUCCESS", "metrics": ctx.metrics}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
