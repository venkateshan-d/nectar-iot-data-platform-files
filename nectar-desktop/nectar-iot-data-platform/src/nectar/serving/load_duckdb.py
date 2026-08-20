"""Serving layer - expose the gold tables to a SQL engine.

The lakehouse is the system of record; the serving layer is whatever the
consumer speaks. In production that is Snowflake/BigQuery external tables or a
Databricks SQL warehouse pointed at the same Delta files - no copy, no drift.

For this submission the same idea is demonstrated locally with **DuckDB**, which
reads the gold Parquet files in place. That keeps the Task 6 SQL runnable by a
reviewer with nothing installed beyond ``pip install -r requirements.txt``, and
the SQL itself is plain ANSI that ports to Spark SQL / Snowflake unchanged
(differences are noted in the individual query files).
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from ..config import Config, load_config

LOG = logging.getLogger("nectar.serving")

#: gold tables published to the serving layer
GOLD_TABLES = [
    "dim_date", "dim_time", "dim_site", "dim_building", "dim_asset",
    "dim_asset_hierarchy", "asset_closure",
    "fact_telemetry", "fact_energy_hourly", "fact_event",
    "curated_hourly_energy", "curated_daily_asset_utilization",
    "curated_daily_environment", "curated_fault_statistics",
    "agg_asset_daily", "agg_building_daily", "agg_site_daily",
]

SILVER_TABLES = ["telemetry", "events", "assets", "buildings", "sites"]


def _has_parquet(path: Path) -> bool:
    return path.exists() and any(path.rglob("*.parquet"))


def build_database(cfg: Optional[Config] = None, db_path: Optional[str] = None,
                   materialise: bool = False) -> dict:
    """Create (or refresh) the DuckDB database over the lakehouse files.

    ``materialise=False`` registers **views** - zero copy, always current.
    ``materialise=True`` creates real tables, which is what you want when the
    lakehouse lives on remote object storage and the dashboard needs low latency.
    """
    import duckdb

    cfg = cfg or load_config()
    target = Path(db_path or cfg.get("serving.duckdb_path", "./data/serving/nectar.duckdb"))
    if not target.is_absolute():
        target = (Path(cfg.path).parents[1] / target).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()

    con = duckdb.connect(str(target))
    registered: List[str] = []
    skipped: List[str] = []

    def register(schema: str, layer: str, table: str) -> None:
        path = Path(cfg.table_path(layer, table))
        if not _has_parquet(path):
            skipped.append(f"{schema}.{table}")
            return
        glob = f"{path}/**/*.parquet"
        keyword = "TABLE" if materialise else "VIEW"
        con.execute(
            f'CREATE {keyword} {schema}.{table} AS '
            f"SELECT * FROM read_parquet('{glob}', hive_partitioning => true, union_by_name => true)"
        )
        registered.append(f"{schema}.{table}")

    for schema in ("gold", "silver", "quality"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for table in GOLD_TABLES:
        register("gold", "gold", table)
    for table in SILVER_TABLES:
        register("silver", "silver", table)
    register("quality", "quality", "dq_results")

    # The analytics queries are written against unqualified names, so publish
    # the gold tables into the default schema too.
    for name in registered:
        schema, table = name.split(".", 1)
        if schema == "gold":
            con.execute(f"CREATE OR REPLACE VIEW main.{table} AS SELECT * FROM gold.{table}")

    con.close()
    LOG.info("duckdb ready at %s (%d tables, %d skipped)", target, len(registered), len(skipped))
    return {"database": str(target), "registered": registered, "skipped": skipped,
            "materialised": materialise}


def connect(cfg: Optional[Config] = None, db_path: Optional[str] = None):
    """Open a read-only connection to the serving database."""
    import duckdb

    cfg = cfg or load_config()
    target = Path(db_path or cfg.get("serving.duckdb_path", "./data/serving/nectar.duckdb"))
    if not target.is_absolute():
        target = (Path(cfg.path).parents[1] / target).resolve()
    if not target.exists():
        raise FileNotFoundError(f"{target} not found - run `python -m nectar.serving.load_duckdb` first")
    return duckdb.connect(str(target), read_only=True)


def main() -> None:
    from ..logging_utils import setup_logging

    parser = argparse.ArgumentParser(description="Publish the gold layer to DuckDB")
    parser.add_argument("--config", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--materialise", action="store_true",
                        help="copy data into real tables instead of registering views")
    args = parser.parse_args()

    setup_logging()
    print(json.dumps(build_database(load_config(args.config), args.db, args.materialise), indent=2))


if __name__ == "__main__":
    main()
