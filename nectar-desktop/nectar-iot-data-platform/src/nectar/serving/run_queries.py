"""Execute the SQL deliverables and capture their output.

Running the queries as part of the build is what turns "here are six SQL files"
into "here are six SQL files that demonstrably return the right answer". Results
are written as CSV (for inspection) and as a single Markdown digest (for the
report), so a reviewer can see the output without installing anything.

    python -m nectar.serving.run_queries
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

from ..config import Config, load_config
from .load_duckdb import connect

LOG = logging.getLogger("nectar.serving.queries")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ANALYTICS_DIR = PROJECT_ROOT / "sql" / "analytics"
HIERARCHY_FILE = PROJECT_ROOT / "sql" / "hierarchy" / "hierarchy_queries.sql"


def _split_statements(sql: str) -> List[str]:
    """Split a script into statements, ignoring semicolons inside comments."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    body = "\n".join(lines)
    return [s.strip() for s in body.split(";") if s.strip()]


#: Public alias - the notebook and any ad-hoc script need this too.
split_statements = _split_statements


def _bind(sql: str, params: Dict[str, str]) -> str:
    """Substitute ``:name`` placeholders (DuckDB accepts $name, not :name)."""
    for key, value in params.items():
        sql = re.sub(rf":{key}\b", f"'{value}'", sql)
    return sql


def run_analytics(cfg: Optional[Config] = None, out_dir: Optional[Path] = None,
                  limit_preview: int = 10) -> dict:
    cfg = cfg or load_config()
    out = Path(out_dir or (PROJECT_ROOT / "data" / "query_results"))
    out.mkdir(parents=True, exist_ok=True)

    con = connect(cfg)
    results: Dict[str, dict] = {}
    digest: List[str] = ["# SQL challenge - executed results", ""]

    for path in sorted(ANALYTICS_DIR.glob("*.sql")):
        name = path.stem
        statements = _split_statements(path.read_text(encoding="utf-8"))
        sql = statements[0]
        try:
            df = con.execute(sql).fetchdf()
        except Exception as exc:
            LOG.error("%s failed: %s", name, exc)
            results[name] = {"status": "FAILED", "error": str(exc)}
            digest += [f"## {name}", "", f"**FAILED** - {exc}", ""]
            continue

        csv_path = out / f"{name}.csv"
        df.to_csv(csv_path, index=False)
        results[name] = {"status": "OK", "rows": int(len(df)),
                         "columns": list(df.columns), "csv": str(csv_path)}
        LOG.info("%s -> %d rows", name, len(df))

        preview = df.head(limit_preview)
        digest += [
            f"## {name}",
            "",
            f"`{path.relative_to(PROJECT_ROOT)}` returned **{len(df):,} rows**.",
            "",
            preview.to_markdown(index=False) if len(preview) else "_(no rows)_",
            "",
        ]

    con.close()
    digest_path = out / "sql_results.md"
    digest_path.write_text("\n".join(digest), encoding="utf-8")
    return {"results": results, "digest": str(digest_path), "out_dir": str(out)}


def run_hierarchy(cfg: Optional[Config] = None, asset_id: Optional[str] = None,
                  site_id: Optional[str] = None, out_dir: Optional[Path] = None) -> dict:
    """Run the Task 4 hierarchy statements against a real asset.

    The focus asset defaults to the one with the largest blast radius, because
    that is the row that actually exercises multi-level traversal.
    """
    cfg = cfg or load_config()
    out = Path(out_dir or (PROJECT_ROOT / "data" / "query_results"))
    out.mkdir(parents=True, exist_ok=True)
    con = connect(cfg)

    if not asset_id:
        asset_id = con.execute(
            "SELECT asset_id FROM dim_asset_hierarchy ORDER BY descendant_count DESC, asset_id LIMIT 1"
        ).fetchone()[0]
    if not site_id:
        site_id = con.execute("SELECT site_id FROM dim_site ORDER BY site_id LIMIT 1").fetchone()[0]

    script = HIERARCHY_FILE.read_text(encoding="utf-8")
    script = script.replace("'SITE-CBE'", f"'{site_id}'")
    statements = _split_statements(_bind(script, {"asset_id": asset_id}))

    digest = [f"# Asset hierarchy queries (focus asset `{asset_id}`, site `{site_id}`)", ""]
    outcome: Dict[str, dict] = {}
    for i, sql in enumerate(statements, start=1):
        label = f"hierarchy_q{i}"
        try:
            df = con.execute(sql).fetchdf()
        except Exception as exc:
            LOG.error("%s failed: %s", label, exc)
            outcome[label] = {"status": "FAILED", "error": str(exc)}
            digest += [f"## {label}", "", f"**FAILED** - {exc}", ""]
            continue
        df.to_csv(out / f"{label}.csv", index=False)
        outcome[label] = {"status": "OK", "rows": int(len(df))}
        digest += [f"## {label}", "", f"**{len(df):,} rows**", "",
                   df.head(10).to_markdown(index=False) if len(df) else "_(no rows)_", ""]

    con.close()
    (out / "hierarchy_results.md").write_text("\n".join(digest), encoding="utf-8")
    return {"focus_asset": asset_id, "site_id": site_id, "results": outcome}


def main() -> None:
    from ..logging_utils import setup_logging

    parser = argparse.ArgumentParser(description="Run the SQL deliverables")
    parser.add_argument("--config", default=None)
    parser.add_argument("--asset-id", default=None)
    parser.add_argument("--site-id", default=None)
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    analytics = run_analytics(cfg)
    hierarchy = run_hierarchy(cfg, args.asset_id, args.site_id)
    print(json.dumps({"analytics": analytics["results"], "hierarchy": hierarchy}, indent=2))


if __name__ == "__main__":
    main()
