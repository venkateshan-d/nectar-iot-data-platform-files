"""Automated data quality reporting (Task 5 bonus).

Reads ``quality.dq_results`` plus the freshness/completeness signals and emits

* ``data_quality_report.json`` - machine readable, what CI and alerting consume;
* ``data_quality_report.html`` - a self-contained page for humans.

The HTML is deliberately dependency-free (no CDN, no JS framework): it has to
open from an S3 link or an email attachment on a phone. Status is never carried
by colour alone - every verdict ships an icon and a word.
"""

from __future__ import annotations

import html
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config
from ..io_layer import read_table, table_exists
from ..logging_utils import RunContext
from .engine import completeness_report, freshness_report

LOG = logging.getLogger("nectar.quality.report")


# ---------------------------------------------------------------------------
# gather
# ---------------------------------------------------------------------------
def collect_metrics(spark: SparkSession, cfg: Config, batch_id: Optional[str] = None) -> dict:
    fmt = cfg.get("_resolved_format", cfg.table_format)
    results_path = cfg.table_path("quality", "dq_results")
    if not table_exists(spark, results_path, fmt):
        raise FileNotFoundError(f"No quality results at {results_path}; run the silver layer first.")

    all_results = read_table(spark, results_path, fmt)

    def _latest_batch() -> str:
        latest = all_results.agg(F.max("evaluated_at")).collect()[0][0]
        return all_results.filter(F.col("evaluated_at") == latest).select("batch_id").first()["batch_id"]

    results = all_results.filter(F.col("batch_id") == batch_id) if batch_id else None
    # Reporting can be run as its own task (or on its own schedule) after the
    # silver job, in which case this process has a different batch id and would
    # otherwise render an empty - and misleadingly green - report.
    if results is None or not results.take(1):
        if batch_id:
            LOG.warning("no quality results for batch %s; falling back to the latest batch", batch_id)
        batch_id = _latest_batch()
        results = all_results.filter(F.col("batch_id") == batch_id)

    rules = [r.asDict(recursive=True) for r in results.orderBy(
        F.col("passed").asc(), F.col("failure_rate").desc()).collect()]

    by_dimension = [r.asDict() for r in (
        results.groupBy("dimension")
        .agg(
            F.count(F.lit(1)).alias("rules"),
            F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("breached"),
            F.sum("rows_failed").alias("rows_failed"),
        ).orderBy("dimension").collect())]

    by_table = [r.asDict() for r in (
        results.groupBy("layer", "table_name")
        .agg(
            F.max("rows_evaluated").alias("rows_evaluated"),
            F.count(F.lit(1)).alias("rules"),
            F.sum(F.when(~F.col("passed"), 1).otherwise(0)).alias("breached"),
        ).orderBy("table_name").collect())]

    # --- freshness / completeness ----------------------------------------
    freshness_rows: List[dict] = []
    completeness_rows: List[dict] = []
    silver_tel = cfg.table_path("silver", "telemetry")
    if table_exists(spark, silver_tel, fmt):
        telemetry = read_table(spark, silver_tel, fmt)
        fresh = freshness_report(telemetry, cfg, batch_id)
        freshness_rows = [r.asDict() for r in fresh.orderBy(F.col("lag_minutes").desc()).limit(25).collect()]
        comp = (
            completeness_report(telemetry, cfg, batch_id)
            .groupBy("asset_id")
            .agg(F.round(F.avg("completeness_pct"), 2).alias("completeness_pct"),
                 F.sum("missing").alias("missing_readings"))
            .orderBy(F.col("completeness_pct").asc())
        )
        completeness_rows = [r.asDict() for r in comp.limit(25).collect()]

    # --- quarantine -------------------------------------------------------
    quarantine: Dict[str, int] = {}
    for table in ("telemetry", "events"):
        qpath = cfg.table_path("quarantine", table)
        if table_exists(spark, qpath, fmt):
            quarantine[table] = read_table(spark, qpath, fmt).filter(F.col("_batch_id") == batch_id).count()

    total_rows = sum(t["rows_evaluated"] for t in by_table)
    breached = [r for r in rules if not r["passed"]]
    blocking_breaches = [r for r in breached if r["severity"] == "BLOCKING"]

    return {
        "batch_id": batch_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "table_format": fmt,
        "totals": {
            "rows_evaluated": total_rows,
            "rules_evaluated": len(rules),
            "rules_breached": len(breached),
            "blocking_breaches": len(blocking_breaches),
            "rows_quarantined": sum(quarantine.values()),
            "pass_rate_pct": round(100.0 * (len(rules) - len(breached)) / len(rules), 2) if rules else 100.0,
        },
        "quarantine": quarantine,
        "rules": rules,
        "by_dimension": by_dimension,
        "by_table": by_table,
        "freshness": freshness_rows,
        "completeness": completeness_rows,
        "verdict": "FAIL" if blocking_breaches else ("WARN" if breached else "PASS"),
    }


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------
_CSS = """
:root{color-scheme:light dark;
  --surface-1:#fcfcfb; --plane:#f9f9f7; --ink:#0b0b0b; --ink-2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,.10);
  --good:#0ca30c; --warning:#fab219; --serious:#ec835a; --critical:#d03b3b;
  --series-1:#2a78d6;}
@media (prefers-color-scheme:dark){:root{
  --surface-1:#1a1a19; --plane:#0d0d0d; --ink:#fff; --ink-2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10); --series-1:#3987e5;}}
*{box-sizing:border-box}
body{margin:0;padding:32px 20px 64px;background:var(--plane);color:var(--ink);
  font:14px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;}
.wrap{max-width:1080px;margin:0 auto}
h1{font-size:24px;margin:0 0 4px} h2{font-size:16px;margin:36px 0 12px;color:var(--ink)}
.sub{color:var(--ink-2);margin:0 0 24px;font-size:13px}
.verdict{display:inline-flex;gap:8px;align-items:center;padding:6px 12px;border-radius:8px;
  font-weight:600;border:1px solid var(--border);background:var(--surface-1)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.tile{background:var(--surface-1);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.tile .k{font-size:12px;color:var(--ink-2);margin-bottom:6px}
.tile .v{font-size:26px;font-weight:600;letter-spacing:-.01em}
table{width:100%;border-collapse:collapse;background:var(--surface-1);
  border:1px solid var(--border);border-radius:10px;overflow:hidden;font-size:13px}
th{text-align:left;font-weight:600;color:var(--ink-2);padding:9px 12px;border-bottom:1px solid var(--grid);
  background:var(--plane);font-size:12px;text-transform:uppercase;letter-spacing:.03em}
td{padding:8px 12px;border-bottom:1px solid var(--grid);vertical-align:top}
tr:last-child td{border-bottom:none}
td.num{text-align:right;font-variant-numeric:tabular-nums}
.meter{position:relative;height:8px;width:110px;background:var(--grid);border-radius:4px;overflow:hidden;display:inline-block;vertical-align:middle}
.meter>span{position:absolute;left:0;top:0;bottom:0;border-radius:4px;background:var(--series-1)}
.meter.bad>span{background:var(--critical)} .meter.warn>span{background:var(--serious)}
.pill{display:inline-flex;gap:5px;align-items:center;font-size:12px;font-weight:600}
.rule{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.desc{color:var(--ink-2);font-size:12px}
.foot{color:var(--muted);font-size:12px;margin-top:40px}
"""

_ICON = {"PASS": "&#10003;", "FAIL": "&#10007;", "WARN": "&#9888;"}


def _verdict_style(verdict: str) -> str:
    return {"PASS": "var(--good)", "WARN": "var(--serious)", "FAIL": "var(--critical)"}[verdict]


def _rule_row(rule: dict) -> str:
    rate = float(rule["failure_rate"] or 0)
    threshold = rule["threshold"]
    passed = rule["passed"]
    verdict = "PASS" if passed else ("FAIL" if rule["severity"] == "BLOCKING" else "WARN")
    cls = "" if passed else ("bad" if rule["severity"] == "BLOCKING" else "warn")
    width = min(100.0, rate * 100 * 8 + (2 if rate > 0 else 0))  # 8x zoom: rates are small
    desc = (rule.get("details") or {}).get("description", "")
    return f"""<tr>
<td><div class="rule">{html.escape(rule['rule_id'])}</div><div class="desc">{html.escape(desc)}</div></td>
<td>{html.escape(rule['dimension'])}</td>
<td>{html.escape(rule['severity'])}</td>
<td class="num">{rule['rows_failed']:,}</td>
<td class="num">{rate*100:.3f}%</td>
<td class="num">{'' if threshold is None else f'{threshold*100:.2f}%'}</td>
<td><span class="meter {cls}"><span style="width:{width:.1f}%"></span></span></td>
<td><span class="pill" style="color:{_verdict_style(verdict)}">{_ICON[verdict]} {verdict}</span></td>
</tr>"""


def render_html(metrics: dict) -> str:
    t = metrics["totals"]
    verdict = metrics["verdict"]
    tiles = [
        ("Rows evaluated", f"{t['rows_evaluated']:,}"),
        ("Rules evaluated", f"{t['rules_evaluated']}"),
        ("Rules breached", f"{t['rules_breached']}"),
        ("Blocking breaches", f"{t['blocking_breaches']}"),
        ("Rows quarantined", f"{t['rows_quarantined']:,}"),
        ("Rule pass rate", f"{t['pass_rate_pct']:.1f}%"),
    ]
    tiles_html = "".join(
        f'<div class="tile"><div class="k">{html.escape(k)}</div><div class="v">{v}</div></div>'
        for k, v in tiles
    )

    dim_rows = "".join(
        f"<tr><td>{html.escape(d['dimension'])}</td><td class='num'>{d['rules']}</td>"
        f"<td class='num'>{d['breached']}</td><td class='num'>{d['rows_failed']:,}</td></tr>"
        for d in metrics["by_dimension"]
    )
    tbl_rows = "".join(
        f"<tr><td>{html.escape(b['layer'])}.{html.escape(b['table_name'])}</td>"
        f"<td class='num'>{b['rows_evaluated']:,}</td><td class='num'>{b['rules']}</td>"
        f"<td class='num'>{b['breached']}</td></tr>"
        for b in metrics["by_table"]
    )
    rule_rows = "".join(_rule_row(r) for r in metrics["rules"])

    stale = [f for f in metrics["freshness"] if f.get("is_stale")]
    stale_rows = "".join(
        f"<tr><td>{html.escape(f['asset_id'])}</td><td>{html.escape(f['site_id'] or '')}</td>"
        f"<td class='num'>{f['lag_minutes']:.0f}</td><td class='num'>{f['readings']:,}</td>"
        f"<td><span class='pill' style='color:var(--critical)'>{_ICON['FAIL']} STALE</span></td></tr>"
        for f in stale[:15]
    ) or "<tr><td colspan='5' class='desc'>No stale assets in this batch.</td></tr>"

    comp_rows = "".join(
        f"<tr><td>{html.escape(c['asset_id'])}</td><td class='num'>{c['completeness_pct']:.1f}%</td>"
        f"<td class='num'>{c['missing_readings']:,}</td></tr>"
        for c in metrics["completeness"][:15]
    ) or "<tr><td colspan='3' class='desc'>No completeness data.</td></tr>"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Nectar IoT - Data Quality Report {html.escape(metrics['batch_id'])}</title>
<style>{_CSS}</style></head><body><div class="wrap">
<h1>Data Quality Report</h1>
<p class="sub">Batch <strong>{html.escape(metrics['batch_id'])}</strong> &middot;
generated {html.escape(metrics['generated_at'])} &middot;
storage format <strong>{html.escape(metrics['table_format'])}</strong></p>
<p><span class="verdict" style="color:{_verdict_style(verdict)}">{_ICON[verdict]} {verdict}</span></p>

<h2>Summary</h2>
<div class="tiles">{tiles_html}</div>

<h2>By quality dimension</h2>
<table><thead><tr><th>Dimension</th><th class="num">Rules</th><th class="num">Breached</th>
<th class="num">Rows failed</th></tr></thead><tbody>{dim_rows}</tbody></table>

<h2>By table</h2>
<table><thead><tr><th>Table</th><th class="num">Rows</th><th class="num">Rules</th>
<th class="num">Breached</th></tr></thead><tbody>{tbl_rows}</tbody></table>

<h2>Rule results</h2>
<table><thead><tr><th>Rule</th><th>Dimension</th><th>Severity</th><th class="num">Failed</th>
<th class="num">Rate</th><th class="num">Threshold</th><th>Rate (8&times;)</th><th>Verdict</th>
</tr></thead><tbody>{rule_rows}</tbody></table>

<h2>Stale assets (freshness)</h2>
<table><thead><tr><th>Asset</th><th>Site</th><th class="num">Lag (min)</th>
<th class="num">Readings</th><th>Status</th></tr></thead><tbody>{stale_rows}</tbody></table>

<h2>Least complete assets (missing records)</h2>
<table><thead><tr><th>Asset</th><th class="num">Completeness</th>
<th class="num">Missing readings</th></tr></thead><tbody>{comp_rows}</tbody></table>

<p class="foot">Generated by <code>nectar.quality.report</code>. Every rejected row is
retained in the quarantine zone under this batch id and can be replayed after the
upstream fix.</p>
</div></body></html>"""


# ---------------------------------------------------------------------------
def generate_reports(spark: SparkSession, cfg: Config, ctx: Optional[RunContext] = None,
                     batch_id: Optional[str] = None, out_dir: Optional[str] = None) -> dict:
    metrics = collect_metrics(spark, cfg, batch_id or (ctx.batch_id if ctx else None))
    out = Path(out_dir) if out_dir else (cfg.layer_path("quality") / "reports")
    out.mkdir(parents=True, exist_ok=True)

    json_path = out / f"data_quality_report_{metrics['batch_id']}.json"
    html_path = out / f"data_quality_report_{metrics['batch_id']}.html"
    json_path.write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    html_path.write_text(render_html(metrics), encoding="utf-8")

    # Stable "latest" symlinks-by-copy so dashboards can bookmark one URL.
    (out / "data_quality_report_latest.json").write_text(
        json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    (out / "data_quality_report_latest.html").write_text(render_html(metrics), encoding="utf-8")

    if ctx:
        ctx.record("quality.verdict", metrics["verdict"])
        ctx.record("quality.pass_rate_pct", metrics["totals"]["pass_rate_pct"])
    LOG.info("quality report %s -> %s", metrics["verdict"], html_path)
    return {"metrics": metrics, "json_path": str(json_path), "html_path": str(html_path)}


if __name__ == "__main__":  # pragma: no cover
    import argparse

    from ..config import load_config
    from ..io_layer import resolve_format
    from ..logging_utils import setup_logging
    from ..spark_session import get_spark

    parser = argparse.ArgumentParser(description="Generate the data quality report")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    spark = get_spark(cfg, "quality-report")
    cfg.data["_resolved_format"] = resolve_format(spark, cfg.table_format)
    result = generate_reports(spark, cfg, batch_id=args.batch_id)
    print(json.dumps({k: v for k, v in result.items() if k != "metrics"}, indent=2))
