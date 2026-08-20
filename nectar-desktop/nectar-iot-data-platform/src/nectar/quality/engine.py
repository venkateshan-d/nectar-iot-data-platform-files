"""Data quality engine.

Applies a :class:`~nectar.quality.rules.Rule` set to a DataFrame in **one pass**
and returns three things:

``results``  one row per rule with counts, failure rate, threshold and verdict
``clean``    rows that passed every BLOCKING rule (WARN flags are carried along)
``quarantine`` rows that failed at least one BLOCKING rule, annotated with the
             list of rule ids they broke

Quarantine rather than drop
---------------------------
Dropping bad rows destroys the evidence needed to fix the upstream device. Every
rejected row is written to ``quarantine/<table>`` partitioned by batch, so an
engineer can query "which gateway produced last night's unparseable timestamps"
and replay the rows after the fix.

The engine also computes two dataset-level signals that no row-level predicate
can express: **freshness** (per-asset watermark lag) and **statistical
outliers** (robust z-score against each asset's own recent behaviour).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from ..config import Config
from ..schemas import QUALITY_RESULT_SCHEMA
from .rules import BLOCKING, Rule

LOG = logging.getLogger("nectar.quality")


class DataQualityError(RuntimeError):
    """Raised when a BLOCKING rule breaches its threshold and the config says stop."""


@dataclass
class QualityOutcome:
    results: DataFrame
    clean: DataFrame
    quarantine: DataFrame
    summary: List[dict]
    passed: bool

    @property
    def breaches(self) -> List[dict]:
        return [r for r in self.summary if not r["passed"]]


class QualityEngine:
    def __init__(
        self,
        spark: SparkSession,
        cfg: Config,
        rules: Sequence[Rule],
        layer: str,
        table_name: str,
        batch_id: str,
    ):
        self.spark = spark
        self.cfg = cfg
        self.rules = list(rules)
        self.layer = layer
        self.table_name = table_name
        self.batch_id = batch_id

    # ------------------------------------------------------------------
    def evaluate(self, df: DataFrame, drop_helper_columns: bool = True) -> QualityOutcome:
        missing = self._missing_helpers(df)
        if missing:
            raise ValueError(
                f"Rules for {self.table_name} need helper columns {sorted(missing)}; "
                "call the prepare_* helpers in pipeline/silver.py first."
            )

        flags = {rule.rule_id: rule.failure_flag(df) for rule in self.rules}
        flagged = df
        for rule_id, flag in flags.items():
            flagged = flagged.withColumn(_flag_col(rule_id), flag)
        flagged = flagged.cache()

        total = flagged.count()
        agg = flagged.agg(
            *[F.sum(F.col(_flag_col(r.rule_id)).cast("long")).alias(r.rule_id) for r in self.rules]
        ).collect()[0].asDict() if self.rules else {}

        evaluated_at = datetime.now(timezone.utc)
        summary: List[dict] = []
        for rule in self.rules:
            failed = int(agg.get(rule.rule_id) or 0)
            rate = (failed / total) if total else 0.0
            threshold = rule.threshold
            passed = True if threshold is None else rate <= threshold
            summary.append({
                "batch_id": self.batch_id,
                "evaluated_at": evaluated_at,
                "layer": self.layer,
                "table_name": self.table_name,
                "rule_id": rule.rule_id,
                "dimension": rule.dimension,
                "column_name": rule.column,
                "severity": rule.severity,
                "rows_evaluated": total,
                "rows_failed": failed,
                "failure_rate": round(rate, 6),
                "threshold": threshold,
                "passed": passed,
                "details": {"description": rule.description},
            })

        blocking_ids = [r.rule_id for r in self.rules if r.severity == BLOCKING]
        if blocking_ids:
            reasons = F.array_compact(F.array(*[
                F.when(F.col(_flag_col(rid)), F.lit(rid)) for rid in blocking_ids
            ]))
        else:
            reasons = F.array().cast("array<string>")
        flagged = flagged.withColumn("_quarantine_reasons", reasons)

        quarantine = (
            flagged.filter(F.size("_quarantine_reasons") > 0)
            .withColumn("_quarantined_at", F.lit(evaluated_at).cast("timestamp"))
            .withColumn("_quarantine_layer", F.lit(self.layer))
            .withColumn("_batch_id", F.lit(self.batch_id))
        )
        clean = flagged.filter(F.size("_quarantine_reasons") == 0).drop("_quarantine_reasons")

        if drop_helper_columns:
            flag_cols = [_flag_col(r.rule_id) for r in self.rules]
            # WARN flags are useful downstream, so keep them as a compact array
            warn_ids = [r.rule_id for r in self.rules if r.severity != BLOCKING]
            if warn_ids:
                clean = clean.withColumn("_dq_warnings", F.array_compact(F.array(*[
                    F.when(F.col(_flag_col(rid)), F.lit(rid)) for rid in warn_ids
                ])))
            clean = clean.drop(*flag_cols)

        results = self.spark.createDataFrame(summary, schema=QUALITY_RESULT_SCHEMA)

        blocking_breaches = [s for s in summary if not s["passed"] and s["severity"] == BLOCKING]
        passed = not blocking_breaches
        for s in summary:
            if not s["passed"]:
                LOG.warning(
                    "DQ %s %s: %s/%s rows failed (%.3f%% > %.3f%%)",
                    s["severity"], s["rule_id"], s["rows_failed"], s["rows_evaluated"],
                    s["failure_rate"] * 100, (s["threshold"] or 0) * 100,
                )
        LOG.info(
            "DQ %s.%s batch=%s: %d rules, %d rows, %d quarantined",
            self.layer, self.table_name, self.batch_id, len(self.rules), total,
            total - clean.count() if total else 0,
        )
        return QualityOutcome(results=results, clean=clean, quarantine=quarantine,
                              summary=summary, passed=passed)

    # ------------------------------------------------------------------
    def enforce(self, outcome: QualityOutcome) -> None:
        """Abort the batch when configured to fail closed."""
        if outcome.passed:
            return
        if not self.cfg.get("quality.fail_on_blocking_breach", True):
            LOG.error("BLOCKING breaches present but fail_on_blocking_breach=false; continuing")
            return
        detail = "; ".join(
            f"{b['rule_id']} {b['failure_rate']:.4f} > {b['threshold']}"
            for b in outcome.breaches if b["severity"] == BLOCKING
        )
        raise DataQualityError(
            f"{self.table_name} failed data quality gate for batch {self.batch_id}: {detail}"
        )

    def _missing_helpers(self, df: DataFrame) -> set:
        needed = {c for rule in self.rules for c in rule.requires}
        return needed - set(df.columns)


def _flag_col(rule_id: str) -> str:
    return "_dqf_" + rule_id.replace(".", "__")


# ---------------------------------------------------------------------------
# dataset-level signals
# ---------------------------------------------------------------------------
def statistical_outliers(
    df: DataFrame,
    metrics: Sequence[str],
    partition_cols: Sequence[str] = ("asset_id", "sensor_id"),
    z_threshold: float = 6.0,
) -> DataFrame:
    """Flag readings far from an asset's *own* normal, using a robust z-score.

    A fixed range check ("temperature must be < 120 C") cannot tell that a
    chiller running at 30 C is broken while a boiler at 30 C is merely cold.
    The modified z-score ``0.6745 * (x - median) / MAD`` is used instead of a
    mean/stddev z-score because a handful of extreme stuck-sensor values would
    otherwise inflate the standard deviation and mask themselves.

    Returns the input with one ``<metric>_zscore`` and one ``<metric>_is_outlier``
    column per metric.
    """
    w = Window.partitionBy(*[F.col(c) for c in partition_cols])
    out = df
    for metric in metrics:
        median = F.percentile_approx(F.col(metric), 0.5).over(w)
        out = out.withColumn(f"_{metric}_median", median)
        # MAD needs a second pass over the deviations.
        out = out.withColumn(f"_{metric}_absdev", F.abs(F.col(metric) - F.col(f"_{metric}_median")))
        out = out.withColumn(f"_{metric}_mad", F.percentile_approx(F.col(f"_{metric}_absdev"), 0.5).over(w))
        out = out.withColumn(
            f"{metric}_zscore",
            F.when(F.col(f"_{metric}_mad") > 0,
                   F.lit(0.6745) * (F.col(metric) - F.col(f"_{metric}_median")) / F.col(f"_{metric}_mad"))
            .otherwise(F.lit(0.0)),
        )
        out = out.withColumn(
            f"{metric}_is_outlier",
            F.coalesce(F.abs(F.col(f"{metric}_zscore")) > F.lit(z_threshold), F.lit(False)),
        )
        out = out.drop(f"_{metric}_median", f"_{metric}_absdev", f"_{metric}_mad")
    return out


def freshness_report(df: DataFrame, cfg: Config, batch_id: str, timestamp_col: str = "timestamp") -> DataFrame:
    """Per-asset watermark lag - the "is this device still alive" signal.

    Missing records are invisible to row-level rules: you cannot validate a row
    that never arrived. Freshness is how absence is detected, and it feeds both
    the quality report and the Q4 SQL query.
    """
    max_lag_minutes = float(cfg.get("quality.thresholds.freshness_max_minutes", 60))
    return (
        df.groupBy("site_id", "building_id", "asset_id")
        .agg(
            F.max(timestamp_col).alias("last_seen_at"),
            F.count(F.lit(1)).alias("readings"),
        )
        .withColumn("lag_minutes",
                    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_seen_at")) / 60.0)
        .withColumn("is_stale", F.col("lag_minutes") > F.lit(max_lag_minutes))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("evaluated_at", F.current_timestamp())
    )


def completeness_report(df: DataFrame, cfg: Config, batch_id: str,
                        expected_interval_minutes: Optional[float] = None) -> DataFrame:
    """Estimate *missing records* per asset/day.

    ``expected = window_length / sampling_interval``; anything materially below
    that means readings were lost in transit, which no per-row rule can see.
    """
    interval = float(expected_interval_minutes or cfg.get("generator.telemetry_interval_minutes", 5))
    per_day = (
        df.withColumn("event_date", F.to_date("timestamp"))
        .groupBy("asset_id", "sensor_id", "event_date")
        .agg(F.count(F.lit(1)).alias("received"))
        .withColumn("expected", F.lit(int(24 * 60 / interval)))
        .withColumn("missing", F.greatest(F.col("expected") - F.col("received"), F.lit(0)))
        .withColumn("completeness_pct",
                    F.least(F.col("received") / F.col("expected"), F.lit(1.0)) * 100.0)
        .withColumn("batch_id", F.lit(batch_id))
    )
    return per_day
