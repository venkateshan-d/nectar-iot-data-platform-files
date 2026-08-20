"""Declarative data quality rules.

A rule is a *named, row-level predicate* plus metadata. Keeping rules as data
(rather than as inline ``if`` statements scattered through the transforms) buys
three things:

1. the same rule set can be applied in batch and in streaming;
2. every rule produces an auditable row in ``quality.dq_results`` with its own
   threshold and severity, so "how healthy is site X's data" is a SQL question;
3. new rules are added by appending to a list, not by editing pipeline code.

Dimensions follow the standard DAMA breakdown - completeness, uniqueness,
validity, consistency, timeliness, accuracy - because that is what the data
quality report is grouped by.

Severity semantics
------------------
``BLOCKING`` the row is quarantined and, if the failure rate breaches the
            configured threshold, the batch is failed.
``WARN``    the row is kept and flagged; the run continues but the metric is
            alerted on.
``INFO``    observability only; recorded, never alerted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ..schemas import (
    EVENT_REQUIRED_COLUMNS,
    TELEMETRY_REQUIRED_COLUMNS,
    VALID_EVENT_TYPES,
    VALID_OPERATING_MODES,
    VALID_SEVERITIES,
)

BLOCKING = "BLOCKING"
WARN = "WARN"
INFO = "INFO"

COMPLETENESS = "completeness"
UNIQUENESS = "uniqueness"
VALIDITY = "validity"
CONSISTENCY = "consistency"
TIMELINESS = "timeliness"
ACCURACY = "accuracy"


@dataclass(frozen=True)
class Rule:
    """A single quality expectation.

    ``predicate`` returns a boolean Column that is **True when the row fails**.
    Nulls are folded to False so that a rule about column A never accidentally
    fails a row because column B is null.
    """

    rule_id: str
    dimension: str
    severity: str
    description: str
    predicate: Callable[[DataFrame], Column]
    column: Optional[str] = None
    threshold: Optional[float] = None
    #: rules needing a prepared helper column (e.g. dedupe rank)
    requires: Sequence[str] = field(default_factory=tuple)

    def failure_flag(self, df: DataFrame) -> Column:
        return F.coalesce(self.predicate(df), F.lit(False))


# ---------------------------------------------------------------------------
# reusable predicate factories
# ---------------------------------------------------------------------------
def _is_null(col: str) -> Callable[[DataFrame], Column]:
    return lambda df: F.col(col).isNull()


def _blank_or_null(col: str) -> Callable[[DataFrame], Column]:
    return lambda df: F.col(col).isNull() | (F.trim(F.col(col)) == "")


def _not_in(col: str, allowed: Sequence[str]) -> Callable[[DataFrame], Column]:
    # A null value is a *completeness* problem, not a validity one - let the
    # null rule own it so a single bad row is not counted twice.
    return lambda df: F.col(col).isNotNull() & (~F.col(col).isin(list(allowed)))


def _out_of_range(col: str, low: float, high: float) -> Callable[[DataFrame], Column]:
    return lambda df: F.col(col).isNotNull() & (~F.col(col).between(F.lit(low), F.lit(high)))


def _timestamp_implausible(col: str = "timestamp") -> Callable[[DataFrame], Column]:
    """Parsed but nonsensical: epoch-zero sentinels or dates in the future.

    Devices with a dead RTC report 1970-01-01; a device whose clock has run
    forward reports next century. Both parse cleanly, so a cast check alone
    would let them through into the aggregates.
    """
    return lambda df: F.col(col).isNotNull() & (
        (F.col(col) < F.lit("2000-01-01").cast("timestamp"))
        | (F.col(col) > F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
    )


def duplicate_rank_column(df: DataFrame, keys: Sequence[str], order_by: str = "_ingested_at") -> DataFrame:
    """Add ``_dup_rank``: 1 for the record we keep, >1 for redundant copies.

    Ordering by ingestion time keeps the *first* copy seen, which matches
    at-least-once gateway delivery where the retry carries identical values.
    """
    window = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col(order_by).asc_nulls_last())
    return df.withColumn("_dup_rank", F.row_number().over(window))


# ---------------------------------------------------------------------------
# rule sets
# ---------------------------------------------------------------------------
def telemetry_rules(cfg) -> List[Rule]:
    ranges = cfg.get("quality.ranges", {}) or {}
    thresholds = cfg.get("quality.thresholds", {}) or {}
    watermark_hours = float(cfg.get("quality.late_arrival_watermark_hours", 24))

    rules: List[Rule] = []

    # -- completeness ------------------------------------------------------
    for col in TELEMETRY_REQUIRED_COLUMNS:
        rules.append(Rule(
            rule_id=f"tel.completeness.{col}_not_null",
            dimension=COMPLETENESS,
            severity=BLOCKING,
            description=f"{col} is mandatory - a reading we cannot attribute is unusable",
            predicate=_blank_or_null(col) if col not in ("timestamp",) else _is_null(col),
            column=col,
            threshold=float(thresholds.get("null_rate_max", 0.05)),
        ))

    # Measures may legitimately be absent (not every sensor reports every
    # metric), so a null measure is a WARN, not a quarantine.
    for col in ["temperature", "humidity", "pressure", "vibration", "power_consumption", "operating_mode"]:
        rules.append(Rule(
            rule_id=f"tel.completeness.{col}_present",
            dimension=COMPLETENESS,
            severity=WARN,
            description=f"{col} missing on a reading",
            predicate=_is_null(col),
            column=col,
            threshold=float(thresholds.get("null_rate_max", 0.05)),
        ))

    # -- uniqueness --------------------------------------------------------
    rules.append(Rule(
        rule_id="tel.uniqueness.business_key",
        dimension=UNIQUENESS,
        severity=BLOCKING,
        description="(asset_id, sensor_id, timestamp) must be unique - redundant "
                    "gateway retries would otherwise double-count energy",
        predicate=lambda df: F.col("_dup_rank") > 1,
        column="asset_id,sensor_id,timestamp",
        threshold=float(thresholds.get("duplicate_rate_max", 0.05)),
        requires=("_dup_rank",),
    ))

    # -- validity ----------------------------------------------------------
    rules.append(Rule(
        rule_id="tel.validity.timestamp_parseable",
        dimension=VALIDITY,
        severity=BLOCKING,
        description="timestamp string could not be cast to a timestamp",
        predicate=lambda df: F.col("_raw_timestamp").isNotNull() & F.col("timestamp").isNull(),
        column="timestamp",
        threshold=0.02,
    ))
    rules.append(Rule(
        rule_id="tel.validity.timestamp_plausible",
        dimension=VALIDITY,
        severity=BLOCKING,
        description="timestamp parses but is before 2000 or more than a day in the future",
        predicate=_timestamp_implausible(),
        column="timestamp",
        threshold=0.02,
    ))
    rules.append(Rule(
        rule_id="tel.validity.operating_mode_enum",
        dimension=VALIDITY,
        severity=WARN,
        description=f"operating_mode outside {VALID_OPERATING_MODES}",
        predicate=_not_in("operating_mode", VALID_OPERATING_MODES),
        column="operating_mode",
        threshold=0.01,
    ))

    # -- accuracy (physical plausibility) ----------------------------------
    for col, (low, high) in ranges.items():
        rules.append(Rule(
            rule_id=f"tel.accuracy.{col}_in_range",
            dimension=ACCURACY,
            severity=BLOCKING,
            description=f"{col} outside the physically possible range [{low}, {high}]",
            predicate=_out_of_range(col, float(low), float(high)),
            column=col,
            threshold=float(thresholds.get("outlier_rate_max", 0.02)),
        ))

    # -- consistency (referential integrity) -------------------------------
    rules.append(Rule(
        rule_id="tel.consistency.asset_registered",
        dimension=CONSISTENCY,
        severity=BLOCKING,
        description="asset_id is not present in the asset register",
        predicate=lambda df: ~F.coalesce(F.col("_asset_known"), F.lit(False)),
        column="asset_id",
        threshold=float(thresholds.get("unknown_asset_rate_max", 0.02)),
        requires=("_asset_known",),
    ))
    rules.append(Rule(
        rule_id="tel.consistency.site_matches_register",
        dimension=CONSISTENCY,
        severity=WARN,
        description="site_id on the reading disagrees with the asset register",
        predicate=lambda df: F.col("_register_site_id").isNotNull()
        & F.col("site_id").isNotNull()
        & (F.col("site_id") != F.col("_register_site_id")),
        column="site_id",
        threshold=0.005,
        requires=("_register_site_id",),
    ))

    # -- timeliness --------------------------------------------------------
    rules.append(Rule(
        rule_id="tel.timeliness.late_arrival",
        dimension=TIMELINESS,
        severity=WARN,
        description=f"record landed more than {watermark_hours}h after the event occurred; "
                    "downstream aggregates for that window must be recomputed",
        predicate=lambda df: F.col("_lateness_seconds") >= F.lit(watermark_hours * 3600),
        column="timestamp",
        threshold=0.05,
        requires=("_lateness_seconds",),
    ))

    return rules


def event_rules(cfg) -> List[Rule]:
    thresholds = cfg.get("quality.thresholds", {}) or {}
    rules: List[Rule] = []

    for col in EVENT_REQUIRED_COLUMNS:
        rules.append(Rule(
            rule_id=f"evt.completeness.{col}_not_null",
            dimension=COMPLETENESS,
            severity=BLOCKING,
            description=f"{col} is mandatory on an operational event",
            predicate=_blank_or_null(col) if col != "timestamp" else _is_null(col),
            column=col,
            threshold=float(thresholds.get("null_rate_max", 0.05)),
        ))

    rules.append(Rule(
        rule_id="evt.completeness.message_present",
        dimension=COMPLETENESS,
        severity=WARN,
        description="event carries no human-readable message",
        predicate=_blank_or_null("message"),
        column="message",
        threshold=0.05,
    ))
    rules.append(Rule(
        rule_id="evt.uniqueness.event_id",
        dimension=UNIQUENESS,
        severity=BLOCKING,
        description="event_id must be unique - duplicate faults would inflate MTBF",
        predicate=lambda df: F.col("_dup_rank") > 1,
        column="event_id",
        threshold=float(thresholds.get("duplicate_rate_max", 0.05)),
        requires=("_dup_rank",),
    ))
    rules.append(Rule(
        rule_id="evt.validity.timestamp_parseable",
        dimension=VALIDITY,
        severity=BLOCKING,
        description="event timestamp could not be parsed",
        predicate=lambda df: F.col("_raw_timestamp").isNotNull() & F.col("timestamp").isNull(),
        column="timestamp",
        threshold=0.02,
    ))
    rules.append(Rule(
        rule_id="evt.validity.timestamp_plausible",
        dimension=VALIDITY,
        severity=BLOCKING,
        description="event timestamp is implausible",
        predicate=_timestamp_implausible(),
        column="timestamp",
        threshold=0.02,
    ))
    rules.append(Rule(
        rule_id="evt.validity.event_type_enum",
        dimension=VALIDITY,
        severity=BLOCKING,
        description=f"event_type outside {VALID_EVENT_TYPES}",
        predicate=_not_in("event_type", VALID_EVENT_TYPES),
        column="event_type",
        threshold=0.01,
    ))
    rules.append(Rule(
        rule_id="evt.validity.severity_enum",
        dimension=VALIDITY,
        severity=BLOCKING,
        description=f"severity outside {VALID_SEVERITIES}",
        predicate=_not_in("severity", VALID_SEVERITIES),
        column="severity",
        threshold=0.01,
    ))
    rules.append(Rule(
        rule_id="evt.consistency.asset_registered",
        dimension=CONSISTENCY,
        severity=BLOCKING,
        description="event references an asset that is not in the register",
        predicate=lambda df: ~F.coalesce(F.col("_asset_known"), F.lit(False)),
        column="asset_id",
        threshold=float(thresholds.get("unknown_asset_rate_max", 0.02)),
        requires=("_asset_known",),
    ))
    rules.append(Rule(
        rule_id="evt.timeliness.late_arrival",
        dimension=TIMELINESS,
        severity=WARN,
        description="event landed after the late-arrival watermark",
        predicate=lambda df: F.col("_lateness_seconds")
        >= F.lit(float(cfg.get("quality.late_arrival_watermark_hours", 24)) * 3600),
        column="timestamp",
        threshold=0.05,
        requires=("_lateness_seconds",),
    ))
    return rules


RULE_SETS = {
    "telemetry": telemetry_rules,
    "events": event_rules,
}


def get_rules(table: str, cfg) -> List[Rule]:
    if table not in RULE_SETS:
        raise KeyError(f"No rule set registered for {table!r}; known: {sorted(RULE_SETS)}")
    return RULE_SETS[table](cfg)
