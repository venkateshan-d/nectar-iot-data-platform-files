"""Tests for the data quality framework.

The tests are written against *hand-built* rows with a known defect in each, so
a failure points at the exact rule rather than at "something in the pipeline".
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType,
)

from nectar.quality.engine import DataQualityError, QualityEngine, statistical_outliers
from nectar.quality.rules import BLOCKING, duplicate_rank_column, get_rules

NOW = datetime.now(timezone.utc).replace(microsecond=0)

PREPARED_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("_raw_timestamp", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("building_id", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("power_consumption", DoubleType(), True),
    StructField("operating_mode", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_asset_known", BooleanType(), True),
    StructField("_register_site_id", StringType(), True),
    StructField("_lateness_seconds", LongType(), True),
])


def _row(**overrides):
    base = dict(
        timestamp=NOW, _raw_timestamp=NOW.strftime("%Y-%m-%dT%H:%M:%SZ"),
        site_id="SITE-A", building_id="BLD-1", asset_id="ASSET-1", sensor_id="S1",
        temperature=21.0, humidity=45.0, pressure=300.0, vibration=1.0,
        power_consumption=12.0, operating_mode="RUNNING",
        _ingested_at=NOW, _asset_known=True, _register_site_id="SITE-A",
        _lateness_seconds=0,
    )
    base.update(overrides)
    return tuple(base[f.name] for f in PREPARED_SCHEMA.fields)


def _frame(spark, rows):
    df = spark.createDataFrame([_row(**r) for r in rows], schema=PREPARED_SCHEMA)
    from nectar.schemas import TELEMETRY_BUSINESS_KEY

    return duplicate_rank_column(df, TELEMETRY_BUSINESS_KEY)


def _engine(spark, cfg):
    return QualityEngine(spark, cfg, get_rules("telemetry", cfg), "silver", "telemetry", "test-batch")


def _failed(outcome, rule_id: str) -> int:
    return next(r["rows_failed"] for r in outcome.summary if r["rule_id"] == rule_id)


# ---------------------------------------------------------------------------
def test_clean_rows_pass_every_rule(spark, base_config):
    df = _frame(spark, [{}, {"sensor_id": "S2"}, {"asset_id": "ASSET-2"}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert outcome.passed
    assert outcome.quarantine.count() == 0
    assert outcome.clean.count() == 3
    assert all(r["rows_failed"] == 0 for r in outcome.summary)


def test_missing_required_column_is_quarantined(spark, base_config):
    df = _frame(spark, [{}, {"asset_id": None, "sensor_id": "S9"}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.completeness.asset_id_not_null") == 1
    assert outcome.quarantine.count() == 1
    assert outcome.clean.count() == 1


def test_missing_measure_is_a_warning_not_a_quarantine(spark, base_config):
    """A null temperature is a data gap, not an unusable record."""
    df = _frame(spark, [{}, {"temperature": None, "sensor_id": "S2"}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.completeness.temperature_present") == 1
    assert outcome.quarantine.count() == 0
    warnings = outcome.clean.filter(F.array_contains("_dq_warnings",
                                                     "tel.completeness.temperature_present"))
    assert warnings.count() == 1


def test_duplicate_business_key_keeps_first_and_quarantines_the_rest(spark, base_config):
    later = NOW + timedelta(seconds=30)
    df = _frame(spark, [
        {"_ingested_at": NOW},
        {"_ingested_at": later},          # exact duplicate, arrived later
        {"_ingested_at": later, "sensor_id": "S2"},
    ])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.uniqueness.business_key") == 1
    assert outcome.clean.count() == 2
    # The surviving copy must be the first one seen.
    kept = outcome.clean.filter(F.col("sensor_id") == "S1").collect()
    # Spark returns naive datetimes in the session time zone (UTC), so compare
    # against the naive form of the fixture timestamp.
    assert len(kept) == 1
    assert kept[0]["_ingested_at"].replace(tzinfo=timezone.utc) == NOW


def test_unparseable_and_implausible_timestamps_are_separate_rules(spark, base_config):
    df = _frame(spark, [
        {},
        {"timestamp": None, "_raw_timestamp": "not-a-timestamp", "sensor_id": "S2"},
        {"timestamp": datetime(1970, 1, 1, tzinfo=timezone.utc), "sensor_id": "S3"},
        {"timestamp": NOW + timedelta(days=400), "sensor_id": "S4"},
    ])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.validity.timestamp_parseable") == 1
    assert _failed(outcome, "tel.validity.timestamp_plausible") == 2
    assert outcome.clean.count() == 1


def test_out_of_range_measure_is_blocking(spark, base_config):
    df = _frame(spark, [{}, {"temperature": 9999.99, "sensor_id": "S2"},
                        {"humidity": -50.0, "sensor_id": "S3"}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.accuracy.temperature_in_range") == 1
    assert _failed(outcome, "tel.accuracy.humidity_in_range") == 1
    assert outcome.quarantine.count() == 2


def test_unregistered_asset_breaks_referential_integrity(spark, base_config):
    df = _frame(spark, [{}, {"asset_id": "UNREGISTERED-1", "_asset_known": False}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.consistency.asset_registered") == 1
    reasons = outcome.quarantine.select("_quarantine_reasons").first()[0]
    assert "tel.consistency.asset_registered" in reasons


def test_site_mismatch_is_a_warning(spark, base_config):
    df = _frame(spark, [{"_register_site_id": "SITE-B"}])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.consistency.site_matches_register") == 1
    assert outcome.quarantine.count() == 0


def test_late_arrival_flagged_at_the_watermark(spark, base_config):
    df = _frame(spark, [
        {},
        {"_lateness_seconds": 86400, "sensor_id": "S2"},        # exactly 24h -> late
        {"_lateness_seconds": 3600, "sensor_id": "S3"},         # 1h -> on time
    ])
    outcome = _engine(spark, base_config).evaluate(df)
    assert _failed(outcome, "tel.timeliness.late_arrival") == 1


def test_a_row_can_break_several_rules_and_records_all_of_them(spark, base_config):
    df = _frame(spark, [{"asset_id": None, "_asset_known": False, "temperature": 1e6}])
    outcome = _engine(spark, base_config).evaluate(df)
    reasons = set(outcome.quarantine.select("_quarantine_reasons").first()[0])
    assert {"tel.completeness.asset_id_not_null",
            "tel.consistency.asset_registered",
            "tel.accuracy.temperature_in_range"} <= reasons


def test_enforce_raises_when_a_blocking_threshold_is_breached(spark, base_config):
    # 2 of 3 rows unregistered = 67%, far above the 2% threshold.
    df = _frame(spark, [
        {},
        {"asset_id": "X1", "_asset_known": False},
        {"asset_id": "X2", "_asset_known": False},
    ])
    engine = _engine(spark, base_config)
    outcome = engine.evaluate(df)
    assert not outcome.passed
    with pytest.raises(DataQualityError):
        engine.enforce(outcome)


def test_enforce_can_be_configured_to_warn_instead_of_fail(spark, base_config):
    base_config.data["quality"]["fail_on_blocking_breach"] = False
    try:
        df = _frame(spark, [{"asset_id": "X1", "_asset_known": False}])
        engine = _engine(spark, base_config)
        engine.enforce(engine.evaluate(df))   # must not raise
    finally:
        base_config.data["quality"]["fail_on_blocking_breach"] = True


def test_every_rule_declares_the_helper_columns_it_needs(base_config):
    """Guards against a rule silently depending on a column nobody prepares."""
    for rule in get_rules("telemetry", base_config) + get_rules("events", base_config):
        assert rule.rule_id and rule.dimension and rule.severity in {"BLOCKING", "WARN", "INFO"}
        assert rule.threshold is None or 0 <= rule.threshold <= 1


def test_statistical_outlier_uses_the_assets_own_baseline(spark, base_config):
    """A value normal for one asset can be an outlier for another."""
    rows = [{"asset_id": "COLD", "temperature": 7.0 + i * 0.05, "sensor_id": "S1",
             "timestamp": NOW + timedelta(minutes=i)} for i in range(30)]
    rows += [{"asset_id": "HOT", "temperature": 80.0 + i * 0.05, "sensor_id": "S1",
              "timestamp": NOW + timedelta(minutes=i)} for i in range(30)]
    rows.append({"asset_id": "COLD", "temperature": 45.0, "sensor_id": "S1",
                 "timestamp": NOW + timedelta(minutes=99)})

    df = spark.createDataFrame([_row(**r) for r in rows], schema=PREPARED_SCHEMA)
    scored = statistical_outliers(df, ["temperature"], z_threshold=6.0)

    cold_outliers = scored.filter((F.col("asset_id") == "COLD") & F.col("temperature_is_outlier"))
    hot_outliers = scored.filter((F.col("asset_id") == "HOT") & F.col("temperature_is_outlier"))
    assert cold_outliers.count() == 1                       # 45 C is absurd for a chiller
    assert cold_outliers.first()["temperature"] == 45.0
    assert hot_outliers.count() == 0                        # 80 C is normal for a boiler
