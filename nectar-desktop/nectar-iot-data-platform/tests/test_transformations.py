"""Tests for the gold-layer transformations.

These target the two calculations that are easy to get subtly wrong and
impossible to spot afterwards: energy integration and utilisation weighting.
Each test uses numbers whose correct answer can be worked out by hand.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from nectar.pipeline.gold import (
    asset_level_readings,
    curated_daily_asset_utilization,
    curated_fault_statistics,
)

BASE = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

TELEMETRY_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
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
])


def _telemetry(spark, rows):
    prepared = []
    for r in rows:
        base = dict(site_id="SITE-A", building_id="BLD-1", asset_id="ASSET-1", sensor_id="S1",
                    temperature=20.0, humidity=40.0, pressure=100.0, vibration=1.0,
                    power_consumption=10.0, operating_mode="RUNNING")
        base.update(r)
        prepared.append(tuple(base[f.name] for f in TELEMETRY_SCHEMA.fields))
    return spark.createDataFrame(prepared, schema=TELEMETRY_SCHEMA)


# ---------------------------------------------------------------------------
# energy integration
# ---------------------------------------------------------------------------
def test_energy_is_duration_weighted_not_a_plain_average(spark, base_config):
    """12 readings of 10 kW at 5-minute spacing = 1 hour at 10 kW = 10 kWh."""
    rows = [{"timestamp": BASE + timedelta(minutes=5 * i), "power_consumption": 10.0}
            for i in range(13)]
    readings = asset_level_readings(_telemetry(spark, rows), base_config)
    total = readings.agg(F.sum("energy_kwh")).collect()[0][0]
    # 12 full intervals of 1/12 h at 10 kW, plus the last reading assumed to
    # hold for one more interval: 13 x (10 x 1/12) = 10.833 kWh
    assert total == pytest.approx(10.8333, rel=1e-3)


def test_a_gap_is_capped_and_does_not_bill_the_whole_outage(spark, base_config):
    """A 6-hour silence must not be charged at the last known load."""
    rows = [
        {"timestamp": BASE, "power_consumption": 100.0},
        {"timestamp": BASE + timedelta(hours=6), "power_consumption": 100.0},
    ]
    readings = asset_level_readings(_telemetry(spark, rows), base_config)
    total = readings.agg(F.sum("energy_kwh")).collect()[0][0]
    # First reading: gap is 6 h, capped at 2 x 5 min = 1/6 h  -> 16.67 kWh
    # Last reading:  no successor, assumed to hold one interval (1/12 h) -> 8.33
    assert total == pytest.approx(25.0, rel=1e-3)
    assert total < 100 * 6          # the naive answer would be 600 kWh


def test_two_sensors_on_one_asset_do_not_double_count_power(spark, base_config):
    """power_consumption is asset-level but arrives once per sensor."""
    rows = []
    for i in range(12):
        ts = BASE + timedelta(minutes=5 * i)
        rows.append({"timestamp": ts, "sensor_id": "S1", "power_consumption": 10.0})
        rows.append({"timestamp": ts, "sensor_id": "S2", "power_consumption": 10.0})
    readings = asset_level_readings(_telemetry(spark, rows), base_config)

    assert readings.count() == 12                      # collapsed to asset grain
    total = readings.agg(F.sum("energy_kwh")).collect()[0][0]
    assert total == pytest.approx(10.0, rel=1e-3)      # not 20


# ---------------------------------------------------------------------------
# utilisation
# ---------------------------------------------------------------------------
def test_utilisation_is_share_of_observed_time_not_of_the_calendar_day(spark, base_config, tmp_lakehouse):
    """An asset observed for 2 hours, running for 1, is 50% utilised - not 4%."""
    rows = []
    for i in range(12):     # 1 hour RUNNING
        rows.append({"timestamp": BASE + timedelta(minutes=5 * i), "operating_mode": "RUNNING"})
    for i in range(12, 24):  # 1 hour IDLE
        rows.append({"timestamp": BASE + timedelta(minutes=5 * i), "operating_mode": "IDLE"})

    readings = asset_level_readings(_telemetry(spark, rows), base_config)
    util = curated_daily_asset_utilization(spark, base_config, readings).collect()[0]

    assert util["utilization_pct"] == pytest.approx(50.0, abs=1.0)
    # Coverage tells the consumer the day was only ~2 hours observed.
    assert util["data_coverage_pct"] < 15


def test_downtime_modes_reduce_availability(spark, base_config, tmp_lakehouse):
    rows = [{"timestamp": BASE + timedelta(minutes=5 * i),
             "operating_mode": "RUNNING" if i < 6 else "FAULT"} for i in range(12)]
    readings = asset_level_readings(_telemetry(spark, rows), base_config)
    util = curated_daily_asset_utilization(spark, base_config, readings).collect()[0]
    assert util["availability_pct"] == pytest.approx(50.0, abs=1.0)


# ---------------------------------------------------------------------------
# fault statistics
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("asset_id", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("building_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("severity", StringType(), True),
])

ASSET_SCHEMA = StructType([
    StructField("asset_id", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("rated_power_kw", DoubleType(), True),
])


def test_mtbf_and_health_score(spark, base_config, tmp_lakehouse):
    events = spark.createDataFrame([
        ("E1", BASE, "ASSET-1", "SITE-A", "BLD-1", "Fault", "High"),
        ("E2", BASE + timedelta(hours=10), "ASSET-1", "SITE-A", "BLD-1", "Fault", "High"),
        ("E3", BASE + timedelta(hours=20), "ASSET-1", "SITE-A", "BLD-1", "Fault", "Medium"),
        ("E4", BASE + timedelta(hours=21), "ASSET-1", "SITE-A", "BLD-1", "Alarm", "Low"),
        ("E5", BASE, "ASSET-2", "SITE-A", "BLD-1", "Warning", "Low"),
    ], schema=EVENT_SCHEMA).withColumn("event_date", F.to_date("timestamp"))

    assets = spark.createDataFrame([("ASSET-1", "Chiller", "Trane", 320.0),
                                    ("ASSET-2", "Pump", "Grundfos", 22.0)], schema=ASSET_SCHEMA)

    stats = {r["asset_id"]: r for r in
             curated_fault_statistics(spark, base_config, events, assets).collect()}

    a1 = stats["ASSET-1"]
    assert a1["faults"] == 3 and a1["alarms"] == 1
    # 20 hours between first and last fault, 3 faults -> 2 intervals -> 10 h
    assert a1["mtbf_hours"] == pytest.approx(10.0)
    # 100 - (3 faults x 5) - (1 alarm x 1) = 84
    assert a1["health_score"] == pytest.approx(84.0)
    assert a1["risk_band"] == "Low"

    a2 = stats["ASSET-2"]
    assert a2["faults"] == 0
    assert a2["mtbf_hours"] is None            # a single/zero fault cannot yield an MTBF


def test_single_fault_gives_no_mtbf(spark, base_config, tmp_lakehouse):
    events = spark.createDataFrame([
        ("E1", BASE, "ASSET-9", "SITE-A", "BLD-1", "Fault", "High"),
    ], schema=EVENT_SCHEMA).withColumn("event_date", F.to_date("timestamp"))
    assets = spark.createDataFrame([("ASSET-9", "Boiler", "Carrier", 180.0)], schema=ASSET_SCHEMA)
    row = curated_fault_statistics(spark, base_config, events, assets).collect()[0]
    assert row["fault_count"] == 1
    assert row["mtbf_hours"] is None
