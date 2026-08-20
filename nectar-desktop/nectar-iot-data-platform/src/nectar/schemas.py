"""Explicit schemas for every source and curated table.

Never infer a schema on an ingestion path. Inference reads the data twice, is
non-deterministic across batches (a column that is all-null today becomes a
string tomorrow), and silently accepts upstream changes that should raise. The
contracts below are the single source of truth; the quality engine validates
against them and the streaming reader parses JSON with them.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Source contracts (as received from the device gateway)
# ---------------------------------------------------------------------------

#: Raw telemetry as emitted by the edge gateway. Everything is a string on the
#: wire; casting happens in bronze so that a malformed value is quarantined
#: rather than dropped by the reader.
TELEMETRY_RAW_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("building_id", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("pressure", StringType(), True),
    StructField("vibration", StringType(), True),
    StructField("power_consumption", StringType(), True),
    StructField("operating_mode", StringType(), True),
])

#: Typed telemetry after bronze casting.
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

EVENT_RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("asset_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("message", StringType(), True),
])

ASSET_METADATA_SCHEMA = StructType([
    StructField("asset_id", StringType(), True),
    StructField("asset_name", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("installation_date", DateType(), True),
    StructField("rated_power_kw", DoubleType(), True),
    StructField("site_id", StringType(), True),
    StructField("building_id", StringType(), True),
    StructField("parent_asset_id", StringType(), True),
])

SITE_SCHEMA = StructType([
    StructField("site_id", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("customer_id", StringType(), True),
])

BUILDING_SCHEMA = StructType([
    StructField("building_id", StringType(), True),
    StructField("building_name", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("floor_area_sqm", DoubleType(), True),
    StructField("building_type", StringType(), True),
])

# ---------------------------------------------------------------------------
# Technical / lineage columns added by the platform
# ---------------------------------------------------------------------------

#: Appended to every bronze record. ``_ingest_id`` is the batch identity used to
#: make retries idempotent; ``_source_file`` and ``_ingested_at`` carry lineage.
BRONZE_AUDIT_FIELDS = [
    StructField("_ingested_at", TimestampType(), False),
    StructField("_ingest_id", StringType(), False),
    StructField("_source_file", StringType(), True),
    StructField("_source_system", StringType(), False),
    StructField("_payload_hash", StringType(), True),
]

#: Appended in silver.
SILVER_AUDIT_FIELDS = [
    StructField("_processed_at", TimestampType(), False),
    StructField("_record_hash", StringType(), False),
    StructField("_is_late", BooleanType(), False),
    StructField("_lateness_seconds", LongType(), True),
]

#: Rows that fail a blocking rule land here instead of being dropped.
QUARANTINE_SCHEMA_FIELDS = [
    StructField("_quarantined_at", TimestampType(), False),
    StructField("_quarantine_reasons", ArrayType(StringType()), False),
    StructField("_quarantine_layer", StringType(), False),
    StructField("_batch_id", StringType(), False),
]

#: One row per (batch, table, rule) written by the quality engine.
QUALITY_RESULT_SCHEMA = StructType([
    StructField("batch_id", StringType(), False),
    StructField("evaluated_at", TimestampType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("dimension", StringType(), False),      # completeness/validity/...
    StructField("column_name", StringType(), True),
    StructField("severity", StringType(), False),       # BLOCKING | WARN | INFO
    StructField("rows_evaluated", LongType(), False),
    StructField("rows_failed", LongType(), False),
    StructField("failure_rate", DoubleType(), False),
    StructField("threshold", DoubleType(), True),
    StructField("passed", BooleanType(), False),
    StructField("details", MapType(StringType(), StringType()), True),
])


def with_fields(schema: StructType, extra_fields) -> StructType:
    """Return ``schema`` extended with ``extra_fields`` (no mutation)."""
    return StructType(list(schema.fields) + list(extra_fields))


BRONZE_TELEMETRY_SCHEMA = with_fields(TELEMETRY_SCHEMA, BRONZE_AUDIT_FIELDS)
BRONZE_EVENT_SCHEMA = with_fields(EVENT_SCHEMA, BRONZE_AUDIT_FIELDS)
SILVER_TELEMETRY_SCHEMA = with_fields(BRONZE_TELEMETRY_SCHEMA, SILVER_AUDIT_FIELDS)
SILVER_EVENT_SCHEMA = with_fields(BRONZE_EVENT_SCHEMA, SILVER_AUDIT_FIELDS)

#: Columns that must never be null in silver telemetry.
TELEMETRY_REQUIRED_COLUMNS = ["timestamp", "asset_id", "site_id", "building_id"]
EVENT_REQUIRED_COLUMNS = ["event_id", "timestamp", "asset_id", "event_type", "severity"]

#: Allowed enum values - anything else is a validity breach.
VALID_OPERATING_MODES = ["RUNNING", "IDLE", "STANDBY", "BOOST", "MAINTENANCE", "OFF", "FAULT"]
VALID_EVENT_TYPES = ["Alarm", "Warning", "Fault", "Info"]
VALID_SEVERITIES = ["Low", "Medium", "High"]

#: Business keys used for dedupe / MERGE.
TELEMETRY_BUSINESS_KEY = ["asset_id", "sensor_id", "timestamp"]
EVENT_BUSINESS_KEY = ["event_id"]
