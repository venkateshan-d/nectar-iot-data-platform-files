-- ============================================================================
-- Nectar IoT Platform - lakehouse DDL (Spark SQL / Delta Lake)
-- ============================================================================
-- This is the physical model the pipeline writes. It is the authoritative copy;
-- the PostgreSQL file next to it mirrors the same star schema for a classic
-- warehouse serving layer.
--
-- Conventions
--   *  bronze_  raw, string-typed, append-only, partitioned by landing date
--   *  silver_  typed + validated, partitioned by event_date
--   *  gold_    dimensional model, partitioned by event_date where time-variant
--   *  columns prefixed `_` are platform metadata, never business data
--
-- PARTITIONING STRATEGY
--   Facts partition on `event_date` (not ingest date, not timestamp):
--     - every analytical predicate is a date range, so date is the pruning key;
--     - one day of the full estate is ~250 MB at present volumes, which lands
--       inside the 128 MB - 1 GB partition sweet spot after compaction;
--     - hour-level partitioning was rejected: 24x the partition count for the
--       same data volume produces small files and a slow metadata scan.
--   Bronze partitions on `ingest_date` instead, because replay and retention
--   are expressed in terms of when data landed.
--   Dimensions are unpartitioned - they are small and always broadcast.
--
-- INDEXING / DATA SKIPPING STRATEGY
--   Delta has no secondary indexes; the equivalents are:
--     1. partition pruning on event_date (above);
--     2. Z-ORDER on the highest-cardinality filter column (`asset_id`), which
--        co-locates an asset's rows into the same files - a single-asset,
--        single-week dashboard query then touches a handful of files;
--     3. min/max statistics on the first 32 columns, so the ordering of the
--        column list matters: timestamps and ids are declared early;
--     4. bloom filters on `sensor_id` / `event_id` for point lookups.
--   On PostgreSQL the same access patterns are served by BRIN on the timestamp
--   and B-tree on (asset_id, timestamp) - see the Postgres file.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS nectar_bronze;
CREATE DATABASE IF NOT EXISTS nectar_silver;
CREATE DATABASE IF NOT EXISTS nectar_gold;
CREATE DATABASE IF NOT EXISTS nectar_quality;

-- ---------------------------------------------------------------------------
-- BRONZE
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nectar_bronze.telemetry (
    timestamp           STRING    COMMENT 'raw event timestamp, unparsed',
    site_id             STRING,
    building_id         STRING,
    asset_id            STRING,
    sensor_id           STRING,
    temperature         STRING,
    humidity            STRING,
    pressure            STRING,
    vibration           STRING,
    power_consumption   STRING,
    operating_mode      STRING,
    _ingested_at        TIMESTAMP NOT NULL,
    _ingest_id          STRING    NOT NULL COMMENT 'batch identity; makes retries idempotent',
    _source_file        STRING,
    _source_system      STRING    NOT NULL,
    _payload_hash       STRING    COMMENT 'sha256 of business columns',
    ingest_date         DATE      NOT NULL
)
USING DELTA
PARTITIONED BY (ingest_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true',
    'delta.logRetentionDuration'        = 'interval 30 days',
    'delta.deletedFileRetentionDuration'= 'interval 7 days'
);

CREATE TABLE IF NOT EXISTS nectar_bronze.events (
    event_id        STRING,
    timestamp       STRING,
    asset_id        STRING,
    event_type      STRING,
    severity        STRING,
    message         STRING,
    _ingested_at    TIMESTAMP NOT NULL,
    _ingest_id      STRING    NOT NULL,
    _source_file    STRING,
    _source_system  STRING    NOT NULL,
    _payload_hash   STRING,
    ingest_date     DATE      NOT NULL
)
USING DELTA PARTITIONED BY (ingest_date);

-- ---------------------------------------------------------------------------
-- SILVER
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nectar_silver.telemetry (
    timestamp           TIMESTAMP NOT NULL,
    asset_id            STRING    NOT NULL,
    sensor_id           STRING,
    site_id             STRING    NOT NULL,
    building_id         STRING    NOT NULL,
    temperature         DOUBLE,
    humidity            DOUBLE,
    pressure            DOUBLE,
    vibration           DOUBLE,
    power_consumption   DOUBLE,
    operating_mode      STRING,
    event_hour          TIMESTAMP,
    _ingested_at        TIMESTAMP,
    _processed_at       TIMESTAMP,
    _ingest_id          STRING,
    _record_hash        STRING COMMENT 'fingerprint of the grain (asset, sensor, ts)',
    _is_late            BOOLEAN,
    _lateness_seconds   BIGINT,
    _dq_warnings        ARRAY<STRING> COMMENT 'non-blocking rule ids this row tripped',
    event_date          DATE      NOT NULL
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.dataSkippingNumIndexedCols' = '12',
    'delta.bloomFilter.sensor_id.enabled' = 'true'
);
-- Applied by the nightly maintenance DAG:
-- OPTIMIZE nectar_silver.telemetry ZORDER BY (asset_id, timestamp);

CREATE TABLE IF NOT EXISTS nectar_silver.events (
    event_id        STRING NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    asset_id        STRING NOT NULL,
    site_id         STRING,
    building_id     STRING,
    event_type      STRING NOT NULL,
    severity        STRING NOT NULL,
    message         STRING,
    event_hour      TIMESTAMP,
    _is_late        BOOLEAN,
    _dq_warnings    ARRAY<STRING>,
    event_date      DATE NOT NULL
)
USING DELTA PARTITIONED BY (event_date);

-- ---------------------------------------------------------------------------
-- GOLD - dimensions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nectar_gold.dim_date (
    date_key        INT    NOT NULL COMMENT 'yyyyMMdd surrogate key',
    full_date       DATE   NOT NULL,
    year            INT, quarter INT, month INT, month_name STRING,
    week_of_year    INT, day_of_month INT, day_of_week INT, day_name STRING,
    is_weekend      BOOLEAN,
    fiscal_year     INT, fiscal_quarter INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS nectar_gold.dim_time (
    time_key          INT NOT NULL COMMENT 'minute of day, 0..1439',
    hour_of_day       INT, minute_of_hour INT, hh_mm STRING,
    day_part          STRING, is_business_hours BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS nectar_gold.dim_site (
    site_key    BIGINT NOT NULL,
    site_id     STRING NOT NULL,
    site_name   STRING, city STRING, country STRING, timezone STRING,
    customer_id STRING,
    is_current  BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS nectar_gold.dim_building (
    building_key    BIGINT NOT NULL,
    building_id     STRING NOT NULL,
    building_name   STRING,
    site_key        BIGINT, site_id STRING, site_name STRING,
    floor_area_sqm  DOUBLE, building_type STRING, size_band STRING,
    is_current      BOOLEAN
) USING DELTA;

-- SCD Type 2. Assets are relocated, re-rated and re-parented; overwriting those
-- attributes would silently restate historical per-building energy.
CREATE TABLE IF NOT EXISTS nectar_gold.dim_asset (
    asset_key        BIGINT    NOT NULL COMMENT 'surrogate: hash(asset_id, attribute hash)',
    asset_id         STRING    NOT NULL COMMENT 'natural key',
    asset_name       STRING,
    asset_type       STRING,
    manufacturer     STRING,
    model            STRING,
    installation_date DATE,
    asset_age_years  DOUBLE,
    rated_power_kw   DOUBLE,
    site_id          STRING,
    building_id      STRING,
    parent_asset_id  STRING,
    is_orphan        BOOLEAN,
    is_root          BOOLEAN,
    _scd_hash        STRING    COMMENT 'sha256 of tracked attributes; drives change detection',
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP NOT NULL COMMENT '9999-12-31 for the live row',
    is_current       BOOLEAN   NOT NULL
) USING DELTA;

-- Hierarchy: transitive closure + a denormalised per-asset view.
CREATE TABLE IF NOT EXISTS nectar_gold.asset_closure (
    ancestor_id     STRING NOT NULL,
    descendant_id   STRING NOT NULL,
    depth           INT    NOT NULL COMMENT '0 = self pair',
    path            STRING COMMENT 'A > B > C'
) USING DELTA;
-- OPTIMIZE nectar_gold.asset_closure ZORDER BY (ancestor_id, descendant_id);

CREATE TABLE IF NOT EXISTS nectar_gold.dim_asset_hierarchy (
    asset_id            STRING NOT NULL,
    asset_name          STRING, asset_type STRING,
    site_id             STRING, building_id STRING,
    parent_asset_id     STRING,
    root_asset_id       STRING,
    level               INT,
    hierarchy_path      STRING,
    child_count         BIGINT,
    descendant_count    BIGINT,
    is_leaf             BOOLEAN, is_root BOOLEAN, is_orphan BOOLEAN,
    is_disconnected     BOOLEAN,
    connectivity_status STRING COMMENT 'CONNECTED | STANDALONE | ORPHANED | UNASSIGNED'
) USING DELTA;

-- ---------------------------------------------------------------------------
-- GOLD - facts
-- ---------------------------------------------------------------------------
-- Atomic grain. Kept because ML feature engineering needs the raw series.
CREATE TABLE IF NOT EXISTS nectar_gold.fact_telemetry (
    asset_nk_key      BIGINT, site_key BIGINT, building_key BIGINT,
    date_key          INT, time_key INT,
    asset_id          STRING NOT NULL,
    sensor_id         STRING,
    site_id           STRING, building_id STRING,
    timestamp         TIMESTAMP NOT NULL,
    temperature       DOUBLE, humidity DOUBLE, pressure DOUBLE,
    vibration         DOUBLE, power_consumption DOUBLE,
    operating_mode    STRING,
    is_late_arrival   BOOLEAN,
    event_hour        TIMESTAMP,
    event_date        DATE NOT NULL
)
USING DELTA PARTITIONED BY (event_date)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
-- OPTIMIZE nectar_gold.fact_telemetry ZORDER BY (asset_id, timestamp);

-- Energy fact at asset x hour: the grain most energy questions want, ~1/12th
-- the rows of the atomic fact at a 5-minute sampling interval.
CREATE TABLE IF NOT EXISTS nectar_gold.fact_energy_hourly (
    asset_nk_key    BIGINT, site_key BIGINT, building_key BIGINT, date_key INT,
    asset_id        STRING NOT NULL,
    site_id         STRING, building_id STRING,
    event_hour      TIMESTAMP NOT NULL,
    hour_of_day     INT,
    energy_kwh      DOUBLE COMMENT 'duration-weighted integral of power_consumption',
    avg_power_kw    DOUBLE, peak_power_kw DOUBLE, min_power_kw DOUBLE,
    covered_hours   DOUBLE COMMENT 'observed time in the hour; <1 means readings were lost',
    reading_count   BIGINT,
    data_coverage_ratio DOUBLE,
    event_date      DATE NOT NULL
) USING DELTA PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS nectar_gold.fact_event (
    asset_nk_key    BIGINT, site_key BIGINT, building_key BIGINT,
    date_key        INT, time_key INT,
    event_id        STRING NOT NULL,
    asset_id        STRING NOT NULL,
    site_id         STRING, building_id STRING,
    timestamp       TIMESTAMP NOT NULL,
    event_type      STRING, severity STRING, message STRING,
    is_fault        BOOLEAN, severity_rank INT,
    is_late_arrival BOOLEAN,
    event_hour      TIMESTAMP,
    event_date      DATE NOT NULL
) USING DELTA PARTITIONED BY (event_date);

-- ---------------------------------------------------------------------------
-- GOLD - curated marts and roll-ups (created by the pipeline; shapes documented)
-- ---------------------------------------------------------------------------
--   curated_hourly_energy            asset x hour  + day-over-day + rolling 24h
--   curated_daily_asset_utilization  asset x day   utilisation / availability
--   curated_daily_environment        asset x day   avg/min/max/p95 conditions
--   curated_fault_statistics         asset         counts, MTBF, health score
--   agg_asset_daily                  asset x day   the union of the above
--   agg_building_daily               building x day + energy intensity per sqm
--   agg_site_daily                   site x day    + trailing-baseline z-score

-- ---------------------------------------------------------------------------
-- QUALITY
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nectar_quality.dq_results (
    batch_id        STRING    NOT NULL,
    evaluated_at    TIMESTAMP NOT NULL,
    layer           STRING    NOT NULL,
    table_name      STRING    NOT NULL,
    rule_id         STRING    NOT NULL,
    dimension       STRING    NOT NULL COMMENT 'completeness|uniqueness|validity|consistency|timeliness|accuracy',
    column_name     STRING,
    severity        STRING    NOT NULL COMMENT 'BLOCKING|WARN|INFO',
    rows_evaluated  BIGINT    NOT NULL,
    rows_failed     BIGINT    NOT NULL,
    failure_rate    DOUBLE    NOT NULL,
    threshold       DOUBLE,
    passed          BOOLEAN   NOT NULL,
    details         MAP<STRING, STRING>
) USING DELTA PARTITIONED BY (layer, table_name);
