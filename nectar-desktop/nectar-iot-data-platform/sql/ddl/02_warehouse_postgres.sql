-- ============================================================================
-- Nectar IoT Platform - warehouse serving DDL (PostgreSQL 15+)
-- ============================================================================
-- The same star schema as the lakehouse, expressed for a relational serving
-- layer. Use this when the consumer is an operational API or a BI tool that
-- wants sub-second point queries rather than scans.
--
-- Differences from the Delta model and why:
--   * declarative range partitioning on event_date replaces Delta partitions;
--   * real indexes replace Z-ORDER / data skipping;
--   * foreign keys are declared on the dimensions (cheap, small tables) but NOT
--     on the fact partitions - FK checks on a 100M-row bulk load cost more than
--     the referential guarantee is worth here, and the silver layer has already
--     enforced it.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS nectar;
SET search_path TO nectar;

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    month_name      TEXT,
    week_of_year    SMALLINT,
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        TEXT,
    is_weekend      BOOLEAN NOT NULL,
    fiscal_year     SMALLINT,
    fiscal_quarter  SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key         SMALLINT PRIMARY KEY,   -- minute of day 0..1439
    hour_of_day      SMALLINT NOT NULL,
    minute_of_hour   SMALLINT NOT NULL,
    hh_mm            CHAR(5)  NOT NULL,
    day_part         TEXT     NOT NULL,
    is_business_hours BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_site (
    site_key    BIGINT PRIMARY KEY,
    site_id     TEXT NOT NULL UNIQUE,
    site_name   TEXT,
    city        TEXT,
    country     CHAR(2),
    timezone    TEXT,
    customer_id TEXT,
    is_current  BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_building (
    building_key   BIGINT PRIMARY KEY,
    building_id    TEXT NOT NULL UNIQUE,
    building_name  TEXT,
    site_key       BIGINT REFERENCES dim_site (site_key),
    site_id        TEXT,
    floor_area_sqm NUMERIC(12, 2),
    building_type  TEXT,
    size_band      TEXT,
    is_current     BOOLEAN NOT NULL DEFAULT TRUE
);

-- SCD Type 2: one row per version of an asset.
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_key         BIGINT PRIMARY KEY,
    asset_id          TEXT NOT NULL,
    asset_name        TEXT,
    asset_type        TEXT,
    manufacturer      TEXT,
    model             TEXT,
    installation_date DATE,
    asset_age_years   NUMERIC(6, 2),
    rated_power_kw    NUMERIC(10, 3),
    site_id           TEXT,
    building_id       TEXT,
    parent_asset_id   TEXT,
    is_orphan         BOOLEAN NOT NULL DEFAULT FALSE,
    is_root           BOOLEAN NOT NULL DEFAULT FALSE,
    scd_hash          CHAR(64) NOT NULL,
    valid_from        TIMESTAMPTZ NOT NULL,
    valid_to          TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31',
    is_current        BOOLEAN NOT NULL
);

-- Exactly one live version per natural key - enforced, not assumed.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_asset_current
    ON dim_asset (asset_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_asset_nk        ON dim_asset (asset_id, valid_from DESC);
CREATE INDEX IF NOT EXISTS ix_dim_asset_building  ON dim_asset (building_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_asset_parent    ON dim_asset (parent_asset_id) WHERE is_current;

-- ---------------------------------------------------------------------------
-- Hierarchy (closure table)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asset_closure (
    ancestor_id   TEXT NOT NULL,
    descendant_id TEXT NOT NULL,
    depth         SMALLINT NOT NULL,
    path          TEXT,
    PRIMARY KEY (ancestor_id, descendant_id)
);
-- Subtree lookups ("everything under X") and ancestor lookups ("what feeds X")
-- are the two access patterns; one index each, both covering `depth`.
CREATE INDEX IF NOT EXISTS ix_closure_desc  ON asset_closure (descendant_id, depth);
CREATE INDEX IF NOT EXISTS ix_closure_anc   ON asset_closure (ancestor_id, depth);

CREATE TABLE IF NOT EXISTS dim_asset_hierarchy (
    asset_id            TEXT PRIMARY KEY,
    asset_name          TEXT,
    asset_type          TEXT,
    site_id             TEXT,
    building_id         TEXT,
    parent_asset_id     TEXT,
    root_asset_id       TEXT,
    level               SMALLINT,
    hierarchy_path      TEXT,
    child_count         INTEGER,
    descendant_count    INTEGER,
    is_leaf             BOOLEAN,
    is_root             BOOLEAN,
    is_orphan           BOOLEAN,
    is_disconnected     BOOLEAN,
    connectivity_status TEXT CHECK (connectivity_status IN
                          ('CONNECTED', 'STANDALONE', 'ORPHANED', 'UNASSIGNED'))
);
CREATE INDEX IF NOT EXISTS ix_hier_site ON dim_asset_hierarchy (site_id, building_id);

-- ---------------------------------------------------------------------------
-- Facts (declaratively partitioned by month; daily partitions for the atomic
-- fact if volume demands it)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_telemetry (
    asset_id          TEXT        NOT NULL,
    sensor_id         TEXT,
    site_id           TEXT        NOT NULL,
    building_id       TEXT        NOT NULL,
    ts                TIMESTAMPTZ NOT NULL,
    date_key          INTEGER     NOT NULL,
    time_key          SMALLINT    NOT NULL,
    temperature       REAL,
    humidity          REAL,
    pressure          REAL,
    vibration         REAL,
    power_consumption REAL,
    operating_mode    TEXT,
    is_late_arrival   BOOLEAN     NOT NULL DEFAULT FALSE,
    event_date        DATE        NOT NULL
) PARTITION BY RANGE (event_date);

-- One partition per month; created ahead of time by the maintenance DAG.
CREATE TABLE IF NOT EXISTS fact_telemetry_2026_08
    PARTITION OF fact_telemetry FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

-- Index strategy for fact_telemetry:
--   * BRIN on ts - the table is physically ordered by time, so a BRIN index is
--     ~1000x smaller than a B-tree and prunes ranges just as well;
--   * B-tree on (asset_id, ts DESC) - the single-asset time series query, which
--     is what every asset dashboard and every ML feature job runs;
--   * partial index on late arrivals - a small, frequently-audited subset.
CREATE INDEX IF NOT EXISTS ix_ft_ts_brin
    ON fact_telemetry USING BRIN (ts) WITH (pages_per_range = 32);
CREATE INDEX IF NOT EXISTS ix_ft_asset_ts
    ON fact_telemetry (asset_id, ts DESC);
CREATE INDEX IF NOT EXISTS ix_ft_late
    ON fact_telemetry (event_date) WHERE is_late_arrival;

CREATE TABLE IF NOT EXISTS fact_energy_hourly (
    asset_id        TEXT        NOT NULL,
    site_id         TEXT        NOT NULL,
    building_id     TEXT        NOT NULL,
    event_hour      TIMESTAMPTZ NOT NULL,
    date_key        INTEGER     NOT NULL,
    hour_of_day     SMALLINT    NOT NULL,
    energy_kwh      NUMERIC(14, 4),
    avg_power_kw    NUMERIC(12, 3),
    peak_power_kw   NUMERIC(12, 3),
    min_power_kw    NUMERIC(12, 3),
    covered_hours   NUMERIC(6, 4),
    reading_count   INTEGER,
    event_date      DATE        NOT NULL,
    PRIMARY KEY (asset_id, event_hour)
) PARTITION BY RANGE (event_date);

CREATE INDEX IF NOT EXISTS ix_feh_site_date  ON fact_energy_hourly (site_id, event_date);
CREATE INDEX IF NOT EXISTS ix_feh_bldg_hour  ON fact_energy_hourly (building_id, event_hour DESC);

CREATE TABLE IF NOT EXISTS fact_event (
    event_id        TEXT        NOT NULL,
    asset_id        TEXT        NOT NULL,
    site_id         TEXT,
    building_id     TEXT,
    ts              TIMESTAMPTZ NOT NULL,
    date_key        INTEGER     NOT NULL,
    event_type      TEXT        NOT NULL,
    severity        TEXT        NOT NULL,
    message         TEXT,
    is_fault        BOOLEAN     NOT NULL,
    severity_rank   SMALLINT,
    event_date      DATE        NOT NULL,
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Fault analysis is the dominant read pattern; a partial index keeps it small.
CREATE INDEX IF NOT EXISTS ix_fe_asset_ts   ON fact_event (asset_id, ts DESC);
CREATE INDEX IF NOT EXISTS ix_fe_faults     ON fact_event (asset_id, ts DESC) WHERE is_fault;
CREATE INDEX IF NOT EXISTS ix_fe_severity   ON fact_event (severity, event_date) WHERE severity = 'High';

-- ---------------------------------------------------------------------------
-- Serving conveniences
-- ---------------------------------------------------------------------------
-- Asset health, refreshed by the pipeline rather than computed per request.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_asset_health AS
SELECT
    a.asset_id,
    a.asset_name,
    a.asset_type,
    a.site_id,
    a.building_id,
    h.descendant_count                                   AS assets_downstream,
    MAX(e.ts) FILTER (WHERE e.is_fault)                  AS last_fault_at,
    COUNT(e.event_id) FILTER (WHERE e.is_fault)          AS faults_30d,
    COUNT(e.event_id) FILTER (WHERE e.severity = 'High') AS high_severity_30d
FROM dim_asset a
LEFT JOIN dim_asset_hierarchy h ON h.asset_id = a.asset_id
LEFT JOIN fact_event e
       ON e.asset_id = a.asset_id
      AND e.event_date > CURRENT_DATE - INTERVAL '30 days'
WHERE a.is_current
GROUP BY a.asset_id, a.asset_name, a.asset_type, a.site_id, a.building_id, h.descendant_count;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_asset_health ON mv_asset_health (asset_id);
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_asset_health;   -- from the DAG
