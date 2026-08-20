-- ============================================================================
-- Nectar IoT Platform - complete deployment in pure SQL
--
-- No CLI, no bundle, no notebooks. Paste into a Databricks SQL editor and run,
-- or let Claude run it through the Databricks SQL MCP connector.
--
-- CREATE STREAMING TABLE and CREATE MATERIALIZED VIEW in Databricks SQL create
-- and manage a serverless Lakeflow pipeline behind the scenes. So this file is
-- a real declarative pipeline - the same medallion as the notebook version,
-- expressed in the one language a SQL warehouse accepts.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Catalog, schemas, landing volume
-- ---------------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS nectar;
CREATE SCHEMA  IF NOT EXISTS nectar.bronze;
CREATE SCHEMA  IF NOT EXISTS nectar.silver;
CREATE SCHEMA  IF NOT EXISTS nectar.gold;
CREATE SCHEMA  IF NOT EXISTS nectar.quality;
CREATE SCHEMA  IF NOT EXISTS nectar.serving;
CREATE VOLUME  IF NOT EXISTS nectar.bronze.landing;

-- Upload the generated files into:
--   /Volumes/nectar/bronze/landing/{telemetry,events,assets,sites,buildings}/

-- ---------------------------------------------------------------------------
-- 2. BRONZE - land raw data verbatim
--
-- read_files() is Auto Loader in SQL form. schemaHints forces every column to
-- STRING: bronze must hold "temperature": "not-a-number" exactly as it arrived,
-- or the failure is not reproducible and the row is not replayable.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE nectar.bronze.telemetry
COMMENT 'Raw telemetry as landed, plus lineage. Never transformed.'
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at,
    to_date(regexp_extract(_metadata.file_path,
            'ingest_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1)) AS ingest_date
FROM STREAM read_files(
    '/Volumes/nectar/bronze/landing/telemetry',
    format            => 'json',
    inferColumnTypes  => false,
    schemaHints       => 'timestamp STRING, site_id STRING, building_id STRING,
                          asset_id STRING, sensor_id STRING, temperature STRING,
                          humidity STRING, pressure STRING, vibration STRING,
                          power_consumption STRING, operating_mode STRING',
    rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE nectar.bronze.events
COMMENT 'Raw operational events as landed, plus lineage.'
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/nectar/bronze/landing/events',
    format            => 'json',
    inferColumnTypes  => false,
    schemaHints       => 'event_id STRING, timestamp STRING, asset_id STRING,
                          event_type STRING, severity STRING, message STRING',
    rescuedDataColumn => '_rescued_data'
);

CREATE OR REPLACE MATERIALIZED VIEW nectar.bronze.assets AS
SELECT * FROM read_files('/Volumes/nectar/bronze/landing/assets', format => 'csv', header => true);

CREATE OR REPLACE MATERIALIZED VIEW nectar.bronze.sites AS
SELECT * FROM read_files('/Volumes/nectar/bronze/landing/sites', format => 'csv', header => true);

CREATE OR REPLACE MATERIALIZED VIEW nectar.bronze.buildings AS
SELECT * FROM read_files('/Volumes/nectar/bronze/landing/buildings', format => 'csv', header => true);

-- ---------------------------------------------------------------------------
-- 3. SILVER - cast, enrich, validate
--
-- The prepared view flags every violation per row. The clean table and the
-- quarantine table are complements over that flag, so a row is either usable or
-- kept as evidence - never silently dropped.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW nectar.silver.telemetry_prepared AS
WITH register AS (
    SELECT DISTINCT upper(trim(asset_id)) AS reg_asset_id FROM nectar.bronze.assets
),
cast_rows AS (
    SELECT
        t.timestamp AS _raw_timestamp,
        coalesce(try_to_timestamp(t.timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                 try_to_timestamp(t.timestamp))     AS ts,
        upper(trim(t.site_id))                      AS site_id,
        upper(trim(t.building_id))                  AS building_id,
        upper(trim(t.asset_id))                     AS asset_id,
        upper(trim(t.sensor_id))                    AS sensor_id,
        upper(trim(t.operating_mode))               AS operating_mode,
        try_cast(t.temperature       AS DOUBLE)     AS temperature,
        try_cast(t.humidity          AS DOUBLE)     AS humidity,
        try_cast(t.pressure          AS DOUBLE)     AS pressure,
        try_cast(t.vibration         AS DOUBLE)     AS vibration,
        try_cast(t.power_consumption AS DOUBLE)     AS power_consumption,
        t.ingest_date, t._ingested_at
    FROM nectar.bronze.telemetry t
)
SELECT c.*,
       r.reg_asset_id IS NOT NULL AS asset_known,
       coalesce(datediff(c.ingest_date, to_date(c.ts)) * 86400 >= 86400, false) AS is_late,
       array_compact(array(
         CASE WHEN c.ts IS NULL THEN 'tel.completeness.timestamp_not_null' END,
         CASE WHEN c.asset_id IS NULL OR c.asset_id = '' THEN 'tel.completeness.asset_id_not_null' END,
         CASE WHEN c.site_id  IS NULL OR c.site_id  = '' THEN 'tel.completeness.site_id_not_null' END,
         CASE WHEN c._raw_timestamp IS NOT NULL AND c.ts IS NULL THEN 'tel.validity.timestamp_parseable' END,
         CASE WHEN c.ts IS NOT NULL AND (c.ts < '2000-01-01' OR c.ts > current_timestamp() + INTERVAL 1 DAY)
              THEN 'tel.validity.timestamp_plausible' END,
         CASE WHEN r.reg_asset_id IS NULL THEN 'tel.consistency.asset_registered' END,
         CASE WHEN c.temperature       NOT BETWEEN  -40 AND  120 THEN 'tel.accuracy.temperature_in_range' END,
         CASE WHEN c.humidity          NOT BETWEEN    0 AND  100 THEN 'tel.accuracy.humidity_in_range' END,
         CASE WHEN c.pressure          NOT BETWEEN    0 AND 1200 THEN 'tel.accuracy.pressure_in_range' END,
         CASE WHEN c.vibration         NOT BETWEEN    0 AND  100 THEN 'tel.accuracy.vibration_in_range' END,
         CASE WHEN c.power_consumption NOT BETWEEN    0 AND 5000 THEN 'tel.accuracy.power_in_range' END
       )) AS _failed_rules
FROM cast_rows c
LEFT JOIN register r ON c.asset_id = r.reg_asset_id;

CREATE OR REPLACE MATERIALIZED VIEW nectar.silver.telemetry
COMMENT 'Clean telemetry. One row per (asset, sensor, timestamp).'
AS SELECT * EXCEPT (_failed_rules, _rank) FROM (
    SELECT p.*,
           row_number() OVER (PARTITION BY asset_id, sensor_id, ts ORDER BY _ingested_at) AS _rank
    FROM nectar.silver.telemetry_prepared p
    WHERE size(p._failed_rules) = 0
) WHERE _rank = 1;

CREATE OR REPLACE MATERIALIZED VIEW nectar.silver.quarantine_telemetry
COMMENT 'Rejected rows with the rule ids they broke. Replayable after the fix.'
AS SELECT *, current_timestamp() AS _quarantined_at
FROM nectar.silver.telemetry_prepared WHERE size(_failed_rules) > 0;

CREATE OR REPLACE MATERIALIZED VIEW nectar.silver.events AS
WITH register AS (
    SELECT DISTINCT upper(trim(asset_id)) AS reg_asset_id,
           upper(trim(site_id)) AS site_id, upper(trim(building_id)) AS building_id
    FROM nectar.bronze.assets
)
SELECT upper(trim(e.event_id)) AS event_id,
       coalesce(try_to_timestamp(e.timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                try_to_timestamp(e.timestamp)) AS ts,
       upper(trim(e.asset_id)) AS asset_id, r.site_id, r.building_id,
       initcap(trim(e.event_type)) AS event_type,
       initcap(trim(e.severity))   AS severity, e.message,
       to_date(coalesce(try_to_timestamp(e.timestamp, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                        try_to_timestamp(e.timestamp))) AS event_date
FROM nectar.bronze.events e
LEFT JOIN register r ON upper(trim(e.asset_id)) = r.reg_asset_id
WHERE e.event_id IS NOT NULL AND r.reg_asset_id IS NOT NULL
  AND initcap(trim(e.event_type)) IN ('Alarm','Warning','Fault','Info');

-- ---------------------------------------------------------------------------
-- 4. GOLD
--
-- Two corrections a naive version gets wrong:
--   * power is asset-level but arrives once per sensor -> average first, or a
--     two-sensor chiller reports double its energy;
--   * energy is an integral -> weight each reading by the gap to the next,
--     capped at 2x the 5-minute interval, so a 6-hour outage is not billed as
--     6 hours at the last known load.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.asset_readings AS
WITH per_asset AS (
    SELECT site_id, building_id, asset_id, ts,
           avg(temperature) AS temperature, avg(humidity) AS humidity,
           avg(pressure) AS pressure, avg(vibration) AS vibration,
           avg(power_consumption) AS power_consumption,
           min(operating_mode) AS operating_mode
    FROM nectar.silver.telemetry WHERE ts IS NOT NULL
    GROUP BY site_id, building_id, asset_id, ts
),
weighted AS (
    SELECT *, least(coalesce(datediff(SECOND, ts,
                    lead(ts) OVER (PARTITION BY asset_id ORDER BY ts)), 300), 600) / 3600.0
              AS duration_hours
    FROM per_asset
)
SELECT *, to_date(ts) AS event_date, date_trunc('hour', ts) AS event_hour,
       power_consumption * duration_hours AS energy_kwh
FROM weighted;

CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.fact_energy_hourly
COMMENT 'Energy at asset x hour. energy_kwh is the only fully additive measure here.'
AS SELECT site_id, building_id, asset_id, event_hour,
       to_date(event_hour) AS event_date, hour(event_hour) AS hour_of_day,
       round(sum(energy_kwh), 4)        AS energy_kwh,
       round(avg(power_consumption), 3) AS avg_power_kw,
       round(max(power_consumption), 3) AS peak_power_kw,
       round(sum(duration_hours), 4)    AS covered_hours,
       count(*)                         AS reading_count
FROM nectar.gold.asset_readings
GROUP BY site_id, building_id, asset_id, event_hour;

CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.curated_daily_asset_utilization
COMMENT 'Utilisation over OBSERVED hours, not calendar hours - otherwise idle and
         stopped-reporting collapse into one number.'
AS SELECT site_id, building_id, asset_id, event_date,
       round(sum(duration_hours), 3) AS observed_hours,
       round(sum(energy_kwh), 3)     AS energy_kwh,
       round(max(power_consumption), 3) AS peak_power_kw,
       round(100.0 * sum(CASE WHEN operating_mode IN ('RUNNING','BOOST') THEN duration_hours ELSE 0 END)
             / nullif(sum(duration_hours), 0), 2) AS utilization_pct,
       round(100.0 * (1 - sum(CASE WHEN operating_mode IN ('OFF','FAULT','MAINTENANCE')
             THEN duration_hours ELSE 0 END) / nullif(sum(duration_hours), 0)), 2) AS availability_pct,
       round(100.0 * least(sum(duration_hours) / 24.0, 1.0), 2) AS data_coverage_pct
FROM nectar.gold.asset_readings
GROUP BY site_id, building_id, asset_id, event_date;

CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.curated_fault_statistics AS
WITH faults AS (
    SELECT asset_id, min(ts) AS first_fault_at, max(ts) AS last_fault_at, count(*) AS fault_count
    FROM nectar.silver.events WHERE event_type = 'Fault' GROUP BY asset_id
)
SELECT e.site_id, e.building_id, e.asset_id,
       count(*) AS total_events,
       count_if(e.event_type = 'Fault')   AS faults,
       count_if(e.event_type = 'Alarm')   AS alarms,
       count_if(e.event_type = 'Warning') AS warnings,
       count_if(e.severity = 'High')      AS high_severity_events,
       f.first_fault_at, f.last_fault_at,
       CASE WHEN f.fault_count > 1 THEN
            round(datediff(HOUR, f.first_fault_at, f.last_fault_at) / (f.fault_count - 1), 2) END
            AS mtbf_hours,
       greatest(0, round(100 - (count_if(e.event_type='Fault') * 5 + count_if(e.event_type='Alarm')
              + count_if(e.event_type='Warning') * 0.25), 2)) AS health_score
FROM nectar.silver.events e
LEFT JOIN faults f ON e.asset_id = f.asset_id
GROUP BY e.site_id, e.building_id, e.asset_id, f.first_fault_at, f.last_fault_at, f.fault_count;

CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.agg_building_daily AS
SELECT u.site_id, u.building_id, b.building_name, u.event_date,
       count(DISTINCT u.asset_id)         AS active_assets,
       round(sum(u.energy_kwh), 3)        AS energy_kwh,
       round(avg(u.utilization_pct), 2)   AS avg_utilization_pct,
       round(avg(u.data_coverage_pct), 2) AS data_coverage_pct,
       round(max(u.peak_power_kw), 3)     AS peak_power_kw,
       round(sum(u.energy_kwh) / nullif(try_cast(b.floor_area_sqm AS DOUBLE), 0), 5)
            AS energy_intensity_kwh_per_sqm
FROM nectar.gold.curated_daily_asset_utilization u
LEFT JOIN nectar.bronze.buildings b ON upper(trim(b.building_id)) = u.building_id
GROUP BY u.site_id, u.building_id, b.building_name, u.event_date, b.floor_area_sqm;

CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.agg_site_daily
COMMENT 'Site x day with a trailing baseline that EXCLUDES today, so an anomaly
         cannot raise its own threshold.'
AS
WITH daily AS (
    SELECT site_id, event_date,
           count(DISTINCT building_id)      AS buildings,
           sum(active_assets)               AS active_assets,
           round(sum(energy_kwh), 3)        AS energy_kwh,
           round(avg(data_coverage_pct), 2) AS data_coverage_pct,
           round(max(peak_power_kw), 3)     AS peak_power_kw
    FROM nectar.gold.agg_building_daily GROUP BY site_id, event_date
)
SELECT d.*,
       round(avg(energy_kwh) OVER w, 3) AS baseline_energy_kwh,
       round((energy_kwh - avg(energy_kwh) OVER w)
             / nullif(stddev_samp(energy_kwh) OVER w, 0), 3) AS energy_zscore,
       round(100.0 * (energy_kwh - avg(energy_kwh) OVER w)
             / nullif(avg(energy_kwh) OVER w, 0), 1)         AS pct_vs_baseline,
       lag(energy_kwh, 7) OVER (PARTITION BY site_id ORDER BY event_date)
                                                             AS same_weekday_last_week_kwh
FROM daily d
WINDOW w AS (PARTITION BY site_id ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING);

-- ---------------------------------------------------------------------------
-- 5. SERVING views
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW nectar.serving.v_site_energy_daily AS
SELECT s.*, sr.site_name, sr.city,
       -- A z-score alone fires on a site so stable that a 0.5% move is extreme.
       -- Pairing it with a minimum effect size keeps the alert actionable.
       coalesce(s.energy_zscore >= 2.0 AND s.pct_vs_baseline >= 10.0, false) AS is_energy_anomaly
FROM nectar.gold.agg_site_daily s
LEFT JOIN nectar.bronze.sites sr ON upper(trim(sr.site_id)) = s.site_id
WHERE s.data_coverage_pct >= 80;   -- never compare a partial day with a full one

CREATE OR REPLACE VIEW nectar.serving.v_silent_assets
COMMENT 'Starts from the register, not the telemetry - this is a question about
         records that do NOT exist, so it cannot be a filter over the facts.'
AS
WITH anchor AS (SELECT max(ts) AS t FROM nectar.silver.telemetry),
last_seen AS (SELECT asset_id, max(ts) AS last_reading_at, count(*) AS lifetime_readings
              FROM nectar.silver.telemetry GROUP BY asset_id)
SELECT upper(trim(a.asset_id)) AS asset_id, a.asset_name, a.asset_type,
       upper(trim(a.site_id)) AS site_id, l.last_reading_at, l.lifetime_readings,
       CASE WHEN l.asset_id IS NULL THEN 'NEVER_REPORTED' ELSE 'SILENT' END AS status
FROM nectar.bronze.assets a
CROSS JOIN anchor an
LEFT JOIN last_seen l ON upper(trim(a.asset_id)) = l.asset_id
WHERE l.last_reading_at IS NULL OR l.last_reading_at < an.t - INTERVAL 24 HOURS;

CREATE OR REPLACE VIEW nectar.serving.v_data_quality AS
SELECT rule_id, count(*) AS rows_failed
FROM (SELECT explode(_failed_rules) AS rule_id FROM nectar.silver.quarantine_telemetry)
GROUP BY rule_id ORDER BY rows_failed DESC;
