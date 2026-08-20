-- ============================================================================
-- Q5. Hourly utilisation for each building
-- ============================================================================
-- Utilisation = share of observed time an asset spent in a productive operating
-- mode (RUNNING / BOOST), rolled up to the building.
--
-- This is computed from the atomic telemetry fact rather than a pre-aggregate,
-- because the correct calculation is time-weighted and a naive
-- COUNT(mode='RUNNING')/COUNT(*) is wrong whenever sampling is irregular - a
-- device that reports twice as often while idle would look less utilised than
-- it is.
--
-- Steps:
--   1. collapse sensor-grain readings to asset grain (power is an asset-level
--      measure reported once per sensor - averaging prevents double counting);
--   2. give each reading a duration weight = gap to the next reading, capped at
--      2x the nominal 5-minute interval so an outage is not billed as runtime;
--   3. weight the productive modes and roll up asset -> building -> hour.
-- ============================================================================

WITH asset_readings AS (
    SELECT
        site_id,
        building_id,
        asset_id,
        timestamp,
        event_hour,
        AVG(power_consumption)                       AS power_kw,
        -- Deterministic pick of the mode when sensors disagree.
        MIN(operating_mode)                          AS operating_mode
    FROM fact_telemetry
    WHERE timestamp IS NOT NULL
    GROUP BY site_id, building_id, asset_id, timestamp, event_hour
),
weighted AS (
    SELECT
        r.*,
        LEAST(
            COALESCE(
                DATE_DIFF('second', r.timestamp,
                          LEAD(r.timestamp) OVER (PARTITION BY r.asset_id ORDER BY r.timestamp)),
                300                                  -- last reading: assume one interval
            ),
            600                                      -- cap at 2x the 5-minute interval
        ) / 3600.0 AS duration_hours
    FROM asset_readings r
),
per_building_hour AS (
    SELECT
        w.site_id,
        w.building_id,
        w.event_hour,
        COUNT(DISTINCT w.asset_id)                                                   AS reporting_assets,
        SUM(w.duration_hours)                                                        AS observed_asset_hours,
        SUM(CASE WHEN w.operating_mode IN ('RUNNING', 'BOOST') THEN w.duration_hours ELSE 0 END)
                                                                                     AS productive_asset_hours,
        SUM(CASE WHEN w.operating_mode IN ('OFF', 'FAULT', 'MAINTENANCE') THEN w.duration_hours ELSE 0 END)
                                                                                     AS downtime_asset_hours,
        SUM(w.power_kw * w.duration_hours)                                           AS energy_kwh,
        AVG(w.power_kw)                                                              AS avg_power_kw,
        MAX(w.power_kw)                                                              AS peak_power_kw
    FROM weighted w
    GROUP BY w.site_id, w.building_id, w.event_hour
)
SELECT
    p.site_id,
    p.building_id,
    b.building_name,
    b.building_type,
    p.event_hour,
    CAST(p.event_hour AS DATE)                                          AS event_date,
    EXTRACT(hour FROM p.event_hour)                                     AS hour_of_day,
    p.reporting_assets,
    ROUND(p.observed_asset_hours, 3)                                    AS observed_asset_hours,
    ROUND(p.productive_asset_hours, 3)                                  AS productive_asset_hours,
    ROUND(100.0 * p.productive_asset_hours / NULLIF(p.observed_asset_hours, 0), 2) AS utilization_pct,
    ROUND(100.0 * (1 - p.downtime_asset_hours / NULLIF(p.observed_asset_hours, 0)), 2) AS availability_pct,
    ROUND(p.energy_kwh, 3)                                              AS energy_kwh,
    ROUND(p.avg_power_kw, 3)                                            AS avg_power_kw,
    ROUND(p.peak_power_kw, 3)                                           AS peak_power_kw,
    -- Data coverage: reporting_assets x 1 hour is the ideal; anything lower
    -- means readings were lost and the utilisation figure is less reliable.
    ROUND(100.0 * p.observed_asset_hours / NULLIF(p.reporting_assets, 0), 1) AS data_coverage_pct
FROM per_building_hour p
LEFT JOIN dim_building b ON b.building_id = p.building_id
ORDER BY p.building_id, p.event_hour;
