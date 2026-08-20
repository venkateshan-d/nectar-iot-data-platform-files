-- ============================================================================
-- Q1. Top 10 assets by energy consumption
-- ============================================================================
-- Reads the pre-aggregated hourly energy fact rather than raw telemetry: the
-- integration of instantaneous kW into kWh (duration-weighted, de-duplicated
-- across sensors) already happened in the gold layer, so this query is a plain
-- SUM over ~1/60th of the rows.
--
-- `energy_share_pct` is included because a bare kWh ranking is hard to act on -
-- knowing the top asset is 12% of the estate's consumption is what justifies
-- the retrofit.
--
-- Portability: identical on Spark SQL, Snowflake and BigQuery. On Snowflake,
-- replace nothing; on BigQuery, `QUALIFY` could replace the outer LIMIT.
-- ============================================================================

WITH asset_energy AS (
    SELECT
        f.asset_id,
        SUM(f.energy_kwh)                             AS total_energy_kwh,
        AVG(f.avg_power_kw)                           AS avg_power_kw,
        MAX(f.peak_power_kw)                          AS peak_power_kw,
        COUNT(DISTINCT f.event_date)                  AS days_observed,
        SUM(f.energy_kwh) / NULLIF(COUNT(DISTINCT f.event_date), 0) AS avg_daily_kwh
    FROM fact_energy_hourly f
    GROUP BY f.asset_id
),
estate_total AS (
    SELECT SUM(total_energy_kwh) AS estate_energy_kwh FROM asset_energy
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ae.total_energy_kwh DESC)      AS rank,
    ae.asset_id,
    a.asset_name,
    a.asset_type,
    a.manufacturer,
    a.site_id,
    a.building_id,
    a.rated_power_kw,
    ROUND(ae.total_energy_kwh, 2)                              AS total_energy_kwh,
    ROUND(ae.avg_daily_kwh, 2)                                 AS avg_daily_kwh,
    ROUND(ae.peak_power_kw, 2)                                 AS peak_power_kw,
    -- Load factor: how hard the asset is actually worked versus its nameplate.
    -- A high-kWh asset with a low load factor is oversized, not overworked.
    ROUND(100.0 * ae.avg_power_kw / NULLIF(a.rated_power_kw, 0), 1) AS load_factor_pct,
    ROUND(100.0 * ae.total_energy_kwh / NULLIF(e.estate_energy_kwh, 0), 2) AS energy_share_pct
FROM asset_energy ae
CROSS JOIN estate_total e
LEFT JOIN dim_asset a
       ON a.asset_id = ae.asset_id
      AND a.is_current            -- SCD2: only the live version of the row
ORDER BY ae.total_energy_kwh DESC
LIMIT 10;
