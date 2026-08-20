-- ============================================================================
-- Q4. Assets that have not reported telemetry in the last 24 hours
-- ============================================================================
-- The trap in this question: the answer is about records that **do not exist**,
-- so it cannot be written as a filter over the telemetry table. It has to start
-- from the asset register and LEFT JOIN the last-seen watermark, otherwise an
-- asset that has been silent for a week simply disappears from the result.
--
-- Three states are distinguished, because they need different responses:
--   SILENT           - reported before, has now stopped   -> investigate device
--   NEVER_REPORTED   - in the register, never seen        -> commissioning gap
--   REPORTING        - healthy
--
-- Assets that have no sensors attached (pure structural nodes) would otherwise
-- flood the result, so `expected_to_report` marks them and they are excluded.
-- ============================================================================

WITH anchor AS (
    -- Latest observation in the dataset; substitute CURRENT_TIMESTAMP when the
    -- pipeline is guaranteed to be running live.
    SELECT MAX(timestamp) AS data_anchor FROM fact_telemetry
),
last_seen AS (
    SELECT
        asset_id,
        MAX(timestamp)               AS last_reading_at,
        COUNT(*)                     AS lifetime_readings,
        COUNT(DISTINCT sensor_id)    AS sensors_seen
    FROM fact_telemetry
    GROUP BY asset_id
),
expected AS (
    -- An asset is expected to report if it has ever reported, or if it is a
    -- leaf node (leaves are the instrumented equipment in this topology).
    SELECT
        a.asset_id,
        a.asset_name,
        a.asset_type,
        a.site_id,
        a.building_id,
        h.connectivity_status,
        h.is_leaf,
        (ls.asset_id IS NOT NULL OR h.is_leaf) AS expected_to_report
    FROM dim_asset a
    LEFT JOIN dim_asset_hierarchy h ON h.asset_id = a.asset_id
    LEFT JOIN last_seen ls          ON ls.asset_id = a.asset_id
    WHERE a.is_current
)
SELECT
    e.asset_id,
    e.asset_name,
    e.asset_type,
    e.site_id,
    e.building_id,
    e.connectivity_status,
    ls.last_reading_at,
    ls.lifetime_readings,
    ls.sensors_seen,
    ROUND(DATE_DIFF('minute', ls.last_reading_at, an.data_anchor) / 60.0, 1) AS hours_since_last_reading,
    CASE
        WHEN ls.last_reading_at IS NULL                                  THEN 'NEVER_REPORTED'
        WHEN ls.last_reading_at < an.data_anchor - INTERVAL 24 HOUR      THEN 'SILENT'
        ELSE 'REPORTING'
    END AS status
FROM expected e
CROSS JOIN anchor an
LEFT JOIN last_seen ls ON ls.asset_id = e.asset_id
WHERE e.expected_to_report
  AND (ls.last_reading_at IS NULL OR ls.last_reading_at < an.data_anchor - INTERVAL 24 HOUR)
ORDER BY hours_since_last_reading DESC NULLS FIRST, e.asset_id;
