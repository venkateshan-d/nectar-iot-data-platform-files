-- ============================================================================
-- Q3. Assets that generated more than 10 faults in the last 30 days
-- ============================================================================
-- Notes on correctness:
--
-- * "Last 30 days" is anchored to the **latest event in the data**, not to
--   CURRENT_DATE. Anchoring a backfill or a demo dataset to wall-clock time
--   silently returns nothing; anchoring to the data makes the query
--   reproducible. Swap `data_anchor` for CURRENT_DATE in production if the
--   pipeline is guaranteed current.
-- * Only `event_type = 'Fault'` counts. Alarms and Warnings are reported
--   separately so the reader can see whether an asset is genuinely failing or
--   merely noisy.
-- * The predicate on `event_date` (the partition column) rather than on
--   `timestamp` is deliberate - it lets the engine prune partitions instead of
--   scanning and filtering.
-- ============================================================================

WITH anchor AS (
    SELECT MAX(event_date) AS data_anchor FROM fact_event
),
window_events AS (
    SELECT e.*
    FROM fact_event e
    CROSS JOIN anchor a
    WHERE e.event_date > a.data_anchor - INTERVAL 30 DAY   -- partition pruning
      AND e.event_date <= a.data_anchor
),
per_asset AS (
    SELECT
        asset_id,
        site_id,
        building_id,
        COUNT(*) FILTER (WHERE event_type = 'Fault')                          AS fault_count,
        COUNT(*) FILTER (WHERE event_type = 'Fault' AND severity = 'High')    AS high_severity_faults,
        COUNT(*) FILTER (WHERE event_type = 'Alarm')                          AS alarm_count,
        COUNT(*) FILTER (WHERE event_type = 'Warning')                        AS warning_count,
        COUNT(DISTINCT event_date) FILTER (WHERE event_type = 'Fault')        AS days_with_faults,
        MIN(timestamp) FILTER (WHERE event_type = 'Fault')                    AS first_fault_at,
        MAX(timestamp) FILTER (WHERE event_type = 'Fault')                    AS last_fault_at
    FROM window_events
    GROUP BY asset_id, site_id, building_id
)
SELECT
    p.asset_id,
    a.asset_name,
    a.asset_type,
    a.manufacturer,
    p.site_id,
    p.building_id,
    p.fault_count,
    p.high_severity_faults,
    p.alarm_count,
    p.warning_count,
    p.days_with_faults,
    p.first_fault_at,
    p.last_fault_at,
    -- Mean time between failures over the observed fault sequence.
    ROUND(
        DATE_DIFF('hour', p.first_fault_at, p.last_fault_at)
        / NULLIF(p.fault_count - 1, 0), 1
    )                                                        AS mtbf_hours,
    h.connectivity_status,
    h.descendant_count                                       AS assets_downstream
FROM per_asset p
LEFT JOIN dim_asset a
       ON a.asset_id = p.asset_id AND a.is_current
LEFT JOIN dim_asset_hierarchy h
       ON h.asset_id = p.asset_id
WHERE p.fault_count > 10
ORDER BY p.fault_count DESC, p.high_severity_faults DESC;

-- Spark SQL / Snowflake portability:
--   COUNT(*) FILTER (WHERE x)  ->  COUNT(CASE WHEN x THEN 1 END)
--   DATE_DIFF('hour', a, b)    ->  Spark: (unix_timestamp(b) - unix_timestamp(a))/3600
--                                  Snowflake: DATEDIFF('hour', a, b)
--   INTERVAL 30 DAY            ->  Snowflake: INTERVAL '30 days'
