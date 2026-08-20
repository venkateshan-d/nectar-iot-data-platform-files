-- ============================================================================
-- Q6. Sites with abnormal increases in power consumption
-- ============================================================================
-- "Abnormal" needs a definition, and a fixed threshold ("more than 20% up") is
-- the wrong one: it fires every Monday morning and never fires at a site whose
-- baseline is already high. Two complementary detectors are used instead, and a
-- site is flagged when either trips:
--
--   1. **Robust z-score vs the site's own trailing baseline.** The baseline is
--      the previous 7 days, excluding today, so the anomaly cannot pull its own
--      threshold up. A z-score is scale-free, so a 300 kW site and a 30 MW site
--      are judged on the same footing.
--
--   2. **Week-over-week same-weekday comparison.** HVAC load is strongly
--      weekly - comparing Saturday to the Friday before it manufactures
--      anomalies. Comparing Saturday to the previous Saturday does not.
--
-- Only *increases* are reported, per the question; drop the direction predicate
-- to catch under-consumption (which usually means a metering failure, and is
-- worth alerting on separately).
-- ============================================================================

WITH site_daily AS (
    SELECT
        s.site_id,
        s.event_date,
        s.energy_kwh,
        s.peak_power_kw,
        s.data_coverage_pct,
        d.day_of_week,
        d.is_weekend
    FROM agg_site_daily s
    LEFT JOIN dim_date d ON d.full_date = s.event_date
    -- Never compare a partially-observed day with a complete one. The first and
    -- last day of any window are partial by construction, and a half-day looks
    -- exactly like a 50% consumption drop (and makes the *next* day look like a
    -- 100% spike). This single predicate removes the whole class of false
    -- positives that boundary days would otherwise generate.
    WHERE s.data_coverage_pct >= 80
),
baselined AS (
    SELECT
        sd.*,
        -- Trailing 7-day baseline, today excluded.
        AVG(sd.energy_kwh) OVER (
            PARTITION BY sd.site_id ORDER BY sd.event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_kwh,
        STDDEV_SAMP(sd.energy_kwh) OVER (
            PARTITION BY sd.site_id ORDER BY sd.event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_stddev,
        COUNT(*) OVER (
            PARTITION BY sd.site_id ORDER BY sd.event_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_days,
        -- Same weekday, previous week.
        LAG(sd.energy_kwh, 7) OVER (
            PARTITION BY sd.site_id ORDER BY sd.event_date
        ) AS same_weekday_last_week_kwh
    FROM site_daily sd
),
scored AS (
    SELECT
        b.*,
        CASE WHEN b.baseline_stddev > 0
             THEN (b.energy_kwh - b.baseline_kwh) / b.baseline_stddev
        END AS energy_zscore,
        CASE WHEN b.baseline_kwh > 0
             THEN 100.0 * (b.energy_kwh - b.baseline_kwh) / b.baseline_kwh
        END AS pct_vs_baseline,
        CASE WHEN b.same_weekday_last_week_kwh > 0
             THEN 100.0 * (b.energy_kwh - b.same_weekday_last_week_kwh) / b.same_weekday_last_week_kwh
        END AS pct_vs_same_weekday
    FROM baselined b
)
SELECT
    s.site_id,
    ds.site_name,
    ds.city,
    s.event_date,
    ROUND(s.energy_kwh, 2)                    AS energy_kwh,
    ROUND(s.baseline_kwh, 2)                  AS baseline_kwh,
    s.baseline_days,
    ROUND(s.energy_zscore, 2)                 AS energy_zscore,
    ROUND(s.pct_vs_baseline, 1)               AS pct_vs_baseline,
    ROUND(s.pct_vs_same_weekday, 1)           AS pct_vs_same_weekday,
    ROUND(s.peak_power_kw, 2)                 AS peak_power_kw,
    CASE
        WHEN s.energy_zscore >= 3 THEN 'CRITICAL'
        WHEN s.energy_zscore >= 2 THEN 'HIGH'
        ELSE 'MEDIUM'
    END                                       AS anomaly_severity,
    CASE
        WHEN s.energy_zscore >= 2 AND s.pct_vs_same_weekday >= 20 THEN 'both_detectors'
        WHEN s.energy_zscore >= 2                                 THEN 'zscore_vs_baseline'
        ELSE 'week_over_week'
    END                                       AS detected_by
FROM scored s
LEFT JOIN dim_site ds ON ds.site_id = s.site_id
WHERE
    -- Require at least 3 baseline days, otherwise the first days of the window
    -- would all look anomalous against an empty baseline.
    s.baseline_days >= 3
    AND (
        -- Detector 1 pairs the z-score with a minimum effect size. A z-score
        -- alone fires on a site whose consumption is so stable that a 0.5%
        -- move is statistically extreme but operationally meaningless; the
        -- magnitude floor is what keeps the alert actionable.
        (s.energy_zscore >= 2.0 AND s.pct_vs_baseline >= 10.0)
        OR s.pct_vs_same_weekday >= 20.0  -- detector 2
    )
ORDER BY s.energy_zscore DESC NULLS LAST, s.event_date;
