-- ============================================================================
-- Q2. Average daily energy consumption for each site
-- ============================================================================
-- "Average daily" needs a stated denominator. Two are reported because they
-- answer different questions and quietly disagree:
--
--   avg_daily_kwh_per_active_day  - mean over days the site actually reported.
--                                   The right number for "how much does this
--                                   site use on a working day".
--   avg_daily_kwh_per_calendar_day- total energy / days in the window, counting
--                                   silent days as zero. The right number for
--                                   billing and for period-over-period totals.
--
-- Quoting one without the other is how energy reports end up wrong after an
-- outage. Weekday/weekend split is included because HVAC load is dominated by
-- occupancy, so a single mean hides the pattern that matters.
-- ============================================================================

WITH window_bounds AS (
    SELECT
        MIN(event_date) AS window_start,
        MAX(event_date) AS window_end,
        (CAST(MAX(event_date) AS DATE) - CAST(MIN(event_date) AS DATE)) + 1 AS calendar_days
    FROM agg_site_daily
),
site_daily AS (
    SELECT
        s.site_id,
        s.event_date,
        s.energy_kwh,
        d.is_weekend
    FROM agg_site_daily s
    LEFT JOIN dim_date d
           ON d.full_date = s.event_date
)
SELECT
    sd.site_id,
    ds.site_name,
    ds.city,
    ds.country,
    COUNT(*)                                                   AS active_days,
    w.calendar_days,
    ROUND(SUM(sd.energy_kwh), 2)                               AS total_energy_kwh,
    ROUND(AVG(sd.energy_kwh), 2)                               AS avg_daily_kwh_per_active_day,
    ROUND(SUM(sd.energy_kwh) / NULLIF(w.calendar_days, 0), 2)  AS avg_daily_kwh_per_calendar_day,
    ROUND(AVG(CASE WHEN sd.is_weekend THEN sd.energy_kwh END), 2)     AS avg_weekend_kwh,
    ROUND(AVG(CASE WHEN NOT sd.is_weekend THEN sd.energy_kwh END), 2) AS avg_weekday_kwh,
    ROUND(MIN(sd.energy_kwh), 2)                               AS min_daily_kwh,
    ROUND(MAX(sd.energy_kwh), 2)                               AS max_daily_kwh,
    -- Coefficient of variation: a stable site sits near 0; a spiky one needs
    -- investigating before its "average" is used for anything.
    ROUND(STDDEV_SAMP(sd.energy_kwh) / NULLIF(AVG(sd.energy_kwh), 0), 3) AS daily_cv
FROM site_daily sd
CROSS JOIN window_bounds w
LEFT JOIN dim_site ds
       ON ds.site_id = sd.site_id
GROUP BY sd.site_id, ds.site_name, ds.city, ds.country, w.calendar_days
ORDER BY total_energy_kwh DESC;
