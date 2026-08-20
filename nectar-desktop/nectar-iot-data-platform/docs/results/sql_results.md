# SQL challenge - executed results

## q1_top_energy_assets

`sql/analytics/q1_top_energy_assets.sql` returned **10 rows**.

|   rank | asset_id                  | asset_name   | asset_type   | manufacturer   | site_id   | building_id     |   rated_power_kw |   total_energy_kwh |   avg_daily_kwh |   peak_power_kw |   load_factor_pct |   energy_share_pct |
|-------:|:--------------------------|:-------------|:-------------|:---------------|:----------|:----------------|-----------------:|-------------------:|----------------:|----------------:|------------------:|-------------------:|
|      1 | SITE-BLR-BLD-01-CHILL-008 | Chiller-08   | Chiller      | Grundfos       | SITE-BLR  | SITE-BLR-BLD-01 |              320 |            71549.8 |         3252.26 |          574.61 |              43.3 |               7    |
|      2 | SITE-SIN-BLD-02-CHILL-008 | Chiller-08   | Chiller      | Atlas Copco    | SITE-SIN  | SITE-SIN-BLD-02 |              320 |            70101.3 |         3186.42 |          472.48 |              42.5 |               6.86 |
|      3 | SITE-CBE-BLD-01-CHILL-008 | Chiller-08   | Chiller      | Trane          | SITE-CBE  | SITE-CBE-BLD-01 |              320 |            70007.7 |         3182.17 |          523.48 |              42.4 |               6.85 |
|      4 | SITE-BLR-BLD-01-CHILL-010 | Chiller-10   | Chiller      | Trane          | SITE-BLR  | SITE-BLR-BLD-01 |              320 |            69755   |         3170.68 |          569.88 |              42.3 |               6.83 |
|      5 | SITE-BLR-BLD-01-CHILL-003 | Chiller-03   | Chiller      | Siemens        | SITE-BLR  | SITE-BLR-BLD-01 |              320 |            67891.4 |         3085.97 |          422.65 |              41.3 |               6.65 |
|      6 | SITE-SIN-BLD-01-BOILE-003 | Boiler-03    | Boiler       | Carrier        | SITE-SIN  | SITE-SIN-BLD-01 |              180 |            40061.5 |         1820.98 |          307.76 |              43.3 |               3.92 |
|      7 | SITE-SIN-BLD-02-BOILE-004 | Boiler-04    | Boiler       | Grundfos       | SITE-SIN  | SITE-SIN-BLD-02 |              180 |            39253.7 |         1784.26 |          274.54 |              42.3 |               3.84 |
|      8 | SITE-SIN-BLD-01-BOILE-005 | Boiler-05    | Boiler       | Carrier        | SITE-SIN  | SITE-SIN-BLD-01 |              180 |            39068.7 |         1775.85 |          295.87 |              42.2 |               3.82 |
|      9 | SITE-CBE-BLD-02-BOILE-001 | Boiler-01    | Boiler       | Schneider      | SITE-CBE  | SITE-CBE-BLD-02 |              180 |            38928.2 |         1769.46 |          287.3  |              42.1 |               3.81 |
|     10 | SITE-BLR-BLD-03-BOILE-001 | Boiler-01    | Boiler       | Trane          | SITE-BLR  | SITE-BLR-BLD-03 |              180 |            38213.4 |         1736.97 |          228.47 |              41.3 |               3.74 |

## q2_avg_daily_energy_per_site

`sql/analytics/q2_avg_daily_energy_per_site.sql` returned **3 rows**.

| site_id   | site_name                | city       | country   |   active_days |   calendar_days |   total_energy_kwh |   avg_daily_kwh_per_active_day |   avg_daily_kwh_per_calendar_day |   avg_weekend_kwh |   avg_weekday_kwh |   min_daily_kwh |   max_daily_kwh |   daily_cv |
|:----------|:-------------------------|:-----------|:----------|--------------:|----------------:|-------------------:|-------------------------------:|---------------------------------:|------------------:|------------------:|----------------:|----------------:|-----------:|
| SITE-BLR  | Whitefield Tech Park     | Bengaluru  | IN        |            22 |              22 |             424322 |                        19287.4 |                          19287.4 |           4764.78 |           24733.3 |         4703.54 |         27733.1 |      0.508 |
| SITE-SIN  | Changi Business Hub      | Singapore  | SG        |            22 |              22 |             323829 |                        14719.5 |                          14719.5 |           3715.51 |           18846   |         3288.39 |         25957.9 |      0.516 |
| SITE-CBE  | Nectar Coimbatore Campus | Coimbatore | IN        |            22 |              22 |             273525 |                        12433   |                          12433   |           3161.98 |           15909.6 |         2785.67 |         21695.1 |      0.515 |

## q3_assets_over_10_faults_30d

`sql/analytics/q3_assets_over_10_faults_30d.sql` returned **6 rows**.

| asset_id                  | asset_name    | asset_type   | manufacturer   | site_id   | building_id     |   fault_count |   high_severity_faults |   alarm_count |   warning_count |   days_with_faults | first_fault_at      | last_fault_at       |   mtbf_hours | connectivity_status   |   assets_downstream |
|:--------------------------|:--------------|:-------------|:---------------|:----------|:----------------|--------------:|-----------------------:|--------------:|----------------:|-------------------:|:--------------------|:--------------------|-------------:|:----------------------|--------------------:|
| SITE-CBE-BLD-02-PUMP-002  | Pump-02       | Pump         | Atlas Copco    | SITE-CBE  | SITE-CBE-BLD-02 |            97 |                     69 |            69 |              42 |                  8 | 2026-07-30 05:18:56 | 2026-08-17 08:33:00 |          4.5 | CONNECTED             |                   2 |
| SITE-BLR-BLD-03-UPS-002   | UPS-02        | UPS          | Daikin         | SITE-BLR  | SITE-BLR-BLD-03 |            59 |                     49 |            42 |              33 |                  5 | 2026-07-27 22:14:03 | 2026-08-15 07:25:57 |          7.6 | STANDALONE            |                   0 |
| SITE-BLR-BLD-01-CHILL-010 | Chiller-10    | Chiller      | Trane          | SITE-BLR  | SITE-BLR-BLD-01 |            57 |                     42 |            45 |              36 |                 13 | 2026-07-27 07:03:12 | 2026-08-16 18:14:00 |          8.8 | CONNECTED             |                   3 |
| SITE-SIN-BLD-01-BOILE-003 | Boiler-03     | Boiler       | Carrier        | SITE-SIN  | SITE-SIN-BLD-01 |            55 |                     40 |            56 |              40 |                  8 | 2026-07-30 14:47:01 | 2026-08-14 18:57:06 |          6.7 | STANDALONE            |                   0 |
| SITE-BLR-BLD-01-CHILL-008 | Chiller-08    | Chiller      | Grundfos       | SITE-BLR  | SITE-BLR-BLD-01 |            53 |                     43 |            30 |              23 |                  6 | 2026-07-27 05:12:06 | 2026-08-06 14:25:17 |          4.8 | CONNECTED             |                   1 |
| SITE-BLR-BLD-02-COMPR-005 | Compressor-05 | Compressor   | Siemens        | SITE-BLR  | SITE-BLR-BLD-02 |            47 |                     36 |            33 |              39 |                  6 | 2026-08-06 20:00:27 | 2026-08-13 10:54:12 |          3.4 | CONNECTED             |                   1 |

## q4_assets_silent_24h

`sql/analytics/q4_assets_silent_24h.sql` returned **10 rows**.

| asset_id                  | asset_name         | asset_type      | site_id   | building_id     | connectivity_status   | last_reading_at     | lifetime_readings   | sensors_seen   |   hours_since_last_reading | status         |
|:--------------------------|:-------------------|:----------------|:----------|:----------------|:----------------------|:--------------------|:--------------------|:---------------|---------------------------:|:---------------|
| SITE-BLR-BLD-01-UPS-014   | UPS-14             | UPS             | SITE-BLR  | SITE-BLR-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-BLR-BLD-01-UPS-015   | UPS-15             | UPS             | SITE-BLR  | SITE-BLR-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-CBE-BLD-01-UPS-015   | UPS-15             | UPS             | SITE-CBE  | SITE-CBE-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-CBE-BLD-01-UPS-016   | UPS-16             | UPS             | SITE-CBE  | SITE-CBE-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-SIN-BLD-01-UPS-006   | UPS-06             | UPS             | SITE-SIN  | SITE-SIN-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-SIN-BLD-01-UPS-007   | UPS-07             | UPS             | SITE-SIN  | SITE-SIN-BLD-01 | ORPHANED              | NaT                 | <NA>                | <NA>           |                      nan   | NEVER_REPORTED |
| SITE-SIN-BLD-02-FLOWS-006 | Flow Sensor-06     | Flow Sensor     | SITE-SIN  | SITE-SIN-BLD-02 | CONNECTED             | 2026-08-14 20:35:00 | 5368                | 1              |                       63.3 | SILENT         |
| SITE-SIN-BLD-01-UPS-004   | UPS-04             | UPS             | SITE-SIN  | SITE-SIN-BLD-01 | STANDALONE            | 2026-08-15 13:45:00 | 5559                | 1              |                       46.1 | SILENT         |
| SITE-CBE-BLD-02-PRESS-009 | Pressure Sensor-09 | Pressure Sensor | SITE-CBE  | SITE-CBE-BLD-02 | CONNECTED             | 2026-08-15 22:55:00 | 5665                | 1              |                       36.9 | SILENT         |
| SITE-CBE-BLD-03-COMPR-006 | Compressor-06      | Compressor      | SITE-CBE  | SITE-CBE-BLD-03 | CONNECTED             | 2026-08-16 07:40:00 | 5769                | 1              |                       28.2 | SILENT         |

## q5_hourly_building_utilization

`sql/analytics/q5_hourly_building_utilization.sql` returned **4,644 rows**.

| site_id   | building_id     | building_name         | building_type   | event_hour          | event_date          |   hour_of_day |   reporting_assets |   observed_asset_hours |   productive_asset_hours |   utilization_pct |   availability_pct |   energy_kwh |   avg_power_kw |   peak_power_kw |   data_coverage_pct |
|:----------|:----------------|:----------------------|:----------------|:--------------------|:--------------------|--------------:|-------------------:|-----------------------:|-------------------------:|------------------:|-------------------:|-------------:|---------------:|----------------:|--------------------:|
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 00:00:00 | 2026-07-27 00:00:00 |             0 |                 13 |                 12.917 |                    5     |             38.71 |              72.26 |       60.61  |          4.785 |          27.338 |                99.4 |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 01:00:00 | 2026-07-27 00:00:00 |             1 |                 13 |                 13     |                    5     |             38.46 |              72.44 |       61.135 |          4.764 |          27.772 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 02:00:00 | 2026-07-27 00:00:00 |             2 |                 13 |                 13     |                    4.917 |             37.82 |              71.15 |       61.998 |          4.831 |          29.974 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 03:00:00 | 2026-07-27 00:00:00 |             3 |                 13 |                 13.083 |                    5     |             38.22 |              70.7  |       64.377 |          4.801 |          28.885 |               100.6 |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 04:00:00 | 2026-07-27 00:00:00 |             4 |                 13 |                 12.917 |                    5     |             38.71 |              94.19 |      170.447 |         13.196 |          83.82  |                99.4 |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 05:00:00 | 2026-07-27 00:00:00 |             5 |                 13 |                 13     |                    5     |             38.46 |              99.36 |      189.39  |         14.568 |          81.851 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 06:00:00 | 2026-07-27 00:00:00 |             6 |                 13 |                 13     |                    5     |             38.46 |             100    |      211.166 |         16.454 |          91.451 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 07:00:00 | 2026-07-27 00:00:00 |             7 |                 13 |                 13     |                    9.667 |             74.36 |              98.72 |      622.896 |         48.657 |         267.806 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 08:00:00 | 2026-07-27 00:00:00 |             8 |                 13 |                 13     |                   11.333 |             87.18 |              98.72 |      814.809 |         63.492 |         274.965 |               100   |
| SITE-BLR  | SITE-BLR-BLD-01 | Whitefield Building 1 | Data Centre     | 2026-07-27 09:00:00 | 2026-07-27 00:00:00 |             9 |                 13 |                 13.083 |                   11.333 |             86.62 |             100    |      925.569 |         68.607 |         295.701 |               100.6 |

## q6_site_power_anomalies

`sql/analytics/q6_site_power_anomalies.sql` returned **4 rows**.

| site_id   | site_name                | city       | event_date          |   energy_kwh |   baseline_kwh |   baseline_days |   energy_zscore |   pct_vs_baseline |   pct_vs_same_weekday |   peak_power_kw | anomaly_severity   | detected_by    |
|:----------|:-------------------------|:-----------|:--------------------|-------------:|---------------:|----------------:|----------------:|------------------:|----------------------:|----------------:|:-------------------|:---------------|
| SITE-SIN  | Changi Business Hub      | Singapore  | 2026-08-14 00:00:00 |     25957.9  |        14811.4 |               7 |            1.46 |              75.3 |                  31.3 |          472.48 | MEDIUM             | week_over_week |
| SITE-CBE  | Nectar Coimbatore Campus | Coimbatore | 2026-08-14 00:00:00 |     21695.1  |        12482.7 |               7 |            1.42 |              73.8 |                  33.4 |          523.48 | MEDIUM             | week_over_week |
| SITE-CBE  | Nectar Coimbatore Campus | Coimbatore | 2026-08-15 00:00:00 |      4035.23 |        13259.5 |               7 |           -1.27 |             -69.6 |                  33.8 |          119.14 | MEDIUM             | week_over_week |
| SITE-SIN  | Changi Business Hub      | Singapore  | 2026-08-15 00:00:00 |      4664.54 |        15696   |               7 |           -1.28 |             -70.3 |                  28.1 |          119.81 | MEDIUM             | week_over_week |
