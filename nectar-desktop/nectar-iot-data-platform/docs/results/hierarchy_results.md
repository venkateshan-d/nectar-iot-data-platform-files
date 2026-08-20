# Asset hierarchy queries (focus asset `SITE-CBE-BLD-01-CHILL-008`, site `SITE-BLR`)

## hierarchy_q1

**28 rows**

| site_id   | building_id     | asset_id                  | asset_name     | asset_type   |   level | hierarchy_path                                                                  |   child_count |   descendant_count | connectivity_status   |
|:----------|:----------------|:--------------------------|:---------------|:-------------|--------:|:--------------------------------------------------------------------------------|--------------:|-------------------:|:----------------------|
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-CHILL-003 | Chiller-03     | Chiller      |       0 | SITE-BLR-BLD-01-CHILL-003                                                       |             2 |                  4 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-AHU-004   | AHU-04         | AHU          |       1 | SITE-BLR-BLD-01-CHILL-003 > SITE-BLR-BLD-01-AHU-004                             |             2 |                  2 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-TEMPS-005 | Temp Sensor-05 | Temp Sensor  |       2 | SITE-BLR-BLD-01-CHILL-003 > SITE-BLR-BLD-01-AHU-004 > SITE-BLR-BLD-01-TEMPS-005 |             0 |                  0 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-TEMPS-006 | Temp Sensor-06 | Temp Sensor  |       2 | SITE-BLR-BLD-01-CHILL-003 > SITE-BLR-BLD-01-AHU-004 > SITE-BLR-BLD-01-TEMPS-006 |             0 |                  0 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-AHU-007   | AHU-07         | AHU          |       1 | SITE-BLR-BLD-01-CHILL-003 > SITE-BLR-BLD-01-AHU-007                             |             0 |                  0 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-CHILL-008 | Chiller-08     | Chiller      |       0 | SITE-BLR-BLD-01-CHILL-008                                                       |             1 |                  1 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-AHU-009   | AHU-09         | AHU          |       1 | SITE-BLR-BLD-01-CHILL-008 > SITE-BLR-BLD-01-AHU-009                             |             0 |                  0 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-CHILL-010 | Chiller-10     | Chiller      |       0 | SITE-BLR-BLD-01-CHILL-010                                                       |             1 |                  3 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-AHU-011   | AHU-11         | AHU          |       1 | SITE-BLR-BLD-01-CHILL-010 > SITE-BLR-BLD-01-AHU-011                             |             2 |                  2 | CONNECTED             |
| SITE-BLR  | SITE-BLR-BLD-01 | SITE-BLR-BLD-01-TEMPS-012 | Temp Sensor-12 | Temp Sensor  |       2 | SITE-BLR-BLD-01-CHILL-010 > SITE-BLR-BLD-01-AHU-011 > SITE-BLR-BLD-01-TEMPS-012 |             0 |                  0 | CONNECTED             |

## hierarchy_q2

**3 rows**

| relationship   | related_asset_id        | asset_name   | asset_type   |   level |
|:---------------|:------------------------|:-------------|:-------------|--------:|
| child          | SITE-CBE-BLD-01-AHU-009 | AHU-09       | AHU          |       1 |
| child          | SITE-CBE-BLD-01-AHU-010 | AHU-10       | AHU          |       1 |
| child          | SITE-CBE-BLD-01-AHU-013 | AHU-13       | AHU          |       1 |

## hierarchy_q3

**6 rows**

|   hops | impacted_asset_id         | asset_name     | asset_type   | building_id     | impact_path                                                                     |   faults |   health_score | risk_band   |
|-------:|:--------------------------|:---------------|:-------------|:----------------|:--------------------------------------------------------------------------------|---------:|---------------:|:------------|
|      1 | SITE-CBE-BLD-01-AHU-009   | AHU-09         | AHU          | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-009                             |        5 |          56.5  | Medium      |
|      1 | SITE-CBE-BLD-01-AHU-010   | AHU-10         | AHU          | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-010                             |        2 |          83.75 | Low         |
|      1 | SITE-CBE-BLD-01-AHU-013   | AHU-13         | AHU          | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-013                             |        5 |          57    | Medium      |
|      2 | SITE-CBE-BLD-01-TEMPS-011 | Temp Sensor-11 | Temp Sensor  | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-010 > SITE-CBE-BLD-01-TEMPS-011 |        3 |          72.25 | Medium      |
|      2 | SITE-CBE-BLD-01-TEMPS-012 | Temp Sensor-12 | Temp Sensor  | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-010 > SITE-CBE-BLD-01-TEMPS-012 |        5 |          58.75 | Medium      |
|      2 | SITE-CBE-BLD-01-TEMPS-014 | Temp Sensor-14 | Temp Sensor  | SITE-CBE-BLD-01 | SITE-CBE-BLD-01-CHILL-008 > SITE-CBE-BLD-01-AHU-013 > SITE-CBE-BLD-01-TEMPS-014 |        4 |          59    | Medium      |

## hierarchy_q4

**10 rows**

| asset_id                  | asset_name   | asset_type   | site_id   |   blast_radius |   max_depth |   downstream_faults |
|:--------------------------|:-------------|:-------------|:----------|---------------:|------------:|--------------------:|
| SITE-CBE-BLD-01-CHILL-008 | Chiller-08   | Chiller      | SITE-CBE  |              6 |           2 |                  24 |
| SITE-BLR-BLD-01-CHILL-003 | Chiller-03   | Chiller      | SITE-BLR  |              4 |           2 |                  16 |
| SITE-BLR-BLD-01-CHILL-010 | Chiller-10   | Chiller      | SITE-BLR  |              3 |           2 |                  12 |
| SITE-CBE-BLD-03-PUMP-001  | Pump-01      | Pump         | SITE-CBE  |              2 |           1 |                  12 |
| SITE-BLR-BLD-01-AHU-004   | AHU-04       | AHU          | SITE-BLR  |              2 |           1 |                  11 |
| SITE-BLR-BLD-01-AHU-011   | AHU-11       | AHU          | SITE-BLR  |              2 |           1 |                   8 |
| SITE-SIN-BLD-02-PUMP-001  | Pump-01      | Pump         | SITE-SIN  |              2 |           1 |                   8 |
| SITE-CBE-BLD-01-AHU-010   | AHU-10       | AHU          | SITE-CBE  |              2 |           1 |                   8 |
| SITE-SIN-BLD-03-PUMP-004  | Pump-04      | Pump         | SITE-SIN  |              2 |           1 |                   7 |
| SITE-BLR-BLD-03-PUMP-005  | Pump-05      | Pump         | SITE-BLR  |              2 |           1 |                   7 |

## hierarchy_q5

**6 rows**

| asset_id                | asset_name   | asset_type   | site_id   | building_id     | connectivity_status   |
|:------------------------|:-------------|:-------------|:----------|:----------------|:----------------------|
| SITE-BLR-BLD-01-UPS-014 | UPS-14       | UPS          | SITE-BLR  | SITE-BLR-BLD-01 | ORPHANED              |
| SITE-BLR-BLD-01-UPS-015 | UPS-15       | UPS          | SITE-BLR  | SITE-BLR-BLD-01 | ORPHANED              |
| SITE-CBE-BLD-01-UPS-015 | UPS-15       | UPS          | SITE-CBE  | SITE-CBE-BLD-01 | ORPHANED              |
| SITE-CBE-BLD-01-UPS-016 | UPS-16       | UPS          | SITE-CBE  | SITE-CBE-BLD-01 | ORPHANED              |
| SITE-SIN-BLD-01-UPS-006 | UPS-06       | UPS          | SITE-SIN  | SITE-SIN-BLD-01 | ORPHANED              |
| SITE-SIN-BLD-01-UPS-007 | UPS-07       | UPS          | SITE-SIN  | SITE-SIN-BLD-01 | ORPHANED              |

## hierarchy_q6

**2 rows**

| connectivity_status   |   assets | asset_ids                                                                                                                                                                                                                                                                                                            |
|:----------------------|---------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| STANDALONE            |       12 | SITE-BLR-BLD-02-UPS-003, SITE-BLR-BLD-02-UPS-004, SITE-BLR-BLD-03-BOILE-001, SITE-BLR-BLD-03-UPS-002, SITE-CBE-BLD-02-BOILE-001, SITE-CBE-BLD-03-UPS-004, SITE-CBE-BLD-03-UPS-005, SITE-SIN-BLD-01-BOILE-003, SITE-SIN-BLD-01-BOILE-005, SITE-SIN-BLD-01-UPS-004, SITE-SIN-BLD-02-BOILE-004, SITE-SIN-BLD-03-UPS-001 |
| ORPHANED              |        6 | SITE-BLR-BLD-01-UPS-014, SITE-BLR-BLD-01-UPS-015, SITE-CBE-BLD-01-UPS-015, SITE-CBE-BLD-01-UPS-016, SITE-SIN-BLD-01-UPS-006, SITE-SIN-BLD-01-UPS-007                                                                                                                                                                 |

## hierarchy_q7

**15 rows**

| asset_id                  | asset_name   | asset_type   |   subtree_energy_kwh |   assets_in_subtree |
|:--------------------------|:-------------|:-------------|---------------------:|--------------------:|
| SITE-CBE-BLD-01-CHILL-008 | Chiller-08   | Chiller      |              99552.7 |                   7 |
| SITE-BLR-BLD-01-CHILL-003 | Chiller-03   | Chiller      |              87051.1 |                   5 |
| SITE-BLR-BLD-01-CHILL-008 | Chiller-08   | Chiller      |              81206.1 |                   2 |
| SITE-SIN-BLD-02-CHILL-008 | Chiller-08   | Chiller      |              80001   |                   3 |
| SITE-BLR-BLD-01-CHILL-010 | Chiller-10   | Chiller      |              79416   |                   4 |
| SITE-SIN-BLD-01-BOILE-003 | Boiler-03    | Boiler       |              40061.5 |                   1 |
| SITE-SIN-BLD-02-BOILE-004 | Boiler-04    | Boiler       |              39253.7 |                   1 |
| SITE-SIN-BLD-01-BOILE-005 | Boiler-05    | Boiler       |              39068.7 |                   1 |
| SITE-CBE-BLD-02-BOILE-001 | Boiler-01    | Boiler       |              38928.2 |                   1 |
| SITE-BLR-BLD-03-BOILE-001 | Boiler-01    | Boiler       |              38213.4 |                   1 |
