# Airflow — verified DAG runs

Both DAGs were executed end to end with `airflow dags test`, which runs the real
task graph in-process (no scheduler, no webserver). Airflow 2.9.3, SQLite
metadata DB, `storage.table_format=parquet`.

## Setup used

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
export NECTAR_HOME=$PWD
export NECTAR__STORAGE__TABLE_FORMAT=parquet
export PYTHONPATH=$PWD/src

pip install apache-airflow==2.9.3
airflow db migrate
airflow pools set spark_pool 2 "bounds concurrent Spark drivers"
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'

airflow dags list                       # both DAGs load, zero import errors
airflow dags test nectar_iot_batch 2026-08-17
airflow dags test nectar_iot_maintenance 2026-08-17
```

> Airflow does not run natively on Windows — use WSL2, or
> `docker compose --profile airflow up -d`. The pipeline itself runs fine on
> Windows without Airflow; the DAGs are a design deliverable.

## Run 1 — happy path (`nectar_iot_batch`, 2026-08-17)

`DagRun Finished: state=success` — all ten tasks green.

| Task | State | Duration |
|---|---|---|
| start | success | — |
| wait_for_landing | success | instant (landing zone present) |
| ingest_bronze | success | 13.5 s |
| build_silver | success | 49.5 s |
| quality_gate | success | 4 s — verdict PASS, 0 blocking breaches, 11,946 quarantined |
| build_gold | success | 42.3 s (incl. OPTIMIZE) |
| build_hierarchy | success | 4.4 s |
| publish_serving | success | 0.5 s |
| quality_report | success | 2.6 s |
| end | success | — |

Total wall clock: **2 m 15 s**.

The gate's XCom payload:

```json
{"verdict": "PASS", "blocking_breaches": 0, "rows_quarantined": 11946, "pass_rate_pct": 100.0}
```

## Run 2 — maintenance DAG (`nectar_iot_maintenance`, 2026-08-17)

`state=success`. Task returns:

```json
{"optimized": ["silver.telemetry", "silver.telemetry_stream", "gold.fact_telemetry",
               "gold.fact_energy_hourly", "gold.fact_event", "gold.asset_closure"],
 "skipped": []}
{"vacuumed": [... same six ...]}
{"affected_dates": [], "recomputed": false}
{"removed": []}
```

(OPTIMIZE/VACUUM are no-ops on the Parquet fallback — logged rather than
silently skipped. On Delta they compact and Z-ORDER.)

## Run 3 — injected quality incident (`nectar_iot_batch`, 2026-08-16)

To prove the gate is real rather than decorative, the unknown-asset threshold was
tightened to 0.01% for one run:

```bash
export NECTAR__QUALITY__FAIL_ON_BLOCKING_BREACH=False   # let silver pass rows through
export NECTAR__QUALITY__THRESHOLDS__UNKNOWN_ASSET_RATE_MAX=0.0001
```

`DagRun Finished: state=failed`.

| Task | State |
|---|---|
| start | success |
| wait_for_landing | success |
| ingest_bronze | success |
| build_silver | success |
| **quality_gate** | **failed** |
| build_gold | upstream_failed |
| build_hierarchy | upstream_failed |
| publish_serving | upstream_failed |
| **quality_report** | **success** |
| end | upstream_failed |

```
airflow.exceptions.AirflowFailException: Data quality gate failed:
  ['tel.consistency.asset_registered', 'evt.consistency.asset_registered']
```

Four behaviours confirmed by this run:

1. **The gate blocks.** `build_gold` never executed, so the bad batch was never
   published. The previous run's gold tables remain intact and queryable.
2. **No retries were consumed.** `AirflowFailException` bypasses the retry policy
   — a data quality breach is not transient, so retrying it only hides it.
3. **The report still ran.** `quality_report` carries
   `trigger_rule=ALL_DONE`, so the diagnostic is produced precisely when the
   batch failed, which is when it is most needed.
4. **The alert fired** with the breached rule ids and the batch id, so the
   responder can query the quarantine partition for that run directly.
