# Task 7 — Orchestration & Scheduling

Orchestrated with **Databricks Workflows**. Three jobs, deliberately separate:
`nectar-iot-batch` (hourly), `nectar-iot-maintenance` (02:30 nightly) and
`nectar-iot-streaming` (continuous).

Everything is declared in YAML in [`databricks/`](../databricks/) — one
`databricks.yml` bundle, `resources/pipelines.yml` and `resources/jobs.yml`.
A schedule change or a retry-policy change shows up in a diff, and the whole set
deploys with `databricks bundle deploy`. There is deliberately no second way to
create these resources: a resource you can create two ways drifts.

## 1. Dependency management

```
run_medallion ──► quality_gate ──► build_hierarchy ──► publish_serving
 (Lakeflow          (hard gate,       (closure table)     (BI views)
  pipeline)          no retries)
```

The graph mirrors **data** dependencies, not code structure:

* `run_medallion` is a Lakeflow Declarative Pipeline. Bronze, silver and gold
  are one task because the dependencies *between* them are declared on the
  tables themselves — the pipeline works out the order. Splitting them into
  three job tasks would restate that ordering in a second place, where it can
  drift.
* `quality_gate` is a hard gate. Nothing downstream runs on a blocking breach.
  Stale dashboards beat wrong dashboards.
* `build_hierarchy` is its own task despite also being "gold": iterative
  closure expansion is not expressible declaratively, and it is cheap and
  idempotent, so it should be retryable without recomputing every aggregate.
* `publish_serving` is last because two of the views join through the closure
  table.
* Ingestion needs no sensor task. Auto Loader tracks which files it has already
  seen, so an early run is a cheap no-op and a late feed is picked up by the
  next hour — no worker sits and polls.

## 2. Failure handling

| Failure | Response |
|---|---|
| Transient (compute start, storage throttling) | 1–2 retries with backoff |
| Data quality breach | `max_retries: 0` — **no retries**, because it is not transient |
| Hung task | `timeout_seconds` per task; a task that is never killed blocks the next hour and cascades |
| Run overruns its hour | `queue.enabled` — the next hour queues instead of being dropped |
| Two runs at once | `max_concurrent_runs: 1` — the layers MERGE into shared tables |
| Whole run lost | Re-run the hour: Auto Loader replays from the checkpoint, MERGE makes the rewrite a no-op where data already landed |
| Stream dies | Continuous mode restarts it from the checkpoint |

**Retries are only safe because every task is idempotent.** Bronze is Auto
Loader with a checkpoint; silver and gold MERGE on the business key. Without
that, automatic retries are a data-duplication mechanism rather than a recovery
one. This is the single property that makes the rest of the retry policy
defensible.

## 3. Alerting strategy

| Signal | Severity | Channel |
|---|---|---|
| Task failure | page | `email_notifications.on_failure` → Slack webhook → PagerDuty |
| Quality gate FAIL | page | Slack, with the breached rule ids |
| SLA miss (`RUN_DURATION_SECONDS > 2700`) | warn | `on_duration_warning_threshold_exceeded`, no page |
| Streaming backlog (`STREAMING_BACKLOG_FILES > 200`) | warn → page if sustained | Health rule on the continuous job |
| Stale asset (freshness) | warn | Quality report + daily digest |
| DLQ rate spike | warn | Metrics — usually a firmware rollout |

Late means the dashboards are stale; failed means they are wrong. Different
severities, different channels — which is why the SLA rule is a *health
warning* and not a failure.

Alerts carry the `run_id`, which is stamped on the data, the quality results and
every log line — so the responder goes straight from the page to the exact rows
that failed, without opening the UI first.

## 4. Nightly maintenance job

Runs at 02:30, after the ingest trough, as its own job so a compaction failure
can never fail the pipeline that produces the data:

1. **Recompute late-affected partitions.** Ingesting a late record is the easy
   half; the half usually forgotten is that the *aggregate for that record's
   event date is now wrong*. This step finds the affected dates and restates
   exactly those partitions.
2. **OPTIMIZE** the hot tables. Micro-batches produce many small files; left
   alone, read performance degrades and metadata scanning starts to dominate
   query time. Liquid Clustering replaces the partition-plus-Z-ORDER pair, so
   the clustering keys can change later without rewriting history.
3. **VACUUM** past the 7-day retention — longer than the longest query and
   longer than any plausible time-travel debugging session.
4. **Prune quarantine** past 30 days. After a month those rows are archaeology,
   and quarantine is the fastest-growing table in the lakehouse when a device
   goes bad.

Order matters: restate first, then compact, so OPTIMIZE runs once over final
files.

With **Predictive Optimization** enabled on the catalog, steps 2 and 3 are
handled by the platform from observed query patterns and this job shrinks to
steps 1 and 4. It is kept explicit here so the reasoning is visible and so the
repo runs the same way on a workspace that does not have it.

## 5. Why Workflows

The pipeline already lives in Databricks: Unity Catalog holds the tables,
Lakeflow holds the transformation graph, Auto Loader holds the ingestion
bookmarks. Putting the scheduler outside that would mean a second system that
has to be told about compute, credentials, retries and lineage that Databricks
already knows.

Concretely:

* **Serverless.** No cluster to size, start or pay for between runs.
* **Lineage.** Job runs, pipeline events, table history and quality metrics are
  queryable together, in one place. There is no correlation step between "the
  orchestrator says it failed" and "which rows are wrong".
* **Continuous mode.** A never-ending stream does not fit a DAG of tasks that
  must complete. It is a first-class job type here.
* **Health rules.** SLA and streaming backlog are declarative settings, not a
  callback someone has to write and maintain.
* **One less system to run.** No scheduler database, no webserver, no worker
  fleet, no dependency conflicts between the orchestrator and the pipeline.

**What would change it.** If the estate grew a second, non-Databricks system —
an on-prem SAP extract, a vendor SFTP drop — the cross-system dependency has to
live somewhere neutral, and Workflows is not that. **Airflow** or **Dagster**
would then sit above it and trigger these jobs through the Jobs API, rather than
replace them. Dagster is the better conceptual fit of the two: its
software-defined assets map onto a medallion lakehouse more naturally than tasks
do, and its data-quality checks are first-class rather than a bespoke gate task.

The migration cost is deliberately low. Every task is a notebook that reads its
arguments from widgets and is safe to re-run, so an external orchestrator calls
the same units in the same order. Airflow DAGs doing exactly that are kept in
[`alternatives/airflow/`](../alternatives/airflow/) — thin wrappers around the
importable `nectar` package, with real verified runs recorded in
[`docs/results/airflow_run_evidence.md`](results/airflow_run_evidence.md).

## 6. Scheduling parameters

| Parameter | Value | Reasoning |
|---|---|---|
| batch `quartz_cron_expression` | `0 0 * * * ?` (hourly, Asia/Kolkata) | Hourly matches the business need; the streaming job covers sub-hour latency |
| maintenance cron | `0 30 2 * * ?` | After the ingest trough |
| `pause_status` | `PAUSED` on create | A job that starts running the moment it is created is a bad default in someone else's workspace |
| `max_concurrent_runs` | 1 | Shared-table MERGE writes |
| `queue.enabled` | true | Backfill the overrun rather than lose the hour |
| `timeout_seconds` | 5400 job / 900–3600 per task | Stops one hung run cascading |
| health `RUN_DURATION_SECONDS` | 2700 (45 min) | The point at which dashboards are meaningfully stale |
| streaming `max_retries` | -1 | The failure mode of a stream is a restart, not an alert |

## 7. What has been verified

The Airflow implementation of this same graph was executed end to end
(`airflow dags test`, Airflow 2.9.3): the happy path ran all ten tasks green in
2 m 15 s, and a deliberately injected quality incident failed the gate and left
everything downstream unrun, while the report task still ran on its `ALL_DONE`
rule. Task-state tables, timings and exact commands are in
[`docs/results/airflow_run_evidence.md`](results/airflow_run_evidence.md).

That evidence is kept because it proves the *gate behaviour* — the part of the
design that matters — with output rather than assertion. The Workflows jobs
express the same graph with the same gate, retry and concurrency settings, in
the platform the pipeline actually runs on.
