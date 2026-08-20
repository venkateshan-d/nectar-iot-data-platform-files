# Databricks deployment

Everything that runs on Databricks is declared in **YAML, in this folder**:
one Lakeflow Declarative Pipeline and four Workflows jobs. There is no second
way to create them — a resource you can create two ways drifts, and then nobody
knows which one production is running.

```
databricks.yml            bundle: variables, dev and prod targets
resources/pipelines.yml   the medallion pipeline
resources/jobs.yml        batch, maintenance, streaming, setup
notebooks/                the code the pipeline and jobs run
sql/00_deploy_all.sql     the same lakehouse in pure SQL, for a workspace with no CLI
```

## Deploy

```bash
databricks auth login --host https://<your-workspace-host>
cd databricks
databricks bundle validate
databricks bundle deploy -t dev
```

Then in the workspace: **Jobs & Pipelines** shows `nectar-etl-pipeline` and the
four jobs, all paused.

Run one:

```bash
databricks bundle run nectar_setup      # once per workspace
databricks bundle run nectar_batch      # the hourly graph, on demand
```

CI does this for you — `.github/workflows/deploy.yml` deploys `dev` on merge to
main and `prod` on a version tag, so nobody runs the CLI from a laptop. See
[`../docs/06_devops.md`](../docs/06_devops.md).

## What gets created

| Resource | Name | What |
|---|---|---|
| Pipeline | `nectar-etl-pipeline` | bronze → silver → gold, expectations attached to the tables |
| Job | `nectar-iot-batch` | hourly: seed → pipeline → quality gate → hierarchy → serving |
| Job | `nectar-iot-maintenance` | 02:30: restate → optimize + vacuum → prune |
| Job | `nectar-iot-streaming` | continuous: producer ∥ Auto Loader bronze ∥ silver rules |
| Job | `nectar-iot-setup` | once: catalog, schemas, landing Volume |

## The choices worth defending

| Setting | Value | Why |
|---|---|---|
| `serverless: true` everywhere | — | Deploys unchanged on Free Edition and on a paid workspace, on any cloud. No node types to edit. |
| pipeline `continuous: false` | triggered | An always-on pipeline bills all day; sub-hour freshness is the streaming job's job. |
| `max_concurrent_runs` | 1 | The layers MERGE into shared tables. Two runs on one partition race, and Delta's optimistic concurrency fails one anyway. |
| `queue.enabled` | true | An hour that overruns queues the next one. Skipping an hour silently loses data. |
| `quality_gate.max_retries` | **0** | A quality breach is not transient. Retrying burns time and hides the problem. |
| other tasks `max_retries` | 1–2 | Rides out transient compute and storage errors. |
| `timeout_seconds` per task | 900–3600 | One hung task must not block the next hour and cascade. |
| health `RUN_DURATION_SECONDS` | 2700 | Warn at 45 min: late means stale, failed means wrong. Different severity, different channel. |
| `pause_status: PAUSED` in dev | — | A job that starts running the moment it is deployed is a bad default in someone else's workspace. `prod` unpauses them. |
| streaming `continuous` | — | The failure response for a stream is a restart, not an alert. |

**Retries are only safe because every task is idempotent.** Bronze is Auto
Loader with a checkpoint; silver and gold MERGE on the business key. Without
that, automatic retries duplicate data rather than recover from failure. This is
the single property that makes the rest of the retry policy defensible.

## Alerting

`notifications_email` is empty by default — a submitted repo should not mail a
real address. Set it at deploy time:

```bash
databricks bundle deploy -t prod --var="notifications_email=you@example.com"
```

In a real deployment these route to Slack through a webhook destination: task
failure and quality gate → page; SLA miss and streaming backlog → warn, no page.

## No CLI? Two credential-free paths

Free Edition has no service principals, so the CI deploy cannot run there. Both
of these work from the browser:

* **`sql/00_deploy_all.sql`** — paste into the SQL editor and run. `CREATE
  STREAMING TABLE` and `CREATE MATERIALIZED VIEW` create and manage a serverless
  Lakeflow pipeline behind the scenes, so this *is* a declarative pipeline,
  expressed in the one language a SQL warehouse accepts.
* **`notebooks/`** — clone the repo as a Git folder and run the notebooks
  directly. Start with `00_seed_landing` to fill the Volume, then create the
  pipeline over `01_bronze`, `02_silver`, `03_gold`.

## Notebooks

| Notebook | Runs as | What |
|---|---|---|
| `00_setup_catalog` | job `nectar_setup` | catalog, schemas, landing Volume |
| `00_seed_landing` | job task | fills the Volume with defect-injected data so the pipeline has input |
| `01_bronze` `02_silver` `03_gold` | pipeline | the medallion, with expectations |
| `04_hierarchy` | job task | closure table |
| `05_quality_report` | job task | flattens expectation metrics, computes freshness, fails the gate |
| `06_kinesis_stream` | manual | the Kinesis variant of the streaming path |
| `07_maintenance` | job tasks | `restate` · `optimize` · `prune` · `serving` |
| `autoloader/` | job `nectar_streaming` | 4-notebook Structured Streaming demo, no broker needed |
| `kafka/` | manual | the same four with Kafka as the source |
