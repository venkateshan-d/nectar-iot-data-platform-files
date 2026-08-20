"""Task 7 - orchestration for the Nectar IoT batch pipeline.

Schedule
--------
Hourly, with ``catchup=True`` so a paused or failed scheduler backfills the gap
rather than silently skipping hours. ``max_active_runs=1`` because the layers
MERGE into shared tables - two concurrent runs writing the same partition would
race, and Delta's optimistic concurrency would fail one of them anyway.

Dependency management
---------------------
The DAG mirrors the data dependencies, not the code structure:

    wait_for_landing >> ingest_bronze >> build_silver >> [quality_gate]
                                                      >> build_gold >> build_hierarchy
                                                                    >> publish_serving
                                                      >> quality_report >> notify

* ``wait_for_landing`` is a sensor with ``reschedule`` mode, so a late upstream
  feed does not hold a worker slot for an hour.
* ``quality_gate`` is a **hard gate**: a BLOCKING rule breach fails it, and
  everything downstream is skipped rather than publishing bad numbers. The
  quality report still runs (``trigger_rule=ALL_DONE``) because a failed batch
  is exactly when you most want the report.
* ``build_gold`` and ``build_hierarchy`` are separate tasks despite both being
  "gold" - the hierarchy rebuild is cheap and idempotent, so it should be
  retryable without recomputing every aggregate.

Failure handling & retries
--------------------------
* 3 retries with exponential backoff and jitter. Every task is idempotent
  (MERGE on the business key / dynamic partition overwrite), so a retry can
  never double-count. That property is what makes automatic retries safe;
  without it, retries are a data-corruption mechanism.
* ``execution_timeout`` on every task - a hung Spark job that is never killed
  blocks the next hour's run and cascades.
* ``on_failure_callback`` pages Slack/PagerDuty with the batch id, so the
  responder can go straight to the quarantine rows for that batch.
* SLA of 45 minutes on the gold build; an SLA miss is a warning (the dashboard
  is stale), a failure is a page (the dashboard is wrong).

Backfill
--------
``--ingest-dates`` is passed from the DAG run's logical date, so a manual
backfill of one day touches exactly that partition.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

PROJECT_HOME = os.environ.get("NECTAR_HOME", "/opt/nectar")
DEFAULT_POOL = "spark_pool"          # bounds concurrent Spark drivers on the cluster


# ---------------------------------------------------------------------------
# alerting
# ---------------------------------------------------------------------------
def alert_on_failure(context) -> None:
    """Page on failure with enough context to act without opening the UI."""
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    message = (
        f":rotating_light: *Nectar IoT pipeline failed*\n"
        f"• DAG `{ti.dag_id}` task `{ti.task_id}`\n"
        f"• run `{dag_run.run_id}` (logical date {context['logical_date']})\n"
        f"• try {ti.try_number} of {ti.max_tries + 1}\n"
        f"• error: `{context.get('exception')}`\n"
        f"• quarantined rows for this batch: "
        f"`SELECT * FROM quarantine.telemetry WHERE _batch_id = '{dag_run.run_id[-12:]}'`\n"
        f"• log: {ti.log_url}"
    )
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id="slack_data_alerts").send(text=message)
    except Exception:  # never let the alerter mask the original failure
        print(message)


def alert_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """SLA miss = stale dashboards. Warn, do not page."""
    print(f":hourglass: SLA missed on {dag.dag_id}: {[s.task_id for s in slas]}")


# ---------------------------------------------------------------------------
# task callables
# ---------------------------------------------------------------------------
def _run_layer(layer: str, **context) -> dict:
    """Invoke one pipeline layer in-process.

    In production this is a ``SparkSubmitOperator`` against the cluster; the
    in-process call keeps the DAG runnable on a laptop and, more importantly,
    keeps the DAG a thin scheduling wrapper - all the logic lives in the
    importable ``nectar`` package where it can be unit tested without Airflow.
    """
    import sys

    sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
    from nectar.config import load_config
    from nectar.logging_utils import RunContext, setup_logging
    from nectar.pipeline.run_batch import run_pipeline

    setup_logging(json_logs=True)
    cfg = load_config(os.path.join(PROJECT_HOME, "config", "pipeline.yaml"))

    ctx = RunContext(batch_id=context["dag_run"].run_id.replace(":", "")[-12:])
    ingest_date = context["logical_date"].strftime("%Y-%m-%d")
    ingest_dates = [ingest_date] if layer == "bronze" else None

    run_pipeline(cfg, [layer], ingest_dates=ingest_dates,
                 optimize=(layer == "gold"), ctx=ctx)
    # XCom carries the metrics so the quality gate and the notifier can read
    # them without touching the lakehouse.
    return ctx.metrics


def check_quality_gate(**context) -> dict:
    """Fail the DAG when a BLOCKING rule breached its threshold.

    Reads the results the silver job already wrote, rather than recomputing -
    the gate and the pipeline must agree by construction, not by coincidence.
    """
    import json
    import sys

    sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
    from nectar.config import load_config
    from nectar.io_layer import resolve_format
    from nectar.quality.report import collect_metrics
    from nectar.spark_session import get_spark

    cfg = load_config(os.path.join(PROJECT_HOME, "config", "pipeline.yaml"))
    spark = get_spark(cfg, "quality-gate")
    cfg.data["_resolved_format"] = resolve_format(spark, cfg.table_format)

    batch_id = context["dag_run"].run_id.replace(":", "")[-12:]
    metrics = collect_metrics(spark, cfg, batch_id)
    summary = {
        "verdict": metrics["verdict"],
        "blocking_breaches": metrics["totals"]["blocking_breaches"],
        "rows_quarantined": metrics["totals"]["rows_quarantined"],
        "pass_rate_pct": metrics["totals"]["pass_rate_pct"],
    }
    print(json.dumps(summary, indent=2))

    if metrics["verdict"] == "FAIL":
        breaches = [r["rule_id"] for r in metrics["rules"]
                    if not r["passed"] and r["severity"] == "BLOCKING"]
        # AirflowFailException skips the retries: a data quality breach is not
        # transient, so retrying it just burns time and hides the problem.
        raise AirflowFailException(f"Data quality gate failed: {breaches}")
    return summary


def publish_serving(**context) -> dict:
    import sys

    sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
    from nectar.config import load_config
    from nectar.serving.load_duckdb import build_database

    return build_database(load_config(os.path.join(PROJECT_HOME, "config", "pipeline.yaml")))


def emit_quality_report(**context) -> dict:
    import sys

    sys.path.insert(0, os.path.join(PROJECT_HOME, "src"))
    from nectar.config import load_config
    from nectar.io_layer import resolve_format
    from nectar.quality.report import generate_reports
    from nectar.spark_session import get_spark

    cfg = load_config(os.path.join(PROJECT_HOME, "config", "pipeline.yaml"))
    spark = get_spark(cfg, "quality-report")
    cfg.data["_resolved_format"] = resolve_format(spark, cfg.table_format)
    result = generate_reports(spark, cfg, batch_id=context["dag_run"].run_id.replace(":", "")[-12:])
    return {k: v for k, v in result.items() if k != "metrics"}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,           # Slack instead; email is not an on-call channel
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2m, 4m, 8m - rides out a transient cluster issue
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": alert_on_failure,
    "execution_timeout": timedelta(minutes=60),
    "pool": DEFAULT_POOL,
}

with DAG(
    dag_id="nectar_iot_batch",
    description="Bronze -> silver -> gold -> serving for IoT telemetry and events",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="0 * * * *",
    catchup=True,
    max_active_runs=1,
    sla_miss_callback=alert_on_sla_miss,
    tags=["nectar", "iot", "lakehouse", "delta"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # Reschedule mode frees the worker slot between pokes - important when the
    # sensor may wait most of an hour.
    wait_for_landing = FileSensor(
        task_id="wait_for_landing",
        filepath=os.path.join(PROJECT_HOME, "data", "raw", "telemetry"),
        poke_interval=120,
        timeout=60 * 45,
        mode="reschedule",
        soft_fail=False,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_run_layer,
        op_kwargs={"layer": "bronze"},
        execution_timeout=timedelta(minutes=30),
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=_run_layer,
        op_kwargs={"layer": "silver"},
        execution_timeout=timedelta(minutes=45),
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=check_quality_gate,
        retries=0,                       # a DQ breach is not transient
        execution_timeout=timedelta(minutes=10),
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=_run_layer,
        op_kwargs={"layer": "gold"},
        sla=timedelta(minutes=45),
        execution_timeout=timedelta(minutes=60),
    )

    build_hierarchy = PythonOperator(
        task_id="build_hierarchy",
        python_callable=_run_layer,
        op_kwargs={"layer": "hierarchy"},
        execution_timeout=timedelta(minutes=15),
    )

    publish = PythonOperator(
        task_id="publish_serving",
        python_callable=publish_serving,
        execution_timeout=timedelta(minutes=20),
    )

    # Runs whether or not the gate passed - a failed batch is when the report
    # matters most.
    quality_report = PythonOperator(
        task_id="quality_report",
        python_callable=emit_quality_report,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=15),
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    start >> wait_for_landing >> ingest_bronze >> build_silver >> quality_gate
    quality_gate >> build_gold >> build_hierarchy >> publish >> end
    build_silver >> quality_report >> end
