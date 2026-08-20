# Databricks notebook source
# MAGIC %md
# MAGIC # Maintenance and serving
# MAGIC
# MAGIC One notebook, four steps, selected by the `step` parameter so the job can
# MAGIC retry each independently.
# MAGIC
# MAGIC | step | what it does |
# MAGIC |---|---|
# MAGIC | `restate` | rebuild aggregates for dates that received late records |
# MAGIC | `optimize` | compact + cluster, then vacuum past retention |
# MAGIC | `prune` | drop quarantine rows past their 30-day retention |
# MAGIC | `serving` | publish the views the BI layer binds to |
# MAGIC
# MAGIC Restating comes **before** compacting, so optimisation runs once over the
# MAGIC final files rather than over files that are about to be rewritten.

# COMMAND ----------

dbutils.widgets.text("catalog", "nectar")
dbutils.widgets.text("step", "optimize")
CATALOG = dbutils.widgets.get("catalog")
STEP = dbutils.widgets.get("step")

HOT_TABLES = [
    (f"{CATALOG}.silver.silver_telemetry", "asset_id, timestamp"),
    (f"{CATALOG}.gold.fact_energy_hourly", "asset_id"),
    (f"{CATALOG}.gold.fact_event", "asset_id, event_type"),
    (f"{CATALOG}.gold.asset_closure", "ancestor_id, descendant_id"),
]

# COMMAND ----------

if STEP == "restate":
    # Ingesting a late record is the easy half. The half usually forgotten is
    # that the aggregate for that record's EVENT date is now wrong.
    from pyspark.sql import functions as F

    late = (spark.table(f"{CATALOG}.silver.silver_telemetry")
            .filter("_is_late AND event_date >= current_date() - INTERVAL 7 DAYS")
            .select("event_date").distinct())
    dates = [r["event_date"].isoformat() for r in late.collect()]
    print(f"dates needing restatement: {dates or 'none'}")
    if dates:
        # Materialized views recompute incrementally; a targeted refresh is
        # enough, and far cheaper than a full refresh of history.
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.fact_energy_hourly")
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.curated_daily_asset_utilization")
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.agg_building_daily")
        spark.sql(f"REFRESH MATERIALIZED VIEW {CATALOG}.gold.agg_site_daily")
    dbutils.jobs.taskValues.set(key="restated_dates", value=dates)

elif STEP == "optimize":
    # With Predictive Optimization enabled these are mostly no-ops, which is the
    # point: the platform decides when compaction is worth it from observed
    # query patterns rather than from a cron guess. Kept explicit so the
    # behaviour is the same on workspaces where PO is off.
    for table, cluster_by in HOT_TABLES:
        try:
            spark.sql(f"ALTER TABLE {table} CLUSTER BY ({cluster_by})")
            spark.sql(f"OPTIMIZE {table}")
            # 168 h is longer than the longest query and longer than any
            # plausible time-travel debugging session.
            spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
            print(f"optimized {table}")
        except Exception as exc:
            print(f"skipped {table}: {exc}")

elif STEP == "prune":
    # Quarantine is the fastest-growing table in the lakehouse when a device
    # goes bad. After 30 days those rows are archaeology.
    for t in ["quarantine_telemetry", "quarantine_events"]:
        try:
            spark.sql(f"DELETE FROM {CATALOG}.silver.{t} "
                      f"WHERE _quarantined_at < current_timestamp() - INTERVAL 30 DAYS")
            print(f"pruned {t}")
        except Exception as exc:
            print(f"skipped {t}: {exc}")

elif STEP == "serving":
    # Stable names for the BI layer, so dashboards never bind to a physical
    # table that the pipeline might rename or repartition.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.serving")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.serving.v_site_energy_daily AS
        SELECT site_id, site_name, city, event_date, energy_kwh,
               baseline_energy_kwh, energy_zscore, pct_vs_baseline,
               is_energy_anomaly, data_coverage_pct
        FROM {CATALOG}.gold.agg_site_daily
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.serving.v_asset_health AS
        SELECT f.asset_id, h.asset_name, h.asset_type, h.site_id, h.building_id,
               h.descendant_count AS assets_downstream, h.connectivity_status,
               f.faults, f.alarms, f.high_severity_events,
               f.mtbf_hours, f.health_score, f.risk_band
        FROM {CATALOG}.gold.curated_fault_statistics f
        LEFT JOIN {CATALOG}.gold.dim_asset_hierarchy h USING (asset_id)
    """)
    spark.sql(f"""
        CREATE OR REPLACE VIEW {CATALOG}.serving.v_silent_assets AS
        WITH anchor AS (SELECT max(timestamp) AS t FROM {CATALOG}.silver.silver_telemetry),
        last_seen AS (
            SELECT asset_id, max(timestamp) AS last_reading_at, count(*) AS lifetime_readings
            FROM {CATALOG}.silver.silver_telemetry GROUP BY asset_id)
        SELECT h.asset_id, h.asset_name, h.site_id, h.building_id,
               l.last_reading_at, l.lifetime_readings,
               CASE WHEN l.asset_id IS NULL THEN 'NEVER_REPORTED' ELSE 'SILENT' END AS status
        FROM {CATALOG}.gold.dim_asset_hierarchy h
        CROSS JOIN anchor a
        LEFT JOIN last_seen l USING (asset_id)
        WHERE h.is_leaf
          AND (l.last_reading_at IS NULL OR l.last_reading_at < a.t - INTERVAL 24 HOURS)
    """)
    print("serving views published")

else:
    raise ValueError(f"unknown step: {STEP}")
