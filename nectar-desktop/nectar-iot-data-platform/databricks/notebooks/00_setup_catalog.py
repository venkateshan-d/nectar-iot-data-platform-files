# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog setup
# MAGIC
# MAGIC Run once. Creates the catalog, the medallion schemas, and the Volume that
# MAGIC acts as the landing zone for raw device files.
# MAGIC
# MAGIC **Why Unity Catalog matters more for Nectar than for a typical project:**
# MAGIC they serve multiple enterprise customers from one platform. UC lets row
# MAGIC filters and column masks be enforced at the catalog rather than
# MAGIC re-implemented in every query — so one customer's analysts cannot see
# MAGIC another customer's sites, and that holds for SQL, notebooks, BI tools and
# MAGIC Delta Sharing alike. It also gives column-level lineage, which is what
# MAGIC answers "where did this energy number come from" without a manual trace.

# COMMAND ----------

dbutils.widgets.text("catalog", "nectar")
CATALOG = dbutils.widgets.get("catalog")
VOLUME = "landing"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold", "quality", "serving"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.{VOLUME}")

base = f"/Volumes/{CATALOG}/bronze/{VOLUME}"
for sub in ["telemetry", "events", "sites", "buildings", "assets", "_checkpoints"]:
    dbutils.fs.mkdirs(f"{base}/{sub}")

print(f"landing zone ready: {base}")
print("upload the generated raw files into telemetry/ events/ assets/ sites/ buildings/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Multi-tenant row filter
# MAGIC
# MAGIC Maps an account group to the sites it may see. Attach it to the fact
# MAGIC tables after the pipeline has created them (the ALTER statements below).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.site_access(site_id STRING)
RETURN
     is_account_group_member('nectar_platform_admins')
  OR is_account_group_member(concat('customer_', lower(replace(site_id, '-', '_'))))
""")
print("row filter function created")

# COMMAND ----------
# MAGIC %md
# MAGIC Run these once the gold tables exist:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE nectar.gold.fact_energy_hourly SET ROW FILTER nectar.gold.site_access ON (site_id);
# MAGIC ALTER TABLE nectar.gold.agg_site_daily     SET ROW FILTER nectar.gold.site_access ON (site_id);
# MAGIC ```
