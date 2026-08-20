# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Gold — the answers, and the proof
# MAGIC
# MAGIC Two jobs.
# MAGIC
# MAGIC **First**, build the tables people actually read: energy per machine,
# MAGIC faults per machine, machines that went quiet.
# MAGIC
# MAGIC **Second**, prove the pipeline is correct. The producer wrote down exactly
# MAGIC what it broke. We check the pipeline found the same things.
# MAGIC
# MAGIC Anyone can build a pipeline that runs. This shows ours is right.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Energy, done properly
# MAGIC
# MAGIC Machines report power — how fast they are using energy *right now*. That is
# MAGIC a speed, like km/h. To get the total you multiply by time.
# MAGIC
# MAGIC Two traps:
# MAGIC
# MAGIC 1. **Two sensors on one machine.** Both send the same power. Add them and
# MAGIC    you get double. So take the average first.
# MAGIC 2. **A gap in the readings.** If a machine goes quiet for 6 hours, do not
# MAGIC    charge it 6 hours at the last known power. Cap the gap at 10 minutes.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.asset_readings AS
WITH per_machine AS (
    -- Trap 1: average across sensors before any maths
    SELECT site_id, building_id, asset_id, ts,
           avg(power_consumption) AS power_kw,
           avg(temperature) AS temperature,
           avg(vibration) AS vibration,
           min(operating_mode) AS operating_mode
    FROM nectar.silver.telemetry
    WHERE ts IS NOT NULL
    GROUP BY site_id, building_id, asset_id, ts
)
SELECT *,
       -- Trap 2: time until the next reading, never more than 10 minutes
       least(coalesce(datediff(SECOND, ts,
             lead(ts) OVER (PARTITION BY asset_id ORDER BY ts)), 300), 600) / 3600.0
             AS hours_covered,
       power_kw * (least(coalesce(datediff(SECOND, ts,
             lead(ts) OVER (PARTITION BY asset_id ORDER BY ts)), 300), 600) / 3600.0)
             AS energy_kwh,
       date_trunc('hour', ts) AS event_hour,
       to_date(ts) AS event_date
FROM per_machine
""")

spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.energy_hourly AS
SELECT site_id, building_id, asset_id, event_hour, to_date(event_hour) AS event_date,
       round(sum(energy_kwh), 4)  AS energy_kwh,
       round(avg(power_kw), 3)    AS avg_power_kw,
       round(max(power_kw), 3)    AS peak_power_kw,
       count(*)                   AS readings
FROM nectar.gold.asset_readings
GROUP BY site_id, building_id, asset_id, event_hour
""")
print("energy tables built")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Which machines are unhealthy
# MAGIC
# MAGIC A damaged machine runs hotter, shakes harder and pulls more power. We rank
# MAGIC by how far each machine sits above the others of its own type — a chiller
# MAGIC at 30°C is broken, a boiler at 30°C is just cold.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE MATERIALIZED VIEW nectar.gold.machine_health AS
WITH stats AS (
    SELECT r.asset_id, a.asset_type, r.site_id, r.building_id,
           avg(r.temperature) AS avg_temp,
           avg(r.vibration)   AS avg_vibration,
           avg(r.power_kw)    AS avg_power,
           count(*)           AS readings
    FROM nectar.gold.asset_readings r
    JOIN nectar.bronze.assets a ON upper(trim(a.asset_id)) = r.asset_id
    GROUP BY r.asset_id, a.asset_type, r.site_id, r.building_id
),
type_normal AS (
    SELECT asset_type,
           avg(avg_vibration) AS normal_vibration,
           avg(avg_temp)      AS normal_temp
    FROM stats GROUP BY asset_type
)
SELECT s.*,
       round(s.avg_vibration / nullif(t.normal_vibration, 0), 2) AS vibration_vs_normal,
       round(s.avg_temp - t.normal_temp, 1)                      AS temp_above_normal,
       CASE WHEN s.avg_vibration > t.normal_vibration * 2 THEN 'UNHEALTHY'
            WHEN s.avg_vibration > t.normal_vibration * 1.4 THEN 'WATCH'
            ELSE 'OK' END                                        AS health
FROM stats s JOIN type_normal t ON s.asset_type = t.asset_type
ORDER BY vibration_vs_normal DESC
""")

spark.sql("""
-- Machines that stopped sending. This CANNOT be a filter on the readings table:
-- you cannot find rows that do not exist. It has to start from the machine list.
CREATE OR REPLACE VIEW nectar.gold.silent_machines AS
WITH latest AS (SELECT max(ts) AS newest FROM nectar.silver.telemetry),
last_seen AS (SELECT asset_id, max(ts) AS last_reading, count(*) AS total_readings
              FROM nectar.silver.telemetry GROUP BY asset_id)
SELECT upper(trim(a.asset_id)) AS asset_id, a.asset_name, a.asset_type, a.site_id,
       l.last_reading, l.total_readings,
       CASE WHEN l.asset_id IS NULL THEN 'NEVER SENT ANYTHING' ELSE 'WENT QUIET' END AS status
FROM nectar.bronze.assets a
CROSS JOIN latest x
LEFT JOIN last_seen l ON upper(trim(a.asset_id)) = l.asset_id
WHERE l.last_reading IS NULL OR l.last_reading < x.newest - INTERVAL 3 MINUTES
""")
print("health tables built")

# COMMAND ----------
# MAGIC %md
# MAGIC # The proof
# MAGIC
# MAGIC The producer saved what it broke. Now we compare.

# COMMAND ----------

truth = spark.table("nectar.quality.producer_truth").first()

caught = {r["reason"]: r["rows_binned"] for r in spark.sql("""
    SELECT reason, count(*) AS rows_binned
    FROM (SELECT explode(rules_failed) AS reason FROM nectar.silver.quarantine)
    GROUP BY reason""").collect()}

dead_letters = spark.table("nectar.bronze.dead_letters").count()
impossible = sum(v for k, v in caught.items() if k.startswith("impossible."))
bad_dates = caught.get("bad.date_unreadable", 0) + caught.get("bad.date_impossible", 0)

print("=" * 62)
print(f"{'WHAT WE BROKE':<26}{'BROKE':>10}{'FOUND':>10}")
print("=" * 62)
print(f"{'impossible values':<26}{truth['injected_impossible']:>10,}{impossible:>10,}")
print(f"{'bad dates':<26}{truth['injected_bad_date']:>10,}{bad_dates:>10,}")
print(f"{'unknown machines':<26}{truth['injected_unknown_machine']:>10,}{caught.get('unknown.machine', 0):>10,}")
print(f"{'not proper JSON':<26}{truth['injected_not_json']:>10,}{dead_letters:>10,}")
print("=" * 62)

# COMMAND ----------
# MAGIC %md ## Did we find the damaged machines?

# COMMAND ----------

print("Machines we damaged on purpose:")
for m in truth["broken_machines"].split(", "):
    print("   ", m)

print("\nMachines the pipeline says are UNHEALTHY:")
display(spark.sql("""
    SELECT asset_id, asset_type, health, vibration_vs_normal, temp_above_normal
    FROM nectar.gold.machine_health
    WHERE health IN ('UNHEALTHY', 'WATCH')
    ORDER BY vibration_vs_normal DESC
"""))

# COMMAND ----------
# MAGIC %md ## Did we find the silent machines?

# COMMAND ----------

print("Machines we silenced on purpose:")
for m in truth["silent_machines"].split(", "):
    print("   ", m)

print("\nMachines the pipeline says went quiet:")
display(spark.table("nectar.gold.silent_machines"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top energy users

# COMMAND ----------

display(spark.sql("""
    SELECT e.asset_id, a.asset_type, e.site_id,
           round(sum(e.energy_kwh), 2) AS total_energy_kwh,
           round(max(e.peak_power_kw), 2) AS peak_power_kw,
           -- How hard it works compared with its nameplate. A big number with a
           -- low load factor means the machine is oversized, not overworked.
           round(100 * avg(e.avg_power_kw) / nullif(a.rated_power_kw, 0), 1) AS load_factor_pct
    FROM nectar.gold.energy_hourly e
    JOIN nectar.bronze.assets a ON upper(trim(a.asset_id)) = e.asset_id
    GROUP BY e.asset_id, a.asset_type, e.site_id, a.rated_power_kw
    ORDER BY total_energy_kwh DESC LIMIT 10
"""))
