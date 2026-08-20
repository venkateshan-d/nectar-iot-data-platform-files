# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Seed the landing zone
# MAGIC
# MAGIC The Lakeflow pipeline (`01_bronze` → `02_silver` → `03_gold`) reads files
# MAGIC from a Unity Catalog Volume. This notebook puts files there, so the
# MAGIC pipeline has something to run on in a fresh workspace.
# MAGIC
# MAGIC It writes the same shapes the portable generator writes locally:
# MAGIC
# MAGIC | Path | Format | What |
# MAGIC |---|---|---|
# MAGIC | `landing/sites` · `buildings` · `assets` | CSV | reference data |
# MAGIC | `landing/telemetry` | JSON | historical readings |
# MAGIC | `landing/events` | JSON | alarms, warnings, faults |
# MAGIC
# MAGIC **Defects are injected on purpose and counted**, and the counts are saved to
# MAGIC `nectar.quality.seed_truth`. That is what makes the quality framework's
# MAGIC output checkable rather than merely believable.
# MAGIC
# MAGIC Run this **once**, before the pipeline. It takes about a minute.

# COMMAND ----------

dbutils.widgets.text("catalog", "nectar", "Catalog")
dbutils.widgets.text("days", "7", "Days of history")
dbutils.widgets.text("interval_minutes", "15", "Minutes between readings")

CATALOG = dbutils.widgets.get("catalog")
DAYS = int(dbutils.widgets.get("days"))
INTERVAL = int(dbutils.widgets.get("interval_minutes"))
LANDING = f"/Volumes/{CATALOG}/bronze/landing"

# COMMAND ----------
# MAGIC %md ## Catalog, schemas, Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ["bronze", "silver", "gold", "quality"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
for sub in ["telemetry", "events", "sites", "buildings", "assets", "_checkpoints"]:
    dbutils.fs.mkdirs(f"{LANDING}/{sub}")
print("landing zone ready:", LANDING)

# COMMAND ----------
# MAGIC %md
# MAGIC ## The estate
# MAGIC
# MAGIC Three levels deep — chillers feed AHUs feed temperature sensors, pumps feed
# MAGIC flow sensors — because a flat asset list would make the hierarchy task
# MAGIC trivial and the roll-ups meaningless.

# COMMAND ----------

import json, math, random
from datetime import datetime, timedelta, timezone

rng = random.Random(42)

SITES = [("SITE-CBE", "Coimbatore"), ("SITE-BLR", "Bengaluru"), ("SITE-SIN", "Singapore")]

PROFILES = {
    "Chiller":     dict(kw=320.0, temp=7.0,  vib=2.4, feeds=("AHU", 3)),
    "AHU":         dict(kw=45.0,  temp=18.5, vib=1.6, feeds=("Temp Sensor", 2)),
    "Pump":        dict(kw=22.0,  temp=32.0, vib=3.1, feeds=("Flow Sensor", 2)),
    "Compressor":  dict(kw=110.0, temp=48.0, vib=4.2, feeds=None),
    "Boiler":      dict(kw=180.0, temp=82.0, vib=1.1, feeds=None),
    "Temp Sensor": dict(kw=0.02,  temp=21.0, vib=0.0, feeds=None),
    "Flow Sensor": dict(kw=0.02,  temp=24.0, vib=0.0, feeds=None),
}
TOP_LEVEL = ["Chiller", "Pump", "Compressor", "Boiler"]
MANUFACTURERS = ["Carrier", "Trane", "Daikin", "Grundfos", "Siemens"]

assets, sites_rows, buildings_rows = [], [], []
for site_id, city in SITES:
    sites_rows.append(dict(site_id=site_id, site_name=f"{city} Campus",
                           city=city, country="IN", timezone="Asia/Kolkata"))
    for b in range(1, 3):
        bid = f"{site_id}-BLD-{b:02d}"
        buildings_rows.append(dict(building_id=bid, building_name=f"Building {b}",
                                   site_id=site_id,
                                   floor_area_sqm=round(rng.uniform(3000, 20000), 1)))
        seq = 0
        for _ in range(4):
            t = rng.choice(TOP_LEVEL); seq += 1
            parent = dict(asset_id=f"{bid}-{t[:5].upper()}-{seq:03d}",
                          asset_name=f"{t}-{seq:02d}", asset_type=t,
                          manufacturer=rng.choice(MANUFACTURERS),
                          installation_date=str((datetime(2019, 1, 1)
                                                 + timedelta(days=rng.randint(0, 2000))).date()),
                          site_id=site_id, building_id=bid,
                          parent_asset_id=None, rated_power_kw=PROFILES[t]["kw"])
            assets.append(parent)
            if PROFILES[t]["feeds"]:
                child_type, n = PROFILES[t]["feeds"]
                for _ in range(rng.randint(1, n)):
                    seq += 1
                    assets.append(dict(asset_id=f"{bid}-{child_type[:5].upper()}-{seq:03d}",
                                       asset_name=f"{child_type}-{seq:02d}", asset_type=child_type,
                                       manufacturer=rng.choice(MANUFACTURERS),
                                       installation_date=str((datetime(2020, 1, 1)
                                                              + timedelta(days=rng.randint(0, 1500))).date()),
                                       site_id=site_id, building_id=bid,
                                       parent_asset_id=parent["asset_id"],
                                       rated_power_kw=PROFILES[child_type]["kw"]))

# Assets we damage on purpose, so Q3 and the health score have something real to
# find. Assets that go silent, because no row-level rule can validate a record
# that never arrived - silence has to be measured separately.
BROKEN = set(rng.sample([a["asset_id"] for a in assets if a["asset_type"] in TOP_LEVEL], 4))
SILENT = set(rng.sample([a["asset_id"] for a in assets], 3))

print(f"{len(sites_rows)} sites · {len(buildings_rows)} buildings · {len(assets)} assets")
print("damaged on purpose:", sorted(BROKEN))
print("will go silent:    ", sorted(SILENT))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Reference data → CSV
# MAGIC
# MAGIC CSV because the asset register arrives as an export from the CMMS, not as
# MAGIC a stream. Whole snapshot each time, so the pipeline reads it as a
# MAGIC materialized view rather than a streaming table.

# COMMAND ----------

def write_csv(rows, name):
    (spark.createDataFrame(rows).coalesce(1)
        .write.mode("overwrite").option("header", True)
        .csv(f"{LANDING}/{name}"))
    print(f"{name:<12} {len(rows):>5} rows")

write_csv(sites_rows, "sites")
write_csv(buildings_rows, "buildings")
write_csv(assets, "assets")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Readings
# MAGIC
# MAGIC Values follow the operating mode and a daily occupancy curve rather than
# MAGIC uniform noise, so the aggregates and the anomaly detection mean something.

# COMMAND ----------

def occupancy(ts):
    hour = ts.hour + ts.minute / 60
    base = math.exp(-((hour - 14.0) ** 2) / (2 * 4.2 ** 2))
    return (0.25 + 0.75 * base) * (1.0 if ts.weekday() < 5 else 0.45)

def make_reading(asset, ts, surge=1.0):
    p = PROFILES[asset["asset_type"]]
    occ = occupancy(ts)
    mode = rng.choices(["RUNNING", "IDLE", "STANDBY", "OFF"],
                       [0.6 + 0.3 * occ, 0.2, 0.1, 0.1])[0]
    load = {"RUNNING": 1.0, "IDLE": 0.35, "STANDBY": 0.12, "OFF": 0.0}[mode] * (0.55 + 0.45 * occ)
    bad = asset["asset_id"] in BROKEN
    return {
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "site_id": asset["site_id"],
        "building_id": asset["building_id"],
        "asset_id": asset["asset_id"],
        "sensor_id": f"{asset['asset_id']}-S01",
        "temperature": round(rng.gauss(p["temp"] + (6 if bad else 0) + 4 * occ, 1.5), 2),
        "humidity": round(min(99, max(8, rng.gauss(52 - 8 * occ, 6))), 2),
        "pressure": round(max(0, rng.gauss(300 * (0.8 + 0.2 * load), 12)), 2),
        "vibration": round(max(0, rng.gauss(p["vib"] * (0.4 + 0.6 * load) * (3.2 if bad else 1), 0.35)), 3),
        "power_consumption": round(max(0, p["kw"] * load * (1.45 if bad else 1) * surge * rng.gauss(1, 0.05)), 3),
        "operating_mode": mode,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Break some of them on purpose, and count every one

# COMMAND ----------

DEFECTS = {"duplicate": 0.010, "null": 0.015, "impossible": 0.005,
           "bad_date": 0.004, "unknown_asset": 0.003}
broke = {k: 0 for k in DEFECTS}

def maybe_break(row):
    out = [row]
    if rng.random() < DEFECTS["duplicate"]:
        out.append(dict(row)); broke["duplicate"] += 1
    if rng.random() < DEFECTS["null"]:
        row[rng.choice(["temperature", "humidity", "power_consumption"])] = None
        broke["null"] += 1
    if rng.random() < DEFECTS["impossible"]:
        row[rng.choice(["temperature", "humidity", "power_consumption"])] = \
            rng.choice([-999.0, 9999.99, 1000000.0]); broke["impossible"] += 1
    if rng.random() < DEFECTS["bad_date"]:
        row["timestamp"] = rng.choice(["not-a-date", "", "1970-01-01T00:00:00Z"])
        broke["bad_date"] += 1
    if rng.random() < DEFECTS["unknown_asset"]:
        row["asset_id"] = f"GHOST-{rng.randint(1000, 9999)}"
        broke["unknown_asset"] += 1
    return out

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate the history
# MAGIC
# MAGIC Two site-wide consumption excursions are injected on a known day, so Q6 has
# MAGIC a right answer. Silent assets stop reporting partway through, so freshness
# MAGIC has one too.

# COMMAND ----------

end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
start = (end - timedelta(days=DAYS)).replace(hour=0)
surge_day = (end - timedelta(days=2)).date()
SURGE_SITES = {"SITE-CBE", "SITE-SIN"}
silence_from = end - timedelta(hours=30)

rows = []
ts = start
step = timedelta(minutes=INTERVAL)
while ts < end:
    for a in assets:
        if a["asset_id"] in SILENT and ts >= silence_from:
            continue
        surge = 1.35 if (ts.date() == surge_day and a["site_id"] in SURGE_SITES) else 1.0
        rows.extend(maybe_break(make_reading(a, ts, surge)))
    ts += step

print(f"{len(rows):,} readings from {start:%Y-%m-%d} to {end:%Y-%m-%d %H:%M}")
print("injected:", broke)
print("power surge day:", surge_day, "sites:", sorted(SURGE_SITES))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Events
# MAGIC
# MAGIC Correlated with the telemetry, not independent of it — damaged assets
# MAGIC produce roughly four times the fault rate, which is what makes Q3
# MAGIC ("more than 10 faults in 30 days") return the damaged assets and nothing
# MAGIC else.

# COMMAND ----------

events, n = [], 0
for a in assets:
    base = 18 if a["asset_id"] in BROKEN else 4
    for _ in range(rng.randint(base // 2, base)):
        n += 1
        when = start + timedelta(seconds=rng.randint(0, int((end - start).total_seconds())))
        etype = rng.choices(["Fault", "Alarm", "Warning"],
                            [0.55, 0.25, 0.20] if a["asset_id"] in BROKEN else [0.2, 0.3, 0.5])[0]
        events.append({
            "event_id": f"EVT-{n:07d}",
            "timestamp": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "asset_id": a["asset_id"],
            "event_type": etype,
            "severity": {"Fault": "High", "Alarm": "Medium", "Warning": "Low"}[etype],
            "message": f"{etype} on {a['asset_name']}",
        })
print(f"{len(events):,} events")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write the JSON files
# MAGIC
# MAGIC Written as text, one JSON object per line, because that is the shape a
# MAGIC gateway actually delivers — and because a malformed line must survive as a
# MAGIC malformed line rather than being fixed on the way in.

# COMMAND ----------

def write_json(records, name):
    (spark.createDataFrame([(json.dumps(r),) for r in records], "value string")
        .coalesce(4).write.mode("overwrite").text(f"{LANDING}/{name}"))
    print(f"{name:<12} {len(records):>8,} lines")

write_json(rows, "telemetry")
write_json(events, "events")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save the proof sheet
# MAGIC
# MAGIC The whole point. After the pipeline runs, compare this against
# MAGIC `nectar.quality.dq_results`. The framework must rediscover these counts
# MAGIC without ever being told them.

# COMMAND ----------

truth = [dict(item=k, injected=v) for k, v in sorted(broke.items())]
truth += [
    dict(item="silent_assets", injected=len(SILENT)),
    dict(item="damaged_assets", injected=len(BROKEN)),
    dict(item="surge_sites", injected=len(SURGE_SITES)),
    dict(item="telemetry_rows", injected=len(rows)),
    dict(item="events", injected=len(events)),
]
spark.createDataFrame(truth).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{CATALOG}.quality.seed_truth")

display(spark.table(f"{CATALOG}.quality.seed_truth"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## What is in the Volume now

# COMMAND ----------

for sub in ["sites", "buildings", "assets", "telemetry", "events"]:
    files = [f for f in dbutils.fs.ls(f"{LANDING}/{sub}") if not f.name.startswith("_")]
    print(f"{sub:<12} {len(files):>3} files  {sum(f.size for f in files)/1e6:>8.1f} MB")

print("\nNext: run the Lakeflow pipeline (01_bronze / 02_silver / 03_gold).")
