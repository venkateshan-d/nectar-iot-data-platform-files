# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Producer — pretend to be the machines
# MAGIC
# MAGIC This notebook is the **producer**. In a Kafka setup it would send messages
# MAGIC to a topic. Here it writes a file into a folder every few seconds.
# MAGIC
# MAGIC | Kafka | Here |
# MAGIC |---|---|
# MAGIC | producer | this notebook |
# MAGIC | topic | a folder in a Volume |
# MAGIC | consumer | notebook 2 |
# MAGIC | offset | checkpoint |
# MAGIC
# MAGIC **It breaks some readings on purpose** and counts every one. Later we check
# MAGIC the pipeline found the same numbers. That is how we *prove* it works
# MAGIC instead of just saying so.

# COMMAND ----------

dbutils.widgets.text("minutes", "4", "How many minutes to run")
dbutils.widgets.text("seconds_between_files", "8", "Seconds between files")

RUN_MINUTES = float(dbutils.widgets.get("minutes"))
GAP = float(dbutils.widgets.get("seconds_between_files"))

CATALOG = "nectar"
LANDING = f"/Volumes/{CATALOG}/bronze/landing/telemetry"

# COMMAND ----------
# MAGIC %md ## Make the catalog, schemas and the landing folder

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in ["bronze", "silver", "gold", "quality"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.checkpoints")
dbutils.fs.mkdirs(LANDING)
print("landing folder ready:", LANDING)

# COMMAND ----------
# MAGIC %md ## Build the machines

# COMMAND ----------

import json, math, random, time
from datetime import datetime, timedelta, timezone

rng = random.Random(42)

SITES = [("SITE-CBE", "Coimbatore"), ("SITE-BLR", "Bengaluru"), ("SITE-SIN", "Singapore")]

# rated power, normal temperature, normal shaking, what it feeds
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

assets, sites_rows, buildings_rows = [], [], []
for site_id, city in SITES:
    sites_rows.append(dict(site_id=site_id, site_name=f"{city} Campus", city=city, country="IN"))
    for b in range(1, 3):
        bid = f"{site_id}-BLD-{b:02d}"
        buildings_rows.append(dict(building_id=bid, building_name=f"Building {b}",
                                   site_id=site_id, floor_area_sqm=round(rng.uniform(3000, 20000), 1)))
        seq = 0
        for _ in range(4):
            t = rng.choice(TOP_LEVEL); seq += 1
            parent = dict(asset_id=f"{bid}-{t[:5].upper()}-{seq:03d}", asset_name=f"{t}-{seq:02d}",
                          asset_type=t, site_id=site_id, building_id=bid,
                          parent_asset_id=None, rated_power_kw=PROFILES[t]["kw"])
            assets.append(parent)
            if PROFILES[t]["feeds"]:
                child_type, n = PROFILES[t]["feeds"]
                for _ in range(rng.randint(1, n)):
                    seq += 1
                    assets.append(dict(asset_id=f"{bid}-{child_type[:5].upper()}-{seq:03d}",
                                       asset_name=f"{child_type}-{seq:02d}", asset_type=child_type,
                                       site_id=site_id, building_id=bid,
                                       parent_asset_id=parent["asset_id"],
                                       rated_power_kw=PROFILES[child_type]["kw"]))

# Machines we damage on purpose, so the pipeline has something real to find.
BROKEN = set(rng.sample([a["asset_id"] for a in assets if a["asset_type"] in TOP_LEVEL], 4))
# Machines that stop sending. You cannot check a row that never arrived - this
# is why the pipeline also measures silence, not just bad values.
SILENT = set(rng.sample([a["asset_id"] for a in assets], 3))

print(f"{len(sites_rows)} sites, {len(buildings_rows)} buildings, {len(assets)} machines")
print(f"damaged on purpose: {sorted(BROKEN)}")
print(f"will go silent:     {sorted(SILENT)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save the machine list
# MAGIC
# MAGIC The pipeline needs this to check that a reading comes from a real machine.
# MAGIC A reading from an unknown machine is a problem, not data.

# COMMAND ----------

spark.createDataFrame(assets).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{CATALOG}.bronze.assets")
spark.createDataFrame(sites_rows).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{CATALOG}.bronze.sites")
spark.createDataFrame(buildings_rows).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable(f"{CATALOG}.bronze.buildings")
print("machine list saved")

# COMMAND ----------
# MAGIC %md ## One reading

# COMMAND ----------

def occupancy(ts):
    """How busy the building is. Busy mid-afternoon, quiet at night and weekends."""
    hour = ts.hour + ts.minute / 60
    base = math.exp(-((hour - 14.0) ** 2) / (2 * 4.2 ** 2))
    return (0.25 + 0.75 * base) * (1.0 if ts.weekday() < 5 else 0.45)

def make_reading(asset, ts):
    p = PROFILES[asset["asset_type"]]
    occ = occupancy(ts)
    mode = rng.choices(["RUNNING", "IDLE", "STANDBY", "OFF"], [0.6 + 0.3 * occ, 0.2, 0.1, 0.1])[0]
    load = {"RUNNING": 1.0, "IDLE": 0.35, "STANDBY": 0.12, "OFF": 0.0}[mode] * (0.55 + 0.45 * occ)
    bad = asset["asset_id"] in BROKEN     # damaged: hotter, shakier, hungrier
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
        "power_consumption": round(max(0, p["kw"] * load * (1.45 if bad else 1) * rng.gauss(1, 0.05)), 3),
        "operating_mode": mode,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Break some readings on purpose
# MAGIC
# MAGIC Every count is written down. We check these against the pipeline later.

# COMMAND ----------

DEFECTS = {"duplicate": 0.010, "null": 0.015, "impossible": 0.005,
           "bad_date": 0.004, "unknown_machine": 0.003, "not_json": 0.001}
broke = {k: 0 for k in DEFECTS}

def maybe_break(row):
    """Usually returns one row. Sometimes two (a copy). Sometimes damaged."""
    out = [row]
    if rng.random() < DEFECTS["duplicate"]:
        out.append(dict(row)); broke["duplicate"] += 1
    if rng.random() < DEFECTS["null"]:
        row[rng.choice(["temperature", "humidity", "power_consumption"])] = None
        broke["null"] += 1
    if rng.random() < DEFECTS["impossible"]:
        row[rng.choice(["temperature", "humidity", "power_consumption"])] = \
            rng.choice([-999.0, 9999.99, 1e6]); broke["impossible"] += 1
    if rng.random() < DEFECTS["bad_date"]:
        row["timestamp"] = rng.choice(["not-a-date", "", "1970-01-01T00:00:00Z"])
        broke["bad_date"] += 1
    if rng.random() < DEFECTS["unknown_machine"]:
        row["asset_id"] = f"UNKNOWN-{rng.randint(1000, 9999)}"; broke["unknown_machine"] += 1
    return out

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run it — one file every few seconds
# MAGIC
# MAGIC Leave this running. Start notebook 2 in another tab and watch the rows
# MAGIC appear. **That is your video demo.**

# COMMAND ----------

start = time.time()
end = start + RUN_MINUTES * 60
file_no = 0
sent = 0
silent_after = datetime.now(timezone.utc) + timedelta(seconds=RUN_MINUTES * 60 * 0.4)

while time.time() < end:
    ts = datetime.now(timezone.utc)
    lines = []
    for asset in assets:
        if asset["asset_id"] in SILENT and ts > silent_after:
            continue                      # dead machine sends nothing
        for r in maybe_break(make_reading(asset, ts)):
            if rng.random() < DEFECTS["not_json"]:
                lines.append('{"timestamp": "broken", ')   # cut in half on purpose
                broke["not_json"] += 1
            else:
                lines.append(json.dumps(r))
            sent += 1
    file_no += 1
    # Write to a temporary name first, then rename. A reader must never see a
    # half-written file.
    tmp = f"{LANDING}/_tmp_{file_no:05d}.json"
    final = f"{LANDING}/batch_{file_no:05d}.json"
    dbutils.fs.put(tmp, "\n".join(lines), overwrite=True)
    dbutils.fs.mv(tmp, final)
    print(f"  file {file_no:>3}   rows so far: {sent:,}")
    time.sleep(GAP)

# COMMAND ----------
# MAGIC %md ## What we broke — this is the proof sheet

# COMMAND ----------

print(json.dumps({"files_written": file_no, "rows_written": sent,
                  "damaged_machines": sorted(BROKEN), "silent_machines": sorted(SILENT),
                  "defects_injected": broke}, indent=2))

spark.createDataFrame([{
    "run_at": datetime.now(timezone.utc), "messages_sent": sent,
    "broken_machines": ", ".join(sorted(BROKEN)),
    "silent_machines": ", ".join(sorted(SILENT)),
    **{f"injected_{k}": v for k, v in broke.items()},
}]).write.mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable(f"{CATALOG}.quality.producer_truth")

print("\nSaved to nectar.quality.producer_truth")
print("Notebook 4 compares this with what the pipeline actually caught.")
