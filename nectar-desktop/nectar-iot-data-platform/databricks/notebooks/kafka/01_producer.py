# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Kafka producer — fake IoT devices
# MAGIC
# MAGIC This notebook pretends to be the machines. It builds a site → building →
# MAGIC asset tree, then sends readings to Kafka every few seconds.
# MAGIC
# MAGIC **It breaks the data on purpose.** Bad values, copies, wrong dates,
# MAGIC unknown machines. It counts every one it breaks and prints the totals at
# MAGIC the end. Later we check the pipeline found the same numbers.
# MAGIC
# MAGIC That is the whole point: we can *prove* the pipeline works, not just say so.
# MAGIC
# MAGIC Messages are **keyed by asset_id**, so all readings for one machine land on
# MAGIC the same partition and stay in order. Ordering only matters per machine.

# COMMAND ----------

# MAGIC %pip install confluent-kafka --quiet
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("bootstrap", "", "Confluent bootstrap server")
dbutils.widgets.text("api_key", "", "API key")
dbutils.widgets.text("api_secret", "", "API secret")
dbutils.widgets.text("topic", "iot-telemetry-raw", "Topic")
dbutils.widgets.text("minutes", "5", "How many minutes to run")
dbutils.widgets.text("rate", "300", "Messages per second")

BOOTSTRAP = dbutils.widgets.get("bootstrap")
API_KEY = dbutils.widgets.get("api_key")
API_SECRET = dbutils.widgets.get("api_secret")
TOPIC = dbutils.widgets.get("topic")
RUN_MINUTES = float(dbutils.widgets.get("minutes"))
RATE = float(dbutils.widgets.get("rate"))

assert BOOTSTRAP and API_KEY and API_SECRET, "Fill in the three Confluent boxes at the top"

# COMMAND ----------
# MAGIC %md ## Build the machines

# COMMAND ----------

import json, math, random, time, uuid
from datetime import datetime, timedelta, timezone

rng = random.Random(42)

SITES = [("SITE-CBE", "Coimbatore"), ("SITE-BLR", "Bengaluru"), ("SITE-SIN", "Singapore")]

# rated power, normal temperature, vibration, what it feeds
PROFILES = {
    "Chiller":    dict(kw=320.0, temp=7.0,  vib=2.4, feeds=("AHU", 3)),
    "AHU":        dict(kw=45.0,  temp=18.5, vib=1.6, feeds=("Temp Sensor", 2)),
    "Pump":       dict(kw=22.0,  temp=32.0, vib=3.1, feeds=("Flow Sensor", 2)),
    "Compressor": dict(kw=110.0, temp=48.0, vib=4.2, feeds=None),
    "Boiler":     dict(kw=180.0, temp=82.0, vib=1.1, feeds=None),
    "Temp Sensor":dict(kw=0.02,  temp=21.0, vib=0.0, feeds=None),
    "Flow Sensor":dict(kw=0.02,  temp=24.0, vib=0.0, feeds=None),
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
                          asset_type=t, site_id=site_id, building_id=bid, parent_asset_id=None,
                          rated_power_kw=PROFILES[t]["kw"])
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

# Machines we deliberately damage, so the pipeline has something real to find.
BROKEN = set(rng.sample([a["asset_id"] for a in assets if a["asset_type"] in TOP_LEVEL], 4))
SILENT = set(rng.sample([a["asset_id"] for a in assets], 3))   # these stop sending

print(f"{len(sites_rows)} sites, {len(buildings_rows)} buildings, {len(assets)} machines")
print(f"broken on purpose: {sorted(BROKEN)}")
print(f"will go silent:    {sorted(SILENT)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save the machine list
# MAGIC
# MAGIC The pipeline needs this to check that every reading comes from a real
# MAGIC machine. A reading from an unknown machine is a problem, not data.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS nectar")
for s in ["bronze", "silver", "gold", "quality"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS nectar.{s}")

spark.createDataFrame(assets).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable("nectar.bronze.assets")
spark.createDataFrame(sites_rows).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable("nectar.bronze.sites")
spark.createDataFrame(buildings_rows).write.mode("overwrite").option("overwriteSchema", "true") \
     .saveAsTable("nectar.bronze.buildings")
print("machine list saved")

# COMMAND ----------
# MAGIC %md ## One reading

# COMMAND ----------

def occupancy(ts):
    """How busy the building is. Peaks mid-afternoon, quiet at night and weekends."""
    hour = ts.hour + ts.minute / 60
    base = math.exp(-((hour - 14.0) ** 2) / (2 * 4.2 ** 2))
    weekday = ts.weekday() < 5
    return (0.25 + 0.75 * base) * (1.0 if weekday else 0.45)

def make_reading(asset, ts):
    p = PROFILES[asset["asset_type"]]
    occ = occupancy(ts)
    mode = rng.choices(["RUNNING", "IDLE", "STANDBY", "OFF"],
                       [0.6 + 0.3 * occ, 0.2, 0.1, 0.1])[0]
    load = {"RUNNING": 1.0, "IDLE": 0.35, "STANDBY": 0.12, "OFF": 0.0}[mode] * (0.55 + 0.45 * occ)
    # A damaged machine runs hotter, shakes more and draws more power.
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
        "power_consumption": round(max(0, p["kw"] * load * (1.45 if bad else 1) * rng.gauss(1, 0.05)), 3),
        "operating_mode": mode,
    }

# COMMAND ----------
# MAGIC %md
# MAGIC ## Break some readings on purpose
# MAGIC
# MAGIC Every count is recorded. We check these numbers against the pipeline later.

# COMMAND ----------

DEFECTS = {"duplicate": 0.010, "null": 0.015, "impossible": 0.005,
           "bad_date": 0.004, "unknown_machine": 0.003, "not_json": 0.001}
broke = {k: 0 for k in DEFECTS}

def maybe_break(row):
    """Returns a list — usually one row, sometimes two (a copy), sometimes junk."""
    out = [row]
    if rng.random() < DEFECTS["duplicate"]:
        out.append(dict(row)); broke["duplicate"] += 1          # sent twice
    if rng.random() < DEFECTS["null"]:
        row[rng.choice(["temperature", "humidity", "power_consumption"])] = None
        broke["null"] += 1                                       # value missing
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
# MAGIC ## Check the connection, and make the topic
# MAGIC
# MAGIC Run this before anything else. It tells you straight away whether your
# MAGIC three Confluent values are right, and it creates the topic for you so you
# MAGIC do not have to do it in the Confluent website.

# COMMAND ----------

from confluent_kafka.admin import AdminClient, NewTopic

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
}

admin = AdminClient(conf)
try:
    meta = admin.list_topics(timeout=15)
    print("CONNECTED to", BOOTSTRAP)
    print("topics that already exist:", sorted(meta.topics)[:10] or "(none)")
except Exception as exc:
    raise SystemExit(f"COULD NOT CONNECT. Check your three values.\n{exc}")

if TOPIC not in meta.topics:
    # 6 partitions: messages are keyed by machine, so each machine's readings
    # stay in order, and six workers can read at the same time.
    admin.create_topics([NewTopic(TOPIC, num_partitions=6, replication_factor=3)])
    print(f"created topic '{TOPIC}'")
    time_to_settle = 5
    import time as _t; _t.sleep(time_to_settle)
else:
    print(f"topic '{TOPIC}' is already there")

# COMMAND ----------
# MAGIC %md ## Send to Kafka

# COMMAND ----------

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    # Do not call a reading sent until the brokers have copied it.
    "acks": "all",
    "retries": 5,
    "linger.ms": 20,
    "compression.type": "gzip",
})

start = time.time()
end = start + RUN_MINUTES * 60
sent = 0
now = datetime.now(timezone.utc)
silent_after = now + timedelta(seconds=RUN_MINUTES * 60 * 0.4)   # they die part way through

while time.time() < end:
    ts = datetime.now(timezone.utc)
    for asset in assets:
        # A dead machine sends nothing at all. You cannot check a row that
        # never arrived - that is why we also measure silence later.
        if asset["asset_id"] in SILENT and ts > silent_after:
            continue
        row = make_reading(asset, ts)
        for r in maybe_break(row):
            if rng.random() < DEFECTS["not_json"]:
                payload = b'{"timestamp": "broken", '   # cut in half on purpose
                broke["not_json"] += 1
            else:
                payload = json.dumps(r).encode()
            producer.produce(TOPIC, key=(r.get("asset_id") or "none").encode(), value=payload)
            sent += 1
    producer.poll(0)
    time.sleep(max(len(assets) / RATE, 0.5))
    if sent % 5000 < len(assets):
        print(f"  sent {sent:,}")

producer.flush()

# COMMAND ----------
# MAGIC %md ## What we broke — write this down

# COMMAND ----------

summary = {"messages_sent": sent, "machines": len(assets),
           "broken_machines": sorted(BROKEN), "silent_machines": sorted(SILENT),
           "defects_injected": broke}
print(json.dumps(summary, indent=2))

spark.createDataFrame([{
    "run_at": datetime.now(timezone.utc), "messages_sent": sent,
    "broken_machines": ", ".join(sorted(BROKEN)),
    "silent_machines": ", ".join(sorted(SILENT)),
    **{f"injected_{k}": v for k, v in broke.items()},
}]).write.mode("overwrite").option("overwriteSchema", "true") \
  .saveAsTable("nectar.quality.producer_truth")

print("\nSaved to nectar.quality.producer_truth")
print("After the pipeline runs, compare that table with what the rules caught.")
