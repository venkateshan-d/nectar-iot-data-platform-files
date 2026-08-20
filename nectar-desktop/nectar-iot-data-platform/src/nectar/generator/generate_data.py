"""Synthetic IoT dataset generator.

The challenge does not ship sample data, so this produces a realistic one:

* a **site -> building -> asset -> sub-asset** hierarchy (chillers feeding AHUs,
  pumps feeding flow sensors), which is what Task 4 needs;
* telemetry whose values follow the asset's *operating mode* and a daily
  occupancy curve rather than being uniform noise, so the gold aggregates and
  the anomaly query actually mean something;
* operational events correlated with the telemetry - a vibration excursion is
  followed by a Fault event on the same asset;
* deliberately injected defects (duplicates, nulls, outliers, unparseable
  timestamps, unknown asset ids, late arrivals) so the Task 5 quality framework
  has real work to do. Rates are configurable in ``config/pipeline.yaml``.

Output layout (raw zone, exactly what a landing bucket looks like)::

    data/raw/
      sites/sites.csv
      buildings/buildings.csv
      assets/assets.csv
      telemetry/ingest_date=YYYY-MM-DD/telemetry-*.jsonl
      events/ingest_date=YYYY-MM-DD/events-*.jsonl

Run: ``python -m nectar.generator.generate_data --days 7``
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

from ..config import Config, load_config
from ..logging_utils import setup_logging

LOG = logging.getLogger("nectar.generator")

# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------
SITE_CATALOG = [
    ("SITE-CBE", "Nectar Coimbatore Campus", "Coimbatore", "IN", "Asia/Kolkata", "CUST-1001"),
    ("SITE-BLR", "Whitefield Tech Park", "Bengaluru", "IN", "Asia/Kolkata", "CUST-1002"),
    ("SITE-SIN", "Changi Business Hub", "Singapore", "SG", "Asia/Singapore", "CUST-1003"),
    ("SITE-DXB", "Dubai Media City Tower", "Dubai", "AE", "Asia/Dubai", "CUST-1004"),
]

BUILDING_TYPES = ["Office", "Data Centre", "Manufacturing", "Retail"]

# asset_type -> (rated kW, base temp C, temp spread, vibration base, has_children)
ASSET_PROFILES: Dict[str, dict] = {
    "Chiller":     dict(rated_kw=320.0, temp=7.0,  temp_sd=1.2, vib=2.4, pressure=850.0, children=("AHU", 3)),
    "AHU":         dict(rated_kw=45.0,  temp=18.5, temp_sd=1.8, vib=1.6, pressure=210.0, children=("Temp Sensor", 2)),
    "Pump":        dict(rated_kw=22.0,  temp=32.0, temp_sd=2.0, vib=3.1, pressure=420.0, children=("Flow Sensor", 2)),
    "Compressor":  dict(rated_kw=110.0, temp=48.0, temp_sd=3.0, vib=4.2, pressure=900.0, children=("Pressure Sensor", 1)),
    "Boiler":      dict(rated_kw=180.0, temp=82.0, temp_sd=4.0, vib=1.1, pressure=650.0, children=None),
    "UPS":         dict(rated_kw=90.0,  temp=27.0, temp_sd=1.0, vib=0.4, pressure=101.0, children=None),
    "Temp Sensor": dict(rated_kw=0.02,  temp=21.0, temp_sd=1.5, vib=0.0, pressure=101.0, children=None),
    "Flow Sensor": dict(rated_kw=0.02,  temp=24.0, temp_sd=1.0, vib=0.0, pressure=310.0, children=None),
    "Pressure Sensor": dict(rated_kw=0.02, temp=26.0, temp_sd=1.0, vib=0.0, pressure=880.0, children=None),
}

PARENT_TYPES = ["Chiller", "Pump", "Compressor", "Boiler", "UPS"]

MANUFACTURERS = ["Carrier", "Trane", "Daikin", "Grundfos", "Atlas Copco", "Siemens", "Honeywell", "Schneider"]

OPERATING_MODES = ["RUNNING", "IDLE", "STANDBY", "BOOST", "MAINTENANCE", "OFF"]

FAULT_MESSAGES = {
    "Fault": [
        "Compressor discharge pressure exceeded safe limit",
        "Bearing vibration above ISO 10816 zone C",
        "Refrigerant leak detected on circuit A",
        "Motor overload trip",
        "Loss of flow across evaporator",
    ],
    "Alarm": [
        "Supply air temperature deviation > 4 C",
        "Filter differential pressure high",
        "Condenser approach temperature high",
        "Power factor below contractual threshold",
    ],
    "Warning": [
        "Scheduled maintenance overdue by 14 days",
        "Runtime hours approaching service interval",
        "Humidity outside comfort band",
        "Sensor drift suspected - recalibration advised",
    ],
    "Info": [
        "Operating mode changed by BMS schedule",
        "Firmware updated to 4.2.1",
        "Setpoint updated by facility operator",
    ],
}


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------
@dataclass
class Site:
    site_id: str
    site_name: str
    city: str
    country: str
    timezone: str
    customer_id: str


@dataclass
class Building:
    building_id: str
    building_name: str
    site_id: str
    floor_area_sqm: float
    building_type: str


@dataclass
class Asset:
    asset_id: str
    asset_name: str
    asset_type: str
    manufacturer: str
    model: str
    installation_date: str
    rated_power_kw: float
    site_id: str
    building_id: str
    parent_asset_id: Optional[str]


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class IoTDataGenerator:
    def __init__(self, cfg: Config, seed: Optional[int] = None, days: Optional[int] = None):
        self.cfg = cfg
        gen = cfg.get("generator", {})
        self.rng = random.Random(seed if seed is not None else gen.get("seed", 42))
        self.n_sites = min(int(gen.get("sites", 3)), len(SITE_CATALOG))
        self.buildings_per_site = int(gen.get("buildings_per_site", 2))
        self.assets_per_building = int(gen.get("assets_per_building", 9))
        self.days = int(days if days is not None else gen.get("days", 7))
        self.interval = int(gen.get("telemetry_interval_minutes", 5))
        self.event_rate = float(gen.get("event_rate_per_asset_per_day", 1.8))
        self.defects = gen.get("defects", {}) or {}

        # Truncate to the sampling interval so the newest reading is "just now";
        # otherwise every asset would look stale to the freshness check.
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        self.end = now - timedelta(minutes=now.minute % max(int(gen.get("telemetry_interval_minutes", 5)), 1))
        # Start at midnight so that every day in the window except the current
        # one is complete; a half-observed boundary day would otherwise look
        # like a consumption anomaly to the Q6 detector.
        self.start = (self.end - timedelta(days=self.days)).replace(hour=0, minute=0)

        self.sites: List[Site] = []
        self.buildings: List[Building] = []
        self.assets: List[Asset] = []
        #: assets that carry sensors and therefore emit telemetry
        self.telemetry_assets: List[Asset] = []
        #: asset_id -> list of sensor ids
        self.sensors: Dict[str, List[str]] = {}
        #: assets pushed into a degraded regime for part of the window, so the
        #: anomaly / fault queries have a real signal to find
        self.degraded: Dict[str, Tuple[datetime, datetime]] = {}
        #: assets that stop reporting entirely part-way through - the "silent
        #: device" case that SQL challenge Q4 has to detect
        self.n_silent = int(gen.get("silent_assets", 4))
        self.silent_from: Dict[str, datetime] = {}
        #: site-wide consumption excursions (a failed BMS schedule, a chiller
        #: plant stuck in manual override). These give the Q6 anomaly detector
        #: a known-correct signal to find.
        self.n_site_anomalies = int(gen.get("site_anomalies", 2))
        self.site_anomaly_factor = float(gen.get("site_anomaly_factor", 1.35))
        self.site_anomalies: Dict[str, Tuple[datetime, datetime]] = {}

    # -- topology ----------------------------------------------------------
    def build_topology(self) -> None:
        for s_idx in range(self.n_sites):
            sid, sname, city, country, tz, cust = SITE_CATALOG[s_idx]
            self.sites.append(Site(sid, sname, city, country, tz, cust))

            for b_idx in range(1, self.buildings_per_site + 1):
                bid = f"{sid}-BLD-{b_idx:02d}"
                self.buildings.append(
                    Building(
                        building_id=bid,
                        building_name=f"{sname.split()[0]} Building {b_idx}",
                        site_id=sid,
                        floor_area_sqm=round(self.rng.uniform(2500, 24000), 1),
                        building_type=self.rng.choice(BUILDING_TYPES),
                    )
                )
                self._build_assets_for_building(sid, bid)

        # A handful of assets run degraded for a contiguous window.
        candidates = [a for a in self.telemetry_assets if a.asset_type in PARENT_TYPES]
        for asset in self.rng.sample(candidates, k=max(1, len(candidates) // 6)):
            offset = self.rng.uniform(0.35, 0.75) * self.days
            begin = self.start + timedelta(days=offset)
            self.degraded[asset.asset_id] = (begin, begin + timedelta(days=self.rng.uniform(0.8, 2.0)))

        # Site-wide excursions, placed in the back half of the window so a
        # trailing baseline exists to compare them against.
        for site in self.rng.sample(self.sites, k=min(self.n_site_anomalies, len(self.sites))):
            offset = self.rng.uniform(0.55, 0.9) * self.days
            begin = self.start + timedelta(days=offset)
            self.site_anomalies[site.site_id] = (begin, begin + timedelta(days=self.rng.uniform(1.0, 2.0)))

        # A few devices simply go offline and never come back inside the window.
        for asset in self.rng.sample(self.telemetry_assets, k=min(self.n_silent, len(self.telemetry_assets))):
            self.silent_from[asset.asset_id] = self.end - timedelta(hours=self.rng.uniform(26, 70))

        LOG.info(
            "topology: %d sites, %d buildings, %d assets (%d emit telemetry), %d degraded, %d silent",
            len(self.sites), len(self.buildings), len(self.assets),
            len(self.telemetry_assets), len(self.degraded), len(self.silent_from),
        )
        LOG.info("site-wide anomalies: %s", {k: v[0].date().isoformat() for k, v in self.site_anomalies.items()})

    def _build_assets_for_building(self, site_id: str, building_id: str) -> None:
        seq = 0
        n_parents = max(2, self.assets_per_building // 3)
        for _ in range(n_parents):
            atype = self.rng.choice(PARENT_TYPES)
            seq += 1
            parent = self._make_asset(site_id, building_id, atype, seq, None)
            self.assets.append(parent)
            self.telemetry_assets.append(parent)
            self._attach_sensors(parent)

            child_spec = ASSET_PROFILES[atype]["children"]
            if not child_spec:
                continue
            child_type, max_children = child_spec
            for _ in range(self.rng.randint(1, max_children)):
                seq += 1
                child = self._make_asset(site_id, building_id, child_type, seq, parent.asset_id)
                self.assets.append(child)
                self.telemetry_assets.append(child)
                self._attach_sensors(child)

                # Third level: AHUs carry their own temperature sensors.
                grand_spec = ASSET_PROFILES[child_type]["children"]
                if grand_spec and self.rng.random() < 0.6:
                    g_type, g_max = grand_spec
                    for _ in range(self.rng.randint(1, g_max)):
                        seq += 1
                        gc = self._make_asset(site_id, building_id, g_type, seq, child.asset_id)
                        self.assets.append(gc)
                        self.telemetry_assets.append(gc)
                        self._attach_sensors(gc)

        # Two orphan assets per site's first building: commissioned in the asset
        # register but never wired into the hierarchy. Task 4 must surface them.
        if building_id.endswith("BLD-01"):
            for _ in range(2):
                seq += 1
                orphan = self._make_asset(site_id, building_id, "UPS", seq, "ASSET-DOES-NOT-EXIST")
                self.assets.append(orphan)

    def _make_asset(self, site_id: str, building_id: str, atype: str, seq: int, parent: Optional[str]) -> Asset:
        profile = ASSET_PROFILES[atype]
        slug = atype.replace(" ", "").upper()[:5]
        asset_id = f"{building_id}-{slug}-{seq:03d}"
        install = date(2018, 1, 1) + timedelta(days=self.rng.randint(0, 2400))
        return Asset(
            asset_id=asset_id,
            asset_name=f"{atype}-{seq:02d}",
            asset_type=atype,
            manufacturer=self.rng.choice(MANUFACTURERS),
            model=f"{self.rng.choice('XZKPQ')}{self.rng.randint(100, 999)}",
            installation_date=install.isoformat(),
            rated_power_kw=profile["rated_kw"],
            site_id=site_id,
            building_id=building_id,
            parent_asset_id=parent,
        )

    def _attach_sensors(self, asset: Asset) -> None:
        n = 1 if "Sensor" in asset.asset_type else self.rng.randint(1, 2)
        self.sensors[asset.asset_id] = [f"{asset.asset_id}-S{i:02d}" for i in range(1, n + 1)]

    # -- telemetry ---------------------------------------------------------
    def _occupancy(self, ts: datetime) -> float:
        """0..1 demand curve: business hours peak, weekends damped."""
        hour = ts.hour + ts.minute / 60.0
        weekday = ts.weekday() < 5
        base = math.exp(-((hour - 14.0) ** 2) / (2 * 4.2 ** 2))
        return (0.25 + 0.75 * base) * (1.0 if weekday else 0.45)

    def _mode_for(self, asset: Asset, ts: datetime, occ: float) -> str:
        if "Sensor" in asset.asset_type:
            return "RUNNING"
        r = self.rng.random()
        if r < 0.012:
            return "MAINTENANCE"
        if occ > 0.85:
            return "BOOST" if r < 0.18 else "RUNNING"
        if occ > 0.45:
            return "RUNNING" if r < 0.88 else "IDLE"
        if occ > 0.3:
            return "IDLE" if r < 0.6 else "STANDBY"
        return "OFF" if r < 0.35 else "STANDBY"

    def _reading(self, asset: Asset, sensor_id: str, ts: datetime) -> dict:
        p = ASSET_PROFILES[asset.asset_type]
        occ = self._occupancy(ts)
        mode = self._mode_for(asset, ts, occ)
        load = {"BOOST": 1.15, "RUNNING": 1.0, "IDLE": 0.35, "STANDBY": 0.12,
                "MAINTENANCE": 0.05, "OFF": 0.0}[mode] * (0.55 + 0.45 * occ)

        degraded_window = self.degraded.get(asset.asset_id)
        degraded = bool(degraded_window and degraded_window[0] <= ts <= degraded_window[1])
        site_window = self.site_anomalies.get(asset.site_id)
        site_excursion = bool(site_window and site_window[0] <= ts <= site_window[1])
        deg_power = (1.45 if degraded else 1.0) * (self.site_anomaly_factor if site_excursion else 1.0)
        deg_vib = 3.2 if degraded else 1.0
        deg_temp = 6.0 if degraded else 0.0

        temp = self.rng.gauss(p["temp"] + deg_temp + 4.0 * occ, p["temp_sd"])
        humidity = min(99.0, max(8.0, self.rng.gauss(52.0 - 8.0 * occ, 6.0)))
        pressure = max(0.0, self.rng.gauss(p["pressure"] * (0.8 + 0.2 * load), p["pressure"] * 0.03))
        vibration = max(0.0, self.rng.gauss(p["vib"] * (0.4 + 0.6 * load) * deg_vib, 0.35))
        power = max(0.0, p["rated_kw"] * load * deg_power * self.rng.gauss(1.0, 0.05))

        return {
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "site_id": asset.site_id,
            "building_id": asset.building_id,
            "asset_id": asset.asset_id,
            "sensor_id": sensor_id,
            "temperature": round(temp, 2),
            "humidity": round(humidity, 2),
            "pressure": round(pressure, 2),
            "vibration": round(vibration, 3),
            "power_consumption": round(power, 3),
            "operating_mode": mode,
        }

    def iter_telemetry(self) -> Iterator[dict]:
        step = timedelta(minutes=self.interval)
        ts = self.start
        while ts < self.end:
            for asset in self.telemetry_assets:
                # A device that has gone offline stops reporting entirely; this
                # is what SQL challenge Q4 ("assets silent for 24h") detects.
                silent_at = self.silent_from.get(asset.asset_id)
                if silent_at and ts >= silent_at:
                    continue
                for sensor_id in self.sensors[asset.asset_id]:
                    # Transient packet loss on an otherwise healthy device.
                    if self.rng.random() < 0.004:
                        continue
                    yield self._reading(asset, sensor_id, ts)
            ts += step

    # -- events ------------------------------------------------------------
    def iter_events(self) -> Iterator[dict]:
        total_minutes = int((self.end - self.start).total_seconds() // 60)
        for asset in self.telemetry_assets:
            degraded_window = self.degraded.get(asset.asset_id)
            expected = self.event_rate * self.days
            if degraded_window:
                expected *= 4.0
            n = max(0, int(self.rng.gauss(expected, expected * 0.35)))
            for _ in range(n):
                if degraded_window and self.rng.random() < 0.7:
                    span = (degraded_window[1] - degraded_window[0]).total_seconds()
                    ts = degraded_window[0] + timedelta(seconds=self.rng.uniform(0, span))
                else:
                    ts = self.start + timedelta(minutes=self.rng.uniform(0, total_minutes))

                if degraded_window and degraded_window[0] <= ts <= degraded_window[1]:
                    etype = self.rng.choices(["Fault", "Alarm", "Warning", "Info"], [0.5, 0.3, 0.15, 0.05])[0]
                else:
                    etype = self.rng.choices(["Fault", "Alarm", "Warning", "Info"], [0.12, 0.28, 0.42, 0.18])[0]

                severity = {
                    "Fault": self.rng.choices(["High", "Medium"], [0.75, 0.25])[0],
                    "Alarm": self.rng.choices(["High", "Medium", "Low"], [0.3, 0.5, 0.2])[0],
                    "Warning": self.rng.choices(["Medium", "Low"], [0.4, 0.6])[0],
                    "Info": "Low",
                }[etype]

                yield {
                    "event_id": f"EVT-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16].upper()}",
                    "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "asset_id": asset.asset_id,
                    "event_type": etype,
                    "severity": severity,
                    "message": self.rng.choice(FAULT_MESSAGES[etype]),
                }

    # -- defect injection --------------------------------------------------
    def inject_defects(self, rows: List[dict], kind: str) -> List[dict]:
        """Corrupt a configurable slice of *rows* in place-ish.

        Each defect maps to a rule in the Task 5 quality framework, so the
        report is verifiable end to end: the generator knows how many bad rows
        it created and the engine should find approximately that many.
        """
        d = self.defects
        out = list(rows)
        n = len(out)
        stats = {}

        # 1. Duplicates - at-least-once delivery from the gateway.
        rate = float(d.get("duplicate_rate", 0))
        if rate > 0 and n:
            dupes = [dict(out[self.rng.randrange(n)]) for _ in range(int(n * rate))]
            out.extend(dupes)
            stats["duplicates"] = len(dupes)

        # 2. Nulls on measure columns - a sensor that briefly stops reporting.
        rate = float(d.get("null_rate", 0))
        nullable = (["temperature", "humidity", "pressure", "vibration", "power_consumption", "operating_mode"]
                    if kind == "telemetry" else ["severity", "message"])
        if rate > 0:
            for idx in self.rng.sample(range(len(out)), int(len(out) * rate)):
                out[idx] = dict(out[idx])
                out[idx][self.rng.choice(nullable)] = None
            stats["nulls"] = int(len(out) * rate)

        if kind == "telemetry":
            # 3. Physically impossible readings - stuck ADC / sensor failure.
            rate = float(d.get("outlier_rate", 0))
            if rate > 0:
                for idx in self.rng.sample(range(len(out)), int(len(out) * rate)):
                    out[idx] = dict(out[idx])
                    col = self.rng.choice(["temperature", "humidity", "power_consumption", "pressure"])
                    out[idx][col] = self.rng.choice([-999.0, 9999.99, 1e6, -45000.0])
                stats["outliers"] = int(len(out) * rate)

        # 4. Unparseable / absurd timestamps.
        rate = float(d.get("invalid_timestamp_rate", 0))
        if rate > 0:
            broken = ["not-a-timestamp", "", "0000-00-00T00:00:00Z", "1970-01-01T00:00:00Z", "2999-12-31T23:59:59Z"]
            for idx in self.rng.sample(range(len(out)), int(len(out) * rate)):
                out[idx] = dict(out[idx])
                out[idx]["timestamp"] = self.rng.choice(broken)
            stats["invalid_timestamps"] = int(len(out) * rate)

        # 5. Referential integrity breaks - a device provisioned in the field
        #    but not yet in the asset register.
        rate = float(d.get("unknown_asset_rate", 0))
        if rate > 0:
            for idx in self.rng.sample(range(len(out)), int(len(out) * rate)):
                out[idx] = dict(out[idx])
                out[idx]["asset_id"] = f"UNREGISTERED-{self.rng.randint(1000, 9999)}"
            stats["unknown_assets"] = int(len(out) * rate)

        LOG.info("defects injected into %s: %s", kind, stats)
        return out

    def assign_ingest_dates(self, rows: List[dict]) -> List[Tuple[str, dict]]:
        """Attach the *landing* date, which is not always the event date.

        A share of records arrive late (gateway buffered them through a network
        outage). ``ingest_date`` therefore differs from the event timestamp,
        which is exactly the late-arrival case the quality framework and the
        streaming watermark have to handle.
        """
        rate = float(self.defects.get("late_arrival_rate", 0))
        max_hours = float(self.defects.get("late_arrival_max_hours", 30))
        tagged: List[Tuple[str, dict]] = []
        late = 0
        for row in rows:
            event_ts = _safe_parse(row.get("timestamp"))
            base = event_ts or self.end
            if self.rng.random() < rate:
                base = base + timedelta(hours=self.rng.uniform(2, max_hours))
                late += 1
            base = min(base, self.end)
            tagged.append((base.date().isoformat(), row))
        LOG.info("late arrivals: %d/%d", late, len(rows))
        return tagged


def _safe_parse(value) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def _write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


#: Payloads that are not valid JSON at all. A rule engine cannot evaluate these
#: - there is no record to evaluate - so they exercise the streaming dead-letter
#: queue and the batch corrupt-record path rather than the quality rules.
CORRUPT_PAYLOADS = [
    '{"timestamp": "2026-01-01T00:00:00Z", "asset_id": "TRUNCATED"',   # truncated
    'not json at all',
    '{"timestamp": nan, "asset_id": "NAN-LITERAL"}',
    '\x00\x01binary-garbage',
    '{"timestamp": "2026-01-01T00:00:00Z",, "asset_id": "DOUBLE-COMMA"}',
]


def _write_partitioned_jsonl(root: Path, tagged: List[Tuple[str, dict]], prefix: str,
                             corrupt_rate: float = 0.0, rng: Optional[random.Random] = None) -> int:
    buckets: Dict[str, List[dict]] = {}
    for part, row in tagged:
        buckets.setdefault(part, []).append(row)
    rng = rng or random.Random(0)
    written = 0
    corrupted = 0
    for part, rows in sorted(buckets.items()):
        out_dir = root / f"ingest_date={part}"
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / f"{prefix}-{part}.jsonl", "w", encoding="utf-8") as fh:
            for row in rows:
                if corrupt_rate and rng.random() < corrupt_rate:
                    fh.write(rng.choice(CORRUPT_PAYLOADS) + "\n")
                    corrupted += 1
                    continue
                fh.write(json.dumps(row) + "\n")
                written += 1
    if corrupted:
        LOG.info("wrote %d unparseable payloads into %s (DLQ material)", corrupted, prefix)
    return written


def generate(cfg: Optional[Config] = None, days: Optional[int] = None,
             seed: Optional[int] = None, clean: bool = True) -> dict:
    cfg = cfg or load_config()
    raw_root = cfg.layer_path("raw")
    if clean and raw_root.exists():
        shutil.rmtree(raw_root)

    gen = IoTDataGenerator(cfg, seed=seed, days=days)
    gen.build_topology()

    _write_csv(raw_root / "sites" / "sites.csv", [asdict(s) for s in gen.sites],
               list(asdict(gen.sites[0]).keys()))
    _write_csv(raw_root / "buildings" / "buildings.csv", [asdict(b) for b in gen.buildings],
               list(asdict(gen.buildings[0]).keys()))
    _write_csv(raw_root / "assets" / "assets.csv", [asdict(a) for a in gen.assets],
               list(asdict(gen.assets[0]).keys()))

    telemetry = gen.inject_defects(list(gen.iter_telemetry()), "telemetry")
    events = gen.inject_defects(list(gen.iter_events()), "events")

    corrupt_rate = float(gen.defects.get("corrupt_payload_rate", 0.0))
    n_tel = _write_partitioned_jsonl(raw_root / "telemetry", gen.assign_ingest_dates(telemetry),
                                     "telemetry", corrupt_rate, gen.rng)
    n_evt = _write_partitioned_jsonl(raw_root / "events", gen.assign_ingest_dates(events),
                                     "events", corrupt_rate, gen.rng)

    summary = {
        "raw_root": str(raw_root),
        "sites": len(gen.sites),
        "buildings": len(gen.buildings),
        "assets": len(gen.assets),
        "telemetry_rows": n_tel,
        "event_rows": n_evt,
        "window_start": gen.start.isoformat(),
        "window_end": gen.end.isoformat(),
        "degraded_assets": sorted(gen.degraded),
        "silent_assets": {k: v.isoformat() for k, v in sorted(gen.silent_from.items())},
        "site_anomalies": {k: [v[0].isoformat(), v[1].isoformat()] for k, v in sorted(gen.site_anomalies.items())},
    }
    (raw_root / "_generation_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    LOG.info("generated %s telemetry rows and %s events into %s", n_tel, n_evt, raw_root)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the synthetic Nectar IoT dataset")
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--keep", action="store_true", help="do not wipe the raw zone first")
    args = parser.parse_args()

    setup_logging()
    summary = generate(load_config(args.config), days=args.days, seed=args.seed, clean=not args.keep)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
