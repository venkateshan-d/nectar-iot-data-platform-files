"""Kafka producer - replays the raw zone as a live telemetry stream.

Used to exercise the streaming consumer without real devices. It preserves the
shape of production traffic that matters for testing:

* messages are **keyed by asset_id**, so all readings for one asset land on the
  same partition and per-asset ordering is preserved (Kafka only guarantees
  ordering within a partition - keying is what makes the streaming dedupe and
  the stateful aggregations correct);
* the deliberately corrupted records are replayed too, so the DLQ path is
  exercised, not just the happy path;
* ``--rate`` throttles to a realistic messages/second instead of dumping the
  whole file, so consumer lag and trigger behaviour can be observed.

    python -m nectar.streaming.producer --rate 500 --topic iot.telemetry.raw
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import random
import time
from pathlib import Path
from typing import Iterator, Optional

from ..config import Config, load_config
from ..logging_utils import setup_logging

LOG = logging.getLogger("nectar.streaming.producer")


def iter_raw_records(root: Path, subdir: str) -> Iterator[dict]:
    """Yield every JSONL record in the raw zone, oldest partition first."""
    base = root / subdir
    for part in sorted(base.glob("ingest_date=*")):
        for f in sorted(part.glob("*.jsonl")):
            with open(f, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)


def build_producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Durability settings a telemetry pipeline actually wants:
        acks="all",              # do not consider a reading sent until replicated
        retries=5,
        linger_ms=20,            # small batching window; throughput without latency
        compression_type="gzip",  # telemetry JSON compresses ~8x
        max_in_flight_requests_per_connection=1,  # preserve per-key ordering on retry
    )


def replay(cfg: Optional[Config] = None, topic: Optional[str] = None, subdir: str = "telemetry",
           rate: float = 500.0, limit: Optional[int] = None, loop: bool = False,
           jitter: bool = True, dry_run: bool = False) -> dict:
    cfg = cfg or load_config()
    bootstrap = cfg.get("streaming.kafka.bootstrap_servers", "localhost:9092")
    topic = topic or cfg.get(
        "streaming.kafka.telemetry_topic" if subdir == "telemetry" else "streaming.kafka.events_topic")
    raw_root = cfg.layer_path("raw")

    producer = None if dry_run else build_producer(bootstrap)
    records = iter_raw_records(raw_root, subdir)
    if loop:
        records = itertools.cycle(list(records))

    sent = 0
    t0 = time.time()
    interval = 1.0 / rate if rate > 0 else 0.0
    rng = random.Random(7)

    for record in records:
        key = record.get("asset_id") or "unknown"
        if producer is not None:
            producer.send(topic, key=key, value=record)
        sent += 1
        if limit and sent >= limit:
            break
        if interval:
            # Jitter avoids a perfectly periodic stream, which hides batching
            # and backpressure problems that real traffic would expose.
            time.sleep(interval * (rng.uniform(0.5, 1.5) if jitter else 1.0))
        if sent % 5000 == 0:
            LOG.info("sent %d messages (%.0f msg/s)", sent, sent / max(time.time() - t0, 1e-6))

    if producer is not None:
        producer.flush()
        producer.close()

    elapsed = time.time() - t0
    LOG.info("done: %d messages to %s in %.1fs (%.0f msg/s)", sent, topic, elapsed, sent / max(elapsed, 1e-6))
    return {"topic": topic, "messages": sent, "seconds": round(elapsed, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay the raw zone into Kafka")
    parser.add_argument("--config", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--source", default="telemetry", choices=["telemetry", "events"])
    parser.add_argument("--rate", type=float, default=500.0, help="messages per second (0 = unthrottled)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--loop", action="store_true", help="replay forever")
    parser.add_argument("--dry-run", action="store_true", help="count records without connecting to Kafka")
    args = parser.parse_args()

    setup_logging()
    result = replay(load_config(args.config), args.topic, args.source, args.rate,
                    args.limit, args.loop, dry_run=args.dry_run)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
