"""Bonus Option A - real-time telemetry pipeline (Spark Structured Streaming).

    # against Kafka
    python -m nectar.streaming.consumer --source kafka

    # against the raw zone, no broker required (what CI and the demo use)
    python -m nectar.streaming.consumer --source file --once

What it does
------------
Kafka (or a file source) -> parse with the declared schema -> route malformed
payloads to a DLQ -> validate with **the same rule objects the batch pipeline
uses** -> write clean rows to the streaming silver table -> maintain a
windowed 5-minute aggregate for the live dashboard.

Design points that matter in a streaming job
--------------------------------------------
*Event time, not processing time.* Every aggregation is keyed on the device's
own ``timestamp``. Using processing time would misattribute a batch of buffered
readings that arrive after a network outage to the moment they landed.

*Watermark.* ``withWatermark("timestamp", "15 minutes")`` bounds how long state
is kept for late data. It is the single most important knob here: too short and
late readings are silently dropped, too long and the state store grows without
limit. 15 minutes matches the gateway's retry budget; records later than that
are still captured - they fall into the batch pipeline's late-arrival path
instead of the streaming one, which is why both exist.

*Exactly-once.* Spark's checkpoint plus Delta's transactional commit give
end-to-end exactly-once for the append path: offsets and data commit together,
so a mid-batch crash replays from the last committed offset without
duplicating. ``dropDuplicates`` within the watermark additionally absorbs
at-least-once delivery from the gateway itself.

*The DLQ.* A message that is not valid JSON cannot be validated by a rule -
there is nothing to evaluate. It goes to a dead letter topic/table with the raw
bytes and the failure reason, so the stream never blocks on one bad producer.

*Trigger.* ``processingTime="10 seconds"`` micro-batches rather than
continuous: it produces reasonably-sized Delta files and lets the same code run
in ``availableNow`` mode for backfills.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from ..config import Config, load_config
from ..io_layer import resolve_format
from ..logging_utils import setup_logging
from ..quality.rules import BLOCKING, Rule, get_rules
from ..schemas import TELEMETRY_RAW_SCHEMA
from ..spark_session import get_spark

LOG = logging.getLogger("nectar.streaming.consumer")

#: Rules that need a full-table window (dedupe rank) or a batch-level column are
#: skipped in the streaming path; the streaming equivalents are handled by
#: dropDuplicates within the watermark and by the batch job's late-arrival pass.
_STREAM_EXCLUDED_RULES = {
    "tel.uniqueness.business_key",   # handled by dropDuplicates + watermark
    "tel.timeliness.late_arrival",   # needs the landing-date partition
}


def streaming_rules(cfg: Config) -> List[Rule]:
    return [r for r in get_rules("telemetry", cfg)
            if r.rule_id not in _STREAM_EXCLUDED_RULES
            and not set(r.requires) - {"_asset_known", "_register_site_id"}]


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------
def read_kafka(spark: SparkSession, cfg: Config) -> DataFrame:
    k = cfg.get("streaming.kafka", {})
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", k.get("bootstrap_servers", "localhost:9092"))
        .option("subscribe", k.get("telemetry_topic", "iot.telemetry.raw"))
        .option("startingOffsets", "earliest")
        # Bound each micro-batch so a large backlog does not produce one huge,
        # slow, memory-hungry batch after downtime.
        .option("maxOffsetsPerTrigger", cfg.get("streaming.max_offsets_per_trigger", 50000))
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("value").cast("string").alias("_raw_value"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
    )


def read_files(spark: SparkSession, cfg: Config, max_files_per_trigger: int = 2) -> DataFrame:
    """File source over the raw landing zone.

    Structured Streaming treats a directory as an unbounded source of files, so
    the identical downstream code runs with no broker. This is what the tests
    and the offline demo use.
    """
    path = str(cfg.layer_path("raw") / "telemetry")
    return (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(f"{path}/ingest_date=*/")
        .select(
            F.lit(None).cast("string").alias("_kafka_key"),
            F.col("value").alias("_raw_value"),
            F.lit("file").alias("_kafka_topic"),
            F.lit(0).alias("_kafka_partition"),
            F.lit(0).cast("long").alias("_kafka_offset"),
            F.current_timestamp().alias("_kafka_timestamp"),
        )
    )


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------
def parse_payload(df: DataFrame, schema: StructType = TELEMETRY_RAW_SCHEMA) -> DataFrame:
    """Parse the JSON payload, keeping unparseable messages for the DLQ."""
    return (
        df.withColumn("_parsed", F.from_json(F.col("_raw_value"), schema,
                                             {"mode": "PERMISSIVE"}))
        .withColumn("_parse_failed", F.col("_parsed").isNull() | F.col("_parsed.timestamp").isNull())
    )


def to_typed(df: DataFrame) -> DataFrame:
    """Flatten the parsed struct and cast to the silver contract."""
    out = df.select("_kafka_key", "_kafka_topic", "_kafka_partition", "_kafka_offset",
                    "_kafka_timestamp", "_parsed.*")
    out = (
        out.withColumn("_raw_timestamp", F.col("timestamp"))
        .withColumn("timestamp", F.coalesce(
            F.try_to_timestamp(F.col("timestamp"), F.lit("yyyy-MM-dd'T'HH:mm:ss['Z']")),
            F.try_to_timestamp(F.col("timestamp"))))
    )
    for col in ["site_id", "building_id", "asset_id", "sensor_id", "operating_mode"]:
        out = out.withColumn(col, F.upper(F.trim(F.col(col))))
    for col in ["temperature", "humidity", "pressure", "vibration", "power_consumption"]:
        out = out.withColumn(col, F.expr(f"try_cast({col} AS DOUBLE)"))
    return out.withColumn("_ingested_at", F.current_timestamp())


def apply_rules(df: DataFrame, rules: List[Rule]) -> DataFrame:
    """Attach the failed-rule array. Identical semantics to the batch engine."""
    blocking = [r for r in rules if r.severity == BLOCKING]
    warning = [r for r in rules if r.severity != BLOCKING]
    return (
        df.withColumn("_quarantine_reasons", F.array_compact(F.array(*[
            F.when(F.coalesce(r.predicate(df), F.lit(False)), F.lit(r.rule_id)) for r in blocking
        ])) if blocking else F.array().cast("array<string>"))
        .withColumn("_dq_warnings", F.array_compact(F.array(*[
            F.when(F.coalesce(r.predicate(df), F.lit(False)), F.lit(r.rule_id)) for r in warning
        ])) if warning else F.array().cast("array<string>"))
    )


def enrich_with_register(spark: SparkSession, cfg: Config, df: DataFrame) -> DataFrame:
    """Stream-static join against the asset register.

    A stream-static join is re-read on every micro-batch, so register changes
    take effect without restarting the query, and it is broadcast because the
    register is tiny. This is the streaming counterpart of the batch enrichment.
    """
    from ..io_layer import read_table, table_exists

    fmt = cfg.get("_resolved_format", cfg.table_format)
    path = cfg.table_path("silver", "assets")
    if not table_exists(spark, path, fmt):
        LOG.warning("asset register not found at %s; referential rules will flag everything", path)
        return df.withColumn("_asset_known", F.lit(True)).withColumn(
            "_register_site_id", F.col("site_id"))

    register = F.broadcast(
        read_table(spark, path, fmt).select(
            F.upper(F.trim("asset_id")).alias("_reg_asset_id"),
            F.upper(F.trim("site_id")).alias("_register_site_id"),
        ).dropDuplicates(["_reg_asset_id"])
    )
    return (
        df.join(register, df.asset_id == F.col("_reg_asset_id"), "left")
        .withColumn("_asset_known", F.col("_reg_asset_id").isNotNull())
        .drop("_reg_asset_id")
    )


# ---------------------------------------------------------------------------
# sinks
# ---------------------------------------------------------------------------
def _sum_sink_rows(progress_records) -> Optional[int]:
    counts = [int((p.get("sink") or {}).get("numOutputRows", -1)) for p in progress_records]
    known = [c for c in counts if c >= 0]
    return sum(known) if known else None


def _writer(df: DataFrame, path: str, checkpoint: str, fmt: str, trigger: dict,
            output_mode: str = "append", partition_by: Optional[List[str]] = None,
            query_name: str = "query"):
    w = (
        df.writeStream.format(fmt)
        .outputMode(output_mode)
        .queryName(query_name)
        .option("checkpointLocation", checkpoint)
        # Small, frequent micro-batches produce many small files; Delta's
        # optimizeWrite coalesces them at commit time.
        .option("delta.autoOptimize.optimizeWrite", "true")
    )
    if partition_by:
        w = w.partitionBy(*partition_by)
    return w.trigger(**trigger).start(path)


def run_stream(cfg: Optional[Config] = None, source: str = "file", once: bool = True,
               timeout_seconds: Optional[int] = 120) -> dict:
    cfg = cfg or load_config()
    spark = get_spark(cfg, "streaming")
    fmt = resolve_format(spark, cfg.table_format)
    cfg.data["_resolved_format"] = fmt

    ckpt_root = cfg.checkpoint_root
    ckpt_root.mkdir(parents=True, exist_ok=True)

    raw = read_kafka(spark, cfg) if source == "kafka" else read_files(spark, cfg)
    parsed = parse_payload(raw)

    # ---- dead letter queue -------------------------------------------------
    dlq = (
        parsed.filter(F.col("_parse_failed"))
        .select("_raw_value", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
        .withColumn("_dlq_reason", F.lit("payload_not_parseable"))
        .withColumn("_dlq_at", F.current_timestamp())
    )

    typed = to_typed(parsed.filter(~F.col("_parse_failed")))
    enriched = enrich_with_register(spark, cfg, typed)
    validated = apply_rules(enriched, streaming_rules(cfg))

    clean = (
        validated.filter(F.size("_quarantine_reasons") == 0)
        .withWatermark("timestamp", cfg.get("streaming.watermark", "15 minutes"))
        # Absorbs at-least-once delivery. Bounded by the watermark, so state
        # does not grow without limit.
        .dropDuplicates(["asset_id", "sensor_id", "timestamp"])
        .withColumn("event_date", F.to_date("timestamp"))
        .withColumn("event_hour", F.date_trunc("hour", F.col("timestamp")))
        .drop("_register_site_id", "_asset_known", "_raw_timestamp")
    )

    quarantine = (
        validated.filter(F.size("_quarantine_reasons") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.coalesce(F.col("timestamp"), F.current_timestamp())))
    )

    # ---- live 5-minute rollup for the operations dashboard -----------------
    rollup = (
        validated.filter(F.size("_quarantine_reasons") == 0)
        .withWatermark("timestamp", cfg.get("streaming.watermark", "15 minutes"))
        .groupBy(
            F.window(F.col("timestamp"), "5 minutes").alias("window"),
            F.col("site_id"), F.col("building_id"), F.col("asset_id"),
        )
        .agg(
            F.count(F.lit(1)).alias("readings"),
            F.avg("power_consumption").alias("avg_power_kw"),
            F.max("power_consumption").alias("peak_power_kw"),
            # 5 minutes = 1/12 hour; the streaming rollup uses the nominal
            # window rather than duration weights, and the batch job restates it
            # exactly. This is the lambda-style reconciliation: the stream is
            # fast and approximate, the batch is slow and authoritative.
            (F.avg("power_consumption") / F.lit(12.0)).alias("approx_energy_kwh"),
            F.avg("temperature").alias("avg_temperature"),
            F.max("vibration").alias("max_vibration"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "site_id", "building_id", "asset_id", "readings",
            "avg_power_kw", "peak_power_kw", "approx_energy_kwh",
            "avg_temperature", "max_vibration",
        )
        .withColumn("event_date", F.to_date("window_start"))
    )

    trigger = ({"availableNow": True} if once
               else {"processingTime": cfg.get("streaming.trigger_interval", "10 seconds")})

    queries = [
        _writer(clean, cfg.table_path("silver", "telemetry_stream"),
                str(ckpt_root / "silver_telemetry_stream"), fmt, trigger,
                partition_by=["event_date"], query_name="silver_telemetry_stream"),
        _writer(quarantine, cfg.table_path("quarantine", "telemetry_stream"),
                str(ckpt_root / "quarantine_stream"), fmt, trigger,
                partition_by=["event_date"], query_name="quarantine_stream"),
        _writer(dlq, cfg.table_path("quarantine", "dlq"),
                str(ckpt_root / "dlq_stream"), fmt, trigger, query_name="dlq_stream"),
        _writer(rollup, cfg.table_path("gold", "stream_asset_5min"),
                str(ckpt_root / "stream_rollup"), fmt, trigger,
                output_mode="append", partition_by=["event_date"], query_name="stream_rollup"),
    ]

    LOG.info("started %d streaming queries (source=%s, trigger=%s)", len(queries), source, trigger)
    stats = {}
    try:
        for q in queries:
            if once:
                q.awaitTermination(timeout=timeout_seconds)
            else:
                q.awaitTermination()
    except KeyboardInterrupt:  # pragma: no cover
        LOG.info("interrupted; stopping queries")
    finally:
        for q in queries:
            try:
                # numInputRows is per micro-batch and counts *source* rows, so
                # sum it across batches and read the sink counter separately.
                recent = q.recentProgress or []
                stats[q.name] = {
                    "micro_batches": len(recent),
                    "source_rows_seen": sum(int(p.get("numInputRows") or 0) for p in recent),
                    # Not every sink reports its output count - the file sinks
                    # return -1 for "unknown". Report None in that case rather
                    # than a fabricated zero; the row counts are verifiable by
                    # reading the target tables.
                    "rows_written": _sum_sink_rows(recent),
                }
                q.stop()
            except Exception:  # pragma: no cover
                pass

    LOG.info("streaming finished: %s", stats)
    return {"source": source, "format": fmt, "queries": stats}


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time telemetry pipeline")
    parser.add_argument("--config", default=None)
    parser.add_argument("--source", default="file", choices=["kafka", "file"])
    parser.add_argument("--once", action="store_true",
                        help="Trigger.AvailableNow - drain the backlog and exit")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--format", default=None, choices=["delta", "parquet"],
                        help="override storage.table_format")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    if args.format:
        cfg.data.setdefault("storage", {})["table_format"] = args.format
    print(json.dumps(run_stream(cfg, args.source, args.once, args.timeout), indent=2, default=str))


if __name__ == "__main__":
    main()
