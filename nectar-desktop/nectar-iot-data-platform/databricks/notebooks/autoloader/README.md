# Streaming pipeline — Databricks notebooks (Auto Loader)

Four notebooks. Run them in order. **Nothing to sign up for, nothing to pay,
no keys to paste.** Everything runs inside your Databricks Free Edition
workspace.

| # | Notebook | What it does | Run it |
|---|---|---|---|
| 1 | `01_producer.py` | Pretends to be the machines. Drops a file of readings into a folder every few seconds. Breaks some on purpose and writes down what it broke. | Start first, leave running |
| 2 | `02_bronze_stream.py` | Auto Loader watches that folder and saves every line unchanged. Broken lines go to a dead letter table. | Start second, leave running |
| 3 | `03_silver_stream.py` | Reads bronze as a stream. Checks every row. Good rows to silver, bad rows to a bin with reasons. | Start third, leave running |
| 4 | `04_gold_and_proof.py` | Builds the answer tables. Then proves the pipeline found exactly what the producer broke. | Run last, after a few minutes |

## Before you start

Nothing. Open notebook 1, attach it to Serverless, press Run All.

Notebook 1 creates the catalog `nectar`, the four schemas, the landing Volume
and the checkpoint Volume for you.

## How to run it

1. Open `01_producer.py`. Set **minutes** to `6`. Run All. Leave the tab open.
2. Open `02_bronze_stream.py` in a second tab. Run All. Leave it running.
3. Open `03_silver_stream.py` in a third tab. Run All.
4. Wait about four minutes, then open `04_gold_and_proof.py` and Run All.

## Why this counts as real streaming

The challenge asks for "Spark Structured Streaming, Kafka, Flink or Kinesis" —
Spark Structured Streaming is first on that list, and this is exactly that:
`readStream` → `writeStream`, micro-batches on a 10-second trigger,
checkpoints, watermarks, exactly-once writes to Delta.

Auto Loader is the source instead of Kafka. Everything after the source line is
identical — which is the point, and worth saying out loud on camera:

| Kafka word | Here |
|---|---|
| producer | notebook 1 |
| topic | a folder in a Unity Catalog Volume |
| `subscribe` | `cloudFiles` |
| consumer group offset | checkpoint |
| `maxOffsetsPerTrigger` | `cloudFiles.maxFilesPerTrigger` |
| consumer lag | `numFilesOutstanding` |
| exactly once | checkpoint + Delta commit |

The Kafka version of the same four notebooks is in
[`../kafka/`](../kafka/) — same silver and gold code, only the source differs.
Show that folder in the video: it proves the pipeline is source-agnostic and
that the broker swap is a two-line change, not a rewrite.

## Why files are written temp-then-rename

Notebook 1 writes `_tmp_00007.json` first, then renames it to
`batch_00007.json`. A rename is instant, so Auto Loader can never see a
half-written file. Notebook 2 also filters on `batch_*.json` as a second guard.
This is the same problem Kafka solves with a commit — a reader must never see a
partial record.

## What to show in the video

1. Notebook 2, the counting cell — the row count climbs on its own
2. Notebook 2, the lag cell — `numFilesOutstanding` and rows/second
3. Notebook 3, the last cell — the list of reasons rows were binned
4. Notebook 4, the proof table — what we broke next to what was found

That last one is the strongest thing in the whole submission. Most candidates
say "my quality checks work". This shows the injected count and the detected
count side by side.

## If a stream will not restart

Streams remember where they got to, in the checkpoint. To start completely fresh:

```sql
DROP TABLE IF EXISTS nectar.bronze.telemetry;
DROP TABLE IF EXISTS nectar.bronze.dead_letters;
DROP TABLE IF EXISTS nectar.silver.telemetry;
DROP TABLE IF EXISTS nectar.quality.quarantine;
```
```python
dbutils.fs.rm("/Volumes/nectar/bronze/checkpoints", True)
dbutils.fs.rm("/Volumes/nectar/bronze/landing/telemetry", True)
```

Then start again from notebook 1.

## Common errors

| Message | Fix |
|---|---|
| `Table nectar.bronze.telemetry not found` in notebook 2's counting cell | The first batch has not landed yet. Wait 20 seconds and run the cell again. |
| `SCHEMA_NOT_FOUND` | Run notebook 1 first — it creates everything. |
| Row count stuck at 0 | Notebook 1 stopped. Check its tab; if the run finished, set **minutes** higher and run it again. |
| `Volume not found` | Free Edition needs Unity Catalog on. It is on by default; if not, use the default catalog `workspace` and change `CATALOG` at the top of each notebook. |
