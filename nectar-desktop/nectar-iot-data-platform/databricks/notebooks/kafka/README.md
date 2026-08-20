# Kafka streaming pipeline — Databricks notebooks

Four notebooks. Run them in order.

| # | Notebook | What it does | Run it |
|---|---|---|---|
| 1 | `01_producer.py` | Pretends to be the machines. Sends readings to Kafka. Breaks some on purpose and writes down what it broke. | Start first, leave running |
| 2 | `02_bronze_stream.py` | Reads Kafka, saves everything unchanged. Broken messages go to a dead letter table. | Start second, leave running |
| 3 | `03_silver_stream.py` | Reads bronze as a stream. Checks every row. Good rows to silver, bad rows to a bin with reasons. | Start third, leave running |
| 4 | `04_gold_and_proof.py` | Builds the answer tables. Then proves the pipeline found what the producer broke. | Run last, after a few minutes |

## Before you start

Get these from Confluent Cloud and paste them into the boxes at the top of
notebooks 1 and 2:

- **bootstrap server** — Cluster settings, looks like `pkc-xxxxx.region.aws.confluent.cloud:9092`
- **API key** and **API secret** — API keys → Add key → Global access

You do **not** need to create the topic by hand. Notebook 1 checks the
connection and creates the topic for you.

## Why six partitions

Messages are keyed by machine id. Kafka only keeps order inside one partition,
so keying by machine means each machine's readings stay in order, and six
partitions let six workers read at once.

## What to show in the video

1. Notebook 2, the counting cell — watch the row count climb on its own
2. Notebook 3, the last cell — the list of reasons rows were binned
3. Notebook 4, the proof table — what we broke next to what was found

That third one is the strongest thing in the whole submission.

## If a stream will not restart

Streams remember where they got to, in the checkpoint. To start completely fresh:

```sql
DROP TABLE IF EXISTS nectar.bronze.telemetry;
DROP TABLE IF EXISTS nectar.silver.telemetry;
```
```python
dbutils.fs.rm("/Volumes/nectar/bronze/checkpoints", True)
```
