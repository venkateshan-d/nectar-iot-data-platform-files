# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - land raw device data with Auto Loader
# MAGIC
# MAGIC Auto Loader replaces the hand-written incremental reader from the portable
# MAGIC pipeline. It tracks which files it has already seen, so a re-run costs
# MAGIC nothing and a backfill is just more files appearing in the Volume.
# MAGIC
# MAGIC Two options carry the design:
# MAGIC
# MAGIC * `cloudFiles.inferColumnTypes = false` - every column stays a string.
# MAGIC   Bronze must hold `"temperature": "not-a-number"` verbatim, otherwise the
# MAGIC   failure is not reproducible and the row is not replayable.
# MAGIC * `rescuedDataColumn` - fields that do not fit the declared schema are
# MAGIC   captured instead of dropped. This is Auto Loader's built-in equivalent
# MAGIC   of the `_corrupt_record` handling in the portable version.

# COMMAND ----------

# The `dlt` module was renamed when Delta Live Tables became Lakeflow
# Declarative Pipelines. The new import works on current runtimes; the fallback
# keeps this notebook runnable on older ones.
try:
    from pyspark import pipelines as dp
except ImportError:  # DBR < 16.x
    import dlt as dp

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

LANDING = spark.conf.get("nectar.landing_volume", "/Volumes/nectar/bronze/landing")
SCHEMA_LOC = f"{LANDING}/_checkpoints/schema"

TELEMETRY_RAW = StructType([StructField(c, StringType(), True) for c in [
    "timestamp", "site_id", "building_id", "asset_id", "sensor_id",
    "temperature", "humidity", "pressure", "vibration",
    "power_consumption", "operating_mode",
]])

EVENT_RAW = StructType([StructField(c, StringType(), True) for c in [
    "event_id", "timestamp", "asset_id", "event_type", "severity", "message",
]])


def _autoload(path: str, schema: StructType, name: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_LOC}/{name}")
        # New columns appearing upstream stop the stream rather than being
        # silently ignored; the pipeline restarts with the wider schema.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(path)
        # Lineage, identical in intent to the portable pipeline's audit columns.
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_id", F.lit(spark.conf.get("pipelines.id", "local")))
        .withColumn("ingest_date", F.to_date(
            F.regexp_extract(F.col("_metadata.file_path"),
                             r"ingest_date=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1)))
    )


@dp.table(
    name="bronze_telemetry",
    comment="Raw telemetry exactly as it landed, plus lineage. Never transformed.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        # Liquid Clustering replaces the partition-key decision entirely, and
        # the keys can be changed later without rewriting history.
        "clusteringColumns": "asset_id",
    },
)
def bronze_telemetry():
    return _autoload(f"{LANDING}/telemetry", TELEMETRY_RAW, "telemetry")


@dp.table(
    name="bronze_events",
    comment="Raw operational events as landed, plus lineage.",
    table_properties={"quality": "bronze"},
)
def bronze_events():
    return _autoload(f"{LANDING}/events", EVENT_RAW, "events")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference data
# MAGIC
# MAGIC Small, slowly changing, read on every join. Materialized views rather than
# MAGIC streaming tables: the register is a full snapshot each time, not a stream
# MAGIC of changes.

# COMMAND ----------

def _csv(path: str):
    return (spark.read.format("csv").option("header", True)
            .option("inferSchema", True).load(path)
            .withColumn("_ingested_at", F.current_timestamp()))


@dp.materialized_view(name="bronze_assets", comment="Asset register snapshot.")
def bronze_assets():
    return _csv(f"{LANDING}/assets")


@dp.materialized_view(name="bronze_sites", comment="Site reference snapshot.")
def bronze_sites():
    return _csv(f"{LANDING}/sites")


@dp.materialized_view(name="bronze_buildings", comment="Building reference snapshot.")
def bronze_buildings():
    return _csv(f"{LANDING}/buildings")
