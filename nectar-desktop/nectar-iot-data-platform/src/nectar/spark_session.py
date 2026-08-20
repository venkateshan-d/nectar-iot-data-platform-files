"""SparkSession factory.

Centralising session creation keeps Delta configuration, shuffle tuning and
log levels in one place, and lets tests spin up a session with the same
settings the production job uses.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import SparkSession

from .config import Config, load_config

LOG = logging.getLogger(__name__)

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Delta housekeeping defaults - see docs/01_architecture.md
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.databricks.delta.schema.autoMerge.enabled": "false",  # explicit evolution only
}


def get_spark(cfg: Optional[Config] = None, app_suffix: str = "") -> SparkSession:
    """Build (or fetch) the SparkSession for this process."""
    cfg = cfg or load_config()
    app_name = cfg.get("spark.app_name", "nectar-iot-platform")
    if app_suffix:
        app_name = f"{app_name}-{app_suffix}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.get("spark.master", "local[*]"))
        .config("spark.sql.shuffle.partitions", cfg.get("spark.shuffle_partitions", 8))
        .config("spark.driver.memory", cfg.get("spark.driver_memory", "3g"))
        .config("spark.sql.session.timeZone", "UTC")
        # Adaptive execution keeps small local runs and large cluster runs both
        # sane: it coalesces the tiny partitions a 5-minute batch produces and
        # splits skewed joins on the asset dimension.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        # The console progress bar corrupts piped/CI logs; the Spark UI and the
        # structured logs carry the same information.
        .config("spark.ui.showConsoleProgress", "false")
    )

    if cfg.table_format == "delta":
        for key, value in _DELTA_CONF.items():
            builder = builder.config(key, value)
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError:  # pragma: no cover
            LOG.warning(
                "delta-spark not installed; assuming Delta jars are already on the "
                "classpath (spark-submit --packages io.delta:delta-spark_2.12:3.2.1)"
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(cfg.get("spark.log_level", "WARN"))
    LOG.info("SparkSession ready: app=%s format=%s", app_name, cfg.table_format)
    return spark
