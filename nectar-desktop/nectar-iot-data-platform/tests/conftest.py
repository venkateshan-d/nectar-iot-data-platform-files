"""Shared pytest fixtures.

One SparkSession per test session (creating one costs ~5 seconds), configured
identically to production apart from the storage format, which is forced to
Parquet so the suite runs without downloading Delta jars.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nectar.config import Config, load_config  # noqa: E402
from nectar.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def base_config() -> Config:
    cfg = load_config()
    cfg.data.setdefault("storage", {})["table_format"] = "parquet"
    cfg.data["_resolved_format"] = "parquet"
    cfg.data.setdefault("spark", {})["master"] = "local[2]"
    cfg.data["spark"]["shuffle_partitions"] = 2
    return cfg


@pytest.fixture(scope="session")
def spark(base_config):
    session = get_spark(base_config, "tests")
    yield session
    session.stop()


@pytest.fixture
def tmp_lakehouse(base_config, monkeypatch):
    """A throwaway lakehouse root so tests never touch the real data dir."""
    tmp = Path(tempfile.mkdtemp(prefix="nectar-test-"))
    layers = {name: str(tmp / name)
              for name in ["raw", "bronze", "silver", "gold", "quarantine", "quality"]}
    original = dict(base_config.data["storage"]["layers"])
    base_config.data["storage"]["layers"] = layers
    yield tmp
    base_config.data["storage"]["layers"] = original
    shutil.rmtree(tmp, ignore_errors=True)
