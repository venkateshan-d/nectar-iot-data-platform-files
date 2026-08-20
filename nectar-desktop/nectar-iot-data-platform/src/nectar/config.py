"""Configuration loading.

A single ``Config`` object is threaded through every module so that no piece of
the pipeline reaches for a hard-coded path or magic number. Values come from
``config/pipeline.yaml`` and can be overridden by environment variables of the
form ``NECTAR__<SECTION>__<KEY>`` (double underscore separates nesting levels),
which is what the containerised and orchestrated deployments use.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

try:  # PyYAML is in requirements.txt but we degrade gracefully in CI images
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.yaml"

_ENV_PREFIX = "NECTAR__"


def _coerce(value: str) -> Any:
    """Turn an environment string into a python literal when possible."""
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return value


def _apply_env_overrides(data: Dict[str, Any]) -> Dict[str, Any]:
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(_ENV_PREFIX):
            continue
        path = env_key[len(_ENV_PREFIX):].lower().split("__")
        cursor: Dict[str, Any] = data
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
            if not isinstance(cursor, dict):  # pragma: no cover - misconfig
                raise ValueError(f"Cannot override non-mapping key via {env_key}")
        cursor[path[-1]] = _coerce(env_val)
    return data


@dataclass
class Config:
    """Dot/bracket accessible view over the YAML document."""

    data: Dict[str, Any] = field(default_factory=dict)
    path: Path = DEFAULT_CONFIG_PATH

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def get(self, dotted: str, default: Any = None) -> Any:
        """``cfg.get("storage.layers.bronze")`` -> value or *default*."""
        cursor: Any = self.data
        for part in dotted.split("."):
            if not isinstance(cursor, dict) or part not in cursor:
                return default
            cursor = cursor[part]
        return cursor

    # -- convenience accessors used all over the codebase --------------------
    @property
    def table_format(self) -> str:
        return str(self.get("storage.table_format", "delta")).lower()

    def layer_path(self, layer: str) -> Path:
        """Absolute path for a lakehouse layer (raw/bronze/silver/gold/...)."""
        raw = self.get(f"storage.layers.{layer}")
        if raw is None:
            raise KeyError(f"Unknown storage layer: {layer!r}")
        return self._absolute(raw)

    def table_path(self, layer: str, table: str) -> str:
        return str(self.layer_path(layer) / table)

    @property
    def checkpoint_root(self) -> Path:
        return self._absolute(self.get("storage.checkpoints", "./data/checkpoints"))

    def _absolute(self, raw: str) -> Path:
        p = Path(raw)
        return p if p.is_absolute() else (PROJECT_ROOT / p).resolve()


@lru_cache(maxsize=4)
def load_config(path: str | os.PathLike | None = None) -> Config:
    cfg_path = Path(path) if path else Path(os.environ.get("NECTAR_CONFIG", DEFAULT_CONFIG_PATH))
    if yaml is None:  # pragma: no cover
        raise RuntimeError("PyYAML is required: pip install -r requirements.txt")
    with open(cfg_path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    data = _apply_env_overrides(data)
    return Config(data=data, path=cfg_path)
