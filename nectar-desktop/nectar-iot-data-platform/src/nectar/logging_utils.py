"""Structured logging + a small run-context helper.

Every pipeline run gets a ``batch_id`` that is stamped onto the data, the
quality results and the log lines, so a bad row in gold can be traced back to
the exact run that produced it.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

_CONFIGURED = False


class JsonFormatter(logging.Formatter):
    """One JSON object per line - what a log shipper expects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("batch_id", "table", "layer", "rows", "duration_s"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(level: str = "INFO", json_logs: Optional[bool] = None) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    if json_logs is None:
        json_logs = os.environ.get("NECTAR_JSON_LOGS", "0") == "1"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter()
        if json_logs
        else logging.Formatter("%(asctime)s %(levelname)-7s %(name)-28s %(message)s", "%H:%M:%S")
    )
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # py4j is extremely chatty at DEBUG/INFO
    for noisy in ("py4j", "py4j.clientserver", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _CONFIGURED = True


@dataclass
class RunContext:
    """Identity and timing for a single pipeline execution."""

    batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    logical_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: float = field(default_factory=time.time)
    metrics: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "RunContext":
        """Airflow passes ``AIRFLOW_CTX_DAG_RUN_ID`` - reuse it as batch_id so
        the lakehouse and the scheduler agree on what a "run" is."""
        run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID")
        batch_id = run_id.replace(":", "").replace("+", "")[-12:] if run_id else uuid.uuid4().hex[:12]
        return cls(batch_id=batch_id)

    def record(self, key: str, value: Any) -> None:
        self.metrics[key] = value

    @property
    def elapsed(self) -> float:
        return time.time() - self.started_at


@contextmanager
def stage(name: str, ctx: Optional[RunContext] = None):
    """Time a pipeline stage and log start/finish with the batch id."""
    log = logging.getLogger("nectar.stage")
    extra = {"batch_id": ctx.batch_id if ctx else None}
    log.info("-> %s", name, extra=extra)
    t0 = time.time()
    try:
        yield
    except Exception:
        log.exception("xx %s FAILED after %.1fs", name, time.time() - t0, extra=extra)
        raise
    duration = time.time() - t0
    if ctx:
        ctx.record(f"stage.{name}.seconds", round(duration, 2))
    log.info("<- %s done in %.1fs", name, duration, extra={**extra, "duration_s": round(duration, 2)})
