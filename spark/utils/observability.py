"""
spark.utils.observability
=========================

Structured logging and lightweight metric emission for Spark jobs.

Every Bronze ingestion run emits a JSON log envelope per source containing
a deterministic schema: ``timestamp``, ``level``, ``job``, ``source``,
``event``, plus event-specific fields. Downstream log shippers (Vector,
Fluent Bit, Promtail) can index these directly without grok parsing.

The module deliberately avoids OpenTelemetry / Prometheus client deps at
this stage — the structured log is the system of record for run metrics
and is forwarded to the observability stack at the platform layer.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator


_DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


class _JsonFormatter(logging.Formatter):
    """Render every log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach any structured fields passed via ``extra=``.
        for key, value in record.__dict__.items():
            if key in {
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "levelname", "levelno", "lineno", "message", "module",
                "msecs", "msg", "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
            }:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.LoggerAdapter:
    """
    Return a JSON-formatted logger bound to ``name``.

    The returned object is a ``LoggerAdapter`` — call sites can pass
    structured fields via ``logger.info("event", extra={"source": ...})``.
    Repeat configuration calls are idempotent.
    """
    logger = logging.getLogger(name)
    if not getattr(logger, "_lakehouse_configured", False):
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(_DEFAULT_LEVEL)
        logger.propagate = False
        logger._lakehouse_configured = True  # type: ignore[attr-defined]
    return logging.LoggerAdapter(logger, extra={})


@contextmanager
def timed(logger: logging.LoggerAdapter, event: str, **fields: Any) -> Iterator[dict]:
    """
    Context manager that emits ``{event}.start`` / ``{event}.end`` records
    with a ``duration_ms`` field on the end record.

    Yields a mutable dict callers can mutate to attach result metrics
    (e.g. ``ctx["rows_written"] = n``) that will be included in the end log.
    """
    start = time.perf_counter()
    ctx: dict[str, Any] = {}
    logger.info(f"{event}.start", extra=fields)
    try:
        yield ctx
    except Exception as exc:
        duration_ms = int((time.perf_counter() - start) * 1000)
        logger.exception(
            f"{event}.error",
            extra={**fields, **ctx, "duration_ms": duration_ms, "error": str(exc)},
        )
        raise
    else:
        duration_ms = int((time.perf_counter() - start) * 1000)
        logger.info(
            f"{event}.end",
            extra={**fields, **ctx, "duration_ms": duration_ms},
        )
