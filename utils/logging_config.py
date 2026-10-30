"""Structured logging setup shared by Flask and all background workers.

Each process calls :func:`configure_logging` once with its service name.
Logs go to the console and, optionally, to ``<LOG_DIR>/<service>.log``.
Using one file per service avoids the Windows file-locking errors that occur
when several processes rotate the same log file.

Extra fields can be attached to any record::

    logger.info("reading published", extra={"zone_id": "Z1", "power_kw": 3.2})
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import re
import sys
from datetime import datetime, timezone
from typing import Any

from config import LoggingSettings

# Attributes present on every LogRecord; anything else came from ``extra=``.
_RESERVED_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message", "asctime", "taskName",
}
_SAFE_SERVICE_NAME = re.compile(r"[^A-Za-z0-9_.-]")
_HANDLER_MARKER = "_smart_building_handler"


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # ensure_ascii keeps output safe for legacy Windows console code pages.
        return json.dumps(payload, default=str, ensure_ascii=True)


class TextFormatter(logging.Formatter):
    """Human-readable format that still shows ``extra=`` fields."""

    def __init__(self, service: str) -> None:
        super().__init__(
            fmt=f"%(asctime)s | %(levelname)-8s | {service} | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            key: value for key, value in record.__dict__.items()
            if key not in _RESERVED_ATTRS and not key.startswith("_")
        }
        if extras:
            base += " | " + " ".join(f"{k}={v}" for k, v in extras.items())
        return base


def configure_logging(settings: LoggingSettings, service_name: str) -> logging.Logger:
    """Configure the root logger for one process and return a service logger.

    Calling this more than once replaces the handlers it previously installed,
    so it is safe to call from tests and from Flask's reloader.
    """
    safe_service = _SAFE_SERVICE_NAME.sub("_", service_name) or "app"
    formatter: logging.Formatter = (
        JsonFormatter(safe_service) if settings.fmt == "json" else TextFormatter(safe_service)
    )

    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_MARKER, False):
            root.removeHandler(handler)
            handler.close()
    root.setLevel(settings.level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    setattr(console, _HANDLER_MARKER, True)
    root.addHandler(console)

    logger = logging.getLogger(safe_service)

    if settings.file_enabled:
        try:
            settings.log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                settings.log_dir / f"{safe_service}.log",
                maxBytes=settings.max_bytes,
                backupCount=settings.backup_count,
                encoding="utf-8",
                delay=True,
            )
        except OSError:
            # Never let a log-directory problem crash the service.
            logger.warning("File logging disabled: cannot use log directory %s",
                           settings.log_dir, exc_info=True)
        else:
            file_handler.setFormatter(formatter)
            setattr(file_handler, _HANDLER_MARKER, True)
            root.addHandler(file_handler)

    # Third-party libraries are noisy at DEBUG level.
    for noisy in ("werkzeug", "engineio", "socketio", "urllib3"):
        logging.getLogger(noisy).setLevel(max(root.level, logging.INFO))

    return logger
