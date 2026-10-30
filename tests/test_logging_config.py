"""Tests for utils/logging_config.py."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from config import LoggingSettings
from utils.logging_config import configure_logging


@pytest.fixture(autouse=True)
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for handler in list(root.handlers):
        if handler not in handlers:
            root.removeHandler(handler)
            handler.close()
    root.setLevel(level)


def _settings(tmp_path: Path, **overrides) -> LoggingSettings:
    values = {"level": "INFO", "fmt": "json", "log_dir": tmp_path, "file_enabled": True}
    values.update(overrides)
    return LoggingSettings(**values)


def test_json_log_written_to_service_file(tmp_path):
    logger = configure_logging(_settings(tmp_path), "db_writer")
    logger.info("stored reading", extra={"zone_id": "Z1", "power_kw": 3.5})
    for handler in logging.getLogger().handlers:
        handler.flush()

    record = json.loads((tmp_path / "db_writer.log").read_text(encoding="utf-8").splitlines()[-1])
    assert record["message"] == "stored reading"
    assert record["service"] == "db_writer"
    assert record["level"] == "INFO"
    assert record["zone_id"] == "Z1" and record["power_kw"] == 3.5


def test_exceptions_are_serialised(tmp_path, capsys):
    logger = configure_logging(_settings(tmp_path, file_enabled=False), "svc")
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.exception("failed")
    record = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert "RuntimeError: boom" in record["exception"]


def test_text_format_includes_extras(tmp_path, capsys):
    logger = configure_logging(_settings(tmp_path, fmt="text", file_enabled=False), "svc")
    logger.warning("hello", extra={"zone_id": "Z2"})
    out = capsys.readouterr().out
    assert "WARNING" in out and "hello" in out and "zone_id=Z2" in out


def test_reconfiguring_does_not_duplicate_handlers(tmp_path):
    configure_logging(_settings(tmp_path), "svc")
    count = len(logging.getLogger().handlers)
    configure_logging(_settings(tmp_path), "svc")
    assert len(logging.getLogger().handlers) == count


def test_level_and_unsafe_service_name(tmp_path):
    logger = configure_logging(_settings(tmp_path, level="WARNING"), "bad/name:x")
    assert logger.name == "bad_name_x"
    assert logging.getLogger().level == logging.WARNING


def test_unusable_log_dir_falls_back_to_console(tmp_path, capsys):
    blocker = tmp_path / "not_a_dir"
    blocker.write_text("x", encoding="utf-8")
    logger = configure_logging(_settings(tmp_path, log_dir=blocker / "logs"), "svc")
    logger.info("still works")
    out = capsys.readouterr().out
    assert "File logging disabled" in out and "still works" in out
