"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest

# Prefixes of every environment variable read by config.py.
_CONFIG_PREFIXES = ("APP_", "SECRET_KEY", "FLASK_", "SOCKETIO_", "KAFKA_", "POSTGRES_", "LOG_")


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove config-related variables so tests don't depend on the host or .env."""
    for key in list(os.environ):
        if key.startswith(_CONFIG_PREFIXES):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch
