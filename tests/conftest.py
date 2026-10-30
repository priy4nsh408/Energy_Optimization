"""Shared pytest fixtures."""

from __future__ import annotations

import os

import pytest

# Prefixes of every environment variable read by config.py.
_CONFIG_PREFIXES = ("APP_", "SECRET_KEY", "FLASK_", "SOCKETIO_", "KAFKA_", "POSTGRES_", "LOG_",
                    "SIM_", "BUILDING_", "WEATHER_", "CARBON_", "OPEN_METEO_",
                    "ELECTRICITY_MAPS_", "EXTERNAL_DATA_", "HTTP_")


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Remove config-related variables so tests don't depend on the host or .env."""
    for key in list(os.environ):
        if key.startswith(_CONFIG_PREFIXES):
            monkeypatch.delenv(key, raising=False)
    return monkeypatch


# --------------------------------------------------------------------------- PostgreSQL
# Integration tests use the *testing* database (POSTGRES_DB + "_test"), never
# the development database. They are skipped when PostgreSQL is unreachable.
DATA_TABLES = ("sensor_readings", "energy_records", "weather_records", "carbon_intensity_records",
               "security_alerts", "optimization_actions", "invalid_events", "energy_predictions")


def _test_database_settings():
    import dataclasses

    from config import get_config

    settings = get_config("testing", load_dotenv_file=False).DATABASE
    # Fall back to the docker-compose default password for local runs.
    password = os.environ.get("POSTGRES_PASSWORD") or "change_me"
    return dataclasses.replace(settings, password=password, connect_timeout=3)


@pytest.fixture(scope="session")
def pg_database():
    from database.__main__ import zone_registry
    from database.db import Database, DatabaseUnavailableError, create_database_if_missing
    from database.repositories import upsert_zone

    settings = _test_database_settings()
    assert settings.name.endswith("_test"), "refusing to run tests against a non-test database"
    try:
        create_database_if_missing(settings)
    except DatabaseUnavailableError:
        pytest.skip("PostgreSQL not reachable (start docker/docker-compose.yml)")
    db = Database(settings, name="tests")
    db.open(timeout_s=5)
    db.apply_schema()
    with db.transaction() as conn:
        for zone in zone_registry("building-01"):
            upsert_zone(conn, zone)
    yield db
    db.close()


@pytest.fixture
def db(pg_database):
    """The test database with all data tables emptied before the test."""
    with pg_database.transaction() as conn:
        conn.execute("TRUNCATE " + ", ".join(DATA_TABLES))
    return pg_database


# --------------------------------------------------------------------------- Kafka
@pytest.fixture(scope="session")
def kafka():
    """Kafka settings with a unique topic prefix; topics deleted after the session."""
    import dataclasses
    import uuid

    from config import get_config
    from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
    from streaming.topics import TOPICS, ensure_topics, topic_name

    base = get_config("testing", load_dotenv_file=False).KAFKA
    settings = dataclasses.replace(base, topic_prefix=f"it_{uuid.uuid4().hex[:8]}.",
                                   consumer_group_prefix=f"it_{uuid.uuid4().hex[:8]}")
    try:
        wait_for_broker(settings, timeout_s=3)
    except KafkaUnavailableError:
        pytest.skip("Kafka broker not reachable (start docker/docker-compose.yml)")
    admin = create_admin_client(settings)
    ensure_topics(admin, settings)
    yield settings
    for future in admin.delete_topics([topic_name(settings, name) for name in TOPICS]).values():
        future.result()


@pytest.fixture
def fresh_kafka(kafka):
    """Like ``kafka`` but with new, empty topics for this test only."""
    import dataclasses
    import uuid

    from streaming.kafka_config import create_admin_client
    from streaming.topics import TOPICS, ensure_topics, topic_name

    settings = dataclasses.replace(kafka, topic_prefix=f"it_{uuid.uuid4().hex[:8]}.")
    admin = create_admin_client(settings)
    ensure_topics(admin, settings)
    yield settings
    for future in admin.delete_topics([topic_name(settings, name) for name in TOPICS]).values():
        future.result()
