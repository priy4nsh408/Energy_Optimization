"""Tests for config.py: environment selection, parsing, validation and secrecy."""

from __future__ import annotations

from pathlib import Path

import pytest

import config as config_module
from config import (
    ConfigurationError,
    DevelopmentConfig,
    ProductionConfig,
    TestingConfig,
    get_config,
    load_environment,
)


def test_defaults_to_development(clean_env):
    cfg = get_config(load_dotenv_file=False)
    assert isinstance(cfg, DevelopmentConfig)
    assert cfg.DEBUG is True
    assert cfg.LOGGING.level == "DEBUG"
    assert cfg.KAFKA.bootstrap_servers == "localhost:9092"
    assert cfg.DATABASE.name == "smart_building"
    assert cfg.SOCKETIO_ASYNC_MODE == "threading"


def test_app_env_selects_testing(clean_env):
    clean_env.setenv("APP_ENV", "TESTING")
    cfg = get_config(load_dotenv_file=False)
    assert isinstance(cfg, TestingConfig)
    assert cfg.TESTING is True
    assert cfg.DATABASE.name == "smart_building_test"
    assert cfg.KAFKA.topic("building_sensor_data") == "test.building_sensor_data"
    assert cfg.LOGGING.file_enabled is False


def test_unknown_env_raises(clean_env):
    with pytest.raises(ConfigurationError, match="Unknown APP_ENV"):
        get_config("staging", load_dotenv_file=False)


def test_env_overrides_are_parsed(clean_env):
    clean_env.setenv("FLASK_PORT", "8080")
    clean_env.setenv("POSTGRES_PORT", "6543")
    clean_env.setenv("LOG_TO_FILE", "no")
    clean_env.setenv("LOG_LEVEL", "warning")
    clean_env.setenv("SOCKETIO_CORS_ALLOWED_ORIGINS", "http://a.test, http://b.test")
    cfg = get_config("development", load_dotenv_file=False)
    assert cfg.FLASK_PORT == 8080
    assert cfg.DATABASE.port == 6543
    assert cfg.LOGGING.file_enabled is False
    assert cfg.LOGGING.level == "WARNING"
    assert cfg.SOCKETIO_CORS_ALLOWED_ORIGINS == ["http://a.test", "http://b.test"]


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("FLASK_PORT", "abc", "must be an integer"),
        ("FLASK_PORT", "70000", "must be <="),
        ("LOG_TO_FILE", "maybe", "must be a boolean"),
        ("LOG_LEVEL", "VERBOSE", "must be one of"),
        ("SOCKETIO_ASYNC_MODE", "asyncio", "must be one of"),
        ("KAFKA_PRODUCER_ACKS", "2", "must be one of"),
    ],
)
def test_invalid_values_raise(clean_env, name, value, message):
    clean_env.setenv(name, value)
    with pytest.raises(ConfigurationError, match=message):
        get_config("development", load_dotenv_file=False)


def test_pool_bounds_validated(clean_env):
    clean_env.setenv("POSTGRES_POOL_MIN_SIZE", "5")
    clean_env.setenv("POSTGRES_POOL_MAX_SIZE", "2")
    with pytest.raises(ConfigurationError, match="POOL_MAX_SIZE"):
        get_config("development", load_dotenv_file=False)


def test_development_generates_secret_key(clean_env):
    first = get_config("development", load_dotenv_file=False).SECRET_KEY
    second = get_config("development", load_dotenv_file=False).SECRET_KEY
    assert len(first) == 64 and first != second


def test_production_requires_secrets(clean_env):
    with pytest.raises(ConfigurationError, match="SECRET_KEY"):
        get_config("production", load_dotenv_file=False)
    clean_env.setenv("SECRET_KEY", "x" * 40)
    with pytest.raises(ConfigurationError, match="POSTGRES_PASSWORD"):
        get_config("production", load_dotenv_file=False)
    clean_env.setenv("POSTGRES_PASSWORD", "s3cret")
    cfg = get_config("production", load_dotenv_file=False)
    assert isinstance(cfg, ProductionConfig) and cfg.DEBUG is False


def test_production_rejects_short_secret_key(clean_env):
    clean_env.setenv("SECRET_KEY", "short")
    clean_env.setenv("POSTGRES_PASSWORD", "s3cret")
    with pytest.raises(ConfigurationError, match="at least 32"):
        get_config("production", load_dotenv_file=False)


def test_passwords_never_leak(clean_env):
    clean_env.setenv("POSTGRES_PASSWORD", "db-pass-123")
    clean_env.setenv("KAFKA_SASL_PASSWORD", "kafka-pass-456")
    cfg = get_config("development", load_dotenv_file=False)
    text = repr(cfg.DATABASE) + repr(cfg.KAFKA) + str(cfg.summary()) + cfg.DATABASE.safe_dsn
    assert "db-pass-123" not in text
    assert "kafka-pass-456" not in text
    assert cfg.DATABASE.connection_kwargs()["password"] == "db-pass-123"


def test_kafka_client_configs(clean_env):
    cfg = get_config("development", load_dotenv_file=False)
    producer = cfg.KAFKA.producer_config()
    assert producer["bootstrap.servers"] == "localhost:9092"
    assert producer["acks"] == "all" and producer["enable.idempotence"] is True
    assert "sasl.username" not in producer

    consumer = cfg.KAFKA.consumer_config("db_writer")
    assert consumer["group.id"] == "smart-building.db_writer"
    assert consumer["enable.auto.commit"] is False
    with pytest.raises(ConfigurationError):
        cfg.KAFKA.consumer_config("")
    with pytest.raises(ConfigurationError):
        cfg.KAFKA.topic("")
    assert cfg.KAFKA.admin_config()["bootstrap.servers"] == "localhost:9092"
    assert producer["message.timeout.ms"] == 120000


def test_kafka_sasl_config(clean_env):
    clean_env.setenv("KAFKA_SECURITY_PROTOCOL", "sasl_ssl")
    clean_env.setenv("KAFKA_SASL_USERNAME", "user")
    clean_env.setenv("KAFKA_SASL_PASSWORD", "pw")
    producer = get_config("development", load_dotenv_file=False).KAFKA.producer_config()
    assert producer["security.protocol"] == "SASL_SSL"
    assert producer["sasl.mechanism"] == "PLAIN"
    assert producer["sasl.password"] == "pw"


def test_kafka_configs_accepted_by_confluent_client(clean_env):
    """confluent-kafka validates property names when a client is constructed."""
    confluent_kafka = pytest.importorskip("confluent_kafka")
    kafka = get_config("development", load_dotenv_file=False).KAFKA
    confluent_kafka.Producer(kafka.producer_config())
    consumer = confluent_kafka.Consumer(kafka.consumer_config("config_test"))
    consumer.close()


def test_relative_log_dir_resolves_to_project(clean_env):
    clean_env.setenv("LOG_DIR", "custom_logs")
    cfg = get_config("development", load_dotenv_file=False)
    assert cfg.LOGGING.log_dir == config_module.BASE_DIR / "custom_logs"


def test_dotenv_does_not_override_real_env(clean_env, tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("FLASK_PORT=7000\nPOSTGRES_DB=from_dotenv\n", encoding="utf-8")
    clean_env.setenv("FLASK_PORT", "9000")
    assert load_environment(env_file) is True
    cfg = get_config("development", load_dotenv_file=False)
    assert cfg.FLASK_PORT == 9000            # real env wins
    assert cfg.DATABASE.name == "from_dotenv"  # filled from .env


def test_missing_dotenv_is_not_an_error(tmp_path: Path):
    assert load_environment(tmp_path / "missing.env") is False


def test_flask_reads_uppercase_attributes(clean_env):
    from flask import Flask

    app = Flask(__name__)
    app.config.from_object(get_config("testing", load_dotenv_file=False))
    assert app.config["TESTING"] is True
    assert app.config["KAFKA"].topic_prefix == "test."
