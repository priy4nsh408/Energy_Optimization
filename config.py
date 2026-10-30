"""Central configuration for the Smart Building Energy Optimization system.

All settings are read from environment variables (optionally loaded from a
local ``.env`` file). Real environment variables always take precedence over
values in ``.env``, so secrets can be injected by the shell or CI without
editing files.

Usage::

    from config import get_config

    config = get_config()              # uses APP_ENV (default: development)
    config.KAFKA.producer_config()     # dict for confluent_kafka.Producer
    config.DATABASE.connection_kwargs()  # kwargs for psycopg.connect

Flask reads the upper-case attributes of the config object::

    app.config.from_object(get_config())
"""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, ClassVar

from dotenv import load_dotenv

BASE_DIR: Path = Path(__file__).resolve().parent

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_LOG_FORMATS = {"json", "text"}
# eventlet/gevent are unreliable on Windows; "threading" works everywhere.
_VALID_SOCKETIO_MODES = {"threading", "eventlet", "gevent"}


class ConfigurationError(ValueError):
    """Raised when an environment variable is missing or invalid."""


# --------------------------------------------------------------------------- #
# Environment variable helpers
# --------------------------------------------------------------------------- #
def _get_str(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        if required:
            raise ConfigurationError(f"Required environment variable '{name}' is not set.")
        return default if default is not None else ""
    return value.strip()


def _get_int(name: str, default: int, *, minimum: int | None = None,
             maximum: int | None = None) -> int:
    raw = _get_str(name)
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"'{name}' must be an integer, got '{raw}'.") from exc
    if minimum is not None and value < minimum:
        raise ConfigurationError(f"'{name}' must be >= {minimum}, got {value}.")
    if maximum is not None and value > maximum:
        raise ConfigurationError(f"'{name}' must be <= {maximum}, got {value}.")
    return value


def _get_float(name: str, default: float, *, minimum: float | None = None,
               maximum: float | None = None) -> float:
    raw = _get_str(name)
    if raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"'{name}' must be a number, got '{raw}'.") from exc
    if minimum is not None and value < minimum:
        raise ConfigurationError(f"'{name}' must be >= {minimum}, got {value}.")
    if maximum is not None and value > maximum:
        raise ConfigurationError(f"'{name}' must be <= {maximum}, got {value}.")
    return value


def _get_bool(name: str, default: bool) -> bool:
    raw = _get_str(name).lower()
    if raw == "":
        return default
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    raise ConfigurationError(
        f"'{name}' must be a boolean (true/false/1/0/yes/no), got '{raw}'."
    )


def _get_choice(name: str, default: str, choices: set[str], *, upper: bool = False) -> str:
    raw = _get_str(name, default)
    value = raw.upper() if upper else raw.lower()
    if value not in choices:
        raise ConfigurationError(
            f"'{name}' must be one of {sorted(choices)}, got '{raw}'."
        )
    return value


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = _get_str(name)
    if raw == "":
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _resolve_path(raw: str) -> Path:
    """Resolve a path relative to the project root (works on Windows and POSIX)."""
    path = Path(raw).expanduser()
    return path if path.is_absolute() else (BASE_DIR / path).resolve()


# --------------------------------------------------------------------------- #
# Settings groups
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class KafkaSettings:
    """Connection and client settings for Apache Kafka (confluent-kafka)."""

    bootstrap_servers: str
    client_id: str
    consumer_group_prefix: str
    topic_prefix: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""
    sasl_username: str = ""
    sasl_password: str = field(default="", repr=False)
    auto_offset_reset: str = "earliest"
    producer_acks: str = "all"
    request_timeout_ms: int = 10000

    # Logical topic names. The physical name is ``topic_prefix + name``.
    TOPIC_NAMES: ClassVar[tuple[str, ...]] = (
        "sensor.readings",
        "external.context",
        "energy.predictions",
        "control.recommendations",
        "security.alerts",
        "agent.proposals",
        "control.commands",
    )

    @classmethod
    def from_env(cls, *, default_topic_prefix: str = "") -> "KafkaSettings":
        return cls(
            bootstrap_servers=_get_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            client_id=_get_str("KAFKA_CLIENT_ID", "smart-building"),
            consumer_group_prefix=_get_str("KAFKA_CONSUMER_GROUP_PREFIX", "smart-building"),
            topic_prefix=_get_str("KAFKA_TOPIC_PREFIX", default_topic_prefix),
            security_protocol=_get_choice(
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}, upper=True,
            ),
            sasl_mechanism=_get_str("KAFKA_SASL_MECHANISM", ""),
            sasl_username=_get_str("KAFKA_SASL_USERNAME", ""),
            sasl_password=_get_str("KAFKA_SASL_PASSWORD", ""),
            auto_offset_reset=_get_choice(
                "KAFKA_AUTO_OFFSET_RESET", "earliest", {"earliest", "latest"}
            ),
            producer_acks=_get_choice("KAFKA_PRODUCER_ACKS", "all", {"0", "1", "all"}),
            request_timeout_ms=_get_int("KAFKA_REQUEST_TIMEOUT_MS", 10000, minimum=1000),
        )

    def topic(self, name: str) -> str:
        """Return the physical topic name for a logical topic name."""
        if name not in self.TOPIC_NAMES:
            raise ConfigurationError(
                f"Unknown Kafka topic '{name}'. Known topics: {list(self.TOPIC_NAMES)}"
            )
        return f"{self.topic_prefix}{name}"

    def _common_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL"):
            config.update({
                "sasl.mechanism": self.sasl_mechanism or "PLAIN",
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
            })
        return config

    def producer_config(self) -> dict[str, Any]:
        """Configuration dict for ``confluent_kafka.Producer``."""
        return {
            **self._common_config(),
            "acks": self.producer_acks,
            "enable.idempotence": self.producer_acks == "all",
            "message.timeout.ms": self.request_timeout_ms * 3,
            "request.timeout.ms": self.request_timeout_ms,
        }

    def consumer_config(self, group_name: str) -> dict[str, Any]:
        """Configuration dict for ``confluent_kafka.Consumer``.

        Each service passes its own ``group_name`` so independent consumers
        (database writer, ML worker, security worker, ...) each receive
        every event.
        """
        if not group_name:
            raise ConfigurationError("Kafka consumer group_name must not be empty.")
        return {
            **self._common_config(),
            "group.id": f"{self.consumer_group_prefix}.{group_name}",
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": max(self.request_timeout_ms, 6000),
        }


@dataclass(frozen=True)
class DatabaseSettings:
    """PostgreSQL connection settings (psycopg 3)."""

    host: str
    port: int
    name: str
    user: str
    password: str = field(repr=False)
    sslmode: str = "prefer"
    connect_timeout: int = 10
    pool_min_size: int = 1
    pool_max_size: int = 5

    @classmethod
    def from_env(cls, *, name_suffix: str = "") -> "DatabaseSettings":
        pool_min = _get_int("POSTGRES_POOL_MIN_SIZE", 1, minimum=1)
        pool_max = _get_int("POSTGRES_POOL_MAX_SIZE", 5, minimum=1)
        if pool_max < pool_min:
            raise ConfigurationError(
                "POSTGRES_POOL_MAX_SIZE must be >= POSTGRES_POOL_MIN_SIZE."
            )
        return cls(
            host=_get_str("POSTGRES_HOST", "localhost"),
            port=_get_int("POSTGRES_PORT", 5432, minimum=1, maximum=65535),
            name=_get_str("POSTGRES_DB", "smart_building") + name_suffix,
            user=_get_str("POSTGRES_USER", "smart_building"),
            password=_get_str("POSTGRES_PASSWORD", ""),
            sslmode=_get_choice(
                "POSTGRES_SSLMODE", "prefer",
                {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"},
            ),
            connect_timeout=_get_int("POSTGRES_CONNECT_TIMEOUT", 10, minimum=1),
            pool_min_size=pool_min,
            pool_max_size=pool_max,
        )

    def connection_kwargs(self) -> dict[str, Any]:
        """Keyword arguments for ``psycopg.connect`` / ``psycopg_pool``.

        Passing values as keywords avoids quoting problems with special
        characters in passwords.
        """
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout,
        }

    @property
    def safe_dsn(self) -> str:
        """DSN with the password masked — safe to log."""
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.name}"


@dataclass(frozen=True)
class LoggingSettings:
    """Logging configuration consumed by ``utils.logging_config``."""

    level: str = "INFO"
    fmt: str = "json"
    log_dir: Path = BASE_DIR / "logs"
    file_enabled: bool = True
    max_bytes: int = 5 * 1024 * 1024
    backup_count: int = 5

    @classmethod
    def from_env(cls, *, default_level: str = "INFO",
                 default_file_enabled: bool = True) -> "LoggingSettings":
        return cls(
            level=_get_choice("LOG_LEVEL", default_level, _VALID_LOG_LEVELS, upper=True),
            fmt=_get_choice("LOG_FORMAT", "json", _VALID_LOG_FORMATS),
            log_dir=_resolve_path(_get_str("LOG_DIR", "logs")),
            file_enabled=_get_bool("LOG_TO_FILE", default_file_enabled),
            max_bytes=_get_int("LOG_MAX_BYTES", 5 * 1024 * 1024, minimum=1024),
            backup_count=_get_int("LOG_BACKUP_COUNT", 5, minimum=0),
        )


@dataclass(frozen=True)
class SimulatorSettings:
    """Settings for the building simulator (``simulator`` package).

    ``interval_seconds`` is the wall-clock time between readings.
    ``speed_factor`` is simulated seconds per wall-clock second, so each
    reading covers ``interval_seconds * speed_factor`` simulated seconds.
    The outdoor climate parameters drive the built-in synthetic outdoor
    model, which is used until the weather API module provides real data.
    """

    building_id: str = "building-01"
    interval_seconds: float = 2.0
    speed_factor: float = 1.0
    start_time: datetime | None = None
    random_seed: int | None = None
    physics_step_seconds: float = 60.0
    latitude_deg: float = 40.0
    utc_offset_hours: float = 0.0
    outdoor_annual_mean_c: float = 15.0
    outdoor_seasonal_amplitude_c: float = 10.0
    outdoor_diurnal_amplitude_c: float = 5.0

    MIN_INTERVAL_SECONDS: ClassVar[float] = 1.0
    MAX_INTERVAL_SECONDS: ClassVar[float] = 5.0
    MAX_SPEED_FACTOR: ClassVar[float] = 3600.0

    def __post_init__(self) -> None:
        if not self.building_id:
            raise ConfigurationError("Simulator building_id must not be empty.")
        if not self.MIN_INTERVAL_SECONDS <= self.interval_seconds <= self.MAX_INTERVAL_SECONDS:
            raise ConfigurationError(
                f"Simulator interval_seconds must be between {self.MIN_INTERVAL_SECONDS} "
                f"and {self.MAX_INTERVAL_SECONDS}, got {self.interval_seconds}."
            )
        if not 0 < self.speed_factor <= self.MAX_SPEED_FACTOR:
            raise ConfigurationError(
                f"Simulator speed_factor must be in (0, {self.MAX_SPEED_FACTOR}], "
                f"got {self.speed_factor}."
            )
        if not 1 <= self.physics_step_seconds <= 300:
            raise ConfigurationError("Simulator physics_step_seconds must be between 1 and 300.")
        if not -90 <= self.latitude_deg <= 90:
            raise ConfigurationError("Simulator latitude_deg must be between -90 and 90.")
        if not -12 <= self.utc_offset_hours <= 14:
            raise ConfigurationError("Simulator utc_offset_hours must be between -12 and 14.")
        if self.outdoor_seasonal_amplitude_c < 0 or self.outdoor_diurnal_amplitude_c < 0:
            raise ConfigurationError("Simulator outdoor amplitudes must be >= 0.")

    @property
    def tzinfo(self) -> timezone:
        """Fixed-offset time zone of the building (no tzdata needed on Windows)."""
        return timezone(timedelta(hours=self.utc_offset_hours))

    @property
    def simulated_seconds_per_tick(self) -> float:
        return self.interval_seconds * self.speed_factor

    @classmethod
    def from_env(cls) -> "SimulatorSettings":
        utc_offset = _get_float("SIM_UTC_OFFSET_HOURS", 0.0, minimum=-12, maximum=14)
        start_raw = _get_str("SIM_START_TIME")
        start_time: datetime | None = None
        if start_raw:
            try:
                start_time = datetime.fromisoformat(start_raw)
            except ValueError as exc:
                raise ConfigurationError(
                    f"'SIM_START_TIME' must be an ISO 8601 datetime, got '{start_raw}'."
                ) from exc
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=timezone(timedelta(hours=utc_offset)))
        seed_raw = _get_str("SIM_RANDOM_SEED")
        return cls(
            building_id=_get_str("SIM_BUILDING_ID", "building-01"),
            interval_seconds=_get_float("SIM_INTERVAL_SECONDS", 2.0),
            speed_factor=_get_float("SIM_SPEED_FACTOR", 1.0),
            start_time=start_time,
            random_seed=_get_int("SIM_RANDOM_SEED", 0) if seed_raw else None,
            physics_step_seconds=_get_float("SIM_PHYSICS_STEP_SECONDS", 60.0),
            latitude_deg=_get_float("SIM_LATITUDE", 40.0),
            utc_offset_hours=utc_offset,
            outdoor_annual_mean_c=_get_float("SIM_OUTDOOR_ANNUAL_MEAN_C", 15.0),
            outdoor_seasonal_amplitude_c=_get_float("SIM_OUTDOOR_SEASONAL_AMPLITUDE_C", 10.0),
            outdoor_diurnal_amplitude_c=_get_float("SIM_OUTDOOR_DIURNAL_AMPLITUDE_C", 5.0),
        )


# --------------------------------------------------------------------------- #
# Environment-specific configuration classes
# --------------------------------------------------------------------------- #
class BaseConfig:
    """Settings shared by all environments.

    Upper-case attributes are picked up by ``app.config.from_object``.
    Values are read in ``__init__`` (not at import time) so each call to
    :func:`get_config` reflects the current environment.
    """

    ENV_NAME = "base"
    DEBUG = False
    TESTING = False
    DEFAULT_LOG_LEVEL = "INFO"
    DEFAULT_LOG_TO_FILE = True
    DATABASE_NAME_SUFFIX = ""
    KAFKA_DEFAULT_TOPIC_PREFIX = ""

    def __init__(self) -> None:
        self.SECRET_KEY: str = self._load_secret_key()
        self.FLASK_HOST: str = _get_str("FLASK_HOST", "127.0.0.1")
        self.FLASK_PORT: int = _get_int("FLASK_PORT", 5000, minimum=1, maximum=65535)
        self.JSON_SORT_KEYS: bool = False

        self.SOCKETIO_ASYNC_MODE: str = _get_choice(
            "SOCKETIO_ASYNC_MODE", "threading", _VALID_SOCKETIO_MODES
        )
        # Optional message queue so separate worker processes can emit
        # Socket.IO events (configured in the dashboard module).
        self.SOCKETIO_MESSAGE_QUEUE: str = _get_str("SOCKETIO_MESSAGE_QUEUE", "")
        self.SOCKETIO_CORS_ALLOWED_ORIGINS: list[str] = _get_list(
            "SOCKETIO_CORS_ALLOWED_ORIGINS",
            [f"http://{self.FLASK_HOST}:{self.FLASK_PORT}",
             f"http://localhost:{self.FLASK_PORT}"],
        )

        self.KAFKA = KafkaSettings.from_env(
            default_topic_prefix=self.KAFKA_DEFAULT_TOPIC_PREFIX
        )
        self.DATABASE = DatabaseSettings.from_env(name_suffix=self.DATABASE_NAME_SUFFIX)
        self.SIMULATOR = SimulatorSettings.from_env()
        self.LOGGING = LoggingSettings.from_env(
            default_level=self.DEFAULT_LOG_LEVEL,
            default_file_enabled=self.DEFAULT_LOG_TO_FILE,
        )

    def _load_secret_key(self) -> str:
        key = _get_str("SECRET_KEY")
        if key:
            return key
        # Development/testing convenience: an ephemeral random key. Sessions
        # will not survive restarts, which is acceptable outside production.
        return secrets.token_hex(32)

    def summary(self) -> dict[str, Any]:
        """Non-sensitive overview of the active configuration (safe to log)."""
        return {
            "env": self.ENV_NAME,
            "debug": self.DEBUG,
            "testing": self.TESTING,
            "flask": f"{self.FLASK_HOST}:{self.FLASK_PORT}",
            "socketio_async_mode": self.SOCKETIO_ASYNC_MODE,
            "kafka_bootstrap_servers": self.KAFKA.bootstrap_servers,
            "kafka_topic_prefix": self.KAFKA.topic_prefix,
            "database": self.DATABASE.safe_dsn,
            "simulator": (f"{self.SIMULATOR.building_id} every {self.SIMULATOR.interval_seconds}s"
                          f" at {self.SIMULATOR.speed_factor}x"),
            "log_level": self.LOGGING.level,
            "log_format": self.LOGGING.fmt,
            "log_to_file": self.LOGGING.file_enabled,
        }


class DevelopmentConfig(BaseConfig):
    """Local development: verbose logging, Flask debug mode."""

    ENV_NAME = "development"
    DEBUG = True
    DEFAULT_LOG_LEVEL = "DEBUG"


class TestingConfig(BaseConfig):
    """Automated tests: isolated database and topics, console-only logging."""

    __test__ = False  # not a pytest test class
    ENV_NAME = "testing"
    TESTING = True
    DEFAULT_LOG_LEVEL = "WARNING"
    DEFAULT_LOG_TO_FILE = False
    DATABASE_NAME_SUFFIX = "_test"
    KAFKA_DEFAULT_TOPIC_PREFIX = "test."


class ProductionConfig(BaseConfig):
    """Deployment: secrets must be provided explicitly."""

    ENV_NAME = "production"

    def __init__(self) -> None:
        super().__init__()
        if not self.DATABASE.password:
            raise ConfigurationError("POSTGRES_PASSWORD must be set in production.")

    def _load_secret_key(self) -> str:
        key = _get_str("SECRET_KEY", required=True)
        if len(key) < 32:
            raise ConfigurationError("SECRET_KEY must be at least 32 characters in production.")
        return key


CONFIG_BY_NAME: dict[str, type[BaseConfig]] = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
}


def load_environment(dotenv_path: Path | None = None) -> bool:
    """Load variables from a ``.env`` file without overriding real env vars.

    Returns ``True`` if a file was found and loaded.
    """
    path = dotenv_path or BASE_DIR / ".env"
    if not path.is_file():
        return False
    return load_dotenv(path, override=False, encoding="utf-8")


def get_config(env_name: str | None = None, *, load_dotenv_file: bool = True) -> BaseConfig:
    """Build the configuration object for the requested environment.

    Args:
        env_name: ``development``, ``testing`` or ``production``. Defaults to
            the ``APP_ENV`` environment variable, then ``development``.
        load_dotenv_file: Load ``.env`` from the project root first.

    Raises:
        ConfigurationError: If the environment name or any setting is invalid.
    """
    if load_dotenv_file:
        load_environment()
    name = (env_name or _get_str("APP_ENV", "development")).lower()
    try:
        config_cls = CONFIG_BY_NAME[name]
    except KeyError as exc:
        raise ConfigurationError(
            f"Unknown APP_ENV '{name}'. Expected one of {sorted(CONFIG_BY_NAME)}."
        ) from exc
    return config_cls()
