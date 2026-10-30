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
    # How long the producer keeps retrying a message (e.g. while the broker
    # restarts) before reporting it as failed.
    message_timeout_ms: int = 120000
    reconnect_backoff_ms: int = 500
    reconnect_backoff_max_ms: int = 10000
    replication_factor: int = 1

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
            message_timeout_ms=_get_int("KAFKA_MESSAGE_TIMEOUT_MS", 120000, minimum=1000),
            reconnect_backoff_ms=_get_int("KAFKA_RECONNECT_BACKOFF_MS", 500, minimum=10),
            reconnect_backoff_max_ms=_get_int("KAFKA_RECONNECT_BACKOFF_MAX_MS", 10000, minimum=10),
            replication_factor=_get_int("KAFKA_REPLICATION_FACTOR", 1, minimum=1),
        )

    def topic(self, name: str) -> str:
        """Return the physical topic name (``topic_prefix + name``).

        The list of valid topics lives in ``streaming.topics``.
        """
        if not name:
            raise ConfigurationError("Kafka topic name must not be empty.")
        return f"{self.topic_prefix}{name}"

    def _common_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            # librdkafka reconnects automatically; these bound the retry rate.
            "reconnect.backoff.ms": self.reconnect_backoff_ms,
            "reconnect.backoff.max.ms": self.reconnect_backoff_max_ms,
            "socket.keepalive.enable": True,
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
            "message.timeout.ms": self.message_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "linger.ms": 20,
            "compression.type": "lz4",
        }

    def admin_config(self) -> dict[str, Any]:
        """Configuration dict for ``confluent_kafka.admin.AdminClient``."""
        return self._common_config()

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
            # At-least-once: the consumer stores an offset only after a message
            # was processed; only stored offsets are (auto-)committed.
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "auto.commit.interval.ms": 1000,
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


@dataclass(frozen=True)
class ExternalDataSettings:
    """Weather (Open-Meteo) and carbon-intensity (Electricity Maps / UK grid) sources.

    ``mock`` providers generate clearly labelled synthetic data (``source="mock"``)
    for offline development. API keys are read from the environment only and are
    never written to logs or cache files.
    """

    WEATHER_PROVIDERS: ClassVar[tuple[str, ...]] = ("open-meteo", "mock")
    CARBON_PROVIDERS: ClassVar[tuple[str, ...]] = ("electricitymaps", "uk-carbon-intensity", "mock")

    latitude: float = 40.42
    longitude: float = -3.70
    weather_provider: str = "open-meteo"
    open_meteo_base_url: str = "https://api.open-meteo.com/v1"
    open_meteo_api_key: str = field(default="", repr=False)
    weather_poll_interval_s: int = 900
    weather_forecast_hours: int = 24
    carbon_provider: str = "electricitymaps"
    electricity_maps_base_url: str = "https://api.electricitymaps.com/v3"
    electricity_maps_api_key: str = field(default="", repr=False)
    electricity_maps_zone: str = ""
    carbon_poll_interval_s: int = 900
    cache_dir: Path = BASE_DIR / "data" / "cache"
    http_timeout_s: float = 10.0
    http_max_retries: int = 3

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90 or not -180 <= self.longitude <= 180:
            raise ConfigurationError("BUILDING_LATITUDE/LONGITUDE out of range.")
        if self.weather_provider not in self.WEATHER_PROVIDERS:
            raise ConfigurationError(
                f"WEATHER_PROVIDER must be one of {list(self.WEATHER_PROVIDERS)}.")
        if self.carbon_provider not in self.CARBON_PROVIDERS:
            raise ConfigurationError(
                f"CARBON_PROVIDER must be one of {list(self.CARBON_PROVIDERS)}.")

    @classmethod
    def from_env(cls) -> "ExternalDataSettings":
        return cls(
            latitude=_get_float("BUILDING_LATITUDE", 40.42, minimum=-90, maximum=90),
            longitude=_get_float("BUILDING_LONGITUDE", -3.70, minimum=-180, maximum=180),
            weather_provider=_get_str("WEATHER_PROVIDER", "open-meteo").lower(),
            open_meteo_base_url=_get_str("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1"),
            open_meteo_api_key=_get_str("OPEN_METEO_API_KEY", ""),
            weather_poll_interval_s=_get_int("WEATHER_POLL_INTERVAL_S", 900, minimum=60),
            weather_forecast_hours=_get_int("WEATHER_FORECAST_HOURS", 24, minimum=0, maximum=384),
            carbon_provider=_get_str("CARBON_PROVIDER", "electricitymaps").lower(),
            electricity_maps_base_url=_get_str("ELECTRICITY_MAPS_BASE_URL",
                                               "https://api.electricitymaps.com/v3"),
            electricity_maps_api_key=_get_str("ELECTRICITY_MAPS_API_KEY", ""),
            electricity_maps_zone=_get_str("ELECTRICITY_MAPS_ZONE", ""),
            carbon_poll_interval_s=_get_int("CARBON_POLL_INTERVAL_S", 900, minimum=60),
            cache_dir=_resolve_path(_get_str("EXTERNAL_DATA_CACHE_DIR", "data/cache")),
            http_timeout_s=_get_float("HTTP_TIMEOUT_S", 10.0, minimum=1, maximum=120),
            http_max_retries=_get_int("HTTP_MAX_RETRIES", 3, minimum=0, maximum=10),
        )


@dataclass(frozen=True)
class OptimizationSettings:
    """Limits and objective weights of the deterministic HVAC/lighting optimizer."""

    setpoint_min_c: float = 16.0           # absolute safety limits for any setpoint
    setpoint_max_c: float = 28.0
    comfort_min_c: float = 20.0            # occupied comfort band
    comfort_max_c: float = 25.0
    comfort_neutral_c: float = 1.0         # no comfort penalty within +/- this of the preferred setpoint
    setback_min_c: float = 17.0            # unoccupied band
    setback_max_c: float = 27.0
    standby_margin_c: float = 1.5          # empty but scheduled: comfort band widened by this
    thermostat_deadband_c: float = 0.5     # temperature swings +/- this around the setpoint
    preconditioning_lead_s: float = 3600.0 # treat as scheduled this long before opening
    max_setpoint_step_c: float = 1.5       # largest change per decision (comfort recovery excepted)
    decision_interval_s: float = 900.0     # simulated seconds between decisions per zone
    energy_weight_per_kwh: float = 0.15
    carbon_weight_per_kg: float = 0.10
    comfort_weight: float = 0.5            # per occupant per K^2 per hour outside the neutral zone
    min_setpoint_change_c: float = 0.25    # smaller changes are not published
    min_lighting_change: float = 0.05

    def __post_init__(self) -> None:
        if not self.setpoint_min_c <= self.setback_min_c <= self.comfort_min_c \
                < self.comfort_max_c <= self.setback_max_c <= self.setpoint_max_c:
            raise ConfigurationError(
                "Optimization limits must satisfy setpoint_min <= setback_min <= comfort_min < "
                "comfort_max <= setback_max <= setpoint_max.")
        if self.max_setpoint_step_c <= 0 or self.decision_interval_s <= 0:
            raise ConfigurationError("OPT_MAX_SETPOINT_STEP_C and OPT_DECISION_INTERVAL_S must be > 0.")
        if min(self.energy_weight_per_kwh, self.carbon_weight_per_kg, self.comfort_weight) < 0:
            raise ConfigurationError("Optimization weights must be >= 0.")

    @classmethod
    def from_env(cls) -> "OptimizationSettings":
        return cls(
            setpoint_min_c=_get_float("OPT_SETPOINT_MIN_C", 16.0, minimum=5, maximum=40),
            setpoint_max_c=_get_float("OPT_SETPOINT_MAX_C", 28.0, minimum=5, maximum=40),
            comfort_min_c=_get_float("OPT_COMFORT_MIN_C", 20.0, minimum=5, maximum=40),
            comfort_max_c=_get_float("OPT_COMFORT_MAX_C", 25.0, minimum=5, maximum=40),
            comfort_neutral_c=_get_float("OPT_COMFORT_NEUTRAL_C", 1.0, minimum=0, maximum=5),
            setback_min_c=_get_float("OPT_SETBACK_MIN_C", 17.0, minimum=5, maximum=40),
            setback_max_c=_get_float("OPT_SETBACK_MAX_C", 27.0, minimum=5, maximum=40),
            standby_margin_c=_get_float("OPT_STANDBY_MARGIN_C", 1.5, minimum=0, maximum=10),
            thermostat_deadband_c=_get_float("OPT_THERMOSTAT_DEADBAND_C", 0.5, minimum=0, maximum=3),
            preconditioning_lead_s=_get_float("OPT_PRECONDITIONING_LEAD_S", 3600.0, minimum=0),
            max_setpoint_step_c=_get_float("OPT_MAX_SETPOINT_STEP_C", 1.5, minimum=0.1, maximum=10),
            decision_interval_s=_get_float("OPT_DECISION_INTERVAL_S", 900.0, minimum=1),
            energy_weight_per_kwh=_get_float("OPT_ENERGY_WEIGHT_PER_KWH", 0.15, minimum=0),
            carbon_weight_per_kg=_get_float("OPT_CARBON_WEIGHT_PER_KG", 0.10, minimum=0),
            comfort_weight=_get_float("OPT_COMFORT_WEIGHT", 0.5, minimum=0),
        )


@dataclass(frozen=True)
class AgentSettings:
    """Agentic AI (LangGraph) incident agent.

    ``llm_provider``:
      * ``anthropic`` - Claude through the Anthropic API (credentials from the
        environment, e.g. ``ANTHROPIC_API_KEY``; never logged).
      * ``offline`` - deterministic rule-based policy, clearly labelled as such
        in every record. No LLM is called. For development, tests and demos
        without an API key.
      * ``auto`` - ``anthropic`` when ``ANTHROPIC_API_KEY`` is set, else ``offline``.

    Whatever the provider, every action goes through the deterministic safety
    validator and only the *simulated* building is changed.
    """

    PROVIDERS: ClassVar[tuple[str, ...]] = ("auto", "anthropic", "offline")
    EFFORTS: ClassVar[tuple[str, ...]] = ("low", "medium", "high", "xhigh", "max")
    SEVERITIES: ClassVar[tuple[str, ...]] = ("low", "medium", "high", "critical")

    llm_provider: str = "auto"
    llm_model: str = "claude-opus-5-5"
    llm_effort: str = "high"
    llm_max_tokens: int = 16000
    llm_timeout_s: float = 120.0
    llm_refusal_fallback: bool = True       # server-side fallback model on a policy refusal
    max_iterations: int = 10                # LLM turns per incident
    min_alert_severity: str = "medium"      # alerts below this are recorded but not investigated
    incident_cooldown_s: float = 3600.0     # same zone + classification handled at most this often
    unconfirmed_recheck_s: float = 900.0    # ...but re-checked sooner if the last run took no action
    max_actions_per_zone_per_hour: int = 4
    max_data_age_s: float = 1800.0          # control actions need a reading at most this old
    max_isolated_sensors_per_zone: int = 2
    audit_log: Path = BASE_DIR / "logs" / "agent_decisions.jsonl"

    def __post_init__(self) -> None:
        if self.llm_provider not in self.PROVIDERS:
            raise ConfigurationError(f"AGENT_LLM_PROVIDER must be one of {list(self.PROVIDERS)}.")
        if self.llm_effort not in self.EFFORTS:
            raise ConfigurationError(f"AGENT_LLM_EFFORT must be one of {list(self.EFFORTS)}.")
        if self.min_alert_severity not in self.SEVERITIES:
            raise ConfigurationError(f"AGENT_MIN_ALERT_SEVERITY must be one of {list(self.SEVERITIES)}.")

    @property
    def resolved_provider(self) -> str:
        if self.llm_provider != "auto":
            return self.llm_provider
        return "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else "offline"

    @classmethod
    def from_env(cls) -> "AgentSettings":
        return cls(
            llm_provider=_get_str("AGENT_LLM_PROVIDER", "auto").lower(),
            llm_model=_get_str("AGENT_LLM_MODEL", "claude-opus-5-5"),
            llm_effort=_get_str("AGENT_LLM_EFFORT", "high").lower(),
            llm_max_tokens=_get_int("AGENT_LLM_MAX_TOKENS", 16000, minimum=1024, maximum=128000),
            llm_timeout_s=_get_float("AGENT_LLM_TIMEOUT_S", 120.0, minimum=5, maximum=1800),
            llm_refusal_fallback=_get_bool("AGENT_LLM_REFUSAL_FALLBACK", True),
            max_iterations=_get_int("AGENT_MAX_ITERATIONS", 10, minimum=2, maximum=50),
            min_alert_severity=_get_str("AGENT_MIN_ALERT_SEVERITY", "medium").lower(),
            incident_cooldown_s=_get_float("AGENT_INCIDENT_COOLDOWN_S", 3600.0, minimum=0),
            unconfirmed_recheck_s=_get_float("AGENT_UNCONFIRMED_RECHECK_S", 900.0, minimum=0),
            max_actions_per_zone_per_hour=_get_int("AGENT_MAX_ACTIONS_PER_ZONE_PER_HOUR", 4,
                                                   minimum=1, maximum=100),
            max_data_age_s=_get_float("AGENT_MAX_DATA_AGE_S", 1800.0, minimum=1),
            max_isolated_sensors_per_zone=_get_int("AGENT_MAX_ISOLATED_SENSORS_PER_ZONE", 2,
                                                   minimum=1, maximum=10),
            audit_log=_resolve_path(_get_str("AGENT_AUDIT_LOG", "logs/agent_decisions.jsonl")),
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
        self.EXTERNAL_DATA = ExternalDataSettings.from_env()
        self.OPTIMIZATION = OptimizationSettings.from_env()
        self.AGENT = AgentSettings.from_env()
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
            "weather_provider": self.EXTERNAL_DATA.weather_provider,
            "carbon_provider": self.EXTERNAL_DATA.carbon_provider,
            "agent_llm": f"{self.AGENT.resolved_provider}"
                         + (f" ({self.AGENT.llm_model})" if self.AGENT.resolved_provider == "anthropic" else ""),
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
