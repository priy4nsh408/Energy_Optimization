"""Kafka connection helpers shared by producers, consumers and admin tasks.

Connection *settings* live in ``config.KafkaSettings``; this module adds the
runtime behaviour: error callbacks that log broker disconnections, and a
start-up check that waits for the broker with exponential back-off.

Reconnection itself is handled by librdkafka: after a disconnect, clients
reconnect automatically (``reconnect.backoff.ms`` .. ``reconnect.backoff.max.ms``),
the producer keeps undelivered messages queued for up to
``message.timeout.ms``, and consumers resume from their committed offsets.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from config import KafkaSettings

logger = logging.getLogger(__name__)

# Error codes that indicate a (usually temporary) loss of connectivity.
CONNECTIVITY_ERRORS = frozenset({
    KafkaError._TRANSPORT,
    KafkaError._ALL_BROKERS_DOWN,
    KafkaError._RESOLVE,
    KafkaError._TIMED_OUT,
    KafkaError.NETWORK_EXCEPTION,
})


class KafkaUnavailableError(RuntimeError):
    """The Kafka broker could not be reached."""


def make_error_callback(client_name: str) -> Callable[[KafkaError], None]:
    """Return an ``error_cb`` that logs client-level errors with a clear severity."""

    def on_error(error: KafkaError) -> None:
        details = {"client": client_name, "code": error.name(), "reason": error.str()}
        if error.fatal():
            logger.critical("Fatal Kafka client error", extra=details)
        elif error.code() == KafkaError._ALL_BROKERS_DOWN:
            logger.warning("All Kafka brokers are down; reconnecting automatically", extra=details)
        elif error.code() in CONNECTIVITY_ERRORS:
            logger.warning("Kafka connection problem; reconnecting automatically", extra=details)
        else:
            logger.error("Kafka client error", extra=details)

    return on_error


def with_client_logging(config: dict[str, Any], client_name: str) -> dict[str, Any]:
    """Route librdkafka's internal logs and errors through Python logging.

    Without this, librdkafka prints raw lines to stderr that bypass the
    structured log format.
    """
    return {**config,
            "error_cb": make_error_callback(client_name),
            "logger": logging.getLogger(f"librdkafka.{client_name}")}


def create_admin_client(settings: KafkaSettings) -> AdminClient:
    return AdminClient(with_client_logging(settings.admin_config(), "admin"))


def wait_for_broker(settings: KafkaSettings, *, timeout_s: float = 30.0,
                    admin_factory: Callable[[dict[str, Any]], Any] = AdminClient) -> None:
    """Block until the broker answers a metadata request.

    Retries with exponential back-off (0.5 s .. 5 s) until ``timeout_s``.

    Raises:
        KafkaUnavailableError: the broker did not respond in time.
    """
    # Silence the probe client itself; this function logs each attempt instead.
    config = {**settings.admin_config(), "error_cb": lambda error: None, "log_level": 0}
    admin = admin_factory(config)
    deadline = time.monotonic() + timeout_s
    delay, attempt = 0.5, 0
    while True:
        attempt += 1
        remaining = deadline - time.monotonic()
        try:
            metadata = admin.list_topics(timeout=max(0.5, min(2.0, remaining)))
            logger.info("Kafka broker reachable", extra={
                "bootstrap_servers": settings.bootstrap_servers,
                "brokers": len(metadata.brokers), "attempts": attempt})
            return
        except KafkaException as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise KafkaUnavailableError(
                    f"Kafka broker at {settings.bootstrap_servers} not reachable after "
                    f"{timeout_s:.0f}s ({attempt} attempts): {exc}") from exc
            logger.warning("Waiting for Kafka broker", extra={
                "bootstrap_servers": settings.bootstrap_servers, "attempt": attempt,
                "retry_in_s": round(min(delay, remaining), 2)})
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 5.0)
