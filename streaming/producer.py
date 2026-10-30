"""JSON Kafka producer with delivery tracking.

Usage::

    with JsonProducer(config.KAFKA) as producer:
        producer.send("building_sensor_data", event, key=event["zone_id"])

``send`` is asynchronous: the message is queued locally and delivered in the
background. Each delivery report updates :attr:`JsonProducer.stats`, failed
deliveries are logged, and an optional callback is invoked. If the broker is
down, librdkafka keeps retrying queued messages for ``message.timeout.ms``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from config import KafkaSettings
from streaming.kafka_config import with_client_logging
from streaming.serialization import encode_key, serialize_value
from streaming.topics import topic_name

logger = logging.getLogger(__name__)

DeliveryFailureCallback = Callable[[KafkaError, str, str | None], None]


class ProducerError(RuntimeError):
    """A message could not be queued for delivery."""


@dataclass
class ProducerStats:
    sent: int = 0          # queued locally
    delivered: int = 0     # acknowledged by the broker
    failed: int = 0        # delivery failed permanently
    queue_full_retries: int = 0

    @property
    def pending(self) -> int:
        return self.sent - self.delivered - self.failed


class JsonProducer:
    """Thread-safe wrapper around ``confluent_kafka.Producer`` for JSON events."""

    MAX_QUEUE_FULL_RETRIES = 5

    def __init__(self, settings: KafkaSettings, *, name: str = "producer",
                 on_delivery_failure: DeliveryFailureCallback | None = None,
                 producer_factory: Callable[[dict[str, Any]], Any] = Producer) -> None:
        self.settings = settings
        self.name = name
        self.stats = ProducerStats()
        self._on_delivery_failure = on_delivery_failure
        config = with_client_logging(settings.producer_config(), name)
        config["client.id"] = f"{settings.client_id}.{name}"
        self._producer = producer_factory(config)

    def send(self, topic: str, value: dict[str, Any], *, key: str | None = None,
             headers: dict[str, str] | None = None) -> None:
        """Queue ``value`` (a dict) as JSON on the logical ``topic``.

        Raises:
            ConfigurationError: unknown topic.
            SerializationError: value is not JSON-serialisable.
            ProducerError: the local queue stayed full or the client rejected the message.
        """
        physical = topic_name(self.settings, topic)
        payload = serialize_value(value)
        message_headers = [("content-type", b"application/json")]
        message_headers += [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

        for attempt in range(self.MAX_QUEUE_FULL_RETRIES + 1):
            try:
                self._producer.produce(physical, value=payload, key=encode_key(key),
                                       headers=message_headers, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Local queue full (e.g. broker down for a long time): serve
                # delivery reports to free space, then retry.
                self.stats.queue_full_retries += 1
                if attempt == self.MAX_QUEUE_FULL_RETRIES:
                    raise ProducerError(f"Producer queue full; message to {physical} dropped") from None
                logger.warning("Producer queue full; waiting for deliveries",
                               extra={"topic": physical, "attempt": attempt + 1})
                self._producer.poll(1.0)
            except KafkaException as exc:
                raise ProducerError(f"Kafka rejected message to {physical}: {exc}") from exc
        self.stats.sent += 1
        self._producer.poll(0)  # serve delivery callbacks without blocking

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        key = message.key().decode("utf-8", errors="replace") if message.key() else None
        if error is not None:
            self.stats.failed += 1
            logger.error("Kafka delivery failed", extra={
                "topic": message.topic(), "key": key, "code": error.name(), "reason": error.str()})
            if self._on_delivery_failure:
                self._on_delivery_failure(error, message.topic(), key)
            return
        self.stats.delivered += 1
        logger.debug("Kafka message delivered", extra={
            "topic": message.topic(), "key": key,
            "partition": message.partition(), "offset": message.offset()})

    def poll(self, timeout_s: float = 0.0) -> int:
        """Serve delivery callbacks; returns the number of events handled."""
        return self._producer.poll(timeout_s)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Wait for queued messages; returns how many are still undelivered."""
        remaining = self._producer.flush(timeout_s)
        if remaining:
            logger.error("Messages still undelivered after flush",
                         extra={"remaining": remaining, "timeout_s": timeout_s})
        return remaining

    def close(self, timeout_s: float = 10.0) -> None:
        remaining = self.flush(timeout_s)
        logger.info("Producer closed", extra={
            "producer": self.name, "sent": self.stats.sent, "delivered": self.stats.delivered,
            "failed": self.stats.failed, "undelivered": remaining})

    def __enter__(self) -> "JsonProducer":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
