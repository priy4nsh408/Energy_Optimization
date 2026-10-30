"""JSON Kafka consumer with error handling and manual offset commits.

Each service creates its own consumer group, so independent services (database
writer, ML worker, security monitor, ...) each receive every message, while
several instances of the *same* service share the partitions.

Usage::

    consumer = JsonConsumer(config.KAFKA, "db_writer", ["building_sensor_data"])
    consumer.run(handle_message, stop_event=stop)

Error handling:
    * Broker disconnects: logged, the client reconnects and resumes from the
      last committed offset.
    * Invalid JSON ("poison" messages): logged, counted and skipped.
    * Handler exceptions: retried with back-off, then logged and skipped, so
      one bad message cannot block a partition.
    * :class:`TransientProcessingError` from a handler (e.g. database down):
      retried with back-off *indefinitely* and never skipped. On shutdown the
      message stays uncommitted and is processed again on the next start.
    * Fatal client errors: raised as :class:`KafkaConsumerFatalError`.

Offsets are stored only after a message has been handled and committed
automatically from that store (at-least-once delivery), so handlers must be
idempotent (e.g. use ``event_id``).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from config import KafkaSettings
from streaming.kafka_config import CONNECTIVITY_ERRORS, with_client_logging
from streaming.serialization import DeserializationError, decode_key, deserialize_value
from streaming.topics import logical_name, topic_name

logger = logging.getLogger(__name__)


class KafkaConsumerFatalError(RuntimeError):
    """The consumer hit an unrecoverable error."""


class TransientProcessingError(RuntimeError):
    """Raised by a handler when processing should be retried later, never skipped.

    Use it for temporary infrastructure failures (database unavailable, ...),
    not for bad data.
    """


@dataclass(frozen=True)
class ConsumedMessage:
    """A decoded Kafka message passed to handlers."""

    topic: str                      # logical topic name
    key: str | None
    value: dict[str, Any]
    partition: int
    offset: int
    timestamp_ms: int | None
    headers: dict[str, str] = field(default_factory=dict)


MessageHandler = Callable[[ConsumedMessage], None]


@dataclass
class ConsumerStats:
    received: int = 0
    processed: int = 0
    invalid: int = 0            # undecodable payloads skipped
    handler_failures: int = 0   # messages skipped after handler retries
    transient_retries: int = 0  # retries caused by TransientProcessingError
    errors: int = 0             # client/broker errors reported by poll()


class JsonConsumer:
    """Polls JSON messages from one or more topics and dispatches them to a handler."""

    MAX_TRANSIENT_BACKOFF_S = 30.0

    def __init__(self, settings: KafkaSettings, group_name: str, topics: Sequence[str], *,
                 max_handler_retries: int = 3, retry_backoff_s: float = 0.5,
                 consumer_factory: Callable[[dict[str, Any]], Any] = Consumer) -> None:
        if not topics:
            raise ValueError("At least one topic is required.")
        self.settings = settings
        self.group_name = group_name
        self.topics = [topic_name(settings, t) for t in topics]
        self.max_handler_retries = max_handler_retries
        self.retry_backoff_s = retry_backoff_s
        self.stats = ConsumerStats()
        config = with_client_logging(settings.consumer_config(group_name), f"consumer.{group_name}")
        config["client.id"] = f"{settings.client_id}.{group_name}"
        self.group_id = config["group.id"]
        self._consumer = consumer_factory(config)
        self._subscribed = False
        self._closed = False

    # -- lifecycle ------------------------------------------------------------------
    def _subscribe(self) -> None:
        if not self._subscribed:
            self._consumer.subscribe(self.topics, on_assign=self._on_assign,
                                     on_revoke=self._on_revoke)
            self._subscribed = True
            logger.info("Consumer subscribed",
                        extra={"group_id": self.group_id, "topics": self.topics})

    def _on_assign(self, consumer: Any, partitions: list) -> None:
        logger.info("Partitions assigned", extra={
            "group_id": self.group_id,
            "partitions": [f"{p.topic}[{p.partition}]" for p in partitions]})

    def _on_revoke(self, consumer: Any, partitions: list) -> None:
        # Commit progress before another group member takes over these partitions.
        self._commit_sync()
        logger.info("Partitions revoked", extra={
            "group_id": self.group_id,
            "partitions": [f"{p.topic}[{p.partition}]" for p in partitions]})

    def close(self) -> None:
        """Commit processed offsets and leave the consumer group."""
        if self._closed:
            return
        self._closed = True
        if self._subscribed:
            self._commit_sync()
        self._consumer.close()
        logger.info("Consumer closed", extra={"group_id": self.group_id, **vars(self.stats)})

    def __enter__(self) -> "JsonConsumer":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- main loop --------------------------------------------------------------------
    def run(self, handler: MessageHandler, *, stop_event: threading.Event | None = None,
            max_messages: int | None = None, idle_timeout_s: float | None = None,
            poll_timeout_s: float = 1.0) -> ConsumerStats:
        """Consume until stopped.

        Args:
            handler: called for every decoded message.
            stop_event: set from another thread (or a signal handler) to stop.
            max_messages: stop after this many messages were received.
            idle_timeout_s: stop when no message arrived for this long.
            poll_timeout_s: maximum blocking time of one poll.
        """
        stop_event = stop_event or threading.Event()
        self._subscribe()
        last_message_at = time.monotonic()
        while not stop_event.is_set():
            if max_messages is not None and self.stats.received >= max_messages:
                break
            message = self._consumer.poll(poll_timeout_s)
            if message is None:
                if idle_timeout_s is not None and time.monotonic() - last_message_at >= idle_timeout_s:
                    logger.info("Consumer idle timeout reached", extra={"group_id": self.group_id})
                    break
                continue
            if message.error():
                self._handle_error(message.error(), stop_event)
                continue
            last_message_at = time.monotonic()
            self.stats.received += 1
            self._process(message, handler, stop_event)
        return self.stats

    def _handle_error(self, error: KafkaError, stop_event: threading.Event) -> None:
        if error.code() == KafkaError._PARTITION_EOF:
            return
        self.stats.errors += 1
        details = {"group_id": self.group_id, "code": error.name(), "reason": error.str()}
        if error.fatal():
            logger.critical("Fatal Kafka consumer error", extra=details)
            raise KafkaConsumerFatalError(error.str())
        if error.code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
            logger.warning("Subscribed topic does not exist yet "
                           "(run: python -m streaming.topics)", extra=details)
            stop_event.wait(self.retry_backoff_s * 4)
        elif error.code() in CONNECTIVITY_ERRORS:
            logger.warning("Kafka connection problem; consumer will reconnect", extra=details)
        else:
            logger.error("Kafka consumer error", extra=details)

    def _process(self, message: Message, handler: MessageHandler,
                 stop_event: threading.Event) -> None:
        location = {"topic": message.topic(), "partition": message.partition(),
                    "offset": message.offset()}
        try:
            value = deserialize_value(message.value())
        except DeserializationError as exc:
            self.stats.invalid += 1
            logger.error("Skipping undecodable message", extra={**location, "error": str(exc)})
            self._commit(message)
            return

        consumed = ConsumedMessage(
            topic=logical_name(self.settings, message.topic()),
            key=decode_key(message.key()),
            value=value,
            partition=message.partition(),
            offset=message.offset(),
            timestamp_ms=message.timestamp()[1] if message.timestamp() else None,
            headers={k: v.decode("utf-8", errors="replace") if v is not None else ""
                     for k, v in (message.headers() or [])},
        )
        attempts, transient_attempt = self.max_handler_retries + 1, 0
        attempt = 0
        while True:
            try:
                handler(consumed)
                self.stats.processed += 1
                break
            except TransientProcessingError as exc:
                if stop_event.is_set():
                    logger.warning("Stopping during transient failure; message will be "
                                   "processed again after restart", extra=location)
                    return  # offset not stored
                transient_attempt += 1
                self.stats.transient_retries += 1
                delay = min(self.MAX_TRANSIENT_BACKOFF_S,
                            self.retry_backoff_s * 2 ** min(transient_attempt - 1, 10))
                logger.warning("Transient processing failure; retrying", extra={
                    **location, "attempt": transient_attempt, "retry_in_s": round(delay, 2),
                    "error": str(exc)})
                stop_event.wait(delay)
            except Exception as exc:  # noqa: BLE001 - handler errors must not kill the consumer
                attempt += 1
                if stop_event.is_set():
                    logger.warning("Stopping during handler retries; message will be "
                                   "processed again after restart", extra=location)
                    return  # offset not stored
                if attempt >= attempts:
                    self.stats.handler_failures += 1
                    logger.error("Handler failed; skipping message",
                                 extra={**location, "attempts": attempt, "error": repr(exc)},
                                 exc_info=True)
                    break
                logger.warning("Handler failed; retrying",
                               extra={**location, "attempt": attempt, "error": repr(exc)})
                stop_event.wait(self.retry_backoff_s * attempt)
        self._commit(message)

    # -- offsets ------------------------------------------------------------------------
    def _commit(self, message: Message) -> None:
        """Mark ``message`` as processed; librdkafka commits stored offsets periodically."""
        try:
            self._consumer.store_offsets(message=message)
        except KafkaException as exc:
            # e.g. partition revoked meanwhile; the message will be redelivered.
            logger.warning("Could not store offset", extra={
                "group_id": self.group_id, "error": str(exc)})

    def _commit_sync(self) -> None:
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            # _NO_OFFSET simply means nothing new to commit.
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                logger.warning("Synchronous offset commit failed",
                               extra={"group_id": self.group_id, "error": str(exc)})
