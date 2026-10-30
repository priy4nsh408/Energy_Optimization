"""Shared plumbing for Kafka -> PostgreSQL worker services.

Error policy for every write:
    * Database unreachable  -> :class:`TransientProcessingError`; the consumer
      retries indefinitely and never skips the message.
    * Invalid message       -> stored in ``invalid_events`` (data-quality log).
    * Database rejects data -> (e.g. a CHECK constraint) also stored in
      ``invalid_events``; the message is not retried.
    * Duplicate message     -> ignored by ``ON CONFLICT`` and counted.
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections import Counter
from typing import Any, Callable

import psycopg

from config import BaseConfig, ConfigurationError, get_config
from database.db import Database, DatabaseUnavailableError
from database.models import InvalidEventRecord
from database.repositories import insert_record
from processors.sensor_processor import rejection
from streaming.consumer import (
    ConsumedMessage,
    JsonConsumer,
    KafkaConsumerFatalError,
    TransientProcessingError,
)
from streaming.kafka_config import KafkaUnavailableError, wait_for_broker
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

STATS_INTERVAL_S = 30.0


class PersistenceHandler:
    """Base class for message handlers that write to PostgreSQL."""

    worker_name = "worker"

    def __init__(self, db: Database, logger: logging.Logger) -> None:
        self.db = db
        self.logger = logger
        self.stats: Counter[str] = Counter()
        self._last_stats = time.monotonic()

    def __call__(self, message: ConsumedMessage) -> None:
        self.handle(message)
        if time.monotonic() - self._last_stats >= STATS_INTERVAL_S:
            self.log_stats()

    def handle(self, message: ConsumedMessage) -> None:
        raise NotImplementedError

    def log_stats(self) -> None:
        self._last_stats = time.monotonic()
        self.logger.info("Worker statistics", extra={"worker": self.worker_name, **self.stats})

    def write(self, message: ConsumedMessage, operation: Callable[[psycopg.Connection], Any]) -> Any:
        """Run ``operation`` in a transaction with the error policy above.

        Returns the operation's result, or ``None`` if the database rejected the data.
        """
        try:
            with self.db.transaction() as conn:
                return operation(conn)
        except DatabaseUnavailableError as exc:
            self.stats["db_unavailable"] += 1
            raise TransientProcessingError(f"database unavailable: {exc}") from exc
        except psycopg.Error as exc:
            self.stats["db_rejected"] += 1
            self.logger.error("Database rejected message", extra={
                "topic": message.topic, "partition": message.partition,
                "offset": message.offset, "error": str(exc).strip()})
            self.reject(rejection(self.worker_name, message, [f"database: {str(exc).strip()}"]))
            return None

    def reject(self, record: InvalidEventRecord) -> None:
        """Store an invalid message in ``invalid_events`` (idempotent per offset)."""
        self.stats["rejected"] += 1
        self.logger.warning("Invalid message rejected", extra={
            "topic": record.topic, "partition": record.kafka_partition,
            "offset": record.kafka_offset, "event_id": record.event_id, "errors": record.errors})
        try:
            with self.db.transaction() as conn:
                insert_record(conn, record)
        except DatabaseUnavailableError as exc:
            raise TransientProcessingError(f"database unavailable: {exc}") from exc


def parse_worker_args(prog: str, description: str, argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument("--max-messages", type=int, help="stop after N messages")
    parser.add_argument("--idle-timeout", type=float, help="stop after N idle seconds")
    parser.add_argument("--broker-wait", type=float, default=60.0,
                        help="seconds to wait for Kafka at start-up")
    parser.add_argument("--db-wait", type=float, default=60.0,
                        help="seconds to wait for PostgreSQL at start-up")
    return parser.parse_args(argv)


def run_worker(*, service: str, group: str, topics: list[str], args: argparse.Namespace,
               build_handler: Callable[[Database, BaseConfig, logging.Logger], PersistenceHandler],
               stop_event: threading.Event | None = None) -> int:
    """Start a consumer service; returns a process exit code."""
    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, service)
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)

    db = Database(config.DATABASE, name=service)
    try:
        db.open(timeout_s=args.db_wait)
        db.require_schema()
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        handler = build_handler(db, config, logger)
    except (DatabaseUnavailableError, KafkaUnavailableError, RuntimeError) as exc:
        logger.error("Worker not started", extra={"error": str(exc)})
        db.close()
        return 1

    consumer = JsonConsumer(config.KAFKA, group, topics)
    logger.info("Worker started", extra={"group": consumer.group_id, "topics": topics})
    try:
        consumer.run(handler, stop_event=stop_event, max_messages=args.max_messages,
                     idle_timeout_s=args.idle_timeout)
    except KafkaConsumerFatalError:
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        handler.log_stats()
        db.close()
    return 0
