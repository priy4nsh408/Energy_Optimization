"""Service: generic consumer that monitors one or more topics.

Logs every message (or only a periodic summary) and reports end-to-end
latency from the producer's ``emitted_at`` timestamp. It is the template for
the real consumer services (database writer, ML worker, security monitor):
each runs as a separate process with its own consumer group.

    python -m workers.topic_monitor                                   # building_sensor_data
    python -m workers.topic_monitor --topics security_alerts agent_actions --group audit
    python -m workers.topic_monitor --max-messages 50 --idle-timeout 10
"""

from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
from collections import Counter, OrderedDict
from datetime import datetime, timezone

from config import ConfigurationError, get_config
from streaming.consumer import ConsumedMessage, JsonConsumer, KafkaConsumerFatalError
from streaming.kafka_config import KafkaUnavailableError, wait_for_broker
from streaming.topics import BUILDING_SENSOR_DATA, TOPICS
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SUMMARY_INTERVAL_S = 10.0
DEDUP_WINDOW = 10_000  # recent event ids remembered to detect redeliveries


class TopicMonitor:
    """Message handler that keeps simple statistics."""

    def __init__(self, logger, verbose: bool) -> None:
        self.logger = logger
        self.verbose = verbose
        self.per_key: Counter[str] = Counter()
        self.latencies_ms: list[float] = []
        self.duplicates = 0
        self._recent_ids: OrderedDict[str, None] = OrderedDict()
        self._last_summary = time.monotonic()

    def _is_duplicate(self, event_id: str | None) -> bool:
        """Detect at-least-once redeliveries (e.g. after a broker restart)."""
        if not event_id:
            return False
        if event_id in self._recent_ids:
            return True
        self._recent_ids[event_id] = None
        if len(self._recent_ids) > DEDUP_WINDOW:
            self._recent_ids.popitem(last=False)
        return False

    def __call__(self, message: ConsumedMessage) -> None:
        if self._is_duplicate(message.value.get("event_id")):
            self.duplicates += 1
            self.logger.debug("Duplicate delivery ignored", extra={
                "topic": message.topic, "partition": message.partition, "offset": message.offset})
            return
        self.per_key[f"{message.topic}/{message.key}"] += 1
        emitted_at = message.value.get("emitted_at")
        latency_ms = None
        if emitted_at:
            try:
                sent = datetime.fromisoformat(emitted_at)
                latency_ms = (datetime.now(timezone.utc) - sent).total_seconds() * 1000
                self.latencies_ms.append(latency_ms)
            except ValueError:
                pass
        if self.verbose:
            self.logger.info("Message received", extra={
                "topic": message.topic, "key": message.key, "partition": message.partition,
                "offset": message.offset, "timestamp": message.value.get("timestamp"),
                "latency_ms": round(latency_ms, 1) if latency_ms is not None else None})
        if time.monotonic() - self._last_summary >= SUMMARY_INTERVAL_S:
            self.log_summary()

    def log_summary(self) -> None:
        self._last_summary = time.monotonic()
        latency = {}
        if self.latencies_ms:
            ordered = sorted(self.latencies_ms)
            latency = {"latency_ms_median": round(statistics.median(ordered), 1),
                       "latency_ms_p95": round(ordered[int(0.95 * (len(ordered) - 1))], 1)}
        self.logger.info("Monitor summary", extra={
            "messages": sum(self.per_key.values()), "duplicates": self.duplicates,
            "per_key": dict(sorted(self.per_key.items())), **latency})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m workers.topic_monitor",
                                     description="Consume and monitor Kafka topics.")
    parser.add_argument("--topics", nargs="+", default=[BUILDING_SENSOR_DATA],
                        choices=sorted(TOPICS), help="logical topic names")
    parser.add_argument("--group", default="topic_monitor",
                        help="consumer group name (each service uses its own)")
    parser.add_argument("--max-messages", type=int, help="stop after N messages")
    parser.add_argument("--idle-timeout", type=float, help="stop after N idle seconds")
    parser.add_argument("--quiet", action="store_true", help="log only periodic summaries")
    parser.add_argument("--broker-wait", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    args = parse_args(argv)
    try:
        config = get_config()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, f"monitor_{args.group}")
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)

    try:
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
    except KafkaUnavailableError as exc:
        logger.error("Kafka not available; monitor not started", extra={"error": str(exc)})
        return 1

    monitor = TopicMonitor(logger, verbose=not args.quiet)
    consumer = JsonConsumer(config.KAFKA, args.group, args.topics)
    try:
        consumer.run(monitor, stop_event=stop_event, max_messages=args.max_messages,
                     idle_timeout_s=args.idle_timeout)
    except KafkaConsumerFatalError:
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        consumer.close()
        monitor.log_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
