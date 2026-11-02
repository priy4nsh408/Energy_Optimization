"""Message counts per Kafka topic for the Data Pipeline page.

Reads only partition offsets through the admin client (no consumer group, no
messages read), so the dashboard still never consumes Kafka. Results are cached
for a few seconds; if the broker is unreachable the page simply shows no counts.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from config import KafkaSettings
from streaming.topics import TOPICS, topic_name

logger = logging.getLogger(__name__)


class KafkaTopicStats:
    def __init__(self, settings: KafkaSettings, *, cache_s: float = 5.0, timeout_s: float = 3.0) -> None:
        self.settings, self.cache_s, self.timeout_s = settings, cache_s, timeout_s
        self._admin = None
        self._cached: tuple[float, dict[str, Any] | None] = (0.0, None)
        self._lock = threading.Lock()

    def snapshot(self) -> dict[str, Any] | None:
        """``{logical topic: {"messages": retained messages, "end_offset": total ever written}}`` or ``None``."""
        with self._lock:
            at, value = self._cached
            if time.monotonic() - at < self.cache_s:
                return value
            try:
                value = self._read()
            except Exception as exc:  # noqa: BLE001 - optional information
                logger.debug("Kafka topic stats unavailable", extra={"error": str(exc)})
                value = None
            self._cached = (time.monotonic(), value)
            return value

    def _read(self) -> dict[str, Any]:
        from confluent_kafka import TopicPartition
        from confluent_kafka.admin import OffsetSpec

        from streaming.kafka_config import create_admin_client

        if self._admin is None:
            self._admin = create_admin_client(self.settings)
        metadata = self._admin.list_topics(timeout=self.timeout_s)
        names = {topic_name(self.settings, logical): logical for logical in TOPICS}
        partitions = [TopicPartition(name, p) for name, t in metadata.topics.items() if name in names
                      for p in t.partitions]
        result = {logical: {"messages": 0, "end_offset": 0, "partitions": 0} for logical in TOPICS}
        if not partitions:
            return result
        for spec, key in ((OffsetSpec.latest(), "end_offset"), (OffsetSpec.earliest(), "start")):
            futures = self._admin.list_offsets({tp: spec for tp in partitions}, request_timeout=self.timeout_s)
            for tp, future in futures.items():
                entry = result[names[tp.topic]]
                offset = future.result().offset
                if key == "end_offset":
                    entry["end_offset"] += offset
                    entry["messages"] += offset
                    entry["partitions"] += 1
                else:
                    entry["messages"] -= offset
        return result
