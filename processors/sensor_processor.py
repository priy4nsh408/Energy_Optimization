"""Turns raw Kafka messages into records or rejection entries.

Shared by the persistence workers: validates a message with the right
validator and, if it is invalid, produces an :class:`InvalidEventRecord` for
the data-quality log instead of raising.
"""

from __future__ import annotations

from typing import Any, Callable

from database.models import InvalidEventRecord
from processors.data_validator import ValidationError
from streaming.consumer import ConsumedMessage


def rejection(worker: str, message: ConsumedMessage, errors: list[str]) -> InvalidEventRecord:
    value = message.value if isinstance(message.value, dict) else {"raw": message.value}
    event_id = value.get("event_id") or value.get("alert_id") or value.get("action_id")
    zone_id = value.get("zone_id")
    return InvalidEventRecord(
        worker=worker, topic=message.topic, kafka_partition=message.partition,
        kafka_offset=message.offset,
        event_id=str(event_id)[:100] if event_id is not None else None,
        zone_id=str(zone_id)[:100] if zone_id is not None else None,
        errors=errors, payload=value)


def validate_or_reject(worker: str, message: ConsumedMessage,
                       validator: Callable[..., Any], **kwargs: Any) -> tuple[Any | None, InvalidEventRecord | None]:
    """Return ``(record, None)`` if valid, else ``(None, rejection)``."""
    try:
        return validator(message.value, **kwargs), None
    except ValidationError as exc:
        return None, rejection(worker, message, exc.errors)
