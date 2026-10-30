"""JSON (de)serialization for Kafka message values and keys.

Values are UTF-8 encoded JSON objects. ``datetime``/``date`` values are
written as ISO 8601 strings, so producers can pass them directly.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any


class SerializationError(ValueError):
    """A value could not be encoded as JSON."""


class DeserializationError(ValueError):
    """A message payload is not a valid JSON object."""


def _default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def serialize_value(value: dict[str, Any]) -> bytes:
    """Encode a dict as compact UTF-8 JSON."""
    if not isinstance(value, dict):
        raise SerializationError(f"Kafka message value must be a dict, got {type(value).__name__}")
    try:
        return json.dumps(value, default=_default, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SerializationError(str(exc)) from exc


def deserialize_value(payload: bytes | None) -> dict[str, Any]:
    """Decode a UTF-8 JSON object; raises :class:`DeserializationError` otherwise."""
    if payload is None:
        raise DeserializationError("Message has no value")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeserializationError(f"Invalid JSON payload: {exc}") from exc
    if not isinstance(value, dict):
        raise DeserializationError(f"Expected a JSON object, got {type(value).__name__}")
    return value


def encode_key(key: str | None) -> bytes | None:
    return key.encode("utf-8") if key is not None else None


def decode_key(key: bytes | None) -> str | None:
    return key.decode("utf-8", errors="replace") if key is not None else None
