"""Send commands from the dashboard: simulation control and operator decisions.

The dashboard only *publishes*; it never consumes Kafka.

* ``simulation_control`` (start, stop, attack): ``workers.simulation_manager``
  executes them and reports its state in PostgreSQL.
* ``operator_decisions`` (approve/decline an agent action waiting for
  approval): ``workers.agent_service`` applies them.

Messages are validated with the same contract as the consumer uses before they are sent.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from processors.data_validator import validate_control_message, validate_operator_decision
from streaming.topics import OPERATOR_DECISIONS, SIMULATION_CONTROL

logger = logging.getLogger(__name__)


class ControlUnavailableError(RuntimeError):
    """Kafka could not accept the command (broker down or delivery failed)."""


class SimulationControl:
    def __init__(self, producer_factory: Callable[[], Any], building_id: str, *, flush_timeout_s: float = 5.0) -> None:
        self._factory, self.building_id, self.flush_timeout_s = producer_factory, building_id, flush_timeout_s
        self._producer = None
        self._lock = threading.Lock()

    def _get_producer(self):
        with self._lock:
            if self._producer is None:
                self._producer = self._factory()
            return self._producer

    def send(self, command: str, attack: dict[str, Any] | None = None) -> dict[str, Any]:
        """Validate and publish a command; returns the message. Raises ValidationError / ControlUnavailableError."""
        message: dict[str, Any] = {"command_id": str(uuid.uuid4()), "command": command,
                                   "building_id": self.building_id,
                                   "requested_at": datetime.now(timezone.utc).isoformat()}
        if attack is not None:
            message["attack"] = attack
        validate_control_message(message)
        self._publish(SIMULATION_CONTROL, message, self.building_id)
        logger.info("Simulation command sent", extra={"command": command, "command_id": message["command_id"]})
        return message

    def send_operator_decision(self, action_id: str, decision: str, *, operator: str | None = None,
                               note: str | None = None) -> dict[str, Any]:
        """Approve or decline an agent action waiting for approval. Raises ValidationError / ControlUnavailableError."""
        message: dict[str, Any] = {"decision_id": str(uuid.uuid4()), "action_id": str(action_id),
                                   "decision": decision, "requested_at": datetime.now(timezone.utc).isoformat()}
        if operator:
            message["operator"] = operator
        if note:
            message["note"] = note
        validate_operator_decision(message)
        self._publish(OPERATOR_DECISIONS, message, str(action_id))
        logger.info("Operator decision sent", extra={"action_id": str(action_id), "decision": decision})
        return message

    def _publish(self, topic: str, message: dict[str, Any], key: str) -> None:
        producer = self._get_producer()
        failed_before = producer.stats.failed
        try:
            producer.send(topic, message, key=key)
        except Exception as exc:  # noqa: BLE001 - reported to the user as 503
            raise ControlUnavailableError(f"could not queue the command: {exc}") from exc
        if producer.flush(self.flush_timeout_s) or producer.stats.failed > failed_before:
            raise ControlUnavailableError("Kafka did not confirm the command (is the broker running?)")

    def close(self) -> None:
        with self._lock:
            if self._producer is not None:
                self._producer.close(timeout_s=2)
                self._producer = None
