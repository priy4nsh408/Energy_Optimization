"""Record agent decisions and the outcome of every proposed action.

* :class:`JsonlRecorder` - append-only JSON Lines audit log (always on).
* :class:`PostgresRecorder` - ``agent_decisions`` and ``optimization_actions``.
* :class:`KafkaActionRecorder` - final action states on ``agent_actions``
  (``workers.record_persistence`` stores them too).
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from agent.actions import ProposedAction
from database.models import AgentDecisionRecord
from processors.data_validator import validate_action_message

logger = logging.getLogger(__name__)


class DecisionRecorder(Protocol):
    def record(self, decision: AgentDecisionRecord, actions: list[ProposedAction]) -> None: ...


def decision_to_dict(decision: AgentDecisionRecord) -> dict:
    data = asdict(decision)
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()
        elif key in ("decision_id", "alert_id") and value is not None:
            data[key] = str(value)
    return data


class JsonlRecorder:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    def record(self, decision: AgentDecisionRecord, actions: list[ProposedAction]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(decision_to_dict(decision), default=str, ensure_ascii=False)
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


class PostgresRecorder:
    def __init__(self, database) -> None:
        self.db = database

    def record_actions(self, actions: list[ProposedAction]) -> None:
        from database.repositories import upsert_optimization_action

        with self.db.transaction() as conn:
            for action in actions:
                upsert_optimization_action(conn, validate_action_message(action.to_message(
                    updated_at=action.created_at), topic="agent_actions"))

    def record(self, decision: AgentDecisionRecord, actions: list[ProposedAction]) -> None:
        from database.repositories import insert_record, upsert_optimization_action

        with self.db.transaction() as conn:
            insert_record(conn, decision)
            for action in actions:
                upsert_optimization_action(conn, validate_action_message(action.to_message(
                    updated_at=decision.finished_at), topic="agent_actions"))


class KafkaActionRecorder:
    def __init__(self, producer, topic: str) -> None:
        self.producer, self.topic = producer, topic

    def record_actions(self, actions: list[ProposedAction]) -> None:
        for action in actions:
            self.producer.send(self.topic, action.to_message(updated_at=datetime.now(timezone.utc)),
                               key=action.zone_id)

    def record(self, decision: AgentDecisionRecord, actions: list[ProposedAction]) -> None:
        for action in actions:
            self.producer.send(self.topic, action.to_message(updated_at=datetime.now(timezone.utc)),
                               key=action.zone_id)


class CompositeRecorder:
    """Record to several sinks; one failing sink does not stop the others."""

    def __init__(self, *recorders: DecisionRecorder) -> None:
        self.recorders = recorders

    def record_actions(self, actions: list[ProposedAction]) -> None:
        """Actions without an agent decision (e.g. recovery restores)."""
        for recorder in self.recorders:
            if not hasattr(recorder, "record_actions"):
                continue
            try:
                recorder.record_actions(actions)
            except Exception:  # noqa: BLE001
                logger.exception("Action recorder failed", extra={"recorder": type(recorder).__name__})

    def record(self, decision: AgentDecisionRecord, actions: list[ProposedAction]) -> None:
        for recorder in self.recorders:
            try:
                recorder.record(decision, actions)
            except Exception:  # noqa: BLE001
                logger.exception("Decision recorder failed", extra={"recorder": type(recorder).__name__,
                                                                    "decision_id": str(decision.decision_id)})
