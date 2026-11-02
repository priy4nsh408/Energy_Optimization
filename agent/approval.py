"""Operator approval: a human gate between the safety validator and execution.

With ``AGENT_APPROVAL_MODE=manual`` the agent's gateway is wrapped by
:class:`ApprovalGate`. An action that passed the safety validator is not
executed; it becomes ``awaiting_approval`` with a plain-language explanation
(``agent.explainer``, optionally phrased by a small language model). The
operator answers on the dashboard, which publishes the answer to
``operator_decisions``; ``workers.agent_service`` calls :meth:`ApprovalGate.decide`:

* approve -> the safety validator checks the action **again** against the
  latest state (it may have changed while waiting); only if it still passes is
  it executed through the real gateway;
* decline -> ``declined``, nothing changes;
* no answer within ``AGENT_APPROVAL_TIMEOUT_S`` (real seconds) -> ``expired``.

Every transition is recorded like any other action. Recovery restores are not
gated: they return the zone to normal operation after an hour without alerts.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from agent.actions import ProposedAction
from agent.executor import ActionGateway

logger = logging.getLogger(__name__)
_MESSAGE_KEYS = {"decision_id", "result", "approval", "applied_at_sim_time"}


@dataclass
class _Pending:
    action: ProposedAction
    since: float                                   # wall clock (clock())


class ApprovalGate:
    def __init__(self, gateway: ActionGateway, validator, data, recorder, explainer, *, timeout_s: float,
                 clock: Callable[[], float] = time.time) -> None:
        self.gateway, self.validator, self.data = gateway, validator, data
        self.recorder, self.explainer, self.timeout_s, self.clock = recorder, explainer, timeout_s, clock
        self._pending: dict[str, _Pending] = {}
        self._lock = threading.Lock()
        self.stats = {"waiting": 0, "approved": 0, "declined": 0, "expired": 0, "recheck_failed": 0}

    # -- gateway interface ------------------------------------------------------------
    def execute(self, action: ProposedAction, now: datetime) -> tuple[str, str]:
        return self.execute_incident(action, now, None, None)

    def execute_incident(self, action: ProposedAction, now: datetime, alert: dict[str, Any] | None,
                         assessment: dict[str, Any] | None) -> tuple[str, str]:
        """Hold a safety-approved action for the operator (returns ``awaiting_approval``)."""
        with self._lock:
            duplicate = next((p.action for p in self._pending.values() if _same(p.action, action)), None)
        if duplicate is not None:
            return "rejected", f"an identical action is already waiting for operator approval ({duplicate.action_id})"
        action.explanation = self.explainer.explain(action, alert, assessment)
        with self._lock:
            self._pending[action.action_id] = _Pending(action, self.clock())
        self.stats["waiting"] += 1
        logger.info("Action waiting for operator approval", extra={"action_id": action.action_id,
                                                                    "zone_id": action.zone_id,
                                                                    "action_type": action.action_type})
        return "awaiting_approval", f"waiting for operator approval (expires after {self.timeout_s:.0f} s)"

    # -- operator ---------------------------------------------------------------------
    def pending(self) -> list[ProposedAction]:
        with self._lock:
            return [p.action for p in self._pending.values()]

    def decide(self, action_id: str, decision: str, operator: str = "operator",
               note: str | None = None) -> ProposedAction | None:
        """Apply an operator decision; returns the action, or ``None`` if it is not waiting (any more)."""
        with self._lock:
            entry = self._pending.pop(str(action_id), None)
        if entry is None:
            return None
        action, now = entry.action, self.data.now() or datetime.now(timezone.utc)
        comment = f" ({note})" if note else ""
        # Kept with the explanation, so it survives later updates of the result (e.g. the simulator's).
        action.explanation = {**(action.explanation or {}), "decision": {"decision": decision, "by": operator,
                                                                          "note": note, "at_sim_time": now.isoformat()}}
        if decision == "decline":
            action.status, action.result = "declined", f"declined by {operator}{comment}; nothing was changed"
            self.stats["declined"] += 1
        else:
            verdict = self.validator.validate(action, latest_event=self.data.latest_event(action.zone_id), now=now,
                                              zone_alerts=self.data.alerts(action.zone_id, hours=24))
            if not verdict.approved:
                action.status = "rejected"
                action.safety_reason = f"re-checked when approved: {verdict.text}"
                action.result = f"approved by {operator}{comment}, but the situation changed and the safety re-check failed"
                self.stats["recheck_failed"] += 1
            else:
                status, result = self.gateway.execute(action, now)
                action.status, action.result = status, f"approved by {operator}{comment}; {result}"
                self.stats["approved"] += 1
        self.recorder.record_actions([action])
        logger.info("Operator decision applied", extra={"action_id": action.action_id, "decision": decision,
                                                         "status": action.status})
        return action

    def expire(self) -> list[ProposedAction]:
        """Expire actions nobody decided on within the timeout."""
        cutoff = self.clock() - self.timeout_s
        with self._lock:
            expired = [self._pending.pop(k).action for k, p in list(self._pending.items()) if p.since <= cutoff]
        for action in expired:
            action.status = "expired"
            action.result = f"no operator decision within {self.timeout_s:.0f} s; nothing was changed"
            self.stats["expired"] += 1
        if expired:
            self.recorder.record_actions(expired)
        return expired

    def load_from_database(self, database) -> int:
        """Waiting actions survive a restart of the agent service (their timeout starts again)."""
        from psycopg.rows import dict_row

        with database.transaction() as conn, conn.cursor(row_factory=dict_row) as cur:
            rows = cur.execute("SELECT * FROM optimization_actions WHERE status = 'awaiting_approval' "
                               "AND source = 'agent'").fetchall()
        with self._lock:
            for row in rows:
                parameters = dict(row["parameters"] or {})
                action = ProposedAction(
                    zone_id=row["zone_id"], action_type=row["action_type"],
                    parameters={k: v for k, v in parameters.items() if k not in _MESSAGE_KEYS},
                    reason=row["reason"] or "", created_at=row["created_at"],
                    decision_id=str(parameters.get("decision_id", "")),
                    related_alert_id=str(row["related_alert_id"]) if row["related_alert_id"] else None,
                    status="awaiting_approval", safety_reason=row["safety_reason"],
                    action_id=str(row["action_id"]), explanation=parameters.get("approval"))
                self._pending[action.action_id] = _Pending(action, self.clock())
        return len(rows)


def _same(a: ProposedAction, b: ProposedAction) -> bool:
    return (a.zone_id, a.action_type, a.parameters) == (b.zone_id, b.action_type, b.parameters)
