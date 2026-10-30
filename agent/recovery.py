"""Deterministic recovery: restore isolated sensors once their zone is quiet.

After the agent isolates a sensor, the zone runs on its baseline schedule (the
optimizer pauses it). When no security alert (data drift excluded) has been
raised for the zone for ``quiet_s`` of simulated time, this policy proposes
``restore_sensor``. The proposal goes through the same safety validator and
gateway as every other action, so the restore is recorded like any action
and the optimizer resumes the zone. No LLM is involved.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from agent.actions import ProposedAction
from agent.data import BuildingDataSource
from agent.executor import ActionGateway
from agent.isolation import SensorIsolationRegistry
from agent.safety import SafetyValidator, security_relevant


class RecoveryPolicy:
    def __init__(self, data: BuildingDataSource, isolation: SensorIsolationRegistry, validator: SafetyValidator,
                 gateway: ActionGateway, *, quiet_s: float = 3600.0) -> None:
        self.data, self.isolation, self.validator, self.gateway = data, isolation, validator, gateway
        self.quiet = timedelta(seconds=quiet_s)
        self.actions: list[ProposedAction] = []
        self._rejected_at: dict[tuple[str, str], datetime] = {}   # do not retry a rejected restore every tick

    def check(self, now: datetime) -> list[ProposedAction]:
        """Restore every isolated sensor whose zone has been quiet; returns the actions taken."""
        done = []
        for item in self.isolation.isolated():
            if now - item.since < self.quiet:
                continue
            alerts = security_relevant(self.data.alerts(item.zone_id, hours=24))
            if any(datetime.fromisoformat(str(a["detected_at"])) > now - self.quiet for a in alerts):
                continue
            key = (item.zone_id, item.sensor)
            if key in self._rejected_at and now - self._rejected_at[key] < timedelta(minutes=15):
                continue
            action = ProposedAction(
                zone_id=item.zone_id, action_type="restore_sensor", parameters={"sensor": item.sensor},
                reason=f"no security alert for {item.zone_id} in the last {self.quiet.total_seconds() / 60:.0f} min",
                created_at=now, decision_id=f"recovery|{item.zone_id}|{item.sensor}|{item.since.isoformat()}")
            verdict = self.validator.validate(action, latest_event=self.data.latest_event(item.zone_id), now=now,
                                              zone_alerts=self.data.alerts(item.zone_id, hours=24))
            action.safety_reason = verdict.text
            if verdict.approved:
                self.validator.record_approved(action, now)
                action.status, action.result = self.gateway.execute(action, now)
            else:
                action.status = "rejected"
                self._rejected_at[key] = now
            self.actions.append(action)
            done.append(action)
        return done
