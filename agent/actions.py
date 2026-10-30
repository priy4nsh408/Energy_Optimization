"""Proposed agent actions and their lifecycle (proposed -> approved | rejected -> executed | failed)."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

CONTROL_ACTIONS = ("set_setpoint", "set_hvac_mode", "set_lighting", "clear_overrides")
SENSOR_ACTIONS = ("isolate_sensor", "restore_sensor")
ACTION_TYPES = CONTROL_ACTIONS + SENSOR_ACTIONS

# Sensors that can be isolated, and the event fields each one produces.
SENSOR_FIELDS: dict[str, tuple[str, ...]] = {
    "temperature": ("temperature_c",),
    "humidity": ("humidity_pct",),
    "co2": ("co2_ppm",),
    "occupancy": ("occupancy_count",),
    "lighting": ("lighting_lux",),
    "power_meter": ("hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw", "energy_kwh"),
}
SENSORS = tuple(SENSOR_FIELDS)

_NAMESPACE = uuid.UUID("0f8a3c55-2b7e-4d1a-9c64-8e5b2f7d1a90")


@dataclass
class ProposedAction:
    zone_id: str
    action_type: str
    parameters: dict[str, Any]
    reason: str
    created_at: datetime
    decision_id: str
    related_alert_id: str | None = None
    sequence: int = 0
    status: str = "proposed"
    safety_reason: str | None = None
    result: str | None = None
    source: str = "agent"
    action_id: str = field(default="")

    def __post_init__(self) -> None:
        if not self.action_id:
            # Deterministic id: the same decision proposing the same action again is one action.
            self.action_id = str(uuid.uuid5(_NAMESPACE, f"{self.decision_id}|{self.sequence}|"
                                                        f"{self.zone_id}|{self.action_type}"))

    def to_message(self, *, updated_at: datetime | None = None) -> dict[str, Any]:
        """Message for ``optimization_commands`` / ``agent_actions`` (see processors.data_validator)."""
        return {
            "action_id": self.action_id,
            "zone_id": self.zone_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": (updated_at or self.created_at).isoformat(),
            "source": self.source,
            "action_type": self.action_type,
            "status": self.status,
            "parameters": {**self.parameters, "decision_id": self.decision_id,
                           **({"result": self.result} if self.result else {})},
            "reason": self.reason[:2000],
            "safety_reason": (self.safety_reason or "")[:2000] or None,
            "related_alert_id": self.related_alert_id,
        }

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        return data
