"""Cyberattack simulation on sensor data (for research and evaluation only).

Attacks tamper with the *reported* readings between the sensors and the
consumers (data-integrity attacks); the physical building is unchanged.
Each manipulated event gets a ground-truth label that is kept *outside* the
event, so detectors never see it.

Supported attacks:

* ``temperature_spoofing`` - abrupt offset on the temperature reading.
* ``occupancy_manipulation`` - occupancy reported as 0 while people are
  present (``mode=hide``) or as a high count in an empty zone (``mode=inflate``).
* ``replay`` - the values of readings captured ``lag`` intervals earlier are
  re-sent with fresh metadata (event id, time, sequence) to mask the current
  state; ``mode=verbatim`` re-sends the old message unchanged.
* ``abnormal_power`` - all power readings scaled by ``factor`` (kept
  internally consistent so schema validation still passes).
* ``gradual_manipulation`` - temperature offset ramping at ``rate_c_per_h``,
  designed to stay below per-reading detection thresholds.
"""

from __future__ import annotations

import copy
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

ATTACK_TYPES = ("temperature_spoofing", "occupancy_manipulation", "replay",
                "abnormal_power", "gradual_manipulation")
POWER_FIELDS = ("hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw", "energy_kwh")
REPLAY_FIELDS = ("temperature_c", "humidity_pct", "co2_ppm", "occupancy_count", "lighting_lux",
                 "lighting_level_pct", "hvac_status", "hvac_power_kw", "lighting_power_kw",
                 "appliance_power_kw", "total_power_kw", "energy_kwh")
_NAMESPACE = uuid.UUID("3b6e8f2a-5c1d-4e7f-9a2b-6c8d0e1f2a3b")


@dataclass(frozen=True)
class AttackScenario:
    attack_type: str
    zone_id: str
    start: datetime
    end: datetime
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.attack_type not in ATTACK_TYPES:
            raise ValueError(f"Unknown attack type '{self.attack_type}'. Known: {ATTACK_TYPES}")
        if self.end <= self.start:
            raise ValueError("Attack end must be after its start")

    def active(self, zone_id: str, when: datetime) -> bool:
        return zone_id == self.zone_id and self.start <= when < self.end

    @classmethod
    def parse(cls, spec: str, simulation_start: datetime) -> "AttackScenario":
        """Parse ``TYPE:ZONE:START_MIN:DURATION_MIN[:key=value,...]`` (minutes of simulated time)."""
        parts = spec.split(":")
        if len(parts) < 4:
            raise ValueError("Attack spec must be TYPE:ZONE:START_MIN:DURATION_MIN[:key=value,...]")
        params: dict[str, Any] = {}
        if len(parts) > 4 and parts[4]:
            for item in parts[4].split(","):
                key, _, value = item.partition("=")
                try:
                    params[key] = float(value)
                except ValueError:
                    params[key] = value
        start = simulation_start + timedelta(minutes=float(parts[2]))
        return cls(parts[0], parts[1], start, start + timedelta(minutes=float(parts[3])), params)


@dataclass(frozen=True)
class AttackLabel:
    """Ground truth for one emitted event."""

    event_id: str
    zone_id: str
    timestamp: datetime
    attack_type: str | None          # None = clean
    scenario_index: int | None


class AttackInjector:
    """Applies scheduled attacks to a stream of sensor events."""

    def __init__(self, scenarios: list[AttackScenario]) -> None:
        self.scenarios = list(scenarios)
        self.labels: list[AttackLabel] = []
        self._history: dict[str, deque[dict]] = defaultdict(lambda: deque(maxlen=400))

    def _scenario_for(self, zone_id: str, when: datetime) -> tuple[int, AttackScenario] | None:
        for index, scenario in enumerate(self.scenarios):
            if scenario.active(zone_id, when):
                return index, scenario
        return None

    def apply(self, event: dict[str, Any]) -> dict[str, Any]:
        """Return the (possibly manipulated) event and record its label."""
        zone, when = event["zone_id"], datetime.fromisoformat(event["timestamp"])
        history = self._history[zone]
        match = self._scenario_for(zone, when)
        out = event
        if match is not None:
            index, scenario = match
            out = self._manipulate(copy.deepcopy(event), scenario, history, when)
        history.append(event)           # the attacker records the *genuine* readings
        attack_type = match[1].attack_type if match and out != event else None
        self.labels.append(AttackLabel(out["event_id"], zone, when, attack_type,
                                       match[0] if attack_type else None))
        return out

    def wrap(self, sink: Callable[[dict[str, Any]], None]) -> Callable[[dict[str, Any]], None]:
        """Sink wrapper: manipulate each event, then forward it."""
        return lambda event: sink(self.apply(event))

    # -- attacks ------------------------------------------------------------------------
    def _manipulate(self, event: dict, scenario: AttackScenario, history: deque,
                    when: datetime) -> dict:
        p = scenario.params
        kind = scenario.attack_type
        if kind == "temperature_spoofing":
            event["temperature_c"] = round(event["temperature_c"] + float(p.get("offset_c", 4.0)), 1)
        elif kind == "gradual_manipulation":
            hours = (when - scenario.start).total_seconds() / 3600
            offset = min(float(p.get("max_offset_c", 6.0)), float(p.get("rate_c_per_h", 0.5)) * hours)
            event["temperature_c"] = round(event["temperature_c"] + offset, 1)
        elif kind == "occupancy_manipulation":
            if p.get("mode", "hide") == "hide":
                event["occupancy_count"] = 0
            else:
                event["occupancy_count"] = int(p.get("count", 25))
        elif kind == "abnormal_power":
            factor = float(p.get("factor", 3.0))
            for name in POWER_FIELDS:
                event[name] = round(event[name] * factor, 5)
        elif kind == "replay":
            lag = int(p.get("lag", 288))       # default: same time one day earlier
            if len(history) < lag:
                return event                     # nothing captured yet: no manipulation
            old = history[-lag]
            if p.get("mode") == "verbatim":
                return copy.deepcopy(old)        # identical old message (old id and timestamp)
            for name in REPLAY_FIELDS:
                event[name] = old[name]
            event["event_id"] = str(uuid.uuid5(_NAMESPACE, f"{event['event_id']}|replay"))
        return event
