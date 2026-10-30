"""Execute *approved* actions - on the simulated building only.

Two gateways:

* :class:`SimulatorGateway` - the simulator runs in the same process
  (``python -m agent.demo``); actions are applied immediately.
* :class:`KafkaCommandGateway` - the agent runs as a service
  (``workers.agent_service``). Approved control commands are published to
  ``optimization_commands``; the simulator process
  (``workers.sensor_publisher --apply-commands``) applies them and publishes
  ``executed``/``failed`` to ``agent_actions``.

Sensor isolation is a software quarantine kept by the agent itself, so it is
executed directly in both cases. Nothing here talks to physical equipment.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Protocol

from agent.actions import CONTROL_ACTIONS, ProposedAction
from agent.isolation import SensorIsolationRegistry

logger = logging.getLogger(__name__)


class ActionGateway(Protocol):
    def execute(self, action: ProposedAction, now: datetime) -> tuple[str, str]:
        """Return (status, message); status is ``executed``, ``failed`` or ``approved`` (dispatched)."""
        ...


def apply_to_simulator(simulator, zone_id: str, action_type: str, parameters: dict[str, Any]) -> str:
    """Apply one control command to a :class:`simulator.engine.BuildingSimulator`.

    Raises ``KeyError``/``ValueError`` (unknown zone, value outside physical limits).
    """
    if action_type == "set_setpoint":
        simulator.apply_command(zone_id, setpoint_c=float(parameters["setpoint_c"]))
        return f"setpoint override {float(parameters['setpoint_c']):.1f} °C"
    if action_type == "set_hvac_mode":
        simulator.apply_command(zone_id, hvac_mode=str(parameters["hvac_mode"]))
        return f"HVAC mode override '{parameters['hvac_mode']}'"
    if action_type == "set_lighting":
        simulator.apply_command(zone_id, lighting_level=float(parameters["lighting_level"]))
        return f"lighting override {float(parameters['lighting_level']):.0%}"
    if action_type == "clear_overrides":
        simulator.building.clear_overrides(zone_id)
        return "overrides cleared, zone back on its baseline schedule"
    raise ValueError(f"'{action_type}' is not a simulator command")


def _sensor_action(isolation: SensorIsolationRegistry, action: ProposedAction, now: datetime) -> tuple[str, str]:
    sensor = action.parameters["sensor"]
    if action.action_type == "isolate_sensor":
        isolation.isolate(action.zone_id, sensor, since=now, reason=action.reason,
                          alert_id=action.related_alert_id)
        return "executed", f"{sensor} sensor in {action.zone_id} isolated (readings no longer trusted)"
    restored = isolation.restore(action.zone_id, sensor)
    return ("executed", f"{sensor} sensor in {action.zone_id} restored") if restored else \
        ("failed", f"{sensor} sensor in {action.zone_id} was not isolated")


class SimulatorGateway:
    def __init__(self, simulator, isolation: SensorIsolationRegistry) -> None:
        self.simulator, self.isolation = simulator, isolation

    def execute(self, action: ProposedAction, now: datetime) -> tuple[str, str]:
        try:
            if action.action_type in CONTROL_ACTIONS:
                return "executed", apply_to_simulator(self.simulator, action.zone_id, action.action_type,
                                                      action.parameters)
            return _sensor_action(self.isolation, action, now)
        except (KeyError, ValueError) as exc:
            logger.warning("Simulated action failed", extra={"action_id": action.action_id, "error": str(exc)})
            return "failed", str(exc)


class KafkaCommandGateway:
    """Dispatch approved control commands to the simulator over Kafka."""

    def __init__(self, producer, isolation: SensorIsolationRegistry, commands_topic: str) -> None:
        self.producer, self.isolation, self.topic = producer, isolation, commands_topic

    def execute(self, action: ProposedAction, now: datetime) -> tuple[str, str]:
        if action.action_type not in CONTROL_ACTIONS:
            return _sensor_action(self.isolation, action, now)
        action.status = "approved"
        self.producer.send(self.topic, action.to_message(updated_at=datetime.now(timezone.utc)),
                           key=action.zone_id)
        return "approved", f"dispatched to the simulator via {self.topic}"


class SimulatorActuator:
    """Simulator-side executor for approved commands that arrive over Kafka.

    The Kafka consumer thread only queues commands (:meth:`handle`); the
    simulation thread applies them between readings (:meth:`drain`), so the
    building model is only touched by one thread. Each outcome is published to
    the results topic with status ``executed`` or ``failed``. Commands are
    applied at most once (by ``action_id``).
    """

    def __init__(self, simulator, producer, results_topic: str, sources: set[str]) -> None:
        import queue

        self.simulator, self.producer, self.topic, self.sources = simulator, producer, results_topic, sources
        self._queue: "queue.Queue" = queue.Queue()
        self._applied: set[str] = set()
        self.stats = {"queued": 0, "executed": 0, "failed": 0, "ignored": 0}

    def handle(self, message) -> None:
        from processors.data_validator import ValidationError, validate_action_message

        try:
            record = validate_action_message(message.value)
        except ValidationError:
            self.stats["ignored"] += 1
            return
        if record.status != "approved" or record.source not in self.sources \
                or record.action_type not in CONTROL_ACTIONS:
            self.stats["ignored"] += 1
            return
        self._queue.put(record)
        self.stats["queued"] += 1

    def drain(self) -> int:
        import queue

        applied = 0
        while True:
            try:
                record = self._queue.get_nowait()
            except queue.Empty:
                return applied
            action_id = str(record.action_id)
            if action_id in self._applied:
                continue
            self._applied.add(action_id)
            try:
                result, status = apply_to_simulator(self.simulator, record.zone_id, record.action_type,
                                                    record.parameters), "executed"
            except (KeyError, ValueError) as exc:
                result, status = str(exc), "failed"
            self.stats[status] += 1
            applied += 1
            logger.info("Command applied in simulator", extra={"action_id": action_id, "status": status,
                                                                "zone_id": record.zone_id})
            self.producer.send(self.topic, {
                "action_id": action_id, "zone_id": record.zone_id, "created_at": record.created_at.isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(), "source": record.source,
                "action_type": record.action_type, "status": status,
                "parameters": {**record.parameters, "result": result, "applied_at_sim_time":
                               self.simulator.sim_time.isoformat()},
                "reason": record.reason, "safety_reason": record.safety_reason,
                "related_alert_id": str(record.related_alert_id) if record.related_alert_id else None,
            }, key=record.zone_id)
