"""Service: a controllable building simulation for the dashboard.

Like ``workers.sensor_publisher``, but long-running and remote-controlled:

* consumes ``simulation_control`` (published by the dashboard): ``start``,
  ``stop`` (pause; simulated time stands still) and ``attack`` (inject a
  simulated attack into one zone, starting at the next reading);
* publishes readings to ``building_sensor_data`` while running;
* applies *approved* agent commands from ``optimization_commands`` to the
  simulator (as ``sensor_publisher --apply-commands``; disable with
  ``--no-apply-commands``) and reports ``executed``/``failed`` on ``agent_actions``;
* writes its state and the simulated attacks to PostgreSQL
  (``simulation_status``, ``simulated_attacks``) so the dashboard can show them.

Simulated attacks manipulate published readings only (research and testing).

    python -m workers.simulation_manager                       # paused until started from the dashboard
    python -m workers.simulation_manager --autostart --interval 1 --speed 300
"""

from __future__ import annotations

import argparse
import queue
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from confluent_kafka import KafkaException
from psycopg.types.json import Jsonb

from config import ConfigurationError, get_config
from processors.data_validator import ValidationError, validate_control_message
from simulator.__main__ import add_simulation_arguments, build_settings
from simulator.attack_simulator import AttackInjector, AttackScenario
from simulator.engine import BuildingSimulator
from streaming.consumer import ConsumedMessage, JsonConsumer
from streaming.kafka_config import KafkaUnavailableError, create_admin_client, wait_for_broker
from streaming.producer import JsonProducer
from streaming.topics import (
    AGENT_ACTIONS,
    BUILDING_SENSOR_DATA,
    OPTIMIZATION_COMMANDS,
    SIMULATION_CONTROL,
    ensure_topics,
)
from utils.logging_config import configure_logging
from utils.shutdown import install_shutdown_handlers

SERVICE = "simulation_manager"
MAX_COMMAND_AGE = timedelta(minutes=10)      # older control messages are ignored (e.g. after a restart)
STATUS_EVERY_S = 3.0


class PostgresStatusStore:
    """Best-effort writes of the manager's state; the simulation never waits for the database."""

    def __init__(self, database, logger) -> None:
        self.db, self.logger = database, logger

    def _run(self, query: str, params: dict[str, Any]) -> None:
        try:
            with self.db.transaction(timeout_s=2) as conn:
                conn.execute(query, params)
        except Exception as exc:  # noqa: BLE001 - status is informational
            self.logger.warning("Could not store simulation status", extra={"error": str(exc)})

    def status(self, status: dict[str, Any]) -> None:
        self._run("""
            INSERT INTO simulation_status (building_id, state, sim_time, ticks, interval_seconds, speed_factor,
                                           seed, apply_commands, commands_applied, message, updated_at)
            VALUES (%(building_id)s, %(state)s, %(sim_time)s, %(ticks)s, %(interval_seconds)s, %(speed_factor)s,
                    %(seed)s, %(apply_commands)s, %(commands_applied)s, %(message)s, now())
            ON CONFLICT (building_id) DO UPDATE SET state = EXCLUDED.state, sim_time = EXCLUDED.sim_time,
                ticks = EXCLUDED.ticks, interval_seconds = EXCLUDED.interval_seconds,
                speed_factor = EXCLUDED.speed_factor, seed = EXCLUDED.seed,
                apply_commands = EXCLUDED.apply_commands, commands_applied = EXCLUDED.commands_applied,
                message = EXCLUDED.message, updated_at = now()""", status)

    def attack(self, attack: dict[str, Any]) -> None:
        self._run("""
            INSERT INTO simulated_attacks (attack_id, building_id, attack_type, zone_id, starts_at, ends_at, params,
                                           requested_at, source, readings_manipulated)
            VALUES (%(attack_id)s, %(building_id)s, %(attack_type)s, %(zone_id)s, %(starts_at)s, %(ends_at)s,
                    %(params)s, %(requested_at)s, %(source)s, %(readings_manipulated)s)
            ON CONFLICT (attack_id) DO UPDATE SET readings_manipulated = EXCLUDED.readings_manipulated""",
                  {**attack, "params": Jsonb(attack["params"])})


class SimulationManager:
    """Simulation state machine (no Kafka/DB code; those are injected)."""

    def __init__(self, simulator: BuildingSimulator, publish: Callable[[dict[str, Any]], None], *,
                 store=None, actuator=None, running: bool = False, logger=None) -> None:
        self.simulator, self.publish, self.store, self.actuator = simulator, publish, store, actuator
        self.running = running
        self.injector = AttackInjector([])
        self.attacks: list[dict[str, Any]] = []
        self.ticks = 0
        self.message = "running" if running else "waiting for start"
        self.logger = logger
        self.stats: Counter[str] = Counter()
        self._dirty_attacks: set[int] = set()

    @property
    def building_id(self) -> str:
        return self.simulator.settings.building_id

    def handle(self, command: dict[str, Any], *, now: datetime | None = None) -> str:
        """Apply a validated control command; returns a short description."""
        now = now or datetime.now(timezone.utc)
        if command["building_id"] != self.building_id:
            return f"ignored: command for building {command['building_id']}"
        if now - command["requested_at"] > MAX_COMMAND_AGE:
            return "ignored: stale command"
        kind = command["command"]
        if kind == "start":
            self.running, self.message = True, "running"
        elif kind == "stop":
            self.running, self.message = False, "paused from the dashboard"
        else:
            spec = command["attack"]
            if spec["zone_id"] not in self.simulator.building.zones:
                return f"ignored: unknown zone {spec['zone_id']}"
            # Starts with the next reading: [next, next + duration) covers duration / interval readings.
            start = self.simulator.sim_time + timedelta(seconds=self.simulator.settings.simulated_seconds_per_tick)
            scenario = AttackScenario(spec["attack_type"], spec["zone_id"], start,
                                      start + timedelta(minutes=spec["duration_min"]), dict(spec["params"]))
            self.injector.scenarios.append(scenario)
            attack = {"attack_id": str(command["command_id"]), "building_id": self.building_id,
                      "attack_type": scenario.attack_type, "zone_id": scenario.zone_id, "starts_at": scenario.start,
                      "ends_at": scenario.end, "params": scenario.params, "requested_at": command["requested_at"],
                      "source": "dashboard", "readings_manipulated": 0}
            self.attacks.append(attack)
            self._dirty_attacks.add(len(self.attacks) - 1)
            self.message = f"attack {scenario.attack_type} on {scenario.zone_id} scheduled"
        self.stats[f"command_{kind}"] += 1
        return self.message

    def step(self) -> bool:
        """One reading interval if running. Returns whether a tick happened."""
        if self.actuator is not None:
            self.actuator.drain()
        if not self.running:
            return False
        for event in self.simulator.tick():
            self.publish(self.injector.apply(event))
        for label in self.injector.labels:              # count manipulated readings per attack
            if label.scenario_index is not None:
                self.attacks[label.scenario_index]["readings_manipulated"] += 1
                self._dirty_attacks.add(label.scenario_index)
        self.injector.labels.clear()                    # long-running: do not keep every label
        self.ticks += 1
        return True

    def status(self) -> dict[str, Any]:
        s = self.simulator.settings
        return {"building_id": self.building_id, "state": "running" if self.running else "paused",
                "sim_time": self.simulator.sim_time, "ticks": self.ticks, "interval_seconds": s.interval_seconds,
                "speed_factor": s.speed_factor, "seed": self.simulator.seed,
                "apply_commands": self.actuator is not None,
                "commands_applied": self.actuator.stats["executed"] if self.actuator else 0,
                "message": self.message}

    def flush_store(self, state: str | None = None) -> None:
        if self.store is None:
            return
        status = self.status()
        if state:
            status["state"] = state
        self.store.status(status)
        for index in sorted(self._dirty_attacks):
            self.store.attack(self.attacks[index])
        self._dirty_attacks.clear()


def main(argv: list[str] | None = None, stop_event: threading.Event | None = None) -> int:
    parser = argparse.ArgumentParser(prog=f"python -m workers.{SERVICE}",
                                     description="Controllable building simulation for the dashboard.")
    add_simulation_arguments(parser)
    parser.add_argument("--autostart", action="store_true", help="start running immediately")
    parser.add_argument("--no-apply-commands", action="store_true",
                        help="do not execute approved agent commands in the simulator")
    parser.add_argument("--no-db", action="store_true", help="do not write status to PostgreSQL")
    parser.add_argument("--broker-wait", type=float, default=60.0)
    args = parser.parse_args(argv)
    try:
        config = get_config()
        settings = build_settings(config.SIMULATOR, args)
    except (ConfigurationError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    logger = configure_logging(config.LOGGING, SERVICE)
    if stop_event is None:
        stop_event = threading.Event()
        install_shutdown_handlers(stop_event)
    try:
        wait_for_broker(config.KAFKA, timeout_s=args.broker_wait)
        ensure_topics(create_admin_client(config.KAFKA), config.KAFKA)
    except (KafkaUnavailableError, KafkaException) as exc:
        logger.error("Kafka not available", extra={"error": str(exc)})
        return 1

    database = store = None
    if not args.no_db:
        from database.db import Database, DatabaseUnavailableError

        database = Database(config.DATABASE, name=SERVICE)
        try:
            database.open()
            database.require_schema()
            store = PostgresStatusStore(database, logger)
        except (DatabaseUnavailableError, RuntimeError) as exc:
            logger.warning("Status will not be stored in PostgreSQL", extra={"error": str(exc)})
            database.close()
            database = None

    producer = JsonProducer(config.KAFKA, name=SERVICE)
    simulator = BuildingSimulator(settings)

    def publish(event: dict[str, Any]) -> None:
        producer.send(BUILDING_SENSOR_DATA, event, key=event["zone_id"], headers={"event_type": event["event_type"]})

    actuator = None
    consumers: list[tuple[JsonConsumer, threading.Thread]] = []
    if not args.no_apply_commands:
        from agent.executor import SimulatorActuator

        actuator = SimulatorActuator(simulator, producer, AGENT_ACTIONS, {"agent"})
        consumer = JsonConsumer(config.KAFKA, f"{SERVICE}_actuator", [OPTIMIZATION_COMMANDS],
                                auto_offset_reset="latest")
        consumers.append((consumer, threading.Thread(target=consumer.run, args=(actuator.handle,),
                                                     kwargs={"stop_event": stop_event}, daemon=True)))
    manager = SimulationManager(simulator, publish, store=store, actuator=actuator, running=args.autostart,
                                logger=logger)
    commands: queue.Queue = queue.Queue()

    def on_control(message: ConsumedMessage) -> None:
        try:
            commands.put(validate_control_message(message.value))
        except ValidationError as exc:
            logger.warning("Invalid control message ignored", extra={"errors": exc.errors})

    control = JsonConsumer(config.KAFKA, SERVICE, [SIMULATION_CONTROL], auto_offset_reset="latest")
    consumers.append((control, threading.Thread(target=control.run, args=(on_control,),
                                                kwargs={"stop_event": stop_event}, daemon=True)))
    for _, thread in consumers:
        thread.start()
    logger.info("Simulation manager started", extra={"building_id": manager.building_id,
                                                      "running": manager.running, "apply_commands": bool(actuator)})
    last_status = 0.0
    try:
        while not stop_event.is_set() and (args.ticks is None or manager.ticks < args.ticks):
            started = time.monotonic()
            changed = False
            while not commands.empty():
                result = manager.handle(commands.get_nowait())
                changed = True
                logger.info("Control command", extra={"result": result})
            ticked = manager.step()
            if changed or time.monotonic() - last_status > STATUS_EVERY_S:
                manager.flush_store()
                last_status = time.monotonic()
            producer.poll(0)
            wait = settings.interval_seconds if ticked and not args.fast else 0.0 if ticked else 0.2
            stop_event.wait(max(0.0, wait - (time.monotonic() - started)))
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        stop_event.set()
        for consumer, thread in consumers:
            thread.join(timeout=10)
            consumer.close()
        manager.flush_store(state="stopped")
        producer.close(timeout_s=30)
        if database is not None:
            database.close()
        logger.info("Simulation manager stopped", extra={"ticks": manager.ticks, **manager.stats})
    return 0


if __name__ == "__main__":
    sys.exit(main())
