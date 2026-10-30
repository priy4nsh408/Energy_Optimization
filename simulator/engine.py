"""Simulation engine: time, speed control and event output.

The engine is independent of Kafka. It hands every event (a JSON-serialisable
dict) to a *sink* callable; the streaming module will provide a Kafka sink,
while :func:`json_lines_sink` writes JSON Lines to a file or stdout.

Timing:
    * One *tick* happens every ``interval_seconds`` of wall-clock time (1-5 s).
    * Each tick advances simulated time by ``interval_seconds * speed_factor``,
      integrated in physics sub-steps of at most ``physics_step_seconds``.
    * Every tick emits one reading per zone.
"""

from __future__ import annotations

import json
import logging
import math
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TextIO

from config import SimulatorSettings
from simulator.building_model import Building, ZoneSpec, default_zones
from simulator.occupancy_simulator import OccupancySimulator
from simulator.outdoor_model import OutdoorModel
from simulator.sensor_simulator import SensorModel

logger = logging.getLogger(__name__)

EVENT_TYPE = "sensor_reading"
SCHEMA_VERSION = 1

EventSink = Callable[[dict[str, Any]], None]


class BuildingSimulator:
    """Runs the building model and emits timestamped sensor events."""

    def __init__(self, settings: SimulatorSettings, zones: list[ZoneSpec] | None = None) -> None:
        self.settings = settings
        seed = settings.random_seed if settings.random_seed is not None else random.SystemRandom().randrange(2**32)
        self.seed = seed
        # Independent random streams so that, e.g., sensor noise does not
        # change the physics when the number of sensors changes.
        self._physics_rng = random.Random(f"{seed}:physics")
        outdoor = OutdoorModel(
            latitude_deg=settings.latitude_deg,
            annual_mean_c=settings.outdoor_annual_mean_c,
            seasonal_amplitude_c=settings.outdoor_seasonal_amplitude_c,
            diurnal_amplitude_c=settings.outdoor_diurnal_amplitude_c,
            rng=random.Random(f"{seed}:outdoor"),
        )
        occupancy = OccupancySimulator(seed, random.Random(f"{seed}:occupancy"))
        self.building = Building(zones or default_zones(), occupancy, outdoor, self._physics_rng)
        self.sensors = SensorModel(random.Random(f"{seed}:sensors"))

        start = settings.start_time or datetime.now(settings.tzinfo).replace(microsecond=0)
        # Event ids depend on the start time as well as the seed: runs that start at different simulated
        # times (e.g. the bootstrap history and a live run) must not produce the same ids, or the
        # database's duplicate check would silently drop the later run's readings.
        self._event_rng = random.Random(f"{seed}:{start.isoformat()}:event-ids")
        # Run one warm-up physics step ending at the start time, so outdoor
        # conditions and occupancy exist before the first reading.
        warmup_s = min(settings.physics_step_seconds, settings.simulated_seconds_per_tick)
        self.sim_time: datetime = start.astimezone(settings.tzinfo) - timedelta(seconds=warmup_s)
        self._sequence = {zone_id: 0 for zone_id in self.building.zones}
        self.ticks = 0
        self.sink_failures = 0
        self._advance(warmup_s)
        for zone in self.building.zones.values():
            zone.state.energy_kwh = 0.0
        logger.info("Simulator initialised", extra={
            "building_id": settings.building_id, "zones": list(self.building.zones),
            "seed": seed, "start_time": self.sim_time.isoformat(),
            "interval_seconds": settings.interval_seconds, "speed_factor": settings.speed_factor,
        })

    # -- simulation ---------------------------------------------------------------------
    def _advance(self, seconds: float) -> None:
        steps = max(1, math.ceil(seconds / self.settings.physics_step_seconds))
        dt = seconds / steps
        for _ in range(steps):
            self.building.step(self.sim_time, dt)
            self.sim_time += timedelta(seconds=dt)

    def tick(self) -> list[dict[str, Any]]:
        """Advance one reading interval and return one event per zone."""
        interval = self.settings.simulated_seconds_per_tick
        self._advance(interval)
        self.ticks += 1
        emitted_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        outdoor_readings = self.sensors.read_outdoor(self.building.outdoor)
        events = []
        for zone_id, zone in self.building.zones.items():
            self._sequence[zone_id] += 1
            event = {
                "event_id": str(uuid.UUID(int=self._event_rng.getrandbits(128), version=4)),
                "event_type": EVENT_TYPE,
                "schema_version": SCHEMA_VERSION,
                "source": "simulator",
                "building_id": self.settings.building_id,
                "zone_id": zone_id,
                "zone_name": zone.spec.name,
                "zone_type": zone.spec.zone_type,
                "sequence": self._sequence[zone_id],
                "timestamp": self.sim_time.isoformat(timespec="seconds"),
                "emitted_at": emitted_at,
                "interval_seconds": round(interval, 3),
                **self.sensors.read_zone(zone),
                **outdoor_readings,
                "energy_kwh": round(zone.state.energy_kwh, 5),
            }
            zone.state.energy_kwh = 0.0
            events.append(event)
        return events

    def apply_command(self, zone_id: str, **command: Any) -> None:
        """Forward a control command to the building (see ``Building.apply_command``)."""
        self.building.apply_command(zone_id, **command)
        logger.info("Control command applied", extra={"zone_id": zone_id, "command": command})

    # -- run loop -----------------------------------------------------------------------
    def _deliver(self, events: list[dict[str, Any]], sink: EventSink) -> None:
        for event in events:
            try:
                sink(event)
            except Exception:  # noqa: BLE001 - a failing sink must not stop the simulation
                self.sink_failures += 1
                logger.exception("Event sink failed", extra={
                    "zone_id": event["zone_id"], "sequence": event["sequence"]})

    def run(self, sink: EventSink, *, max_ticks: int | None = None, realtime: bool = True,
            stop_event: threading.Event | None = None) -> int:
        """Run until ``max_ticks`` is reached or ``stop_event`` is set.

        Args:
            sink: called once per event.
            max_ticks: stop after this many ticks (``None`` = run forever).
            realtime: pace ticks at ``interval_seconds`` of wall-clock time;
                ``False`` runs as fast as possible (e.g. to generate datasets).
            stop_event: set it from another thread to stop cleanly.

        Returns:
            Number of ticks executed.
        """
        stop_event = stop_event or threading.Event()
        interval = self.settings.interval_seconds
        start_ticks = self.ticks
        next_deadline = time.monotonic()
        while not stop_event.is_set():
            if max_ticks is not None and self.ticks - start_ticks >= max_ticks:
                break
            self._deliver(self.tick(), sink)
            if realtime:
                next_deadline += interval
                delay = next_deadline - time.monotonic()
                if delay > 0:
                    stop_event.wait(delay)
                else:
                    if -delay > interval:
                        logger.warning("Simulator falling behind real time",
                                       extra={"lag_seconds": round(-delay, 3)})
                    next_deadline = time.monotonic()
        executed = self.ticks - start_ticks
        logger.info("Simulator stopped", extra={
            "ticks": executed, "sim_time": self.sim_time.isoformat(),
            "sink_failures": self.sink_failures})
        return executed


def json_lines_sink(stream: TextIO) -> EventSink:
    """Sink that writes each event as one JSON line and flushes immediately."""

    def write(event: dict[str, Any]) -> None:
        stream.write(json.dumps(event, ensure_ascii=True) + "\n")
        stream.flush()

    return write
