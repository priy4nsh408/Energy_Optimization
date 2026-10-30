"""Occupancy simulation driven by time-of-day profiles.

Each zone type has an hourly profile of expected occupancy (as a fraction of
capacity) for weekdays and weekends. Actual head counts move towards the
expected value gradually (people arrive and leave over minutes, they do not
teleport), with random arrivals/departures and day-to-day variation.
Meeting rooms use hourly booking blocks instead of a smooth profile.

All randomness comes from seeded generators, so a run with a fixed seed is
fully reproducible.
"""

from __future__ import annotations

import math
import random
from datetime import datetime

from simulator.building_model import ZoneSpec

# Expected occupancy fraction of capacity at each hour 00..23.
HOURLY_PROFILES: dict[str, dict[str, tuple[float, ...]]] = {
    "office": {
        "weekday": (0, 0, 0, 0, 0, 0, 0, .10, .50, .85, .90, .90,
                    .60, .80, .90, .85, .75, .40, .15, .05, 0, 0, 0, 0),
        "weekend": (0, 0, 0, 0, 0, 0, 0, 0, 0, .03, .05, .05,
                    .05, .05, .05, .03, 0, 0, 0, 0, 0, 0, 0, 0),
    },
    "cafeteria": {
        "weekday": (0, 0, 0, 0, 0, 0, 0, .05, .25, .15, .10, .30,
                    .90, .70, .20, .25, .10, .10, .05, .02, 0, 0, 0, 0),
        "weekend": (0, 0, 0, 0, 0, 0, 0, 0, 0, .02, .03, .05,
                    .08, .05, .03, .02, 0, 0, 0, 0, 0, 0, 0, 0),
    },
    "server": {
        "weekday": (0,) * 24,
        "weekend": (0,) * 24,
    },
}

# Meeting rooms: probability that each working hour is booked.
MEETING_HOURS = range(9, 18)
MEETING_BOOKING_PROBABILITY = 0.6
MEETING_BOOKING_PROBABILITY_WEEKEND = 0.05
# Fraction of an hour actually used by a meeting (people leave early).
MEETING_DURATION_FRACTION = 0.85

ARRIVAL_TIME_CONSTANT_S = 300.0   # how quickly head counts track the profile
DAILY_VARIATION_STD = 0.10        # day-to-day variation of attendance


def _poisson(lam: float, rng: random.Random) -> int:
    """Poisson sample (Knuth's method; ``lam`` is small here)."""
    if lam <= 0:
        return 0
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


class OccupancySimulator:
    """Generates realistic head counts per zone."""

    def __init__(self, seed: int, rng: random.Random) -> None:
        self.seed = seed
        self.rng = rng

    def _seeded(self, *parts: object) -> random.Random:
        # String seeds are hashed deterministically (unlike hash() on str).
        return random.Random(":".join(str(p) for p in (self.seed, *parts)))

    def _daily_factor(self, zone: ZoneSpec, when: datetime) -> float:
        rng = self._seeded("daily", zone.zone_id, when.date().isoformat())
        return max(0.6, min(1.3, rng.gauss(1.0, DAILY_VARIATION_STD)))

    def expected_occupancy(self, zone: ZoneSpec, when: datetime) -> float:
        """Expected number of people in ``zone`` at ``when``."""
        weekend = when.weekday() >= 5
        if zone.zone_type == "meeting":
            return self._meeting_attendance(zone, when, weekend)

        profile = HOURLY_PROFILES.get(zone.zone_type, HOURLY_PROFILES["office"])
        hourly = profile["weekend" if weekend else "weekday"]
        hour = when.hour + when.minute / 60 + when.second / 3600
        # Linear interpolation between hour values (profile value = start of hour).
        low = int(hour) % 24
        frac = hour - int(hour)
        fraction = hourly[low] * (1 - frac) + hourly[(low + 1) % 24] * frac
        return fraction * zone.max_occupancy * self._daily_factor(zone, when)

    def _meeting_attendance(self, zone: ZoneSpec, when: datetime, weekend: bool) -> float:
        if when.hour not in MEETING_HOURS:
            return 0.0
        rng = self._seeded("meeting", zone.zone_id, when.date().isoformat(), when.hour)
        probability = MEETING_BOOKING_PROBABILITY_WEEKEND if weekend else MEETING_BOOKING_PROBABILITY
        if rng.random() >= probability:
            return 0.0
        if when.minute >= 60 * MEETING_DURATION_FRACTION:
            return 0.0
        return float(rng.randint(max(2, zone.max_occupancy // 4), zone.max_occupancy))

    def step(self, zone: ZoneSpec, when: datetime, dt_s: float, current: int) -> int:
        """Return the head count after ``dt_s`` seconds, moving towards the expected value."""
        target = self.expected_occupancy(zone, when)
        gap = target - current
        expected_moves = abs(gap) * (1 - math.exp(-dt_s / ARRIVAL_TIME_CONSTANT_S))
        moves = _poisson(expected_moves, self.rng)
        # Never overshoot the target by more than one person.
        moves = min(moves, math.ceil(abs(gap)))
        new_count = current + (moves if gap > 0 else -moves)
        return max(0, min(zone.max_occupancy, new_count))
