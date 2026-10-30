"""Tests for the physical building model, occupancy and outdoor models."""

from __future__ import annotations

import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from simulator.building_model import (
    CO2_PER_PERSON_M3_S,
    OCCUPIED_DEADBAND_C,
    Building,
    Zone,
    default_zones,
)
from simulator.occupancy_simulator import OccupancySimulator
from simulator.outdoor_model import OutdoorConditions, OutdoorModel
from simulator.psychrometrics import humidity_ratio, relative_humidity_pct

UTC = timezone.utc
WEEKDAY_10AM = datetime(2026, 1, 14, 10, 0, tzinfo=UTC)   # Wednesday
WEEKDAY_3AM = datetime(2026, 1, 14, 3, 0, tzinfo=UTC)
SATURDAY_NOON = datetime(2026, 1, 17, 12, 0, tzinfo=UTC)


def make_outdoor(temperature_c: float, rh: float = 50.0, irradiance: float = 0.0) -> OutdoorConditions:
    return OutdoorConditions(
        temperature_c=temperature_c, relative_humidity_pct=rh,
        humidity_ratio=humidity_ratio(temperature_c, rh),
        solar_irradiance_w_m2=irradiance, illuminance_lux=irradiance * 110, cloud_cover=0.0,
    )


class ConstantOutdoor:
    """Outdoor source with fixed conditions (implements the OutdoorSource protocol)."""

    def __init__(self, conditions: OutdoorConditions) -> None:
        self.conditions = conditions

    def advance(self, when: datetime, dt_s: float) -> OutdoorConditions:
        return self.conditions


def zone_spec(zone_id: str = "Z1"):
    return next(z for z in default_zones() if z.zone_id == zone_id)


def run_zone(zone: Zone, outdoor: OutdoorConditions, *, start: datetime, hours: float,
             occupants: int = 0, dt_s: float = 60.0) -> list[float]:
    rng = random.Random(0)
    temps, when = [], start
    for _ in range(int(hours * 3600 / dt_s)):
        zone.step(when, dt_s, outdoor, occupants, rng)
        temps.append(zone.state.temperature_c)
        when += timedelta(seconds=dt_s)
    return temps


# --------------------------------------------------------------------------- zones
def test_default_building_has_five_unique_zones():
    zones = default_zones()
    assert len(zones) == 5
    assert len({z.zone_id for z in zones}) == 5
    assert {z.zone_type for z in zones} == {"office", "meeting", "cafeteria", "server"}
    for z in zones:
        assert z.floor_area_m2 > 0 and z.thermal_capacitance_j_per_k > 0 and z.max_occupancy > 0


def test_operating_schedule():
    office, server = zone_spec("Z1"), zone_spec("Z5")
    assert office.is_operating(WEEKDAY_10AM)
    assert not office.is_operating(WEEKDAY_3AM)
    assert not office.is_operating(SATURDAY_NOON)
    assert server.is_operating(WEEKDAY_3AM) and server.is_operating(SATURDAY_NOON)


# --------------------------------------------------------------------------- physics
def test_free_running_zone_relaxes_towards_outdoor_temperature():
    zone = Zone(zone_spec("Z2"), initial_temperature_c=21.0)
    zone.overrides.hvac_mode = "off"
    temps = run_zone(zone, make_outdoor(0.0), start=WEEKDAY_3AM, hours=12)
    assert all(b <= a + 1e-9 for a, b in zip(temps, temps[1:])), "should cool monotonically"
    assert 0.0 < temps[-1] < 15.0
    assert zone.state.hvac_status == "off" and zone.state.hvac_power_kw == 0.0


def test_heating_holds_comfort_band_in_cold_weather():
    zone = Zone(zone_spec("Z1"), initial_temperature_c=16.0)
    temps = run_zone(zone, make_outdoor(-5.0), start=WEEKDAY_10AM.replace(hour=7), hours=8)
    setpoint = zone.spec.occupied_setpoint_c
    later = temps[len(temps) // 2:]
    assert min(later) >= setpoint - OCCUPIED_DEADBAND_C - 0.5
    assert max(later) <= setpoint + OCCUPIED_DEADBAND_C + 0.5
    assert zone.state.hvac_power_kw > 0


def test_cooling_holds_comfort_band_in_hot_weather():
    zone = Zone(zone_spec("Z1"), initial_temperature_c=27.0)
    temps = run_zone(zone, make_outdoor(35.0, irradiance=800), start=WEEKDAY_10AM.replace(hour=7),
                     hours=8, occupants=25)
    later = temps[len(temps) // 2:]
    # Near design load a proportional thermostat settles slightly above its
    # band (proportional offset), but it must stay well below the start value.
    assert max(later) <= zone.spec.occupied_setpoint_c + OCCUPIED_DEADBAND_C + 1.0
    assert zone.state.hvac_status == "cooling"
    assert zone.state.hvac_output_fraction > 0.7


def test_night_setback_lets_temperature_drift():
    zone = Zone(zone_spec("Z2"), initial_temperature_c=22.0)
    temps = run_zone(zone, make_outdoor(0.0), start=WEEKDAY_3AM.replace(hour=0), hours=6)
    # Setback band is 22 ± 4 °C, so the zone may drop to ~18 °C but no lower.
    assert 17.5 <= min(temps) < 21.0


def test_solar_and_internal_gains_warm_the_zone():
    cold = Zone(zone_spec("Z1"), initial_temperature_c=20.0)
    sunny = Zone(zone_spec("Z1"), initial_temperature_c=20.0)
    for z in (cold, sunny):
        z.overrides.hvac_mode = "off"
    run_zone(cold, make_outdoor(20.0), start=WEEKDAY_10AM, hours=2)
    run_zone(sunny, make_outdoor(20.0, irradiance=800), start=WEEKDAY_10AM, hours=2, occupants=30)
    assert sunny.state.temperature_c > cold.state.temperature_c + 1.0


def test_co2_reaches_mass_balance_steady_state():
    spec = zone_spec("Z1")
    zone = Zone(spec)
    occupants = 20
    run_zone(zone, make_outdoor(22.0), start=WEEKDAY_10AM.replace(hour=8), hours=6, occupants=occupants)
    airflow_m3_s = spec.ventilation_ach * spec.volume_m3 / 3600
    expected = 420 + occupants * CO2_PER_PERSON_M3_S * 1e6 / airflow_m3_s
    assert zone.state.co2_ppm == pytest.approx(expected, rel=0.02)


def test_occupants_raise_humidity():
    empty, busy = Zone(zone_spec("Z3")), Zone(zone_spec("Z3"))
    run_zone(empty, make_outdoor(20.0, rh=40), start=WEEKDAY_10AM, hours=1)
    run_zone(busy, make_outdoor(20.0, rh=40), start=WEEKDAY_10AM, hours=1, occupants=12)
    assert busy.state.relative_humidity_pct > empty.state.relative_humidity_pct + 2


def test_air_handler_dehumidifies_humid_ventilation_air():
    zone = Zone(zone_spec("Z1"), initial_temperature_c=23.0)
    run_zone(zone, make_outdoor(30.0, rh=80), start=WEEKDAY_10AM, hours=4, occupants=20)
    assert zone.state.relative_humidity_pct < 70


def test_energy_is_integral_of_power():
    zone = Zone(zone_spec("Z4"))
    rng, when, expected = random.Random(1), WEEKDAY_10AM, 0.0
    for _ in range(30):
        zone.step(when, 60, make_outdoor(5.0), 10, rng)
        expected += zone.state.total_power_kw * 60 / 3600
    assert zone.state.energy_kwh == pytest.approx(expected)


def test_lighting_control_and_daylight():
    office, private = Zone(zone_spec("Z1")), Zone(zone_spec("Z2"))
    rng, dark, bright = random.Random(0), make_outdoor(10.0), make_outdoor(10.0, irradiance=500)
    # Open-plan lights follow the schedule even when empty; private offices follow occupancy.
    office.step(WEEKDAY_10AM, 60, dark, 0, rng)
    private.step(WEEKDAY_10AM, 60, dark, 0, rng)
    assert office.state.lighting_level == 1.0 and private.state.lighting_level == 0.0
    assert private.state.lighting_power_kw == 0.0
    private.step(WEEKDAY_10AM, 60, bright, 0, rng)
    assert private.state.lighting_lux == pytest.approx(500 * 110 * private.spec.daylight_factor)


def test_appliance_load_tracks_occupancy():
    zone, rng = Zone(zone_spec("Z1")), random.Random(3)
    empty = [zone.step(WEEKDAY_10AM, 60, make_outdoor(20), 0, rng) or zone.state.appliance_power_kw
             for _ in range(50)]
    full = [zone.step(WEEKDAY_10AM, 60, make_outdoor(20), 30, rng) or zone.state.appliance_power_kw
            for _ in range(50)]
    assert sum(full) / 50 > sum(empty) / 50 + 2.5


# --------------------------------------------------------------------------- commands
def make_building() -> Building:
    return Building(default_zones(), OccupancySimulator(1, random.Random(1)),
                    ConstantOutdoor(make_outdoor(5.0)), random.Random(1))


def test_apply_command_overrides_and_clears():
    building = make_building()
    building.apply_command("Z1", setpoint_c=24, hvac_mode="heat", lighting_level=0.5)
    building.step(WEEKDAY_10AM, 60)
    state = building.zones["Z1"].state
    assert state.setpoint_c == 24 and state.hvac_mode == "heat" and state.lighting_level == 0.5
    building.clear_overrides("Z1")
    building.step(WEEKDAY_10AM, 60)
    assert state.setpoint_c == 22 and state.hvac_mode == "auto"


@pytest.mark.parametrize("kwargs", [
    {"setpoint_c": 50}, {"setpoint_c": 5}, {"hvac_mode": "turbo"},
    {"lighting_level": 1.5}, {"lighting_level": -0.1},
])
def test_apply_command_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        make_building().apply_command("Z1", **kwargs)


def test_apply_command_unknown_zone():
    with pytest.raises(KeyError):
        make_building().apply_command("Z9", setpoint_c=22)


def test_building_rejects_duplicate_zone_ids():
    zones = default_zones()
    with pytest.raises(ValueError, match="Duplicate"):
        Building(zones + [replace(zones[0])], OccupancySimulator(1, random.Random(1)),
                 ConstantOutdoor(make_outdoor(5.0)), random.Random(1))


# --------------------------------------------------------------------------- occupancy
def test_expected_occupancy_follows_time_of_day():
    occ = OccupancySimulator(seed=5, rng=random.Random(5))
    office = zone_spec("Z1")
    assert occ.expected_occupancy(office, WEEKDAY_3AM) == 0
    assert occ.expected_occupancy(office, WEEKDAY_10AM) > 0.5 * office.max_occupancy
    assert occ.expected_occupancy(office, SATURDAY_NOON) < 0.1 * office.max_occupancy
    assert occ.expected_occupancy(zone_spec("Z5"), WEEKDAY_10AM) == 0
    cafeteria = zone_spec("Z4")
    assert (occ.expected_occupancy(cafeteria, WEEKDAY_10AM.replace(hour=12, minute=30))
            > occ.expected_occupancy(cafeteria, WEEKDAY_10AM))


def test_occupancy_changes_gradually_and_stays_in_bounds():
    occ = OccupancySimulator(seed=5, rng=random.Random(5))
    office = zone_spec("Z1")
    count = occ.step(office, WEEKDAY_10AM, 60, 0)
    assert count < occ.expected_occupancy(office, WEEKDAY_10AM)  # people don't all arrive at once
    for minute in range(60):
        count = occ.step(office, WEEKDAY_10AM + timedelta(minutes=minute), 60, count)
        assert 0 <= count <= office.max_occupancy
    assert count == pytest.approx(occ.expected_occupancy(office, WEEKDAY_10AM), abs=5)


def test_meeting_room_bookings_are_deterministic_blocks():
    meeting = zone_spec("Z3")
    a = OccupancySimulator(seed=9, rng=random.Random(9))
    b = OccupancySimulator(seed=9, rng=random.Random(9))
    day = WEEKDAY_10AM.replace(hour=0)
    schedule_a = [a.expected_occupancy(meeting, day + timedelta(hours=h)) for h in range(24)]
    schedule_b = [b.expected_occupancy(meeting, day + timedelta(hours=h)) for h in range(24)]
    assert schedule_a == schedule_b
    assert all(v == 0 for h, v in enumerate(schedule_a) if h < 9 or h >= 18)
    assert all(v == 0 or 2 <= v <= meeting.max_occupancy for v in schedule_a)
    # Within a booked hour attendance is constant until people leave near the end.
    booked = next(h for h in range(9, 18) if schedule_a[h] > 0)
    start = day + timedelta(hours=booked)
    assert a.expected_occupancy(meeting, start + timedelta(minutes=30)) == schedule_a[booked]
    assert a.expected_occupancy(meeting, start + timedelta(minutes=55)) == 0


# --------------------------------------------------------------------------- outdoor
def make_outdoor_model(latitude: float = 40.0) -> OutdoorModel:
    return OutdoorModel(latitude_deg=latitude, annual_mean_c=15, seasonal_amplitude_c=10,
                        diurnal_amplitude_c=5, rng=random.Random(0))


def test_outdoor_daily_and_seasonal_cycles():
    model = make_outdoor_model()
    winter_dawn = model.base_temperature_c(datetime(2026, 1, 15, 4, tzinfo=UTC))
    winter_afternoon = model.base_temperature_c(datetime(2026, 1, 15, 15, tzinfo=UTC))
    summer_afternoon = model.base_temperature_c(datetime(2026, 7, 19, 15, tzinfo=UTC))
    assert winter_afternoon > winter_dawn + 8
    assert summer_afternoon > winter_afternoon + 15


def test_solar_irradiance_depends_on_sun_position():
    north, south = make_outdoor_model(40), make_outdoor_model(-35)
    assert north.clear_sky_irradiance_w_m2(datetime(2026, 6, 21, 0, tzinfo=UTC)) == 0
    june_noon = datetime(2026, 6, 21, 12, tzinfo=UTC)
    december_noon = datetime(2026, 12, 21, 12, tzinfo=UTC)
    assert north.clear_sky_irradiance_w_m2(june_noon) > 800
    assert north.clear_sky_irradiance_w_m2(june_noon) > north.clear_sky_irradiance_w_m2(december_noon)
    assert south.clear_sky_irradiance_w_m2(december_noon) > south.clear_sky_irradiance_w_m2(june_noon)


def test_outdoor_conditions_are_physically_consistent():
    model, when = make_outdoor_model(), datetime(2026, 3, 1, tzinfo=UTC)
    previous = None
    for _ in range(24 * 12):
        cond = model.advance(when, 300)
        assert 5 <= cond.relative_humidity_pct <= 100
        assert 0 <= cond.cloud_cover <= 1 and cond.solar_irradiance_w_m2 >= 0
        assert cond.humidity_ratio == pytest.approx(
            humidity_ratio(cond.temperature_c, cond.relative_humidity_pct))
        if previous is not None:  # weather evolves smoothly, not as white noise
            assert abs(cond.temperature_c - previous.temperature_c) < 1.5
        previous, when = cond, when + timedelta(minutes=5)


def test_psychrometric_round_trip():
    for temperature, rh in [(0, 80), (22, 45), (35, 30)]:
        assert relative_humidity_pct(temperature, humidity_ratio(temperature, rh)) == pytest.approx(rh)
