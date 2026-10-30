"""Builds processed energy records from validated sensor readings.

Adds the energy split by end use (HVAC, lighting, appliances), engineered
features and carbon emissions. Carbon emissions use the most recent observed
grid carbon intensity received on the ``carbon_intensity`` topic; until such
data exists the carbon columns stay NULL (no assumed default value).
"""

from __future__ import annotations

import logging
from datetime import datetime

from database.models import CarbonIntensityRecord, EnergyRecord, SensorReadingRecord
from processors.feature_engineering import FeatureEngineer

logger = logging.getLogger(__name__)


class EnergyProcessor:
    """Stateless energy calculations plus a stateful feature engineer."""

    def __init__(self, features: FeatureEngineer,
                 carbon_intensity_g_per_kwh: float | None = None,
                 carbon_observed_at: datetime | None = None) -> None:
        self.features = features
        self.carbon_intensity_g_per_kwh = carbon_intensity_g_per_kwh
        self._carbon_observed_at = carbon_observed_at

    def update_carbon_intensity(self, record: CarbonIntensityRecord) -> bool:
        """Use the newest observed (non-forecast) intensity. Returns True if it changed."""
        if record.is_forecast:
            return False
        if self._carbon_observed_at and record.observed_at <= self._carbon_observed_at:
            return False
        self.carbon_intensity_g_per_kwh = record.intensity_g_per_kwh
        self._carbon_observed_at = record.observed_at
        logger.info("Carbon intensity updated", extra={
            "intensity_g_per_kwh": record.intensity_g_per_kwh, "region": record.region,
            "observed_at": record.observed_at.isoformat()})
        return True

    def process(self, reading: SensorReadingRecord) -> EnergyRecord:
        total = reading.total_power_kw
        energy = reading.energy_kwh
        if total > 0:
            hvac_e = energy * reading.hvac_power_kw / total
            lighting_e = energy * reading.lighting_power_kw / total
        else:
            hvac_e = lighting_e = 0.0
        appliance_e = max(0.0, energy - hvac_e - lighting_e)

        intensity = self.carbon_intensity_g_per_kwh
        return EnergyRecord(
            event_id=reading.event_id,
            building_id=reading.building_id,
            zone_id=reading.zone_id,
            event_time=reading.event_time,
            interval_seconds=reading.interval_seconds,
            energy_kwh=energy,
            hvac_energy_kwh=hvac_e,
            lighting_energy_kwh=lighting_e,
            appliance_energy_kwh=appliance_e,
            total_power_kw=total,
            carbon_intensity_g_per_kwh=intensity,
            carbon_kg_co2e=energy * intensity / 1000 if intensity is not None else None,
            temperature_c=reading.temperature_c,
            outdoor_temperature_c=reading.outdoor_temperature_c,
            occupancy_count=reading.occupancy_count,
            hvac_status=reading.hvac_status,
            **self.features.compute(reading),
        )
