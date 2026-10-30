"""Create the configured weather and carbon providers."""

from __future__ import annotations

import logging

from config import BaseConfig
from data_sources.carbon import (
    ELECTRICITY_MAPS_MIN_INTERVAL_S,
    UK_CARBON_MIN_INTERVAL_S,
    CarbonProvider,
    ElectricityMapsProvider,
    MockCarbonProvider,
    UkCarbonIntensityProvider,
)
from data_sources.http_client import CachedHttpClient
from data_sources.weather import (
    OPEN_METEO_MIN_INTERVAL_S,
    MockWeatherProvider,
    OpenMeteoProvider,
    WeatherProvider,
)

logger = logging.getLogger(__name__)


def _client(config: BaseConfig, name: str, min_interval_s: float) -> CachedHttpClient:
    settings = config.EXTERNAL_DATA
    return CachedHttpClient(name, cache_dir=settings.cache_dir, timeout_s=settings.http_timeout_s,
                            max_retries=settings.http_max_retries, min_interval_s=min_interval_s)


def create_weather_provider(config: BaseConfig) -> WeatherProvider:
    settings = config.EXTERNAL_DATA
    if settings.weather_provider == "mock":
        logger.warning("MOCK MODE: weather data is synthetic (source='mock'), not real observations")
        return MockWeatherProvider(settings, config.SIMULATOR)
    return OpenMeteoProvider(settings, _client(config, "open-meteo", OPEN_METEO_MIN_INTERVAL_S))


def create_carbon_provider(config: BaseConfig) -> CarbonProvider:
    """Raises ConfigurationError if the provider needs a missing API key."""
    settings = config.EXTERNAL_DATA
    if settings.carbon_provider == "mock":
        logger.warning("MOCK MODE: carbon intensity is synthetic (source='mock'), not real grid data")
        return MockCarbonProvider(forecast_hours=settings.weather_forecast_hours,
                                  longitude=settings.longitude)
    if settings.carbon_provider == "uk-carbon-intensity":
        return UkCarbonIntensityProvider(
            _client(config, "uk-carbon-intensity", UK_CARBON_MIN_INTERVAL_S),
            forecast_hours=settings.weather_forecast_hours)
    return ElectricityMapsProvider(
        settings, _client(config, "electricitymaps", ELECTRICITY_MAPS_MIN_INTERVAL_S))
