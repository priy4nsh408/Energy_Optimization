"""Tests for the external data layer (HTTP client, providers, publisher).

The provider fixtures below are hand-written in the *documented* response
format of each API (Open-Meteo, Electricity Maps v3, UK Carbon Intensity);
they are not recorded real data. No network access is needed.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import requests

from config import ConfigurationError, ExternalDataSettings, SimulatorSettings, get_config
from data_sources.carbon import (
    ElectricityMapsProvider,
    MockCarbonProvider,
    UkCarbonIntensityProvider,
)
from data_sources.http_client import (
    ApiAuthError,
    ApiRequestError,
    ApiUnavailableError,
    CachedHttpClient,
)
from data_sources.observations import CarbonObservation, WeatherObservation
from data_sources.weather import MockWeatherProvider, OpenMeteoProvider
from processors.data_validator import validate_carbon_message, validate_weather_message
from workers.external_data_publisher import (
    RETRY_AFTER_AUTH_FAILURE_S,
    ExternalDataPublisher,
    PollTask,
)

NOW = datetime(2026, 1, 14, 12, 7, tzinfo=timezone.utc)
LOGGER = logging.getLogger("tests.data_sources")


# --------------------------------------------------------------------------- fakes
class FakeResponse:
    def __init__(self, status=200, body=None, headers=None, text=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


class FakeSession:
    """Returns queued responses (or raises queued exceptions) and records calls."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class Clock:
    def __init__(self, start=1_000_000.0):
        self.now = start

    def __call__(self):
        return self.now


def make_client(session, tmp_path=None, **kwargs):
    clock = kwargs.pop("clock", Clock())
    sleeps = []
    client = CachedHttpClient("api", cache_dir=tmp_path, session=session, sleep=sleeps.append,
                              clock=clock, **kwargs)
    return client, clock, sleeps


# --------------------------------------------------------------------------- HTTP client
def test_fresh_cache_avoids_requests(tmp_path):
    session = FakeSession(FakeResponse(body={"v": 1}), FakeResponse(body={"v": 2}))
    client, clock, _ = make_client(session, tmp_path)
    assert client.get_json("https://x/a", ttl_s=60).data == {"v": 1}
    clock.now += 30
    second = client.get_json("https://x/a", ttl_s=60)
    assert second.from_cache and second.data == {"v": 1} and len(session.calls) == 1
    clock.now += 31
    assert client.get_json("https://x/a", ttl_s=60).data == {"v": 2}


def test_disk_cache_survives_restart_and_excludes_secrets(tmp_path):
    session = FakeSession(FakeResponse(body={"v": 1}))
    client, clock, _ = make_client(session, tmp_path)
    client.get_json("https://x/a", params={"lat": 1}, headers={"auth-token": "SECRET-KEY"},
                    secret_params={"apikey": "SECRET-PARAM"})
    assert session.calls[0]["params"] == {"lat": 1, "apikey": "SECRET-PARAM"}
    restarted, _, _ = make_client(FakeSession(), tmp_path, clock=clock)
    assert restarted.get_json("https://x/a", params={"lat": 1}).data == {"v": 1}
    stored = "".join(p.read_text() + p.name for p in tmp_path.iterdir())
    assert "SECRET" not in stored


def test_server_errors_are_retried_with_backoff():
    session = FakeSession(FakeResponse(503), requests.ConnectionError("reset"),
                          FakeResponse(body={"ok": True}))
    client, _, sleeps = make_client(session, max_retries=3, backoff_s=1.0)
    assert client.get_json("https://x").data == {"ok": True}
    assert sleeps == [1.0, 2.0] and client.requests_made == 3


def test_persistent_failure_opens_circuit_and_serves_stale(tmp_path):
    session = FakeSession(FakeResponse(body={"v": 1}), *[FakeResponse(500)] * 3)
    client, clock, _ = make_client(session, tmp_path, max_retries=2)
    client.get_json("https://x", ttl_s=60)
    clock.now += 120                                     # cache expired
    stale = client.get_json("https://x", ttl_s=60)
    assert stale.stale and stale.data == {"v": 1}
    calls = len(session.calls)
    clock.now += 10                                      # circuit open: no new request
    assert client.get_json("https://x", ttl_s=60).stale and len(session.calls) == calls


def test_failure_without_cache_raises():
    client, _, _ = make_client(FakeSession(requests.Timeout("slow")), max_retries=0)
    with pytest.raises(ApiUnavailableError):
        client.get_json("https://x")


def test_stale_data_has_a_maximum_age(tmp_path):
    session = FakeSession(FakeResponse(body={"v": 1}), FakeResponse(500))
    client, clock, _ = make_client(session, tmp_path, max_retries=0)
    client.get_json("https://x", ttl_s=60)
    clock.now += 7200
    with pytest.raises(ApiUnavailableError):
        client.get_json("https://x", ttl_s=60, max_stale_s=3600)


def test_rate_limit_honours_retry_after():
    session = FakeSession(FakeResponse(429, headers={"Retry-After": "120"}))
    client, clock, _ = make_client(session)
    with pytest.raises(ApiUnavailableError) as info:
        client.get_json("https://x")
    assert info.value.retry_after_s == 120 and len(session.calls) == 1   # 429 is not retried
    clock.now += 60
    with pytest.raises(ApiUnavailableError, match="paused"):
        client.get_json("https://x")
    assert len(session.calls) == 1


def test_retry_after_http_date():
    when = datetime.now(timezone.utc) + timedelta(seconds=300)
    session = FakeSession(FakeResponse(429, headers={
        "Retry-After": when.strftime("%a, %d %b %Y %H:%M:%S GMT")}))
    client, _, _ = make_client(session)
    with pytest.raises(ApiUnavailableError) as info:
        client.get_json("https://x")
    assert 250 < info.value.retry_after_s <= 300


def test_auth_failure_is_not_retried_and_pauses():
    session = FakeSession(FakeResponse(401, text="bad key"))
    client, _, _ = make_client(session, max_retries=3)
    with pytest.raises(ApiAuthError, match="API key"):
        client.get_json("https://x")
    with pytest.raises(ApiUnavailableError):
        client.get_json("https://x")
    assert len(session.calls) == 1


def test_client_errors_and_invalid_json():
    client, _, _ = make_client(FakeSession(FakeResponse(400, text="bad latitude")))
    with pytest.raises(ApiRequestError, match="400"):
        client.get_json("https://x")
    client, _, _ = make_client(FakeSession(FakeResponse(200, body=None, text="<html>")))
    with pytest.raises(ApiRequestError, match="JSON"):
        client.get_json("https://x")


def test_minimum_request_interval(tmp_path):
    session = FakeSession(FakeResponse(body={"v": 1}), FakeResponse(body={"v": 2}))
    client, clock, _ = make_client(session, tmp_path, min_interval_s=300)
    client.get_json("https://x", ttl_s=10)
    clock.now += 60                                      # ttl expired but interval not reached
    response = client.get_json("https://x", ttl_s=10)
    assert response.stale and len(session.calls) == 1
    clock.now += 300
    assert client.get_json("https://x", ttl_s=10).data == {"v": 2}


# --------------------------------------------------------------------------- Open-Meteo
OPEN_METEO_RESPONSE = {   # documented structure of /v1/forecast with timezone=GMT
    "latitude": 40.42, "longitude": -3.7, "utc_offset_seconds": 0, "timezone": "GMT",
    "current": {"time": "2026-01-14T12:00", "interval": 900, "temperature_2m": 9.8,
                "relative_humidity_2m": 61, "wind_speed_10m": 3.2, "cloud_cover": 40,
                "shortwave_radiation": 410.0},
    "hourly": {"time": ["2026-01-14T12:00", "2026-01-14T13:00", "2026-01-14T14:00"],
               "temperature_2m": [9.7, 10.5, None], "relative_humidity_2m": [62, 58, 55],
               "wind_speed_10m": [3.1, 3.6, 4.0], "cloud_cover": [45, 30, 20],
               "shortwave_radiation": [400.0, 380.5, 300.0]},
}


def external_settings(**overrides) -> ExternalDataSettings:
    return dataclasses.replace(ExternalDataSettings(), weather_forecast_hours=3, **overrides)


def test_open_meteo_parse():
    observations = OpenMeteoProvider.parse(OPEN_METEO_RESPONSE, now=NOW)
    assert len(observations) == 3                        # 14:00 has no temperature
    current, past_hour, forecast = observations
    assert current == WeatherObservation(datetime(2026, 1, 14, 12, tzinfo=timezone.utc), "open-meteo",
                                         False, 9.8, 61.0, 3.2, 410.0, 40.0)
    assert not past_hour.is_forecast and forecast.is_forecast
    assert forecast.observed_at.utcoffset() == timedelta(0)


def test_open_meteo_request_parameters():
    session = FakeSession(FakeResponse(body=OPEN_METEO_RESPONSE))
    client, _, _ = make_client(session)
    provider = OpenMeteoProvider(external_settings(open_meteo_api_key="K"), client)
    provider.fetch(past_hours=6, now=NOW)
    params = session.calls[0]["params"]
    assert params["latitude"] == 40.42 and params["timezone"] == "GMT"
    assert params["wind_speed_unit"] == "ms" and params["past_hours"] == 6
    assert params["forecast_hours"] == 3 and params["apikey"] == "K"
    assert "temperature_2m" in params["current"] and "shortwave_radiation" in params["hourly"]


@pytest.mark.parametrize("body", [{"current": {"time": "not a time", "temperature_2m": 1}},
                                  {"hourly": {"time": ["2026-01-14T12:00"], "temperature_2m": "x"}}])
def test_open_meteo_malformed_response(body):
    with pytest.raises(ApiRequestError):
        OpenMeteoProvider.parse(body, now=NOW)


# --------------------------------------------------------------------------- carbon providers
EM_LATEST = {"zone": "ES", "carbonIntensity": 142, "datetime": "2026-01-14T12:00:00.000Z",
             "updatedAt": "2026-01-14T12:05:11.000Z", "isEstimated": True,
             "emissionFactorType": "lifecycle"}
EM_HISTORY = {"zone": "ES", "history": [
    {**EM_LATEST, "carbonIntensity": 160, "datetime": "2026-01-14T09:00:00.000Z", "isEstimated": False},
    {**EM_LATEST, "carbonIntensity": 150, "datetime": "2026-01-14T10:00:00.000Z", "isEstimated": False},
    {**EM_LATEST, "carbonIntensity": None, "datetime": "2026-01-14T11:00:00.000Z"},
]}


def test_electricity_maps_requires_key():
    with pytest.raises(ConfigurationError, match="ELECTRICITY_MAPS_API_KEY"):
        ElectricityMapsProvider(external_settings(), make_client(FakeSession())[0])


def test_electricity_maps_latest_and_history():
    session = FakeSession(FakeResponse(body=EM_HISTORY), FakeResponse(body=EM_LATEST))
    client, _, _ = make_client(session)
    provider = ElectricityMapsProvider(external_settings(electricity_maps_api_key="TOKEN"), client)
    observations = provider.fetch(past_hours=2, now=NOW)
    assert [o.intensity_g_per_kwh for o in observations] == [150, 142]   # 09:00 too old, 11:00 null
    latest = observations[-1]
    assert latest.region == "ES" and latest.is_estimated and not latest.is_forecast
    assert session.calls[0]["url"].endswith("/carbon-intensity/history")
    assert session.calls[1]["headers"] == {"auth-token": "TOKEN"}
    assert session.calls[1]["params"] == {"lat": 40.42, "lon": -3.7}


def test_electricity_maps_zone_parameter():
    session = FakeSession(FakeResponse(body=EM_LATEST))
    provider = ElectricityMapsProvider(
        external_settings(electricity_maps_api_key="T", electricity_maps_zone="DE"),
        make_client(session)[0])
    provider.fetch(now=NOW)
    assert session.calls[0]["params"] == {"zone": "DE"}


def test_electricity_maps_bad_format():
    with pytest.raises(ApiRequestError):
        ElectricityMapsProvider.parse_entry({"carbonIntensity": 100, "datetime": "never"})


UK_PERIOD = {"from": "2026-01-14T12:00Z", "to": "2026-01-14T12:30Z",
             "intensity": {"forecast": 180, "actual": 175, "index": "moderate"}}


def test_uk_carbon_intensity_actual_and_forecast():
    current = {"data": [{**UK_PERIOD, "intensity": {"forecast": 180, "actual": None, "index": "moderate"}}]}
    forecast = {"data": [UK_PERIOD["from"] and
                         {"from": "2026-01-14T12:30Z", "to": "2026-01-14T13:00Z",
                          "intensity": {"forecast": 190, "actual": None, "index": "moderate"}}]}
    session = FakeSession(FakeResponse(body=current), FakeResponse(body=forecast))
    provider = UkCarbonIntensityProvider(make_client(session)[0], forecast_hours=24)
    observations = provider.fetch(now=NOW)
    assert [(o.intensity_g_per_kwh, o.is_forecast) for o in observations] == [(180, True), (190, True)]
    assert UkCarbonIntensityProvider.parse_period(UK_PERIOD).is_forecast is False
    assert session.calls[1]["url"].endswith("/intensity/2026-01-14T12:07Z/fw24h")


# --------------------------------------------------------------------------- mocks
def test_mock_weather_is_labelled_deterministic_and_plausible():
    settings = external_settings()
    provider = MockWeatherProvider(settings, SimulatorSettings())
    first, second = provider.fetch(past_hours=2, now=NOW), provider.fetch(past_hours=2, now=NOW)
    assert first == second and len(first) == 1 + 2 + 3
    assert all(o.source == "mock" for o in first)
    assert [o.is_forecast for o in first] == [False, False, False, True, True, True]
    assert all(-30 < o.temperature_c < 45 and 0 <= o.humidity_pct <= 100 for o in first)
    night = provider.fetch(now=NOW.replace(hour=1))[0]
    assert night.solar_irradiance_w_m2 == 0


def test_mock_carbon_is_labelled_and_varies_by_hour():
    provider = MockCarbonProvider(forecast_hours=24)
    observations = provider.fetch(now=NOW)
    assert all(o.source == "mock" and o.region == "mock" for o in observations)
    values = [o.intensity_g_per_kwh for o in observations]
    assert len(set(values)) > 10 and all(50 <= v <= 600 for v in values)


# --------------------------------------------------------------------------- messages
def test_messages_match_the_persistence_contracts():
    weather = OpenMeteoProvider.parse(OPEN_METEO_RESPONSE, now=NOW)
    for observation in weather:
        record = validate_weather_message(observation.to_message("building-01"))
        assert record.source == "open-meteo" and record.is_forecast == observation.is_forecast
    carbon = ElectricityMapsProvider.parse_entry(EM_LATEST)
    record = validate_carbon_message(carbon.to_message("building-01"))
    assert record.region == "ES" and record.intensity_g_per_kwh == 142


def test_event_ids_are_deterministic():
    observation = CarbonObservation(NOW, "mock", "mock", False, 300.0)
    assert observation.to_message("b")["event_id"] == observation.to_message("b")["event_id"]
    other = dataclasses.replace(observation, is_forecast=True)
    assert other.to_message("b")["event_id"] != observation.to_message("b")["event_id"]


# --------------------------------------------------------------------------- publisher
class RecordingProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value, key=None):
        self.sent.append((topic, key, value))

    def flush(self, timeout):
        return 0


def test_publisher_publishes_and_backfills_only_once():
    calls = []
    observations = MockCarbonProvider(forecast_hours=2).fetch(now=NOW)

    def fetch(hours):
        calls.append(hours)
        return observations

    task = PollTask("carbon:mock", "carbon_intensity", fetch, interval_s=900, backfill_hours=6)
    producer = RecordingProducer()
    publisher = ExternalDataPublisher(producer, "building-01", [task], LOGGER)
    assert publisher.run_task(task) == 900 and publisher.run_task(task) == 900
    assert calls == [6, 0] and task.stats["published"] == 2 * len(observations)
    topic, key, value = producer.sent[0]
    assert topic == "carbon_intensity" and key == "building-01" and value["source"] == "mock"


@pytest.mark.parametrize(("error", "expected_delay"), [
    (ApiAuthError("bad key"), RETRY_AFTER_AUTH_FAILURE_S),
    (ApiUnavailableError("down", retry_after_s=300), 300),
    (ApiUnavailableError("down"), 60),
    (ApiRequestError("weird"), 900),
])
def test_publisher_failure_scheduling(error, expected_delay):
    def fetch(hours):
        raise error

    task = PollTask("weather", "weather_data", fetch, interval_s=900)
    publisher = ExternalDataPublisher(RecordingProducer(), "b", [task], LOGGER)
    assert publisher.run_task(task) == expected_delay and task.stats["failures"] == 1


def test_publisher_run_once_and_stop():
    task = PollTask("w", "weather_data", lambda h: [], interval_s=900)
    publisher = ExternalDataPublisher(RecordingProducer(), "b", [task], LOGGER)
    publisher.run(threading.Event(), once=True)
    stop = threading.Event()
    stop.set()
    publisher.run(stop)
    assert task.stats["runs"] == 1


# --------------------------------------------------------------------------- configuration
def test_external_data_settings_from_env(clean_env, tmp_path):
    clean_env.setenv("WEATHER_PROVIDER", "MOCK")
    clean_env.setenv("CARBON_PROVIDER", "uk-carbon-intensity")
    clean_env.setenv("BUILDING_LATITUDE", "51.5")
    clean_env.setenv("ELECTRICITY_MAPS_API_KEY", "secret-key")
    settings = get_config("testing", load_dotenv_file=False).EXTERNAL_DATA
    assert settings.weather_provider == "mock" and settings.carbon_provider == "uk-carbon-intensity"
    assert settings.latitude == 51.5 and "secret-key" not in repr(settings)


@pytest.mark.parametrize(("name", "value"), [("WEATHER_PROVIDER", "darksky"),
                                             ("CARBON_PROVIDER", "watttime"),
                                             ("BUILDING_LONGITUDE", "200")])
def test_invalid_external_data_settings(clean_env, name, value):
    clean_env.setenv(name, value)
    with pytest.raises(ConfigurationError):
        get_config("testing", load_dotenv_file=False)


# --------------------------------------------------------------------------- end to end
@pytest.mark.kafka
@pytest.mark.postgres
def test_mock_data_flows_to_postgres(db, fresh_kafka, clean_env, tmp_path):
    clean_env.setenv("APP_ENV", "testing")
    clean_env.setenv("KAFKA_TOPIC_PREFIX", fresh_kafka.topic_prefix)
    clean_env.setenv("KAFKA_CONSUMER_GROUP_PREFIX", f"{fresh_kafka.consumer_group_prefix}_ext")
    clean_env.setenv("POSTGRES_PASSWORD", db.settings.password)
    clean_env.setenv("WEATHER_PROVIDER", "mock")
    clean_env.setenv("CARBON_PROVIDER", "mock")
    clean_env.setenv("WEATHER_FORECAST_HOURS", "6")
    clean_env.setenv("EXTERNAL_DATA_CACHE_DIR", str(tmp_path))
    from workers import external_data_publisher, record_persistence

    for _ in range(2):   # second run re-publishes the same observations
        assert external_data_publisher.main(["--once", "--backfill-hours", "3", "--broker-wait", "5"],
                                            stop_event=threading.Event()) == 0
    assert record_persistence.main(["--idle-timeout", "6", "--broker-wait", "5", "--db-wait", "5"],
                                   stop_event=threading.Event()) == 0
    with db.transaction() as conn:
        weather = conn.execute("SELECT count(*), count(*) FILTER (WHERE is_forecast), "
                               "min(source), max(source) FROM weather_records").fetchone()
        carbon = conn.execute("SELECT count(*), count(*) FILTER (WHERE is_forecast) "
                              "FROM carbon_intensity_records").fetchone()
    # current + 3 back-filled hours + 6 forecasts, stored once despite two runs
    assert weather == (10, 6, "mock", "mock")
    assert carbon == (10, 6)
