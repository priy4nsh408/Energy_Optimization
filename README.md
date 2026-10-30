# Smart Building Energy Optimization

**Real-time streaming, cybersecurity and agentic AI for smart-building energy management.**

Final-year engineering project. A simulated multi-zone building streams sensor data through
Apache Kafka. Independent workers persist the data to PostgreSQL, predict energy use with ML,
optimise HVAC and lighting, and detect cyberattacks, anomalies and data drift. An agentic AI
proposes corrective actions, and every action must pass a deterministic safety layer before it
is executed. A Flask + Socket.IO dashboard shows everything live.

## Architecture

```
simulator ──► [sensor_publisher] ──► building_sensor_data ─┬─► db_writer ───────────► PostgreSQL
weather / carbon APIs ──► weather_data, carbon_intensity ──┼─► ml_worker / optimizer
                                                           ├─► security_worker ──► security_alerts
agent (LangGraph) ◄── security_alerts ──► agent_actions ──► safety layer ──► optimization_commands ──► simulator
Flask + Socket.IO dashboard ◄── dashboard bridge (Kafka → Socket.IO) + PostgreSQL history
```

Design rules:
- Kafka is the event backbone.
- Each worker is its own process.
- Flask never consumes Kafka directly.
- AI actions are always validated by the safety layer before they run.

## Project structure

| Path | Purpose |
|---|---|
| `config.py` | Central, environment-driven configuration (Flask, Socket.IO, Kafka, PostgreSQL, logging) |
| `utils/logging_config.py` | Structured JSON/text logging, one log file per service |
| `app.py` | Flask application factory (wiring only) |
| `scripts/check_environment.py` | Verifies Python, packages, configuration and service connectivity |
| `simulator/` | Five-zone building physics, occupancy, outdoor climate and sensor simulation (attack simulation added later) |
| `docs/sample_output/` | Sample simulator output (JSON Lines) |
| `streaming/` | Kafka topic registry, JSON producer/consumer, connection helpers (named `streaming` so it does not shadow Kafka client libraries) |
| `workers/` | Long-running services, one process each (sensor publisher, topic monitor, later DB/ML/security workers) |
| `processors/` | Stream processing, validation and feature engineering |
| `database/` | PostgreSQL schema and data access |
| `data_sources/` | Weather and carbon-intensity APIs |
| `ml/` | Energy prediction model (training, inference, evaluation) |
| `optimization/` | HVAC and lighting optimisation under comfort constraints |
| `security/` | Anomaly, attack and drift detection, alert management |
| `agent/` | Agentic AI (LangGraph) and safety validation |
| `routes/`, `templates/`, `static/` | Dashboard |
| `experiments/` | Baseline / optimised / attack comparison experiments |
| `docker/` | Kafka and PostgreSQL containers |
| `tests/` | Unit tests (pytest) |

## Setup (Windows)

Requirements: Python 3.10–3.13 (3.12 recommended) and Git. Docker Desktop is needed later
for Kafka and PostgreSQL.

```powershell
git clone https://github.com/priy4nsh408/Energy_Optimization.git
cd Energy_Optimization
py -3.12 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
copy .env.example .env
```

On Linux/macOS, use `python3 -m venv .venv`, `source .venv/bin/activate` and
`cp .env.example .env` instead.

If PowerShell blocks `activate`, run
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.

## Configuration

All settings come from environment variables. A local `.env` file is loaded automatically.
Real environment variables always override `.env`, and `.env` is git-ignored.
`.env.example` documents every variable.

`APP_ENV` selects the environment:

| `APP_ENV` | Behaviour |
|---|---|
| `development` (default) | Flask debug mode, `DEBUG` logging, random `SECRET_KEY` if none is set |
| `testing` | Database name gets a `_test` suffix, Kafka topics get a `test.` prefix, console-only `WARNING` logging |
| `production` | `SECRET_KEY` (≥ 32 chars) and `POSTGRES_PASSWORD` are required |

Invalid values (e.g. `FLASK_PORT=abc`) fail fast with a clear `ConfigurationError`.

In code:

```python
from config import get_config
cfg = get_config()
cfg.KAFKA.producer_config()            # dict for confluent_kafka.Producer
cfg.KAFKA.consumer_config("db_writer") # each service uses its own consumer group
cfg.KAFKA.topic("sensor.readings")     # physical topic name (with prefix)
cfg.DATABASE.connection_kwargs()       # kwargs for psycopg.connect
```

## Building simulator

The simulator (`simulator/`) models five independent zones and emits one timestamped
JSON event per zone per reading. It does not depend on Kafka or the dashboard: events go
to a *sink* callable (stdout/file now, a Kafka producer in the streaming module).

| Zone | Type | Area | Capacity | Notes |
|---|---|---|---|---|
| Z1 Open-plan office | office | 300 m² | 30 | Lights follow the schedule (baseline waste) |
| Z2 Private offices | office | 120 m² | 8 | Lights follow occupancy |
| Z3 Meeting room | meeting | 60 m² | 12 | Hourly bookings, high ventilation |
| Z4 Cafeteria and lobby | cafeteria | 200 m² | 60 | Lunch peak, security lighting at night |
| Z5 Server room | server | 40 m² | 2 | 24/7 IT load and cooling |

**How the readings are produced (not random values):**

- **Temperature:** lumped RC heat balance covering the envelope, ventilation, people, appliances,
  lighting, solar gain and HVAC output.
- **HVAC:** proportional thermostat with a deadband. Outside operating hours it uses night setback
  (setpoint ± 4 °C). Electrical power = thermal output / COP + fan power, plus the energy the
  air handler spends dehumidifying ventilation air.
- **Humidity and CO₂:** moisture and CO₂ mass balances. Occupants add both; ventilation exchanges
  them with outdoor air.
- **Occupancy:** hourly weekday/weekend profiles per zone type, with day-to-day variation and
  gradual arrivals and departures. Meeting rooms use seeded hourly bookings.
- **Lighting:** daylight (from sun position and cloud cover) plus artificial lighting.
  **Appliances:** base load plus a per-occupant load with slowly varying fluctuation.
- **Outdoor climate:** seasonal and daily temperature cycle, sun position and slowly varying
  weather. *This is synthetic data*, used until the weather API module adds real data.
- **Sensors:** realistic noise, resolution and measuring range on top of the true physical state.
  The people counter occasionally miscounts by ±1, and one rooftop weather station serves all zones.
- **Reproducibility:** a fixed `--seed` reproduces a run exactly (apart from the wall-clock
  `emitted_at` field).

**Timing:** a reading is emitted every `interval_seconds` (1–5 s) of wall-clock time. Each reading
covers `interval_seconds × speed_factor` simulated seconds, integrated in physics steps of at most
60 s.

**Control interface:** `BuildingSimulator.apply_command(zone_id, setpoint_c=..., hvac_mode=...,
lighting_level=...)` is the only way other modules change the building.

```powershell
python -m simulator                                     # real time, settings from .env
python -m simulator --interval 1 --speed 60 --ticks 10  # 1 simulated minute per second
python -m simulator --speed 3600 --fast --ticks 24 --seed 42 --output data/day.jsonl
```

Example event (from `docs/sample_output/sensor_events_sample.jsonl`):

```json
{
  "event_id": "9c139171-1a24-4c73-aed1-da9063f838ca",
  "event_type": "sensor_reading",
  "schema_version": 1,
  "source": "simulator",
  "building_id": "building-01",
  "zone_id": "Z1",
  "zone_name": "Open-plan office",
  "zone_type": "office",
  "sequence": 1,
  "timestamp": "2026-01-14T09:05:00+00:00",
  "emitted_at": "2026-10-30T06:15:01.037+00:00",
  "interval_seconds": 300.0,
  "temperature_c": 21.3,
  "humidity_pct": 43.1,
  "co2_ppm": 478,
  "occupancy_count": 23,
  "lighting_lux": 861,
  "lighting_level_pct": 100.0,
  "hvac_mode": "auto",
  "hvac_status": "heating",
  "hvac_setpoint_c": 22.0,
  "hvac_power_kw": 4.58,
  "lighting_power_kw": 2.719,
  "appliance_power_kw": 3.789,
  "total_power_kw": 11.088,
  "outdoor_temperature_c": 5.7,
  "outdoor_humidity_pct": 66.4,
  "energy_kwh": 0.95716
}
```

## Apache Kafka

Kafka is the event backbone. Each service is its own process with its own consumer group.
Flask never consumes Kafka.

### Topics

| Topic | Partitions | Key | Retention | Content |
|---|---|---|---|---|
| `building_sensor_data` | 5 | `zone_id` | 7 d | Simulator sensor readings |
| `weather_data` | 1 | `building_id` | 30 d | Weather observations/forecasts |
| `carbon_intensity` | 1 | `building_id` | 30 d | Grid carbon intensity |
| `security_alerts` | 3 | `zone_id` | 30 d | Anomaly/attack/drift alerts |
| `optimization_commands` | 3 | `zone_id` | 7 d | Validated HVAC/lighting commands |
| `agent_actions` | 3 | `zone_id` | 30 d | Agent proposals and decisions (audit) |

The registry lives in `streaming/topics.py`. `KAFKA_TOPIC_PREFIX` is prepended to every name;
the testing environment uses `test.`. Topics are created explicitly, because broker
auto-creation is disabled.

### Reliability

- **Serialization:** messages are UTF-8 JSON objects. Sensor events are keyed by `zone_id`, so each
  zone always goes to one partition and its readings stay in order.
- **Producer:** idempotent with `acks=all`, so a retry cannot create duplicate messages. Every
  delivery report is counted (`sent/delivered/failed`) and failures are logged. When the local
  queue is full, it waits and retries.
- **Disconnections:** librdkafka reconnects automatically with back-off. The producer keeps
  undelivered messages for `KAFKA_MESSAGE_TIMEOUT_MS` (default 2 min), and consumers resume from
  their committed offsets. Services wait for the broker at start-up.
- **Consumer:**
  - Offsets are committed manually, after processing, which gives at-least-once delivery.
    **Handlers must be idempotent** (use `event_id`), because messages can be redelivered after a
    rebalance or broker restart.
  - Invalid JSON is logged and skipped.
  - A failing handler is retried with back-off, then the message is skipped.
  - Fatal client errors stop the service.

### Run it

Start Docker Desktop first.

```powershell
docker compose -f docker/docker-compose.yml --env-file .env up -d   # Kafka + PostgreSQL
python -m streaming.topics                                           # create topics (idempotent)
python -m streaming.topics --list                                    # show the registry
```

Then run each service in its own terminal:

```powershell
python -m workers.sensor_publisher --interval 2 --speed 60          # simulator -> Kafka
python -m workers.topic_monitor --group db_writer_demo              # consumer service 1
python -m workers.topic_monitor --group security_demo --quiet       # independent consumer 2
```

The two monitors use different consumer groups, so each receives every reading. Starting a
second monitor with the *same* `--group` splits the partitions between the two instances. Stop
any service with Ctrl+C; it shuts down cleanly and commits its offsets.

To test a broker restart:

```powershell
docker restart smart-building-kafka
```

The services log the disconnection, reconnect, and continue without losing messages.

## Verify the environment

```powershell
python -m scripts.check_environment                   # Python, packages, configuration
python -m scripts.check_environment --check-services  # also probe Kafka and PostgreSQL
python -m pytest                                      # unit + Kafka integration tests
python -m pytest -m "not kafka"                       # skip tests that need a broker
python app.py                                         # then open http://127.0.0.1:5000/health
```

## Development status

| # | Module | Status |
|---|---|---|
| 1 | Project setup and configuration | ✅ Done |
| 2 | Building simulator | ✅ Done |
| 3 | Kafka infrastructure (Docker, producers/consumers) | ✅ Done |
| 4 | PostgreSQL persistence | ⏳ Next |
| 5 | Weather and carbon-intensity data | Planned |
| 6 | Energy prediction (XGBoost) | Planned |
| 7 | HVAC/lighting optimisation (SciPy) | Planned |
| 8 | Cyberattack simulation | Planned |
| 9 | Anomaly and drift detection | Planned |
| 10 | Safety layer and action execution | Planned |
| 11 | Agentic AI (LangGraph) | Planned |
| 12 | Real-time dashboard | Planned |
| 13 | Experiments and evaluation | Planned |

No experimental results are reported yet. Results will be added only after real
experiment runs.
