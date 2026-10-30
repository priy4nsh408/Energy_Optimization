# Project progress and handoff

Status of *Smart Building Energy Optimization Using Real-Time Streaming, Cybersecurity and Agentic AI*.
Use this file to continue the work later, alone or in a new Claude session.

- **Branch:** `claude/nifty-brown-uckrlo`
- **Last completed module:** 8 (cybersecurity and data drift detection)
- **Tests:** 277 passed, with Docker running and the three datasets in `data/raw/`.

## Module status

| # | Module | Status | Commit | Main entry points |
|---|---|---|---|---|
| 1 | Project setup and configuration | ✅ | `3bc492d` | `config.py`, `.env.example`, `utils/logging_config.py`, `python -m scripts.check_environment` |
| 2 | Building simulator | ✅ | `d5f6d3e` | `simulator/`, `python -m simulator` |
| 3 | Kafka integration | ✅ | `449b616` | `streaming/`, `python -m streaming.topics`, `workers.sensor_publisher`, `workers.topic_monitor` |
| 4 | Data processing + PostgreSQL | ✅ | `ca07139` | `database/`, `processors/`, `python -m database init`, `workers.sensor_persistence`, `workers.energy_processor`, `workers.record_persistence` |
| 5 | Weather + carbon data | ✅ | `66204b6` | `data_sources/`, `workers.external_data_publisher` |
| 6 | Energy prediction (ML) | ✅ | `99c5b86` (+ three project datasets, see below) | `ml/`, `python -m ml.train --dataset all`, `workers.prediction_service` |
| 7 | HVAC/lighting optimization | ✅ | `b89ba36`, `9061984` | `optimization/`, `workers.optimization_service`, `python -m experiments.comparison` |
| 8 | Cybersecurity + drift detection | ✅ | `741c57c` | `simulator/attack_simulator.py`, `security/`, `python -m security.train`, `workers.security_service`, `python -m experiments.cyberattack_experiment` |
| 9 | Safety layer and action execution | ⏳ next | | |
| 10 | Agentic AI (LangGraph) | planned | | |
| 11 | Real-time dashboard (Flask-SocketIO) | planned | | |
| 12 | Experiments and evaluation | planned | | |

Measured results are in `docs/results/` and summarised in `README.md`.

## Resume on a new machine (Windows)

These commands assume PowerShell in the project root.

```powershell
git clone https://github.com/priy4nsh408/Energy_Optimization.git
cd Energy_Optimization
git checkout claude/nifty-brown-uckrlo
py -3.12 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env            # then edit: POSTGRES_PASSWORD, location, providers
docker compose -f docker/docker-compose.yml --env-file .env up -d
python -m database init
python -m streaming.topics
python -m pytest -m "not kafka and not postgres"   # quick check without Docker
python -m pytest                                    # full check with Docker running
```

## Things that are NOT in Git and must be regenerated

These are git-ignored, either because they are large or because they are generated.

| What | How to recreate |
|---|---|
| `.env` | `copy .env.example .env`, then set `POSTGRES_PASSWORD` (Docker default `change_me`), `BUILDING_LATITUDE/LONGITUDE`, and `CARBON_PROVIDER` plus `ELECTRICITY_MAPS_API_KEY`, or `mock` |
| Project datasets | Copy your three files into `data/raw/` with these exact names: `KAG_energydata_complete.csv` (SHA-256 `2820bf71…a25d`), `test_energy_data.csv` (`01c85f8d…26da`) and `energy_consumption.csv` (`74cfb5a3…9301`). Then run `python -m ml.train --dataset all --report docs/results/energy_prediction_combined.md`. |
| Building history in PostgreSQL | Run `python -m workers.sensor_persistence` in one terminal, and in another: `python -m workers.sensor_publisher --fast --interval 1 --speed 300 --ticks 8064 --seed 2026 --start 2026-01-05T00:00` |
| ML models (`ml/artifacts/`) | `python -m ml.train --dataset kag`, then `python -m ml.train --dataset building --start 2026-01-05T00:00+00:00 --end 2026-02-02T00:00+00:00` |
| Security detectors (`security/artifacts/`) | `python -m security.train` (28 clean simulated days, seed 2026) |
| API cache (`data/cache/`), logs | Created automatically |

## Key design decisions (keep these when continuing)

- **Kafka is the backbone.** Every service runs as its own process with its own consumer group.
  Flask never consumes Kafka.
- **At-least-once delivery.** Offsets are stored only after successful processing. Handlers are
  idempotent (`event_id` / `alert_id` / `action_id` with `ON CONFLICT`).
  `TransientProcessingError` means "retry forever, never skip" (used for database outages).
- **Topics:** `building_sensor_data` (key `zone_id`), `weather_data`, `carbon_intensity`,
  `security_alerts`, `optimization_commands`, `agent_actions`, `energy_predictions`.
  Message contracts are in `processors/data_validator.py` and `README.md`.
- **Lifecycles.** Action status goes `proposed → approved/rejected → executed/failed`; alert status
  goes `open → acknowledged → resolved`. Database upserts only move status forward.
- **Three datasets.** They are trained separately and *not pooled*, because their targets and units
  differ. KAG is a time series (chronological split); the other two are cross-sectional (repeated
  K-fold CV). `test_energy_data.csv` is formula-generated synthetic data (Linear Regression R² = 1).
- **ML.** The KAG dataset is the research benchmark. The *deployed* model is trained on the
  building's own Kafka-derived history, because KAG features don't exist in the stream.
- **Optimizer.** Deterministic SciPy SLSQP, with no LLM involved. It publishes `approved`/`rejected`
  commands but does **not** execute them; execution is Module 9.
- **Security.** Point anomalies (one reading) are kept separate from drift (a window).
  *Security drift* is distinct from *data drift*. Thresholds are calibrated only on clean data.
- **Honest evaluation.** Results are never tuned on test data. When a held-out week revealed
  problems, the report was kept, the detectors were fixed, and a *new* held-out week was used.

## Known open issues to remember

1. **Security false positives across seasons.** The HVAC-contradiction rule was learned from
   January data and has a 3.9 % false-positive rate in March. Fix ideas: multi-season or recent
   training data, retraining triggered by data drift, or bounds derived from thermostat logic.
2. **The optimizer ignores the ML forecast in practice.** The forecast shifts the objective by a
   constant, so decisions are the same with or without it. Fix idea: calibrate the heat-balance
   model per zone using the forecasts and measurements.
3. **Lighting.** The optimizer has more under-lit occupied readings than the baseline (≤ 4.8 %),
   because it reacts one 5-minute reading after people arrive.
4. **External APIs.** The live Open-Meteo / Electricity Maps / UK grid clients are untested against
   real responses; the cloud environment blocked those hosts. Run
   `python -m workers.external_data_publisher --once` locally to verify.
5. **Windows.** Not yet run on Windows; developed and tested on Linux.

## Next step: Module 9 (safety layer and action execution)

Planned scope, to be confirmed before implementing:

- A deterministic safety validator for *every* proposed action (optimizer, agent, operator):
  - setpoint and lighting bounds, rate limits and critical-zone rules (server room);
  - refuse or hold actions for zones with open high-severity security alerts;
  - stale-data checks.
- An actuator service that consumes approved commands, applies them to the simulator via
  `BuildingSimulator.apply_command`, and publishes the outcome (`executed`/`failed`) to
  `agent_actions`. This needs the simulator to accept commands while running in a Kafka-connected
  process, for example by combining the sensor publisher and the actuator.
- Tests, and a closed-loop check: attack → detection → safe corrective action.

## How to continue with Claude

Start a new session on this repository and branch, and paste:

> Continue the Smart Building Energy Optimization project on branch `claude/nifty-brown-uckrlo`.
> Read `docs/PROGRESS.md` and `README.md` first, then inspect the repository. Follow the same
> working method as before: one module at a time, tests, honest results, commit and push, then stop
> for approval. Implement Module 9: Safety Layer and Action Execution.
