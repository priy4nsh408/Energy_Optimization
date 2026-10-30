# Project progress and handoff

Status of *Smart Building Energy Optimization Using Real-Time Streaming, Cybersecurity and Agentic AI*.
Use this file to continue the work later, alone or in a new Claude session.

- **Branch:** `claude/nifty-brown-uckrlo`
- **Last completed module:** 12 (complete system integration), verified end to end in both control modes
- **Tests:** 374 passed, with Docker running and the three datasets in `data/raw/`.

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
| 9 | Agentic AI (LangGraph) + safety layer + simulated execution | ✅ | see git log | `agent/`, `python -m agent.demo`, `workers.agent_service`, `workers.sensor_publisher --apply-commands` |
| 10 | Flask web dashboard | ✅ | see git log | `app.py`, `routes/`, `services/`, `templates/`, `static/`, `workers.simulation_manager`, `python -m scripts.run_stack` |
| 11 | Experimental evaluation | ✅ | see git log | `python -m experiments.evaluation`, `experiments.streaming_latency`, `experiments.evaluation_report`; results in `docs/results/evaluation/` |
| 12 | Complete system integration | ✅ | see git log | `scripts/windows/*.bat`, `python -m scripts.bootstrap`, `scripts.preflight`, `scripts.run_stack`, `scripts.verify_system`; `docs/ARCHITECTURE.md`, `docs/EXECUTION_GUIDE.md` |

Measured results are in `docs/results/` and summarised in `README.md`.

**Real-data demonstration (after Module 8):** two experiments run on the KAG data.

- *Data drift:* `experiments/kag_drift_analysis.py`. It quantifies the drift and links it to the
  model's accuracy loss.
- *Attacks:* `experiments/kag_anomaly_experiment.py`. It injects attacks and evaluates point
  detection, KS/PSI drift and CUSUM.

The results are in the README section *Real data: drift and anomaly detection on KAG*. The new
`RollingDriftDetector` is in `security/drift_detector.py`.

## Resume on a new machine (Windows)

The short way is `scripts\windows\setup.bat` and then `scripts\windows\start.bat`. See
`docs/EXECUTION_GUIDE.md` for details. The manual steps follow.

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
| Security detectors (`security/artifacts/`) | `python -m security.train` (28 clean simulated days, seed 2026); also needed by `agent.demo` and the agent's anomaly/drift tools |
| Agent audit log (`logs/agent_decisions.jsonl`) | Created automatically |
| Optimizer-controlled detectors (`security/artifacts/optimizer_control/`) | Trained by `python -m experiments.evaluation` (version `20261030T165511Z` was used for the reported runs) |
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
  commands but does **not** execute them.
- **Agent (Module 9).** A LangGraph graph: context → LLM ⇄ tools → finalize → safety_gate → execute →
  record.
  - Action tools only *propose*. `agent/safety.py` decides deterministically, and the final gate
    re-validates against the latest state.
  - Only the simulator is changed: in process (`agent.demo`) or over Kafka
    (`optimization_commands` → `sensor_publisher --apply-commands` → `agent_actions`).
  - LLM providers (`AGENT_LLM_PROVIDER`): `anthropic` (Claude, `claude-opus-5-5`) or `offline` (a
    rule-based playbook, labelled in every record).
  - If the LLM fails, the incident continues with the offline policy and the error is recorded.
  - Sensor isolation is a software quarantine.
  - The tools report `history_hours`; the offline playbook does not act on less than 24 h of
    history (cold start).
- **Dashboard (Module 10).** Flask reads PostgreSQL only.
  - Live push comes from one Socket.IO background task that polls the database.
  - Controls are *published* to `simulation_control` and executed by `workers.simulation_manager`.
  - Windows are anchored on the most recently stored data. Do not reuse the same `--start` dates in
    one database for separate runs.
  - `python -m scripts.run_stack --mock-external` starts everything.
- **Integration (Module 12).**
  - `CONTROL_MODE=advisory|closed_loop` decides which command sources the simulator executes. The
    detectors follow the mode (`SECURITY_DETECTORS=auto`).
  - The optimizer mirrors the agent's quarantine via `agent_actions`.
  - The agent service runs the recovery policy and rebuilds the quarantine from PostgreSQL at
    start-up.
  - Only one simulation manager per building (heartbeat guard).
  - Verify everything with `python -m scripts.verify_system --launch`.
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
5. **Slow manipulation on real data.** The KS/PSI window drift tests are unusable on KAG
   (false alarms on 49–80 % of clean windows). CUSUM works only for faster ramps: it detected
   0.4 °C/h and missed 0.2 °C/h.
6. **Agent vs. detectors.** The Module 8 detectors were trained on baseline-controlled data. With
   the optimizer in the loop, the HVAC-contradiction rule raises false spoofing alerts (84 → 256
   alerts in the demo), and the offline playbook then isolates sensors in zones that are not under
   attack.
   - Fix idea: retrain the detectors on data recorded with the optimizer in control.
   - Also inherited from Module 8: an occupancy-hiding attack is first classified as
     `abnormal_power`, so the power meter is isolated before the occupancy sensor.
7. **Evaluation findings (Module 11).** See the README section *Experimental evaluation*.
   Detectors are specific to the control regime, and zones under attack recover slowly because of
   that.
8. **Claude not run live.** The Anthropic path is unit-tested with a mocked client only; no API key
   was available in the development environment. Run `python -m agent.demo --provider anthropic`.
9. **Windows.** Not yet run on Windows; developed and tested on Linux.

## Possible next steps

All 11 planned modules are done. Ideas the evaluation pointed to, none of them implemented:

- **Control-regime-aware detectors.** Detectors trained on optimizer-controlled data raise false
  alarms when the agent returns a zone to its baseline schedule (live precision 0.32 vs. 0.94
  passively), and this delays recovery. A fix would choose the detector by each zone's control mode.
- **ML model retraining** on optimizer-controlled history. Prediction MAE is 0.33 kW under the
  optimizer vs. 0.13 kW under baseline control.
- **A live Claude run** of the agent (needs `ANTHROPIC_API_KEY`).
- **More seeds or longer runs** for tighter confidence intervals.

## How to continue with Claude

Start a new session on this repository and branch, and paste:

> Continue the Smart Building Energy Optimization project on branch `claude/nifty-brown-uckrlo`.
> Read `docs/PROGRESS.md` and `README.md` first, then inspect the repository. Follow the same
> working method as before: one module at a time, tests, honest results, commit and push, then stop
> for approval. Then tell Claude what to work on next.
