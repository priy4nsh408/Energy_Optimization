# Execution guide (Windows)

How to install, start, use and verify the complete system on Windows 10/11. The architecture is
explained in [`ARCHITECTURE.md`](ARCHITECTURE.md).

> **Tested and not tested.** The Python commands in this guide were tested end to end on Linux.
> The PowerShell scripts in `scripts\windows\` are thin wrappers around those same commands, but
> were *not* run on Windows (no Windows machine was available). If a script fails, run the Python
> command shown next to it.

## 1. Prerequisites

| Software | Version | Notes |
|---|---|---|
| Python | 3.10–3.13 (3.12 recommended) | python.org installer; tick "Add python.exe to PATH" |
| Docker Desktop | current | runs Kafka and PostgreSQL; WSL 2 backend |
| Git | current | to clone the repository |
| Browser | current | the dashboard loads Bootstrap, Plotly and Socket.IO from public CDNs (internet needed) |

Free ports: 5000 (dashboard), 9092 (Kafka), 5432 (PostgreSQL). About 4 GB of RAM for the containers.

## 2. One-time setup (≈ 15–30 minutes)

```powershell
git clone https://github.com/priy4nsh408/Energy_Optimization.git
cd Energy_Optimization
git checkout claude/nifty-brown-uckrlo
scripts\windows\setup.bat                 # or: powershell -ExecutionPolicy Bypass -File scripts\windows\setup.ps1
```

`setup` does the following:

1. creates `.venv` and installs `requirements.txt`;
2. creates `.env` from `.env.example` **only if `.env` does not exist yet**;
3. starts Kafka and PostgreSQL with Docker Compose;
4. runs `python -m scripts.bootstrap`, which:
   - initialises the database schema and creates the Kafka topics;
   - generates 28 simulated days of building history through Kafka into PostgreSQL;
   - trains the ML model and the security detectors;
   - finishes with the preflight checks.

For closed-loop mode, add `-ClosedLoop` (`scripts\windows\setup.bat -ClosedLoop`). This also trains
the detectors for optimizer-controlled operation (about 15 minutes more).

After the first run, edit `.env`. The important settings:

| Variable | Meaning |
|---|---|
| `POSTGRES_PASSWORD` | must match the Docker container (default `change_me`) |
| `CONTROL_MODE` | `advisory` (default) or `closed_loop`, see `ARCHITECTURE.md` §3 |
| `WEATHER_PROVIDER`, `CARBON_PROVIDER` | `mock` for offline use; `ELECTRICITY_MAPS_API_KEY` for real carbon data |
| `AGENT_LLM_PROVIDER`, `ANTHROPIC_API_KEY` | `auto` uses Claude when a key is set; otherwise the offline rule-based policy |

Optional: the KAG research model needs `data/raw/KAG_energydata_complete.csv` and
`python -m ml.train --dataset kag`. The dashboard's measured baseline-vs-optimized chart needs
`python -m experiments.comparison --json docs/results/optimization_comparison.json`; this file is
already in the repository.

## 3. Start the system

```powershell
scripts\windows\start.bat                                 # CONTROL_MODE from .env
scripts\windows\start.bat -Mode closed_loop -MockExternal # optimizer controls the building, synthetic weather/carbon
```

`start` brings up Docker, runs the preflight checks and starts 10 processes: nine workers and the
dashboard. Their output goes to `logs\stack\<service>.log`. The same thing without PowerShell:

```powershell
.venv\Scripts\activate
python -m scripts.run_stack --mode advisory --mock-external
```

Open **http://127.0.0.1:5000**. The simulation starts *paused* at 9 February 2026, so press
**Start**. The simulation runs 300× faster than real time: one simulated hour takes 12 seconds.

**Stop:** press Ctrl+C in the start window, which stops every service gracefully. Then run
`scripts\windows\stop.bat`, which stops the containers and keeps the data.

## 4. Use it

| Page | What to do / see |
|---|---|
| Dashboard | live zone readings, building power, energy/CO₂ totals, latest alerts and agent decision; Start/Stop |
| Energy Analytics | hourly energy and carbon, ML predictions vs. measured power, baseline vs. optimized |
| Security Monitoring | trigger a simulated attack (type, zone, duration, parameters); watch its detection latency, the alerts, drift and isolated sensors |
| Agentic AI Activity | each investigation: diagnosis, impact, recommendations, tools called, proposed actions with the safety verdict and the simulator's result |

**Tips:**

- Detectors and the agent need about **one simulated day of history** (≈ 5 minutes after Start)
  before attacks are investigated. Earlier alerts are recorded but are treated as "cold start".
- A good first test is temperature spoofing on Z1 for 60 minutes with `offset_c` 4. The agent
  isolates the temperature sensor and returns the zone to its baseline schedule. After about an
  hour of quiet simulated time it restores the sensor.

## 5. Verify the installation end to end

```powershell
scripts\windows\verify.bat                     # advisory mode, ≈ 10 minutes
scripts\windows\verify.bat -Mode closed_loop
```

Or: `python -m scripts.verify_system --launch --mode advisory --report docs\results\system_verification_advisory.md`.

- **Isolated run:** the verification starts its own isolated stack (a separate database, topics and
  port 5055) and does not touch your data. It drives the system through the dashboard API and
  checks every stage (see `ARCHITECTURE.md` §8).
- **Afterwards:** it removes its database and topics, and prints `SYSTEM VERIFICATION PASSED` or
  the failing step.

**Other checks:**

```powershell
python -m scripts.preflight                    # what is missing, with the fix
python -m pytest                               # all tests (Docker running)
python -m pytest -m "not kafka and not postgres"   # without Docker
```

## 6. Experiments

| Command | Result |
|---|---|
| `python -m experiments.evaluation --out docs\results\evaluation` | Module 11 evaluation (≈ 40 minutes, 4 processes) |
| `python -m experiments.evaluation_report --run docs\results\evaluation` | tables and graphs |
| `python -m agent.demo --report docs\results\agent_demo.md` | in-process agent demonstration (≈ 2 minutes) |
| `python -m experiments.comparison`, `experiments.cyberattack_experiment`, `experiments.kag_drift_analysis`, `experiments.kag_anomaly_experiment` | the individual module experiments |

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Preflight: `postgresql` fail | Docker not running, or `POSTGRES_PASSWORD` in `.env` differs from the container's |
| Preflight: `database schema` fail | `python -m database init` |
| Preflight: `ML building model` / `security detectors` fail | `python -m scripts.bootstrap` (closed loop: `--closed-loop`) |
| `simulation manager` refuses to start | another one is running (e.g. a second `start`); stop it, or wait 15 s after a crash |
| Dashboard shows "Data unavailable" | PostgreSQL down; the page recovers by itself once it is back |
| No charts or icons | the browser cannot reach the CDNs (offline) |
| Many false alerts in closed loop | expected in zones the agent returned to the baseline schedule (Module 11 finding); advisory mode avoids it |
| A service keeps restarting | read `logs\stack\<service>.log`; the supervisor gives up after 3 restarts |
| `ExecutionPolicy` error | use the `.bat` files, or `powershell -ExecutionPolicy Bypass -File ...` |
