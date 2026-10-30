# System architecture

This document is the reference for how all modules fit together in the running application. The
step-by-step Windows guide is in [`EXECUTION_GUIDE.md`](EXECUTION_GUIDE.md).

## 1. Big picture

```
                         ┌──────────────────────── Flask dashboard (app.py) ───────────────────────┐
                         │ pages + JSON API ◄── PostgreSQL (services/dashboard_data.py)             │
   browser ◄─Socket.IO── │ live push ◄── one background task polling PostgreSQL every 2 s          │
                         │ Start/Stop/Attack ──► simulation_control (Kafka, publish only)          │
                         └──────────────────────────────────────────────────────────────────────────┘
                                                        │
                                                        ▼
 ┌─────────────────────┐  building_sensor_data  ┌──────────────────────┐
 │ simulation_manager  │ ─────────────────────► │ sensor_persistence   │──► sensor_readings
 │ (simulator, attacks,│                        │ energy_processor     │──► energy_records (+CO₂e)
 │  actuator)          │                        │ prediction_service   │──► energy_predictions ─┐
 └─────────▲───────────┘                        │ optimization_service │──► optimization_commands
           │ executes approved commands         │ security_service     │──► security_alerts ──┐ │
           │ (agent; + optimizer in closed loop)└──────────────────────┘                      │ │
           │                                                                                  ▼ │
           │   optimization_commands (approved)  ┌───────────────────────────────────────────────┐
           └──────────────────────────────────── │ agent_service: LangGraph agent → safety       │
                       agent_actions (executed)  │ validator → commands; quarantine; recovery    │
                                                 └───────────────────────────────────────────────┘
 external_data_publisher ──► weather_data, carbon_intensity
 record_persistence: weather, carbon, alerts, commands, agent actions, predictions ──► PostgreSQL
```

Kafka is the backbone. Every arrow between services is a topic, and every box is its own process.
PostgreSQL is the system of record. Flask never consumes Kafka: it reads PostgreSQL and only
*publishes* simulation-control commands.

## 2. Processes

`python -m scripts.run_stack` starts and supervises these processes:

| Process | Consumes (consumer group = process name) | Produces / writes | Module |
|---|---|---|---|
| `simulation_manager` | `simulation_control`; `optimization_commands` (group `simulation_manager_actuator`) | `building_sensor_data`; `agent_actions` (executed/failed); `simulation_status`, `simulated_attacks` | 2, 8, 10 |
| `sensor_persistence` | `building_sensor_data` | `sensor_readings` | 4 |
| `energy_processor` | `building_sensor_data`, `carbon_intensity` | `energy_records` (features, CO₂e) | 4 |
| `external_data_publisher` | – (polls Open-Meteo / Electricity Maps / UK grid, or mock) | `weather_data`, `carbon_intensity` | 5 |
| `record_persistence` | weather, carbon, alerts, commands, agent actions, predictions | the matching tables | 4 |
| `prediction_service` | `building_sensor_data` | `energy_predictions` | 6 |
| `optimization_service` | sensor data, predictions, carbon, `agent_actions` | `optimization_commands` | 7 |
| `security_service` | `building_sensor_data` | `security_alerts` (attacks, anomalies, security drift, data drift) | 8 |
| `agent_service` | `security_alerts` | `optimization_commands` (approved agent commands); `agent_actions`; `agent_decisions`, `optimization_actions`; JSONL audit log | 9 |
| dashboard (`app.py`) | – | `simulation_control` | 10 |

**Consumer groups.** Each service has its own consumer group, so every service receives every
message of its topics. `tests/test_architecture.py` checks that no two services share a group.

**Scaling.** Running two instances of the same service splits partitions between them. Most
services keep per-zone state, and messages are keyed by zone, so the split stays correct. Two
exceptions need care:

- *A second `simulation_manager` refuses to start.* It sees a heartbeat less than 15 s old in
  `simulation_status`, and two simulators would publish duplicate readings.
- *Do not combine `sensor_publisher --apply-commands` with `simulation_manager`.* Both would
  simulate the same building.

## 3. Control modes (`CONTROL_MODE`)

| | `advisory` (default) | `closed_loop` |
|---|---|---|
| Building control | its own schedule + agent actions | the optimizer's approved commands + agent actions |
| Commands executed by the simulator | `agent` | `agent`, `optimizer` |
| Security detectors (`SECURITY_DETECTORS=auto`) | `security/artifacts` (trained on schedule-controlled data) | `security/artifacts/optimizer_control` (trained on optimizer-controlled data) |
| Feedback loops | agent → simulator → readings → detectors | additionally optimizer → simulator → readings → ML → optimizer |

**Why the detectors follow the mode.** Module 11 measured the effect of mismatched detectors:

- Detectors trained on one control regime raise false alarms under the other (precision 0.06
  instead of 0.94).
- *Remaining known effect in closed loop:* zones the agent returns to their own schedule run
  under a different regime from the one their detectors were trained on. This raises false alarms
  and delays recovery.

## 4. Life of an attack (closed loop shown)

1. Dashboard "Trigger attack" → `simulation_control` → `simulation_manager` adds an attack. From
   the next reading on, it manipulates the readings it publishes. The true building state is not
   changed.
2. `security_service` flags a reading (rules, Isolation Forest) or a window (KS + PSI drift) and
   publishes an alert to `security_alerts`.
3. `agent_service` decides whether to investigate (severity, cooldown, containment). The LangGraph
   agent then:
   - gathers evidence with its tools (PostgreSQL data, detector re-runs, optimizer);
   - diagnoses the cause and proposes actions.
4. The deterministic **safety validator** checks every proposal twice: when proposed, and again
   against the latest state before execution.
   - An isolation is a software quarantine, applied in the agent service.
   - Approved control commands go to `optimization_commands`.
5. `simulation_manager` applies the command to the building and reports `executed` on
   `agent_actions`.
   - `optimization_service` reads the isolation from `agent_actions` and stops optimizing that zone.
   - `record_persistence` stores everything.
   - The dashboard shows the alert, the decision, the safety verdicts, the result and the
     quarantine.
6. The agent's recovery thread restores the sensor once the zone has been quiet for an hour of
   simulated time. The restore is recorded like any other action, and the optimizer resumes the
   zone.

## 5. State ownership and consistency

| State | Owner | How others see it |
|---|---|---|
| Building physics, overrides | `simulation_manager` (in memory) | readings on `building_sensor_data` |
| Sensor quarantine | `agent_service` (in memory, thread-safe) | `agent_actions`; PostgreSQL; rebuilt from PostgreSQL at start-up by the agent and optimization services |
| Action lifecycle | upserts in `optimization_actions` | status moves forward only (`proposed → approved/rejected → executed/failed`); later messages add parameters (e.g. the simulator's result) |
| Simulation state | `simulation_status` (heartbeat every 3 s) | dashboard status, single-instance guard |
| Simulated timeline | `run_stack` picks the start: 9 Feb 2026, or one step after the latest stored reading | reading ids depend on seed *and* start time, so a new run never collides with stored history (the duplicate check would otherwise drop its readings) |
| Everything for display | PostgreSQL | dashboard queries, anchored on the most recently *stored* data |

## 6. Concurrency and failure handling

- **At-least-once delivery.**
  - Offsets are committed only after a message has been processed.
  - Every write is idempotent: the record's own id is the key, and statuses only move forward.
  - A database outage raises `TransientProcessingError`, and the message is retried until it
    succeeds; nothing is skipped.
- **Out-of-order messages.** An agent's `approved` message can arrive after the simulator's
  `executed`. The forward-only upsert keeps `executed` and the execution result.
- **Agent service.** Alert handling (the consumer thread) and recovery (a background thread) take
  turns under one lock, so they never act on the quarantine at the same time.
  - The registry, the validator's rate-limit history and the producer are thread-safe.
- **Simulation manager.** Kafka consumer threads only *queue* commands. The simulation thread
  applies them between readings, so only one thread touches the building model.
- **Dashboard.**
  - Exactly one live-update task per process.
  - The Werkzeug auto-reloader is disabled: it forked a second process, which meant two pollers
    and a child that outlived its supervisor.
  - Run one dashboard process. Several Gunicorn workers would each poll the database.
- **Supervisor** (`scripts/run_stack.py`).
  - A crashed service is restarted with backoff, at most `--max-restarts` times.
  - On shutdown, POSIX processes get SIGTERM and Windows processes get Ctrl+Break in their own
    process group. Both are graceful stops that every worker handles.
- **Start-up order** doesn't matter much. Every service waits for Kafka and PostgreSQL, and the
  producing services create missing topics.
  - The three persistence workers expect the topics to exist already;
    `python -m streaming.topics` (run by the bootstrap) creates all of them.
  - `scripts/preflight.py` reports what's missing, with the command that fixes it.

## 7. Imports and configuration

- **Imports.** There are no import cycles at module level; `tests/test_architecture.py` checks
  this with an AST-based graph. Heavy or optional dependencies (Anthropic SDK, matplotlib, the
  agent package in `sensor_publisher`) are imported where they're used.
- **Configuration.**
  - All configuration lives in `config.py` and comes from the environment or `.env`.
  - Every variable read there is documented in `.env.example`; a test checks this.
  - Invalid values raise `ConfigurationError` at start-up.
  - `python -m scripts.preflight` checks the environment as a whole, including mismatched
    detectors and control mode, a missing API key, a busy port, and a simulation that's already
    running.

## 8. Verification

| What | Command | Covers |
|---|---|---|
| Unit and integration tests | `python -m pytest` | everything; Kafka/PostgreSQL tests are skipped without Docker |
| Architecture tests | `python -m pytest tests/test_architecture.py` | imports, consumer groups, configuration, supervisor, quarantine sharing, recovery |
| End-to-end (isolated) | `python -m scripts.verify_system --launch [--mode closed_loop]` | the full chain: simulator → Kafka → workers → PostgreSQL → ML → optimizer → detection → drift → agent → validated actions → simulator → dashboard |
| End-to-end as a test | `RUN_E2E=1 python -m pytest tests/test_system_e2e.py` | the same, both modes (about 20 minutes) |

Results of the runs used for the final check are in `docs/results/system_verification_*.md`.
