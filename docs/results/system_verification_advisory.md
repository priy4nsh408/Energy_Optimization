# System verification (advisory)

Run by `python -m scripts.verify_system --launch --mode advisory` from 2026-10-30 19:36 UTC; result: **PASSED**.

Isolated environment: database `smart_building_verify1030193612`, topic prefix `verify1030193612_`, dashboard port 5055, mock weather/carbon, offline agent policy.

| # | Check | Required | Result | Time (s) | Details |
|---|---|---|---|---|---|
| 1 | dashboard health + Socket.IO | yes | PASS | 0 | health 200; Socket.IO connected |
| 2 | start simulation from the dashboard API | yes | PASS | 2 | POST start -> 202; simulation_status running: True |
| 3 | simulator -> Kafka | yes | PASS | 5 | verify1030193612_building_sensor_data: 10 -> 35 messages |
| 4 | Kafka -> processing workers -> PostgreSQL | yes | PASS | 0 | sensor_readings 35, energy_records_with_co2e 35, weather_records 25, carbon_intensity_records 25 |
| 5 | ML energy predictions | yes | PASS | 8 | 15 predictions, 10 scored, MAE 0.024 kW |
| 6 | energy optimization | yes | PASS | 0 | 25 optimizer commands, 0 executed by the simulator (advisory) |
| 7 | dashboard live updates (Socket.IO) | yes | PASS | 0 | events received: {'status': 8, 'readings': 7, 'actions': 5}; simulation state in pushed status: running |
| 8 | warm-up to 26 simulated hours | yes | PASS | 300 | simulated 02-09 00:05 -> 02-10 02:20 |
| 9 | cyberattack detection (+ live alert) | yes | PASS | 2 | attack temperature_spoofing on Z1 detected after 0 simulated min by rules; pushed to the dashboard: True |
| 10 | drift detection | yes | PASS | 0 | drift alerts by classification: {'data_drift': 20, 'gradual_manipulation': 5} |
| 11 | agent diagnosis and validated actions | yes | PASS | 6 | offline: 'manipulated (spoofed) temperature readings' (high); 1 approved, 1 executed; temperature isolated: True |
| 12 | building feedback loop | yes | PASS | 0 | agent_command_executed_by_simulator: ('overrides cleared, zone back on its baseline schedule', '2026-02-10T02:50:00+00:00'); dashboard_isolated_sensors: ['temperature'] |
| 13 | recovery (sensor restored) | no | PASS | 40 | temperature sensor restored 165 simulated min after the attack ended |
| 14 | stop simulation from the dashboard API | yes | PASS | 8 | POST stop -> 202; paused: True; simulated time frozen: True |
| 15 | no service crashed or restarted | yes | PASS | 0 | 10 processes stopped cleanly |

Evidence for every check is in the JSON file next to this report.
