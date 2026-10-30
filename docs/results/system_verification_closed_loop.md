# System verification (closed_loop)

Run by `python -m scripts.verify_system --launch --mode closed_loop` from 2026-10-30 19:42 UTC; result: **PASSED**.

Isolated environment: database `smart_building_verify1030194232`, topic prefix `verify1030194232_`, dashboard port 5055, mock weather/carbon, offline agent policy.

| # | Check | Required | Result | Time (s) | Details |
|---|---|---|---|---|---|
| 1 | dashboard health + Socket.IO | yes | PASS | 0 | health 200; Socket.IO connected |
| 2 | start simulation from the dashboard API | yes | PASS | 3 | POST start -> 202; simulation_status running: True |
| 3 | simulator -> Kafka | yes | PASS | 5 | verify1030194232_building_sensor_data: 10 -> 35 messages |
| 4 | Kafka -> processing workers -> PostgreSQL | yes | PASS | 0 | sensor_readings 35, energy_records_with_co2e 35, weather_records 25, carbon_intensity_records 25 |
| 5 | ML energy predictions | yes | PASS | 8 | 15 predictions, 10 scored, MAE 0.390 kW |
| 6 | energy optimization | yes | PASS | 0 | 19 optimizer commands, 19 executed by the simulator (closed_loop) |
| 7 | dashboard live updates (Socket.IO) | yes | PASS | 0 | events received: {'status': 8, 'readings': 7, 'alerts': 3, 'actions': 7, 'decisions': 2}; simulation state in pushed status: running |
| 8 | warm-up to 26 simulated hours | yes | PASS | 300 | simulated 02-09 00:05 -> 02-10 02:15 |
| 9 | cyberattack detection (+ live alert) | yes | PASS | 2 | attack temperature_spoofing on Z1 detected after 0 simulated min by rules; pushed to the dashboard: True |
| 10 | drift detection | yes | PASS | 0 | drift alerts by classification: {'data_drift': 19, 'gradual_manipulation': 3} |
| 11 | agent diagnosis and validated actions | yes | PASS | 4 | offline: 'manipulated (spoofed) temperature readings' (high); 1 approved, 1 executed; temperature isolated: True |
| 12 | building feedback loop | yes | PASS | 17 | agent_command_executed_by_simulator: ('overrides cleared, zone back on its baseline schedule', '2026-02-10T02:45:00+00:00'); dashboard_isolated_sensors: ['temperature']; optimizer_commands_for_quarantined_zone: 0 |
| 13 | recovery (sensor restored) | no | PASS | 115 | temperature sensor restored 625 simulated min after the attack ended |
| 14 | stop simulation from the dashboard API | yes | PASS | 8 | POST stop -> 202; paused: True; simulated time frozen: True |
| 15 | no service crashed or restarted | yes | PASS | 0 | 10 processes stopped cleanly |

Evidence for every check is in the JSON file next to this report.
