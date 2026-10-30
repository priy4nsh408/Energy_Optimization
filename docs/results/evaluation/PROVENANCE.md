# How these raw files were produced

* All 12 jobs ran with `python -m experiments.evaluation --out docs/results/evaluation
  --optimizer-detectors 20261030T165511Z` (`config.json`).
* While the six attack jobs (`*_attack_undefended*`, `*_attack_defended*`) were
  running, worker processes left over from an aborted earlier start (same code
  and config) were still running. That put 8 processes on 4 CPUs, which
  inflates wall-clock timings.
* These six jobs were therefore re-run on their own with the same command,
  adding `--scenarios attack_undefended attack_defended`. The re-run was
  compared field by field with the original run: everything except the
  wall-clock fields (`latency_ms`, `wall_seconds`, agent `wall_seconds`) was
  **identical**, including results, detections, alerts, agent decisions,
  actions, predictions and time series.
* The files here are the re-run files, so their timings were measured without
  the extra load. All other numbers are the same as in the original run.
* `streaming_latency.json` was measured afterwards on the idle machine with
  `python -m experiments.streaming_latency --out docs/results/evaluation`.
