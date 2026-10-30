# Evaluation run 1 (superseded design, kept as recorded)

This run is kept unchanged as part of the record. Its attack schedule put the
**gradual manipulation in Z2 at 02:00 on day 2**, while Z2 was still isolated
after the replay attack in the same zone (day 1, 17:00–19:00). The ongoing
alerts kept Z2 from being restored in between, so these metrics cannot be
separated or interpreted:

* the replay's recovery time (1215 min);
* the gradual attack's detection latency and containment (0/3), because the
  sensors were already isolated.

Run 2 (`../evaluation/`) moves the gradual attack to Z4, which was not
attacked during the preceding 10 hours. It uses the same seeds, the same
detectors (`20261030T165511Z`) and the same ML model. All other settings are
identical.

This was a change to the experiment design, decided after seeing run 1. It was
not tuned toward particular results.

**Note on `config.json`:** it was overwritten by mistake when run 2 was first
started in the same directory, and then reconstructed. The details are in its
`_reconstructed` field. The raw files and `streaming_latency.json` are the
originals. `report.md`, `summary.json` and the figures were regenerated from
those raw files after the figure layout was improved; the numbers are the
same.
