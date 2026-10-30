"""End-to-end system test: the whole stack in an isolated database/topic set.

Slow (about 10 minutes per mode) and needs Docker, the trained models and the
dashboard port 5055/5056 free, so it only runs when RUN_E2E=1:

    RUN_E2E=1 python -m pytest tests/test_system_e2e.py -v        (Windows: set RUN_E2E=1)
"""

from __future__ import annotations

import json
import os

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.skipif(os.environ.get("RUN_E2E") != "1",
                                                  reason="set RUN_E2E=1 to run the end-to-end system test")]


@pytest.mark.parametrize("mode, port", [("advisory", 5055), ("closed_loop", 5056)])
def test_full_system(mode, port, tmp_path, monkeypatch):
    from scripts import verify_system

    monkeypatch.setenv("POSTGRES_PASSWORD", os.environ.get("POSTGRES_PASSWORD") or "change_me")
    report = tmp_path / f"verification_{mode}.md"
    code = verify_system.main(["--launch", "--mode", mode, "--port", str(port), "--report", str(report)])
    results = json.loads(report.with_suffix(".json").read_text(encoding="utf-8"))["results"]
    failed = [(r["step"], r["detail"]) for r in results if r["required"] and not r["passed"]]
    assert code == 0 and failed == [], failed
