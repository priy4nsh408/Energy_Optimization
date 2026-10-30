"""Model persistence: versioned bundles with metadata.

Layout::

    ml/artifacts/<spec-name>/<version>/model.joblib    # estimator + feature spec
    ml/artifacts/<spec-name>/<version>/metadata.json   # metrics, dataset provenance, versions
    ml/artifacts/<spec-name>/latest.json               # pointer to the newest version

``joblib`` files can execute code when loaded, so only load bundles you created.
"""

from __future__ import annotations

import json
import os
import platform
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import sklearn
import xgboost

from config import BASE_DIR
from ml.features import FeatureSpec

ARTIFACT_ROOT = BASE_DIR / "ml" / "artifacts"


class ModelNotFoundError(FileNotFoundError):
    pass


@dataclass
class ModelBundle:
    model: Any
    model_name: str
    spec: Any                    # FeatureSpec (time series) or TabularSpec
    feature_names: list[str]
    version: str
    metadata: dict[str, Any]


def _atomic_write(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(text)
    os.replace(tmp, path)


def save_bundle(model: Any, model_name: str, spec: Any, feature_names: list[str],
                metadata: dict[str, Any], root: Path = ARTIFACT_ROOT) -> ModelBundle:
    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory = root / spec.name / version
    directory.mkdir(parents=True, exist_ok=False)
    metadata = {
        **metadata, "version": version, "model_name": model_name, "spec": spec.as_dict(),
        "feature_names": feature_names,
        "library_versions": {"python": platform.python_version(), "sklearn": sklearn.__version__,
                             "xgboost": xgboost.__version__, "joblib": joblib.__version__},
    }
    joblib.dump({"model": model, "model_name": model_name, "spec": spec.as_dict(),
                 "feature_names": feature_names, "version": version}, directory / "model.joblib")
    _atomic_write(directory / "metadata.json", json.dumps(metadata, indent=2, default=str))
    _atomic_write(root / spec.name / "latest.json", json.dumps({"version": version}))
    return ModelBundle(model, model_name, spec, feature_names, version, metadata)


def load_bundle(spec_name: str, version: str | None = None, root: Path = ARTIFACT_ROOT) -> ModelBundle:
    """Load a bundle (latest version by default)."""
    base = root / spec_name
    if version is None:
        pointer = base / "latest.json"
        if not pointer.is_file():
            raise ModelNotFoundError(
                f"No trained model for '{spec_name}' in {base}. Train one with: "
                f"python -m ml.train --dataset {'building' if 'building' in spec_name else 'kag'}")
        version = json.loads(pointer.read_text(encoding="utf-8"))["version"]
    directory = base / version
    if not (directory / "model.joblib").is_file():
        raise ModelNotFoundError(f"Model files missing in {directory}")
    payload = joblib.load(directory / "model.joblib")
    metadata = json.loads((directory / "metadata.json").read_text(encoding="utf-8"))
    if payload["spec"].get("spec_type") == "tabular":
        from ml.tabular import TabularSpec

        spec = TabularSpec.from_dict(payload["spec"])
    else:
        spec = FeatureSpec.from_dict(payload["spec"])
    return ModelBundle(payload["model"], payload["model_name"], spec,
                       list(payload["feature_names"]), payload["version"], metadata)
