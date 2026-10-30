"""Dataset loading and preprocessing.

Two sources produce the same *standardised* frame (one row per time step and
group, sorted by time):

* :func:`load_kag` - ``KAG_energydata_complete.csv`` (UCI "Appliances energy
  prediction", Candanedo et al. 2017): one house, 10-minute readings,
  target ``Appliances`` in Wh per interval.
* :func:`load_building` - the Kafka-derived ``sensor_readings`` table of the
  simulated building: five zones, target average power (kW) per interval.

Preprocessing: explicit numeric conversion (the CSV quotes and pads numbers),
timestamp parsing, de-duplication, sorting, validation of required columns and
removal of the two random-noise columns ``rv1``/``rv2`` that the dataset
authors added on purpose to test feature selection.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import BASE_DIR

logger = logging.getLogger(__name__)

DEFAULT_KAG_PATH = BASE_DIR / "data" / "raw" / "KAG_energydata_complete.csv"
KAG_REQUIRED = ["date", "Appliances", "lights",
                *[f"T{i}" for i in range(1, 10)], *[f"RH_{i}" for i in range(1, 10)],
                "T_out", "Press_mm_hg", "RH_out", "Windspeed", "Visibility", "Tdewpoint"]
KAG_NOISE_COLUMNS = ["rv1", "rv2"]

BUILDING_COLUMNS = [
    "zone_id", "event_time", "interval_seconds", "energy_kwh", "temperature_c", "humidity_pct",
    "co2_ppm", "occupancy_count", "lighting_level_pct", "hvac_setpoint_c", "hvac_status",
    "hvac_power_kw", "lighting_power_kw", "appliance_power_kw", "total_power_kw",
    "outdoor_temperature_c", "outdoor_humidity_pct",
]


class DatasetError(ValueError):
    """The dataset is missing, malformed or too small."""


@dataclass
class DatasetInfo:
    """Provenance and quality summary, stored with every trained model."""

    name: str
    source: str
    rows: int
    start: str
    end: str
    sha256: str | None = None
    dropped_rows: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return dict(vars(self))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_kag(path: Path = DEFAULT_KAG_PATH) -> tuple[pd.DataFrame, DatasetInfo]:
    """Load and clean the KAG/UCI appliances energy dataset."""
    path = Path(path)
    if not path.is_file():
        raise DatasetError(
            f"Dataset not found: {path}. Place KAG_energydata_complete.csv in data/raw/ "
            "or pass --data PATH.")
    raw = pd.read_csv(path, dtype=str, skipinitialspace=True)
    missing = [c for c in KAG_REQUIRED if c not in raw.columns]
    if missing:
        raise DatasetError(f"{path.name} is missing columns: {missing}")

    df = raw.drop(columns=[c for c in KAG_NOISE_COLUMNS if c in raw.columns])
    dropped: dict[str, int] = {}
    df["timestamp"] = pd.to_datetime(df.pop("date").str.strip(), errors="coerce")
    for column in df.columns.drop("timestamp"):
        df[column] = pd.to_numeric(df[column].str.strip(), errors="coerce").astype(float)

    before = len(df)
    df = df.dropna(subset=["timestamp", "Appliances"])
    dropped["unparseable_timestamp_or_target"] = before - len(df)
    before = len(df)
    df = df.drop_duplicates(subset="timestamp", keep="first")
    dropped["duplicate_timestamps"] = before - len(df)
    before = len(df)
    df = df[df["Appliances"] >= 0]
    dropped["negative_target"] = before - len(df)

    df = df.sort_values("timestamp").reset_index(drop=True)
    df.insert(0, "group", "house")
    missing_values = int(df.isna().sum().sum())
    if len(df) < 1000:
        raise DatasetError(f"Only {len(df)} usable rows in {path.name}")
    info = DatasetInfo(
        name="kag_appliances", source=str(path), rows=len(df),
        start=df["timestamp"].min().isoformat(), end=df["timestamp"].max().isoformat(),
        sha256=file_sha256(path), dropped_rows=dropped,
        notes=[f"removed random-noise columns {KAG_NOISE_COLUMNS}",
               f"remaining missing values: {missing_values}"])
    logger.info("KAG dataset loaded", extra={"dataset": info.as_dict()})
    return df, info


def load_building(conn: Any, *, start: datetime | None = None, end: datetime | None = None,
                  building_id: str | None = None) -> tuple[pd.DataFrame, DatasetInfo]:
    """Load the Kafka-derived ``sensor_readings`` history from PostgreSQL."""
    conditions, params = ["TRUE"], []
    if start is not None:
        conditions.append("event_time >= %s")
        params.append(start)
    if end is not None:
        conditions.append("event_time < %s")
        params.append(end)
    if building_id is not None:
        conditions.append("building_id = %s")
        params.append(building_id)
    query = (f"SELECT {', '.join(BUILDING_COLUMNS)} FROM sensor_readings "
             f"WHERE {' AND '.join(conditions)} ORDER BY zone_id, event_time")
    rows = conn.execute(query, params).fetchall()
    df = pd.DataFrame(rows, columns=BUILDING_COLUMNS)
    if len(df) < 1000:
        raise DatasetError(f"Only {len(df)} sensor readings in the selected range; "
                           "generate more history first (see README).")
    df = prepare_building_frame(df)
    info = DatasetInfo(name="building_sensor_readings", source="postgresql:sensor_readings",
                       rows=len(df), start=df["timestamp"].min().isoformat(),
                       end=df["timestamp"].max().isoformat(),
                       notes=[f"zones: {sorted(df['group'].unique())}"])
    logger.info("Building dataset loaded", extra={"dataset": info.as_dict()})
    return df, info


def prepare_building_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise sensor readings (from PostgreSQL or Kafka events).

    Shared by training and the online prediction service, so both see the
    same columns. Target: average power over the interval (kW), which is
    comparable across different simulation speeds.
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df.pop("event_time"), utc=True)
    df = df.rename(columns={"zone_id": "group"})
    for column in df.columns.drop(["timestamp", "group", "hvac_status"]):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["avg_power_kw"] = df["energy_kwh"] * 3600 / df["interval_seconds"]
    for status in ("heating", "cooling", "off"):
        df[f"hvac_{status}"] = (df["hvac_status"] == status).astype(float)
    df = df.drop(columns=["hvac_status"])
    df = df.drop_duplicates(subset=["group", "timestamp"], keep="first")
    return df.sort_values(["group", "timestamp"]).reset_index(drop=True)
