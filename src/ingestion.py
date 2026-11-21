"""Step 2 - Ingest raw CSV / JSON files, validate structure, produce an ingestion report."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src import config


class IngestionError(Exception):
    pass


def _read_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise IngestionError(f"File not found: {path}")
    if path.suffix == ".csv":
        # Everything is read as text so malformed values survive until validation.
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if path.suffix == ".json":
        return pd.read_json(path, orient="records", dtype=False, convert_dates=False)
    raise IngestionError(f"Unsupported file type: {path}")


def _check_columns(df: pd.DataFrame, expected: list, path: Path) -> None:
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise IngestionError(f"{path.name}: missing expected columns {missing}")


def ingest_events(raw_dir: Path = config.DATA_RAW):
    files = sorted(list(raw_dir.glob("streaming_events*.csv")) + list(raw_dir.glob("streaming_events*.json")))
    if not files:
        raise IngestionError(f"No streaming_events*.csv/json files in {raw_dir}. Run the generator first.")
    frames, per_file = [], {}
    for f in files:
        df = _read_file(f)
        _check_columns(df, config.EVENT_COLUMNS, f)
        df = df[config.EVENT_COLUMNS].astype(object)
        df = df.where(df.notna(), None)
        df["source_file"] = f.name
        per_file[f.name] = len(df)
        frames.append(df)
    events = pd.concat(frames, ignore_index=True)
    report = {"files": per_file, "total_records_received": len(events), "columns_ok": True}
    return events, report


def ingest_content(path: Path = None) -> pd.DataFrame:
    path = path or config.DATA_RAW / "content_metadata.csv"
    df = _read_file(path)
    _check_columns(df, config.CONTENT_COLUMNS, path)
    return df[config.CONTENT_COLUMNS].copy()


def write_report(report: dict) -> Path:
    config.REPORTS.mkdir(parents=True, exist_ok=True)
    out = config.REPORTS / "ingestion_report.json"
    out.write_text(json.dumps(report, indent=2))
    return out
