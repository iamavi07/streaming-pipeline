"""Step 1 - Generate synthetic streaming data with controlled data-quality problems.

Usage:  python -m src.generate_data --events 30000 --seed 42
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
from datetime import datetime, timedelta

import pandas as pd

from src import config

ADJECTIVES = ["Silent", "Golden", "Hidden", "Crimson", "Eternal", "Lost", "Brave", "Midnight", "Wild", "Frozen"]
NOUNS = ["Kingdom", "Voyage", "Legacy", "Garden", "Horizon", "Secret", "Island", "Echo"]
GENRES = ["Action", "Comedy", "Drama", "Animation", "Family", "Sci-Fi", "Documentary", "Adventure"]
REGIONS = ["India", "USA", "UK", "Brazil", "Japan", "Germany", "Australia", "Canada"]
REGION_WEIGHTS = [30, 25, 10, 8, 8, 7, 6, 6]
DEVICES = ["Smart TV", "Mobile", "Laptop", "Tablet", "Web Browser"]
DEVICE_WEIGHTS = [35, 30, 15, 10, 10]
# Viewing peaks in the evening
HOUR_WEIGHTS = [2, 1, 1, 1, 1, 2, 3, 4, 4, 4, 5, 5, 6, 6, 6, 7, 8, 10, 14, 18, 20, 18, 10, 5]
TS_FMT = "%Y-%m-%d %H:%M:%S"


def build_content(rng: random.Random, n: int = 40) -> pd.DataFrame:
    combos = list(itertools.product(ADJECTIVES, NOUNS))
    rng.shuffle(combos)
    this_year = datetime.now().year
    rows = []
    for i, (adj, noun) in enumerate(combos[:n]):
        rows.append({
            "content_id": f"C{101 + i}",
            "content_title": f"{adj} {noun}",
            "genre": rng.choice(GENRES),
            "release_year": rng.randint(2015, this_year),
            "content_type": rng.choices(["Movie", "Series"], [60, 40])[0],
        })
    return pd.DataFrame(rows)


def make_session(start: datetime, rng: random.Random):
    """Return a list of (event_type, timestamp, playback_position) for one viewing session."""
    ts, pos = start, 0
    events = [("PLAY", ts, pos)]
    for _ in range(rng.choices([0, 1, 2, 3], [50, 30, 15, 5])[0]):
        watched = rng.randint(120, 1800)
        ts += timedelta(seconds=watched)
        pos += watched
        events.append(("PAUSE", ts, pos))
        ts += timedelta(seconds=rng.randint(30, 900))
        events.append((rng.choice(["RESUME", "PLAY"]), ts, pos))
    watched = rng.randint(120, 3600)
    ts += timedelta(seconds=watched)
    pos += watched
    ending = rng.choices(["COMPLETE", "STOP", None], [55, 35, 10])[0]  # None = no end event (incomplete)
    if ending:
        events.append((ending, ts, pos))
    return events


def build_clean_events(n_events: int, n_users: int, content: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    users = {}
    for i in range(n_users):
        users[f"U{1001 + i}"] = {
            "region": rng.choices(REGIONS, REGION_WEIGHTS)[0],
            "device": rng.choices(DEVICES, DEVICE_WEIGHTS)[0],
        }
    user_ids = list(users)
    content_ids = content["content_id"].tolist()
    popularity = [1 / (i + 1) ** 0.8 for i in range(len(content_ids))]  # a few titles dominate
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    rows = []
    while len(rows) < n_events:
        uid = rng.choice(user_ids)
        profile = users[uid]
        cid = rng.choices(content_ids, popularity)[0]
        device = profile["device"] if rng.random() < 0.7 else rng.choices(DEVICES, DEVICE_WEIGHTS)[0]
        day = today - timedelta(days=1 + rng.randint(0, 29))
        hour = rng.choices(range(24), HOUR_WEIGHTS)[0]
        start = day + timedelta(hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
        for et, ts, pos in make_session(start, rng):
            rows.append({
                "user_id": uid, "content_id": cid, "event_type": et,
                "event_timestamp": ts.strftime(TS_FMT),
                "device_type": device, "region": profile["region"], "playback_position": str(pos),
            })
    df = pd.DataFrame(rows).sort_values("event_timestamp", kind="stable").reset_index(drop=True)
    df.insert(0, "event_id", [f"E{i + 1:07d}" for i in range(len(df))])
    return df.astype(object)


def inject_faults(df: pd.DataFrame, rng: random.Random, seed: int):
    """Corrupt a controlled share of rows. Returns (dirty_df, manifest)."""
    n = len(df)
    df = df.copy()
    manifest = {}

    def pick(frac):
        return rng.sample(range(n), max(1, int(n * frac)))

    # --- fixable problems (normalisation should repair these) ---
    for i in pick(0.03):
        v = df.at[i, "event_type"]
        df.at[i, "event_type"] = rng.choice([v.lower(), f" {v} ", v.capitalize()])
    manifest["messy_event_type_fixable"] = int(n * 0.03)
    for i in pick(0.02):
        df.at[i, "content_id"] = df.at[i, "content_id"].lower()
    manifest["lowercase_content_id_fixable"] = int(n * 0.02)
    for col in ("region", "device_type"):
        for i in pick(0.01):
            df.at[i, col] = rng.choice(["", None])
    manifest["missing_region_device_filled_unknown"] = int(n * 0.01) * 2

    # --- rejectable problems ---
    for i in pick(0.01):
        df.at[i, "user_id"] = rng.choice(["", None])
    manifest["missing_user_id"] = int(n * 0.01)
    for i in pick(0.003):
        df.at[i, "event_id"] = rng.choice(["", None])
    manifest["missing_event_id"] = int(n * 0.003)
    for i in pick(0.01):
        df.at[i, "event_type"] = rng.choice(["SKIP", "PLAYY", "FORWARD", "UNKNOWN", "123"])
    manifest["invalid_event_type"] = int(n * 0.01)
    for i in pick(0.01):
        df.at[i, "content_id"] = "C999"
    manifest["unknown_content_id"] = int(n * 0.01)
    for i in pick(0.01):
        df.at[i, "event_timestamp"] = rng.choice(["not_a_time", "2026-13-45 25:61:00", "", "2099-01-01 00:00:00"])
    manifest["invalid_timestamp"] = int(n * 0.01)
    for i in pick(0.007):
        df.at[i, "playback_position"] = rng.choice(["-50", "abc", ""])
    manifest["invalid_playback_position"] = int(n * 0.007)

    # --- duplicate event ids ---
    dups = df.iloc[pick(0.02)].copy()
    manifest["duplicate_rows_added"] = len(dups)
    df = pd.concat([df, dups], ignore_index=True)
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)  # unordered arrival
    return df, manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=30000, help="approx. clean events before faults (10k-50k)")
    ap.add_argument("--users", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(args.events, args.users, args.seed)


def generate(events: int = 30000, users: int = 500, seed: int = 42) -> dict:
    rng = random.Random(seed)
    config.DATA_RAW.mkdir(parents=True, exist_ok=True)

    content = build_content(rng)
    clean = build_clean_events(events, users, content, rng)
    dirty, manifest = inject_faults(clean, rng, seed)

    for old in list(config.DATA_RAW.glob("streaming_events*")):
        old.unlink()
    cut = int(len(dirty) * 0.8)  # 80% CSV, 20% JSON -> exercises both ingestion paths
    dirty.iloc[:cut].to_csv(config.DATA_RAW / "streaming_events.csv", index=False)
    dirty.iloc[cut:].to_json(config.DATA_RAW / "streaming_events_2.json", orient="records", indent=1)
    content.to_csv(config.DATA_RAW / "content_metadata.csv", index=False)

    manifest.update({"clean_events": len(clean), "total_rows_written": len(dirty), "seed": seed})
    (config.DATA_RAW / "generation_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"Generated {len(dirty)} raw rows ({len(clean)} clean + injected faults) in {config.DATA_RAW}")
    return manifest


if __name__ == "__main__":
    main()
