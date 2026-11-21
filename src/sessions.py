"""Session reconstruction - a small event-state model (pure pandas, no Spark/DB needed).

State machine per (user_id, content_id), events ordered by timestamp:

  IDLE    --PLAY-->            PLAYING   (start session)
  PLAYING --PAUSE-->           PAUSED    (watch time += now - last_event)
  PAUSED  --PLAY | RESUME-->   PLAYING   (pause time is NOT counted)
  PLAYING --COMPLETE-->        closed COMPLETED (watch time += now - last_event)
  PLAYING --STOP-->            closed STOPPED   (watch time += now - last_event)
  PAUSED  --COMPLETE | STOP--> closed COMPLETED / STOPPED (no extra watch time)

Malformed sequences are flagged, never silently "fixed":
  * PLAY while already PLAYING          -> previous session closed INCOMPLETE, new one starts
  * silence longer than the allowed gap -> session closed INCOMPLETE
  * stream ends without COMPLETE/STOP   -> INCOMPLETE
  * PAUSE/RESUME/COMPLETE/STOP with no open session -> orphan event
Watch time of an INCOMPLETE session only includes intervals properly closed by a PAUSE.
"""
from __future__ import annotations

import pandas as pd

from src import config

_END_STATUS = {"COMPLETE": "COMPLETED", "STOP": "STOPPED"}
SESSION_COLUMNS = ["session_id", "user_id", "content_id", "start_ts", "end_ts", "watch_seconds",
                   "event_count", "status", "region", "device_type", "issue"]


def _new(row) -> dict:
    return {
        "user_id": row.user_id, "content_id": row.content_id,
        "start_ts": row.event_timestamp, "end_ts": row.event_timestamp, "last_ts": row.event_timestamp,
        "watch_seconds": 0.0, "event_count": 1, "state": "PLAYING",
        "region": row.region, "device_type": row.device_type, "issue": "",
    }


def reconstruct_sessions(events: pd.DataFrame):
    """events needs: event_id, user_id, content_id, event_type, event_timestamp (datetime),
    region, device_type.  Returns (sessions_df, orphan_events_df)."""
    df = events.sort_values(["user_id", "content_id", "event_timestamp", "event_id"]).reset_index(drop=True)
    sessions, orphans = [], []
    cur = None

    def finish(status, issue=""):
        nonlocal cur
        cur["status"], cur["issue"], cur["end_ts"] = status, issue, cur["last_ts"]
        sessions.append(cur)
        cur = None

    for row in df.itertuples(index=False):
        et, ts = row.event_type, row.event_timestamp

        if cur is not None and (cur["user_id"], cur["content_id"]) != (row.user_id, row.content_id):
            finish("INCOMPLETE", "NO_END_EVENT")

        if cur is not None:
            gap = (ts - cur["last_ts"]).total_seconds()
            limit = config.MAX_PLAYING_GAP_SECONDS if cur["state"] == "PLAYING" else config.MAX_PAUSED_GAP_SECONDS
            if gap > limit:
                finish("INCOMPLETE", "GAP_EXCEEDS_LIMIT")

        def orphan(reason):
            orphans.append({"event_id": row.event_id, "user_id": row.user_id, "content_id": row.content_id,
                            "event_type": et, "event_timestamp": ts, "issue": reason})

        if et == "PLAY":
            if cur is None:
                cur = _new(row)
            elif cur["state"] == "PAUSED":
                cur["state"], cur["last_ts"] = "PLAYING", ts
                cur["event_count"] += 1
            else:  # PLAY while PLAYING
                finish("INCOMPLETE", "PLAY_WHILE_PLAYING")
                cur = _new(row)
        elif et == "RESUME":
            if cur is not None and cur["state"] == "PAUSED":
                cur["state"], cur["last_ts"] = "PLAYING", ts
                cur["event_count"] += 1
            else:
                orphan("RESUME_WITHOUT_PAUSE")
        elif et == "PAUSE":
            if cur is not None and cur["state"] == "PLAYING":
                cur["watch_seconds"] += (ts - cur["last_ts"]).total_seconds()
                cur["state"], cur["last_ts"] = "PAUSED", ts
                cur["event_count"] += 1
            else:
                orphan("PAUSE_WITHOUT_PLAY")
        elif et in _END_STATUS:
            if cur is not None:
                if cur["state"] == "PLAYING":
                    cur["watch_seconds"] += (ts - cur["last_ts"]).total_seconds()
                cur["last_ts"] = ts
                cur["event_count"] += 1
                finish(_END_STATUS[et])
            else:
                orphan(f"{et}_WITHOUT_PLAY")
        else:
            orphan("UNKNOWN_EVENT_TYPE")

    if cur is not None:
        finish("INCOMPLETE", "NO_END_EVENT")

    sdf = pd.DataFrame(sessions)
    if sdf.empty:
        sdf = pd.DataFrame(columns=SESSION_COLUMNS)
    else:
        sdf = sdf.sort_values("start_ts", kind="stable").reset_index(drop=True)
        sdf["session_id"] = [f"S{i + 1:07d}" for i in range(len(sdf))]
        sdf["watch_seconds"] = sdf["watch_seconds"].round().astype(int)
        sdf = sdf[SESSION_COLUMNS]
    odf = pd.DataFrame(orphans, columns=["event_id", "user_id", "content_id", "event_type",
                                         "event_timestamp", "issue"])
    return sdf, odf
