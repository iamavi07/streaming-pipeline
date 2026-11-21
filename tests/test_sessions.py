"""Pure-pandas tests of the session reconstruction logic (no Spark / DB needed)."""
import pandas as pd

from src.sessions import reconstruct_sessions


def _events(rows):
    df = pd.DataFrame(rows, columns=["event_id", "user_id", "content_id", "event_type", "ts"])
    df["event_timestamp"] = pd.to_datetime("2026-11-20 " + df["ts"])
    df["region"], df["device_type"] = "India", "Smart TV"
    return df.drop(columns="ts")


def test_spec_example_is_40_minutes():
    ev = _events([
        ("E1", "U101", "C101", "PLAY", "10:00"), ("E2", "U101", "C101", "PAUSE", "10:20"),
        ("E3", "U101", "C101", "PLAY", "10:25"), ("E4", "U101", "C101", "COMPLETE", "10:45"),
    ])
    s, o = reconstruct_sessions(ev)
    assert len(s) == 1 and o.empty
    assert s.loc[0, "watch_seconds"] == 40 * 60      # 5-minute pause excluded
    assert s.loc[0, "status"] == "COMPLETED"


def test_stop_and_resume_event():
    ev = _events([
        ("E1", "U1", "C1", "PLAY", "10:00"), ("E2", "U1", "C1", "PAUSE", "10:10"),
        ("E3", "U1", "C1", "RESUME", "10:30"), ("E4", "U1", "C1", "STOP", "10:35"),
    ])
    s, _ = reconstruct_sessions(ev)
    assert s.loc[0, "watch_seconds"] == 15 * 60 and s.loc[0, "status"] == "STOPPED"


def test_missing_end_event_is_incomplete():
    ev = _events([("E1", "U1", "C1", "PLAY", "10:00"), ("E2", "U1", "C1", "PAUSE", "10:10")])
    s, _ = reconstruct_sessions(ev)
    assert s.loc[0, "status"] == "INCOMPLETE" and s.loc[0, "watch_seconds"] == 600


def test_play_while_playing_closes_previous_as_incomplete():
    ev = _events([("E1", "U1", "C1", "PLAY", "10:00"), ("E2", "U1", "C1", "PLAY", "10:30"),
                  ("E3", "U1", "C1", "COMPLETE", "10:40")])
    s, _ = reconstruct_sessions(ev)
    assert list(s["status"]) == ["INCOMPLETE", "COMPLETED"]
    assert s.loc[0, "issue"] == "PLAY_WHILE_PLAYING"


def test_orphan_events_reported():
    ev = _events([("E1", "U1", "C1", "PAUSE", "10:00"), ("E2", "U1", "C1", "COMPLETE", "10:05")])
    s, o = reconstruct_sessions(ev)
    assert s.empty and len(o) == 2


def test_different_users_are_separate_sessions():
    ev = _events([("E1", "U1", "C1", "PLAY", "10:00"), ("E2", "U2", "C1", "PLAY", "10:01"),
                  ("E3", "U1", "C1", "COMPLETE", "10:30"), ("E4", "U2", "C1", "STOP", "10:11")])
    s, _ = reconstruct_sessions(ev)
    assert len(s) == 2 and sorted(s["watch_seconds"]) == [600, 1800]
