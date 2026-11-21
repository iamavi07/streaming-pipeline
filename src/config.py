"""Central configuration for the streaming pipeline."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "reports"
SQL_DIR = ROOT / "sql"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://streaming_user:streaming_pass@localhost:5432/streaming_db",
)

EVENT_COLUMNS = [
    "event_id", "user_id", "content_id", "event_type",
    "event_timestamp", "device_type", "region", "playback_position",
]
CONTENT_COLUMNS = ["content_id", "content_title", "genre", "release_year", "content_type"]

ALLOWED_EVENT_TYPES = ["PLAY", "PAUSE", "RESUME", "COMPLETE", "STOP"]

# Session reconstruction assumptions (see README, section "Session reconstruction")
MAX_PLAYING_GAP_SECONDS = 4 * 3600    # no event for 4h while PLAYING -> session abandoned
MAX_PAUSED_GAP_SECONDS = 24 * 3600    # no event for 24h while PAUSED  -> session abandoned
MIN_VALID_TIMESTAMP = "2020-01-01"
