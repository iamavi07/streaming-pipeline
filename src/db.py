"""Step 5 - PostgreSQL connectivity and loading."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text

from src import config

LOAD_TABLES = "streaming_events, viewing_sessions, viewing_analytics, rejected_events, content_metadata"
DROP_ORDER = ["viewing_analytics", "viewing_sessions", "streaming_events", "rejected_events",
              "data_quality_logs", "content_metadata"]


def get_engine():
    return create_engine(config.DATABASE_URL, pool_pre_ping=True)


def run_sql_file(engine, path):
    """Execute a .sql file (statements separated by ';', no procedural blocks)."""
    sql = path.read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)


def init_schema(engine, reset: bool = False):
    if reset:
        with engine.begin() as conn:
            for t in DROP_ORDER:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {t} CASCADE")
    run_sql_file(engine, config.SQL_DIR / "schema.sql")


def _nullable(df: pd.DataFrame) -> pd.DataFrame:
    out = df.astype(object)
    return out.where(out.notna(), None)


def _append(conn, df: pd.DataFrame, table: str):
    _nullable(df).to_sql(table, conn, if_exists="append", index=False, method="multi", chunksize=1000)


def load_processed(engine, content, events, sessions, rejected, dq_rows, run_id):
    """Full refresh of base tables (idempotent). data_quality_logs keeps history across runs."""
    rejected = rejected.copy()
    rejected.insert(0, "run_id", run_id)
    events_cols = ["event_id", "user_id", "content_id", "event_type", "event_timestamp", "event_date",
                   "event_hour", "device_type", "region", "playback_position"]
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE {LOAD_TABLES} RESTART IDENTITY CASCADE")
        _append(conn, content, "content_metadata")
        _append(conn, events[events_cols], "streaming_events")
        _append(conn, sessions, "viewing_sessions")
        _append(conn, rejected, "rejected_events")
        _append(conn, dq_rows, "data_quality_logs")
    run_sql_file(engine, config.SQL_DIR / "build_viewing_analytics.sql")


def table_counts(engine) -> dict:
    out = {}
    with engine.connect() as conn:
        for t in ["content_metadata", "streaming_events", "viewing_sessions",
                  "rejected_events", "data_quality_logs", "viewing_analytics"]:
            out[t] = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
    return out
