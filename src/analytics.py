"""Step 6 - Named SQL analytics queries, loaded from sql/analytics_queries.sql."""
from __future__ import annotations

import re

import pandas as pd
from sqlalchemy import text

from src import config


def load_queries(path=None) -> dict:
    path = path or config.SQL_DIR / "analytics_queries.sql"
    queries, name, buf = {}, None, []
    for line in path.read_text().splitlines():
        m = re.match(r"--\s*name:\s*(\w+)", line)
        if m:
            if name:
                queries[name] = "\n".join(buf).strip().rstrip(";")
            name, buf = m.group(1), []
        elif name:
            buf.append(line)
    if name:
        queries[name] = "\n".join(buf).strip().rstrip(";")
    return queries


def run_query(engine, name: str) -> pd.DataFrame:
    queries = load_queries()
    if name not in queries:
        raise KeyError(f"Unknown query '{name}'. Available: {sorted(queries)}")
    with engine.connect() as conn:
        return pd.read_sql(text(queries[name]), conn)


def print_all(engine):
    for name in load_queries():
        print(f"\n=== {name} ===")
        print(run_query(engine, name).to_string(index=False))


if __name__ == "__main__":
    from src.db import get_engine
    print_all(get_engine())
