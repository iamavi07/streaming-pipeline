# Media Streaming Data Engineering Pipeline
Python · Pandas · PySpark · PostgreSQL · SQL · Streamlit

An end-to-end pipeline for a Disney+-style streaming platform: it generates synthetic playback events
(with deliberate data-quality problems), ingests, cleans and validates them with PySpark, reconstructs
viewing sessions, loads PostgreSQL, answers business questions in SQL and shows them in a dashboard.
All data is fictional.

## Architecture
```
Synthetic generator (Python)  ->  data/raw/*.csv + *.json
        |
Ingestion (Pandas): file + column checks, ingestion report
        |
Cleaning / transformation (Pandas for metadata, PySpark for events)
        |
Data-quality validation (PySpark)  ->  valid events | rejected events (with reasons) | summary
        |
Session reconstruction (event-state model)
        |
PostgreSQL: content_metadata, streaming_events, viewing_sessions,
            rejected_events, data_quality_logs, viewing_analytics
        |
SQL analytics (sql/analytics_queries.sql)  ->  Streamlit dashboard
```

## What you need to install
| Software | Version | Why |
|---|---|---|
| Python | 3.9 – 3.11 (3.12 may work) | everything |
| Java (JDK) | 11 or 17 | PySpark runs on the JVM. Check: `java -version` |
| PostgreSQL | 14+ | database. Easiest via Docker (below) |
| Docker Desktop | optional | runs PostgreSQL with one command |
| Git | optional | version control / GitHub |

Python packages (`pip install -r requirements.txt`): pandas, numpy<2, pyspark 3.5.1, SQLAlchemy 2, psycopg2-binary, streamlit, pytest.

Windows: PySpark works best inside **WSL2** (Ubuntu). If you stay on native Windows, set `JAVA_HOME`
and expect to need Hadoop `winutils.exe` for some operations.

## Setup and run
```bash
# 1. Python environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. PostgreSQL (Docker)
docker compose up -d
#   credentials: streaming_user / streaming_pass, db streaming_db, port 5432
#   (no Docker? install PostgreSQL, then run:
#    CREATE USER streaming_user WITH PASSWORD 'streaming_pass';
#    CREATE DATABASE streaming_db OWNER streaming_user;)
#   Different credentials? export DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/db

# 3. Run the whole pipeline (generates data, validates, loads DB, prints analytics)
python -m src.pipeline --generate

# 4. Dashboard
streamlit run dashboard/app.py        # opens http://localhost:8501

# 5. Tests
pytest
```
Useful options: `--events 50000`, `--seed 7`, `--skip-db` (no PostgreSQL needed; writes files only),
`--reset-db` (drop and recreate tables). Run all SQL analytics from the terminal: `python -m src.analytics`.
You can also run the SQL in `sql/analytics_queries.sql` directly in psql/pgAdmin/DBeaver.

## Project layout
```
src/generate_data.py   Step 1  synthetic data + fault injection
src/ingestion.py       Step 2  CSV/JSON ingestion, checks, report
src/transform.py       Step 3  normalisation, timestamp parsing, metadata join (Pandas + PySpark)
src/quality.py         Step 4  validation rules, valid/rejected split, summary
src/sessions.py        1.7     session reconstruction state machine
src/db.py              Step 5  PostgreSQL loading
src/analytics.py       Step 6  named-query runner
src/pipeline.py        Step 8  orchestration + reconciliation checks
sql/                   schema, analytics queries, analytics-table build
dashboard/app.py       Step 7  Streamlit dashboard
tests/                 session logic, generator/ingestion, Spark validation rules
reports/               ingestion_report.json, data_quality_report.{json,md}
data/raw, data/processed   generated and processed datasets
```

## Data-quality rules
Rejected rows are saved (data/processed/rejected_events.csv and the `rejected_events` table) with their reasons.

| Rule | Meaning |
|---|---|
| NULL_EVENT_ID / NULL_USER_ID / NULL_CONTENT_ID | mandatory field missing or blank |
| UNKNOWN_CONTENT_ID | content_id not in metadata |
| INVALID_EVENT_TYPE | not in PLAY, PAUSE, RESUME, COMPLETE, STOP |
| INVALID_TIMESTAMP | unparseable, before 2020, or more than 1 day in the future |
| INVALID_PLAYBACK_POSITION | missing, non-numeric or negative |
| DUPLICATE_EVENT_ID | second and later occurrence of an event_id (first is kept) |

Repaired rather than rejected: whitespace/case in event_type, user_id and content_id; missing region/device become `Unknown`;
timestamps accepted as `yyyy-MM-dd HH:mm:ss` or `yyyy-MM-dd HH:mm`.
Row accounting is asserted at the end of every run: `received = valid + rejected`, and the DB row counts match.

## Session reconstruction (assumptions)
Events are grouped by (user_id, content_id) and ordered by time. Watch time only counts intervals where playback was running:
PLAY/RESUME -> PAUSE, PLAY/RESUME -> COMPLETE/STOP. Pause time is excluded (the 10:00-10:45 example gives 40 minutes).
Anything that does not fit is flagged, not hidden: no end event, a new PLAY while already playing, more than 4h silence while
playing (24h while paused) -> status `INCOMPLETE`; PAUSE/RESUME/COMPLETE/STOP without an open session -> orphan event
(`data/processed/orphan_events.csv`). Because rejected rows are removed before this step, a dropped PAUSE can cause an
INCOMPLETE session; that is expected and visible in the data-quality counts.
Definitions used in the analytics: **view** = one reconstructed session; **completion rate** = COMPLETED sessions / all sessions.

## Business questions -> queries
| Question | Query name |
|---|---|
| Most viewed content | `top_content_by_views`, `events_by_content` |
| Users per day | `daily_active_viewers` |
| Average viewing duration | `avg_viewing_duration` |
| Regions with most activity | `activity_by_region` |
| % sessions reaching completion | `completion_rate` |
| Popular viewing hours | `popular_hours` |
| Most used devices | `device_distribution` |
| Popular genres | `popular_genres` |

## Troubleshooting
- `JAVA_HOME is not set` / `Java gateway process exited`: install JDK 17 and set JAVA_HOME.
- `connection refused` on 5432: PostgreSQL is not running (`docker compose up -d`) or the port/credentials differ; set DATABASE_URL.
- `Python worker failed to connect back` (Spark): use Python 3.9–3.11 inside the venv, restart the terminal.
- Schema changed after editing sql/schema.sql: run with `--reset-db`.

## Ideas to extend
Add Airflow scheduling, partitioned Parquet output, incremental loads, dbt models, GitHub Actions running `pytest`.
