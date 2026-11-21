# Streaming Analytics Pipeline

A data engineering project that takes messy playback events from a Disney+-style streaming service and turns them into trustworthy numbers: top titles, daily active viewers, watch time, completion rate and more.

It covers the whole path. A Python script generates raw event files, PySpark cleans and validates them, a small state machine rebuilds viewing sessions, PostgreSQL stores the results, SQL answers the business questions, and a Streamlit dashboard shows them.

Built with Python, Pandas, PySpark, PostgreSQL, SQL and Streamlit. All data is synthetic. No real customer data is involved.

![Dashboard overview](docs/screenshots/content.png)

---

## Why I built this

Every product with a play button produces event data, and that data is never clean. Users go missing, events arrive twice, timestamps are garbage, and someone sends `"play "` instead of `PLAY`. If you aggregate it as-is, the dashboard looks fine and the numbers are wrong.

I wanted a project where the interesting part is not the charts but what happens before them: deciding what counts as a valid event, keeping track of what was thrown away and why, and defining "watch time" in a way I can defend.

## What it does

1. **Generates** about 30,000 raw events across 500 users and 40 titles. A controlled set of problems is injected on purpose: duplicate event IDs, missing user IDs, invalid event types, unknown content IDs, bad timestamps, negative playback positions, plus messy-but-fixable values like lowercase IDs and padded strings. About 80% of the data lands in a CSV file and 20% in a JSON file, so both ingestion paths get used.
2. **Ingests** the files, checks that they exist and have the expected columns, and writes an ingestion report.
3. **Cleans and validates** with PySpark. Fixable problems are repaired. Everything else is rejected with a named reason and kept, not silently dropped.
4. **Reconstructs viewing sessions** from the valid events (more on this below).
5. **Loads PostgreSQL** and checks that the numbers reconcile.
6. **Answers business questions in SQL** (14 named queries).
7. **Shows it all in Streamlit**, including a data quality tab.

```
 Synthetic generator ──► data/raw/*.csv, *.json
          │
 Ingestion (Pandas) ──► file and column checks, ingestion report
          │
 Cleaning + validation (PySpark)
          │            ├─► valid events
          │            └─► rejected events, each with a reason
          │
 Session reconstruction (event-state model)
          │
 PostgreSQL ──► content_metadata, streaming_events, viewing_sessions,
          │     rejected_events, data_quality_logs, viewing_analytics
          │
 SQL analytics ──► Streamlit dashboard
```

## Results from a default run

These come from running `python -m src.pipeline --generate` with the default settings.

| | |
|---|---|
| Raw rows received | 30,603 |
| Valid events loaded | 28,537 |
| Rejected events | 2,066 (6.75%) |
| Sessions reconstructed | 8,554 |
| Sessions completed / stopped / incomplete | 4,372 / 2,844 / 1,338 |
| Orphan events (no matching session) | 923 |
| Completion rate | 51.11% |
| Average watch time per session | 37.03 minutes |

The generator is designed so the analysis has something to find: a few titles dominate (the top title gets 1,401 views against 240 for tenth place), viewing peaks around 8 to 9 PM, and India and the USA are the largest regions. If the dashboard shows that, the pipeline is working end to end.

Exact numbers will differ slightly between runs because dates are generated relative to today.

## Dashboard

| Content | Trends |
|---|---|
| ![Content](docs/screenshots/content.png) | ![Trends](docs/screenshots/trends.png) |

| Audience | Data quality |
|---|---|
| ![Audience](docs/screenshots/audience.png) | ![Data quality](docs/screenshots/data-quality.png) |

## The interesting parts

### Data quality: reject, don't delete

Every row that fails a rule goes to a separate rejected dataset along with the reason it failed. Nothing disappears. A row can fail several rules at once, so the per-rule counts add up to more than the total rejected.

| Rule | What it catches | Rows in default run |
|---|---|---|
| `NULL_EVENT_ID` | missing or blank event ID | 93 |
| `NULL_USER_ID` | missing or blank user ID | 305 |
| `NULL_CONTENT_ID` | missing or blank content ID | 0 |
| `UNKNOWN_CONTENT_ID` | content ID not present in the metadata | 309 |
| `INVALID_EVENT_TYPE` | anything outside PLAY, PAUSE, RESUME, COMPLETE, STOP | 308 |
| `INVALID_TIMESTAMP` | unparseable, before 2020, or more than a day in the future | 308 |
| `INVALID_PLAYBACK_POSITION` | missing, non-numeric or negative | 215 |
| `DUPLICATE_EVENT_ID` | second and later occurrence of an event ID (the first is kept) | 597 |

Some problems are repaired instead of rejected, because throwing away a good event over a stray space would be wasteful: whitespace and casing in event type, user ID and content ID are normalised, missing region or device becomes `Unknown`, and timestamps are accepted with or without seconds.

At the end of each run the pipeline asserts that `received = valid + rejected` and that the row counts in PostgreSQL match. If either check fails, the run fails.

### Watch time is not "end minus start"

Subtracting the first timestamp from the last one counts pauses as viewing. So sessions are rebuilt per user and title with a small state machine:

- `PLAY` starts a session, `PAUSE` stops the clock, `PLAY` or `RESUME` starts it again
- `COMPLETE` or `STOP` closes the session
- only the time between "playing" and the next pause or end counts

The example from the spec: PLAY at 10:00, PAUSE at 10:20, PLAY at 10:25, COMPLETE at 10:45 gives 40 minutes. The 5-minute pause is excluded. This is covered by a test.

Real data does not always behave, so malformed sequences are labelled rather than quietly fixed:

- no end event, a second PLAY while already playing, or a long silence (4 hours while playing, 24 hours while paused) makes the session `INCOMPLETE`
- a PAUSE, RESUME, COMPLETE or STOP with no open session is stored as an orphan event

Because invalid rows are removed before this step, a rejected PAUSE can leave a session incomplete. That is expected, and it shows up in the data quality numbers.

**Definitions used in the analytics.** A *view* is one reconstructed session. *Completion rate* is completed sessions divided by all sessions.

## Tech stack and why

| Tool | Role |
|---|---|
| Python | generator, orchestration, session logic |
| Pandas | ingestion and cleaning of the small metadata table |
| PySpark | event normalisation, validation and the metadata join |
| PostgreSQL | storage for events, sessions, rejected rows and quality logs |
| SQL | all analytics |
| Streamlit | dashboard |
| pytest | tests |

A fair question is why PySpark for 30,000 rows. You would not need it at this size. I used it to write the validation as scalable DataFrame logic (window functions for duplicate detection, a broadcast join for metadata) that would carry over to much larger volumes unchanged. The pipeline runs Spark in local mode.

## Project structure

```
src/
  generate_data.py   synthetic data and fault injection
  ingestion.py       CSV and JSON loading, structure checks, report
  transform.py       normalisation, timestamp parsing, metadata join
  quality.py         validation rules, valid/rejected split, summary
  sessions.py        session reconstruction state machine
  db.py              PostgreSQL schema setup and loading
  analytics.py       runs the named SQL queries
  pipeline.py        runs everything end to end
sql/
  schema.sql                   table definitions
  build_viewing_analytics.sql  daily per-title aggregate table
  analytics_queries.sql        the 14 analytical queries
dashboard/app.py     Streamlit app
tests/               session logic, generator and ingestion, Spark validation
reports/             ingestion report and data quality report (generated)
data/                raw and processed files (generated, not committed)
```

## Running it yourself

### What you need

- Python 3.9 to 3.11. Python 3.12 and newer will give you trouble with this PySpark version, and 3.14 will fail to install pandas.
- Java 11 or 17 (`java -version` to check). PySpark needs it.
- PostgreSQL 14 or newer.
- On Windows, use WSL2 with Ubuntu. PySpark is much smoother there.

### Setup

```bash
git clone https://github.com/iamavi07/streaming-pipeline.git
cd streaming-pipeline

python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If your system Python is too new, [uv](https://docs.astral.sh/uv/) is the easiest way to get 3.11: `uv venv --python 3.11 .venv`, then `uv pip install -r requirements.txt`.

### Database

With Docker:

```bash
docker compose up -d
```

Without Docker (Ubuntu or WSL):

```bash
sudo apt install postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres psql -c "CREATE USER streaming_user WITH PASSWORD 'streaming_pass';"
sudo -u postgres psql -c "CREATE DATABASE streaming_db OWNER streaming_user;"
```

The defaults are `streaming_user` / `streaming_pass` on `localhost:5432`, database `streaming_db`. To use different credentials, set `DATABASE_URL`:

```bash
export DATABASE_URL=postgresql+psycopg2://user:password@host:5432/dbname
```

### Run

```bash
python -m src.pipeline --generate     # generate data, validate, load, print a summary
streamlit run dashboard/app.py        # open http://localhost:8501
pytest                                # run the tests
```

Useful options for the pipeline:

| Flag | Effect |
|---|---|
| `--events 50000` | change the data volume |
| `--seed 7` | change the random seed |
| `--skip-db` | run without PostgreSQL and write files only |
| `--reset-db` | drop and recreate all tables first |

To print every analytical query result in the terminal: `python -m src.analytics`. The queries also run as plain SQL in psql, pgAdmin or DBeaver (`sql/analytics_queries.sql`).

## Business questions and where they are answered

| Question | Query |
|---|---|
| Which titles are watched the most? | `top_content_by_views`, `events_by_content` |
| How many users watch per day? | `daily_active_viewers` |
| What is the average viewing duration? | `avg_viewing_duration` |
| Which regions are most active? | `activity_by_region` |
| What share of sessions reach completion? | `completion_rate` |
| When do people watch? | `popular_hours` |
| Which devices are used most? | `device_distribution` |
| Which genres are most popular? | `popular_genres` |

## Tests

Eight tests cover the parts most likely to be wrong:

- session reconstruction: the 40-minute example, resume events, missing end events, a PLAY while already playing, orphan events, and separate users
- generator and ingestion: both file formats load and duplicates are present
- validation: a hand-built set of rows where each rule should fire exactly once

The Spark test is skipped automatically on machines without a working Java setup.

## Troubleshooting

| Problem | Fix |
|---|---|
| `JAVA_HOME is not set` or `Java gateway process exited` | Install JDK 17 and set `JAVA_HOME`. |
| `connection refused` on port 5432 | PostgreSQL is not running. Start it, or check `DATABASE_URL`. |
| pip tries to compile pandas and fails | Your Python is too new. Use 3.11. |
| `pyarrow requires NumPy 2.0` in the dashboard | Run `pip install "pyarrow<19"` (already in `requirements.txt`). |
| Tables out of date after editing the schema | Run the pipeline once with `--reset-db`. |

## Limitations

Being upfront about what this is not:

- The data is synthetic and the faults are ones I chose, so the rejection rate says more about the generator than about real traffic.
- It is a batch pipeline. Each run does a full refresh of the base tables. There is no streaming, scheduling or incremental loading.
- Session reconstruction uses simple, documented rules. Real playback telemetry has more edge cases (multiple devices at once, clock skew, offline viewing).
- Spark runs locally on one machine.

## What I would add next

- Scheduling with Airflow and incremental loads instead of full refreshes
- Partitioned Parquet output for the processed data
- A GitHub Actions workflow to run the tests on every push
- A lightweight data quality alert when the rejection rate jumps between runs
