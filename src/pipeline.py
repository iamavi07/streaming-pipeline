"""Orchestrates the full pipeline end to end.

  python -m src.pipeline --generate            # generate data + run everything
  python -m src.pipeline                       # reuse existing raw files
  python -m src.pipeline --skip-db             # no PostgreSQL (files + reports only)
  python -m src.pipeline --generate --reset-db # drop & recreate tables first
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime

import pandas as pd

from src import config
from src.generate_data import generate
from src.ingestion import ingest_content, ingest_events, write_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("pipeline")


def build_dq_rows(run_id, run_ts, summary, sessions, orphans, valid_count):
    rows = [(r, summary[r], summary["total_received"]) for r in
            ["NULL_EVENT_ID", "NULL_USER_ID", "NULL_CONTENT_ID", "UNKNOWN_CONTENT_ID", "INVALID_EVENT_TYPE",
             "INVALID_TIMESTAMP", "INVALID_PLAYBACK_POSITION", "DUPLICATE_EVENT_ID"]]
    rows.append(("OVERALL_REJECTED_ROWS", summary["rejected"], summary["total_received"]))
    rows.append(("ORPHAN_SESSION_EVENTS", len(orphans), valid_count))
    rows.append(("INCOMPLETE_SESSIONS", int((sessions["status"] == "INCOMPLETE").sum()), len(sessions)))
    return pd.DataFrame([{"run_id": run_id, "run_timestamp": run_ts, "check_name": n,
                          "records_failed": int(f), "records_total": int(t)} for n, f, t in rows])


def write_dq_report(run_id, summary, sessions, orphans):
    config.REPORTS.mkdir(parents=True, exist_ok=True)
    report = {"run_id": run_id, "validation": summary,
              "sessions": {"total": len(sessions), "by_status": sessions["status"].value_counts().to_dict(),
                           "orphan_events": len(orphans)}}
    (config.REPORTS / "data_quality_report.json").write_text(json.dumps(report, indent=2, default=str))
    lines = [f"# Data Quality Report (run {run_id})", "",
             f"- Records received: **{summary['total_received']}**",
             f"- Valid: **{summary['valid']}**",
             f"- Rejected: **{summary['rejected']}** ({summary['rejection_rate_pct']}%)", "",
             "| Rule | Rows failing |", "|---|---|"]
    from src.quality import RULES
    lines += [f"| {r} | {summary[r]} |" for r in RULES]
    lines += ["", "Note: one row can fail several rules, so rule counts can exceed the rejected total.",
              "", f"- Reconstructed sessions: {len(sessions)} {sessions['status'].value_counts().to_dict()}",
              f"- Orphan events (no open session): {len(orphans)}"]
    (config.REPORTS / "data_quality_report.md").write_text("\n".join(lines))


def run(args):
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    run_ts = datetime.now()

    # Step 1 - generate
    if args.generate or not (config.DATA_RAW / "content_metadata.csv").exists():
        log.info("STEP 1: generating synthetic data")
        generate(args.events, args.users, args.seed)

    # Step 2 - ingest
    log.info("STEP 2: ingesting raw files")
    events_raw, report = ingest_events()
    content_raw = ingest_content()
    report["content_records"] = len(content_raw)
    write_report(report)
    log.info("Received %s event records from %s", report["total_records_received"], list(report["files"]))

    # Steps 3 & 4 - clean, transform, validate (pandas + PySpark)
    log.info("STEP 3/4: cleaning and validating with PySpark")
    from src.quality import apply_quality_checks, split_valid_rejected, validation_summary
    from src.transform import clean_content_metadata, get_spark, normalize_and_enrich, to_spark_content, to_spark_events

    content = clean_content_metadata(content_raw)
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ev = normalize_and_enrich(to_spark_events(spark, events_raw), to_spark_content(spark, content))
        ev = apply_quality_checks(ev)
        summary = validation_summary(ev)
        valid_sdf, rejected_sdf = split_valid_rejected(ev)
        valid = valid_sdf.toPandas()
        rejected = rejected_sdf.toPandas()
    finally:
        spark.stop()
    log.info("Valid: %s | Rejected: %s (%s%%)", summary["valid"], summary["rejected"], summary["rejection_rate_pct"])

    # Session reconstruction
    from src.sessions import reconstruct_sessions
    sessions, orphans = reconstruct_sessions(valid)
    log.info("Reconstructed %s sessions (%s) and %s orphan events", len(sessions),
             sessions["status"].value_counts().to_dict(), len(orphans))

    # Save processed files + reports
    config.DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    valid.to_csv(config.DATA_PROCESSED / "valid_events.csv", index=False)
    rejected.to_csv(config.DATA_PROCESSED / "rejected_events.csv", index=False)
    sessions.to_csv(config.DATA_PROCESSED / "viewing_sessions.csv", index=False)
    orphans.to_csv(config.DATA_PROCESSED / "orphan_events.csv", index=False)
    write_dq_report(run_id, summary, sessions, orphans)
    dq_rows = build_dq_rows(run_id, run_ts, summary, sessions, orphans, len(valid))

    if args.skip_db:
        log.info("--skip-db set: skipping PostgreSQL load. Files are in %s", config.DATA_PROCESSED)
        return

    # Step 5 - PostgreSQL
    log.info("STEP 5: loading PostgreSQL")
    from src.db import get_engine, init_schema, load_processed, table_counts
    engine = get_engine()
    init_schema(engine, reset=args.reset_db)
    load_processed(engine, content, valid, sessions, rejected, dq_rows, run_id)
    counts = table_counts(engine)
    log.info("Table row counts: %s", counts)

    # Step 8 - reconciliation checks
    assert counts["streaming_events"] == summary["valid"], "valid rows != rows loaded"
    assert counts["rejected_events"] == summary["rejected"], "rejected rows != rows stored"
    assert summary["valid"] + summary["rejected"] == summary["total_received"], "row accounting mismatch"
    log.info("Reconciliation OK: received = valid + rejected, and DB matches")

    # Step 6 - analytics preview
    log.info("STEP 6: analytics preview")
    from src.analytics import run_query
    print(run_query(engine, "kpis").to_string(index=False))
    print(run_query(engine, "top_content_by_views").head(5).to_string(index=False))
    log.info("Done. Launch dashboard:  streamlit run dashboard/app.py")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--generate", action="store_true", help="(re)generate synthetic raw data first")
    ap.add_argument("--events", type=int, default=30000)
    ap.add_argument("--users", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--skip-db", action="store_true")
    ap.add_argument("--reset-db", action="store_true")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
