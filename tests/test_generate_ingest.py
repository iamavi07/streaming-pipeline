"""Generator + ingestion smoke test (no Spark / DB needed)."""
import json

from src import config, generate_data, ingestion


def test_generate_and_ingest(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_RAW", tmp_path)
    manifest = generate_data.generate(events=2000, users=50, seed=1)
    assert manifest["duplicate_rows_added"] > 0
    events, report = ingestion.ingest_events(tmp_path)
    assert report["total_records_received"] == manifest["total_rows_written"]
    assert set(report["files"]) == {"streaming_events.csv", "streaming_events_2.json"}
    content = ingestion.ingest_content(tmp_path / "content_metadata.csv")
    assert len(content) == 40
    assert events["event_id"].duplicated().sum() > 0     # duplicates were injected
