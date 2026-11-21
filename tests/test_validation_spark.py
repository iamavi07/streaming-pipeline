"""Spark validation test - skipped automatically when PySpark/Java is unavailable."""
import pandas as pd
import pytest

pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from src.transform import get_spark
    try:
        s = get_spark("tests")
        s.range(1).count()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Spark not runnable: {exc}")
    yield s
    s.stop()


def test_quality_rules(spark):
    from src.quality import apply_quality_checks, validation_summary
    from src.transform import normalize_and_enrich, to_spark_content, to_spark_events

    cols = ["event_id", "user_id", "content_id", "event_type", "event_timestamp",
            "device_type", "region", "playback_position", "source_file"]
    rows = [
        ["E1", "U1", "C101", "PLAY", "2026-11-20 10:00:00", "Smart TV", "India", "0", "f"],       # valid
        ["E2", "U1", "c101", " pause ", "2026-11-20 10:20", "Smart TV", "India", "1200", "f"],     # fixable -> valid
        ["E3", None, "C101", "PLAY", "2026-11-20 10:00:00", "Mobile", "India", "0", "f"],          # null user
        ["E4", "U2", "C101", "SKIP", "2026-11-20 10:00:00", "Mobile", "India", "0", "f"],          # bad type
        ["E5", "U2", "C999", "PLAY", "2026-11-20 10:00:00", "Mobile", "India", "0", "f"],          # unknown content
        ["E6", "U2", "C101", "PLAY", "garbage", "Mobile", "India", "0", "f"],                      # bad timestamp
        ["E7", "U2", "C101", "PLAY", "2026-11-20 10:00:00", "Mobile", "India", "-5", "f"],         # bad position
        ["E1", "U1", "C101", "PLAY", "2026-11-20 10:00:00", "Smart TV", "India", "0", "f"],        # duplicate of E1
    ]
    events = pd.DataFrame(rows, columns=cols)
    content = pd.DataFrame({"content_id": ["C101"], "content_title": ["A"], "genre": ["Action"],
                            "content_type": ["Movie"]})
    ev = apply_quality_checks(normalize_and_enrich(to_spark_events(spark, events), to_spark_content(spark, content)))
    s = validation_summary(ev)
    assert s["total_received"] == 8 and s["valid"] == 2 and s["rejected"] == 6
    for rule in ["NULL_USER_ID", "INVALID_EVENT_TYPE", "UNKNOWN_CONTENT_ID", "INVALID_TIMESTAMP",
                 "INVALID_PLAYBACK_POSITION", "DUPLICATE_EVENT_ID"]:
        assert s[rule] == 1, rule
