"""Step 4 - Data quality validation. Invalid rows go to a rejected dataset (never silently dropped)."""
from __future__ import annotations

from pyspark.sql import Window, functions as F

from src import config

RULES = [
    "NULL_EVENT_ID",
    "NULL_USER_ID",
    "NULL_CONTENT_ID",
    "UNKNOWN_CONTENT_ID",
    "INVALID_EVENT_TYPE",
    "INVALID_TIMESTAMP",
    "INVALID_PLAYBACK_POSITION",
    "DUPLICATE_EVENT_ID",
]


def apply_quality_checks(ev):
    """Adds a `rejection_reasons` column (';'-separated rule names, '' = valid)."""
    w = Window.partitionBy("event_id").orderBy("_row_id")
    ev = ev.withColumn(
        "_dup_rank", F.when(F.col("event_id").isNotNull(), F.row_number().over(w))
    )  # first occurrence kept, later ones rejected

    reasons = F.concat_ws(
        ";",
        F.when(F.col("event_id").isNull(), F.lit("NULL_EVENT_ID")),
        F.when(F.col("user_id").isNull(), F.lit("NULL_USER_ID")),
        F.when(F.col("content_id").isNull(), F.lit("NULL_CONTENT_ID")),
        F.when(F.col("content_id").isNotNull() & F.col("_meta_content_id").isNull(), F.lit("UNKNOWN_CONTENT_ID")),
        F.when(F.col("event_type").isNull() | ~F.col("event_type").isin(*config.ALLOWED_EVENT_TYPES),
               F.lit("INVALID_EVENT_TYPE")),
        F.when(
            F.col("_ts").isNull()
            | (F.col("_ts") > F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
            | (F.col("_ts") < F.lit(config.MIN_VALID_TIMESTAMP).cast("timestamp")),
            F.lit("INVALID_TIMESTAMP"),
        ),
        F.when(F.col("_pos").isNull() | (F.col("_pos") < 0), F.lit("INVALID_PLAYBACK_POSITION")),
        F.when(F.col("_dup_rank") > 1, F.lit("DUPLICATE_EVENT_ID")),
    )
    return ev.withColumn("rejection_reasons", reasons).cache()


def split_valid_rejected(ev):
    valid = ev.filter(F.col("rejection_reasons") == "").select(
        "event_id", "user_id", "content_id", "event_type",
        F.col("_ts").alias("event_timestamp"), "event_date", "event_hour",
        "device_type", "region", F.col("_pos").alias("playback_position"),
        "content_title", "genre", "content_type",
    )
    rejected = ev.filter(F.col("rejection_reasons") != "").select(
        "event_id", "user_id", "content_id", "event_type", "event_timestamp",
        "device_type", "region", "playback_position", "source_file", "rejection_reasons",
    )
    return valid, rejected


def validation_summary(ev) -> dict:
    """One pass over the data: total, valid, rejected and the count per rule."""
    split = F.split(F.col("rejection_reasons"), ";")
    row = ev.agg(
        F.count(F.lit(1)).alias("total_received"),
        F.sum(F.when(F.col("rejection_reasons") == "", 1).otherwise(0)).alias("valid"),
        *[F.sum(F.when(F.array_contains(split, r), 1).otherwise(0)).alias(r) for r in RULES],
    ).collect()[0].asDict()
    summary = {k: int(v or 0) for k, v in row.items()}
    summary["rejected"] = summary["total_received"] - summary["valid"]
    summary["rejection_rate_pct"] = round(100 * summary["rejected"] / max(summary["total_received"], 1), 2)
    return summary
