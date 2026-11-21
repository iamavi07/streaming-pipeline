"""Step 3 - Cleaning & transformation (Pandas for metadata, PySpark for events)."""
from __future__ import annotations

import os
import sys

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src import config


def get_spark(app_name: str = "streaming-pipeline") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.master("local[*]").appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")                    # bad casts -> NULL instead of crash
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")     # bad timestamps -> NULL
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def clean_content_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Pandas cleaning of the (small) content metadata table."""
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].astype("string").str.strip()
    out["content_id"] = out["content_id"].str.upper()
    out = out[out["content_id"].notna() & (out["content_id"] != "")]
    out = out.drop_duplicates("content_id", keep="first")
    out["release_year"] = pd.to_numeric(out["release_year"], errors="coerce").astype("Int64")
    return out.reset_index(drop=True)


def _blank_to_null(col: str):
    t = F.trim(F.col(col))
    return F.when(t == "", F.lit(None)).otherwise(t)


def to_spark_events(spark: SparkSession, events: pd.DataFrame):
    cols = config.EVENT_COLUMNS + ["source_file"]
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    clean = events[cols].astype(object)
    clean = clean.where(clean.notna(), None)
    return spark.createDataFrame(clean, schema=schema)


def to_spark_content(spark: SparkSession, content: pd.DataFrame):
    cols = ["content_id", "content_title", "genre", "content_type"]
    schema = StructType([StructField(c, StringType(), True) for c in cols])
    sub = content[cols].astype(object)
    sub = sub.where(sub.notna(), None)
    return spark.createDataFrame(sub, schema=schema)


def normalize_and_enrich(events_sdf, content_sdf):
    """Normalise text fields, parse timestamps/positions, derive event_date/hour, join metadata.
    Raw timestamp/position strings are kept so rejected rows still show the original value."""
    ev = events_sdf.withColumn("_row_id", F.monotonically_increasing_id())

    for c in ["event_id", "user_id", "content_id", "event_type", "event_timestamp",
              "device_type", "region", "playback_position"]:
        ev = ev.withColumn(c, _blank_to_null(c))

    ev = (
        ev.withColumn("event_type", F.upper("event_type"))
          .withColumn("content_id", F.upper("content_id"))
          .withColumn("user_id", F.upper("user_id"))
          .withColumn("device_type", F.coalesce(F.col("device_type"), F.lit("Unknown")))
          .withColumn("region", F.coalesce(F.col("region"), F.lit("Unknown")))
    )

    ts = F.coalesce(
        F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm"),
    )
    ev = (
        ev.withColumn("_ts", ts)
          .withColumn("_pos", F.col("playback_position").cast("long"))
          .withColumn("event_date", F.to_date("_ts"))
          .withColumn("event_hour", F.hour("_ts"))
    )

    meta = content_sdf.select(
        F.col("content_id").alias("_meta_content_id"), "content_title", "genre", "content_type"
    )
    return ev.join(F.broadcast(meta), ev["content_id"] == meta["_meta_content_id"], "left")
