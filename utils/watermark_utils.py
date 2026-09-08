# utils/watermark_utils.py
# ===========================================================================
# Watermark control mechanism.
#
# Core guarantees, unchanged: the watermark moves forward only, and a failed
# run leaves it exactly where it was.
#
# Two corrections over the naive version of this pattern:
#
#   1. update_watermark() takes the batch maximum and row count as plain
#      values instead of a DataFrame. Passing a lazy JDBC DataFrame here
#      meant isEmpty(), agg(max) and count() each re-executed the source
#      query — so a "single" incremental pull hit the shared production
#      server several times per table per run. The caller now materialises
#      the batch once and passes the numbers down.
#
#   2. get_watermark() supports a lag. Polling on update_date has a blind
#      spot: if the source assigns update_date at transaction start but the
#      commit lands after our read, those rows are already below the
#      watermark by the time we look again and are never picked up.
#      Re-reading a small overlap window costs almost nothing and is safe,
#      because the dbt staging layer deduplicates by row_number() anyway.
# ===========================================================================

from datetime import datetime, timedelta
from typing import Union

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S"
SEED_WATERMARK = datetime(1900, 1, 1, 0, 0, 0)

CONTROL_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("last_watermark", TimestampType(), True),
        StructField("rows_processed", IntegerType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("run_status", StringType(), True),
    ]
)


def _read_current(spark: SparkSession, control_path: str, table_name: str):
    rows = (
        spark.read.format("delta")
        .load(control_path)
        .filter(F.col("table_name") == table_name)
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return rows[0] if rows else None


def _merge(spark: SparkSession, control_path: str, row: tuple) -> None:
    control_table = DeltaTable.forPath(spark, control_path)
    update_df = spark.createDataFrame([row], schema=CONTROL_SCHEMA)

    (
        control_table.alias("ctrl")
        .merge(update_df.alias("new"), "ctrl.table_name = new.table_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def get_watermark(
    spark: SparkSession,
    control_path: str,
    table_name: str,
    lag_minutes: int = 0,
) -> str:
    """
    Read the last successfully processed watermark for a source table,
    optionally rewound by lag_minutes to re-read a safety overlap.
    """
    row = _read_current(spark, control_path, table_name)
    watermark = row["last_watermark"] if row and row["last_watermark"] else SEED_WATERMARK

    if lag_minutes:
        watermark = max(watermark - timedelta(minutes=lag_minutes), SEED_WATERMARK)

    return watermark.strftime(WATERMARK_FORMAT)


def update_watermark(
    spark: SparkSession,
    control_path: str,
    table_name: str,
    batch_max_watermark: Union[datetime, None],
    row_count: int,
    status: str = "success",
) -> None:
    """
    Advance the watermark to the batch maximum — forward only, and only for
    a non-empty batch. Takes plain values, not a DataFrame: the caller is
    responsible for materialising the batch exactly once.
    """
    if not row_count or batch_max_watermark is None:
        print(f"[watermark] {table_name}: empty batch, watermark unchanged")
        return

    row = _read_current(spark, control_path, table_name)
    current = row["last_watermark"] if row and row["last_watermark"] else SEED_WATERMARK

    if batch_max_watermark < current:
        print(
            f"[watermark] {table_name}: WARNING — batch max ({batch_max_watermark}) "
            f"is older than the current watermark ({current}). Held in place."
        )

    new_watermark = max(batch_max_watermark, current)

    _merge(
        spark,
        control_path,
        (table_name, new_watermark, int(row_count), datetime.now(), status),
    )

    print(f"[watermark] {table_name}: advanced to {new_watermark} ({row_count:,} rows)")


def mark_failed(spark: SparkSession, control_path: str, table_name: str, error_msg: str) -> None:
    """Record a failed run without advancing the watermark."""
    row = _read_current(spark, control_path, table_name)
    current = row["last_watermark"] if row and row["last_watermark"] else SEED_WATERMARK

    _merge(
        spark,
        control_path,
        (table_name, current, 0, datetime.now(), f"failed: {error_msg[:200]}"),
    )

    print(f"[watermark] {table_name}: FAILED — watermark unchanged at {current}")
