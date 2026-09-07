# utils/ingest.py
# ===========================================================================
# Shared ingestion routine for the Bronze notebooks.
#
# The four bronze notebooks were previously copies of each other, which
# meant a fix to the materialisation logic had to be applied four times.
# They now differ only in configuration and call this.
#
# The important detail here is the persist() + single aggregate pass. The
# JDBC DataFrame is lazy: every action on it re-runs the query against the
# source. Counting, writing and then deriving the batch maximum separately
# meant several full pulls of the same window per table per run — the exact
# load on the shared server this pipeline exists to reduce. Caching first,
# then deriving the count and the maximum in one aggregation, brings it
# back to a single execution.
#
# Caching also makes ingest_ts stable. current_timestamp() is
# non-deterministic, so on an uncached DataFrame the value written to disk
# is not necessarily the value that was counted.
# ===========================================================================

from typing import Optional

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.jdbc_config import read_jdbc_incremental
from utils.watermark_utils import get_watermark, mark_failed, update_watermark

# Overlap re-read, in minutes. Covers rows whose update_date was stamped at
# transaction start but committed after the previous run's read. Safe to
# widen: the dbt staging layer deduplicates on the primary key.
WATERMARK_LAG_MINUTES = 15

BRONZE_SCHEMA = "bronze"


def _register_bronze_table(spark, schema: str, table_name: str, path: str) -> None:
    """
    Register the Delta path as an external table.

    Without this, the pipeline wrote to a storage path while the dbt sources
    referenced `bronze.<table>` in the metastore — a table nothing ever
    created. The write succeeded and dbt then failed on an unresolvable
    source, which is a confusing way to find out the two halves were never
    connected to each other.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} "
        f"USING DELTA LOCATION '{path}'"
    )


def ingest_source(
    spark: SparkSession,
    delta_base: str,
    table_name: str,
    source_table: str,
    select_columns: str,
    watermark_column: str = "update_date",
    extra_filter: str = "",
    partition_column: Optional[str] = None,
    num_partitions: int = 8,
    bronze_schema: str = BRONZE_SCHEMA,
) -> int:
    """
    Pull one incremental batch into Bronze and advance the watermark.
    Returns the number of rows ingested.
    """
    control_path = f"{delta_base}/control/watermarks"
    bronze_path = f"{delta_base}/bronze/{table_name}"

    last_watermark = get_watermark(
        spark, control_path, table_name, lag_minutes=WATERMARK_LAG_MINUTES
    )
    print(f"[{table_name}] Pulling rows where {watermark_column} > '{last_watermark}'")

    batch = None
    try:
        batch = (
            read_jdbc_incremental(
                spark,
                table=source_table,
                watermark_column=watermark_column,
                last_watermark=last_watermark,
                select_columns=select_columns,
                extra_filter=extra_filter,
                partition_column=partition_column,
                num_partitions=num_partitions,
            )
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.current_date())
        )

        # One materialisation of the source query, reused by everything below.
        batch.persist(StorageLevel.MEMORY_AND_DISK)

        stats = batch.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.max(watermark_column).alias("batch_max"),
        ).collect()[0]

        row_count = stats["row_count"]
        batch_max = stats["batch_max"]

        print(f"[{table_name}] Pulled {row_count:,} new/changed rows")

        if row_count > 0:
            (
                batch.write.format("delta")
                .mode("append")
                .partitionBy("ingest_date")
                .save(bronze_path)
            )
            _register_bronze_table(spark, bronze_schema, table_name, bronze_path)

        update_watermark(spark, control_path, table_name, batch_max, row_count)
        return row_count

    except Exception as exc:
        mark_failed(spark, control_path, table_name, str(exc))
        raise

    finally:
        if batch is not None:
            batch.unpersist()
