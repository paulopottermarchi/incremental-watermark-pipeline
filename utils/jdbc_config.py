# utils/jdbc_config.py
# ===========================================================================
# JDBC helper for a read-only SQL Server application login.
# Secret scope: "penetration-pipeline"
#
# Two things this module is careful about:
#
#   1. Parallelism. A plain spark.read.jdbc() pull runs through a single
#      connection on a single executor, regardless of cluster size. For the
#      high-volume `dialer` source — and for the very first backfill run,
#      where the watermark is still at its seed value and the predicate
#      matches the whole table — that is the difference between minutes and
#      hours. read_jdbc_incremental() therefore supports a numeric
#      partition_column and issues a cheap MIN/MAX bounds query to derive
#      lowerBound/upperBound before splitting the read across executors.
#
#   2. Predicate hygiene. The watermark is interpolated into a SQL string
#      because JDBC pushdown has no bind-parameter path. The value comes
#      from our own control table, but it is validated against a strict
#      timestamp format anyway — a control table is still an input.
# ===========================================================================

import os
import re
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

SECRET_SCOPE = "penetration-pipeline"

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


# Env var fallback, used by the local Docker run in local/. Databricks
# secrets are resolved first when dbutils is in scope; outside Databricks
# there is no dbutils at all, and the alternative to this fallback would be
# a second copy of the extraction code that the tests never touch.
_ENV_FALLBACK = {
    "sqlserver_host": "SQLSERVER_HOST",
    "sqlserver_port": "SQLSERVER_PORT",
    "sqlserver_db": "SQLSERVER_DB",
    "sqlserver_user": "SQLSERVER_USER",
    "sqlserver_password": "SQLSERVER_PASSWORD",
}


def _get_secret(key: str) -> str:
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)  # noqa: F821
    except NameError:
        pass

    env_key = _ENV_FALLBACK.get(key)
    value = os.environ.get(env_key) if env_key else None

    if value is None:
        raise KeyError(
            f"No Databricks secret scope available and {env_key} is unset. "
            f"Outside Databricks, set the SQLSERVER_* environment variables."
        )
    return value


def get_jdbc_url() -> str:
    host = _get_secret("sqlserver_host")
    port = _get_secret("sqlserver_port")
    database = _get_secret("sqlserver_db")
    return (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        "encrypt=true;trustServerCertificate=true;loginTimeout=30;"
    )


def get_jdbc_properties() -> dict:
    return {
        "user": _get_secret("sqlserver_user"),
        "password": _get_secret("sqlserver_password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def _validate_watermark(last_watermark: str) -> str:
    """Reject anything that is not a plain 'YYYY-MM-DD HH:MM:SS' literal."""
    if not _TIMESTAMP_RE.match(last_watermark):
        raise ValueError(
            f"Refusing to build a predicate from a malformed watermark: {last_watermark!r}"
        )
    return last_watermark


def _build_where(watermark_column: str, last_watermark: str, extra_filter: str) -> str:
    where = f"WHERE {watermark_column} > '{_validate_watermark(last_watermark)}'"
    if extra_filter:
        where += f" AND {extra_filter}"
    return where


def _read_query(spark: SparkSession, query: str, **options) -> DataFrame:
    url = get_jdbc_url()
    props = get_jdbc_properties()

    reader = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", f"({query}) AS q")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .option("fetchsize", "10000")
    )
    for key, value in options.items():
        reader = reader.option(key, value)

    return reader.load()


def _get_partition_bounds(
    spark: SparkSession,
    table: str,
    partition_column: str,
    where_clause: str,
) -> Tuple[Optional[int], Optional[int]]:
    """
    One cheap aggregate round-trip to find the range of the partition column
    within the incremental window. Returns (None, None) for an empty window.
    """
    query = (
        f"SELECT MIN({partition_column}) AS lo, MAX({partition_column}) AS hi "
        f"FROM {table} {where_clause}"
    )
    row = _read_query(spark, query).collect()[0]
    return row["lo"], row["hi"]


def read_jdbc_incremental(
    spark: SparkSession,
    table: str,
    watermark_column: str,
    last_watermark: str,
    select_columns: str = "*",
    extra_filter: str = "",
    partition_column: Optional[str] = None,
    num_partitions: int = 8,
) -> DataFrame:
    """
    Pull rows where watermark_column > last_watermark, with an optional extra
    WHERE clause (used here to scope to the clients in the report).

    When partition_column is given (it must be numeric), the read is split
    into num_partitions parallel connections over the observed id range.
    Otherwise it falls back to a single connection.
    """
    where_clause = _build_where(watermark_column, last_watermark, extra_filter)
    query = f"SELECT {select_columns} FROM {table} {where_clause}"

    if partition_column is None:
        return _read_query(spark, query)

    lower, upper = _get_partition_bounds(spark, table, partition_column, where_clause)

    # Empty window, or a degenerate range where splitting buys nothing.
    if lower is None or upper is None or lower == upper:
        return _read_query(spark, query)

    return _read_query(
        spark,
        query,
        partitionColumn=partition_column,
        lowerBound=str(lower),
        upperBound=str(upper),
        numPartitions=str(num_partitions),
    )
