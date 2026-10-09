"""
local/spark_session.py
===========================================================================
Builds the local Spark session that stands in for a Databricks cluster.

Two things it has to provide that Databricks provides for free:

  * Delta. On Databricks the extensions are already configured; here they
    come from delta-spark and have to be wired in explicitly.

  * A persistent metastore. `ingest_source` registers each Bronze path as an
    external table, and dbt resolves its sources by name against that
    metastore. An in-memory catalog would work only if ingestion and dbt ran
    in the same process — they do not, so a Derby-backed Hive metastore on
    disk is what connects them.

Derby permits exactly one JVM at a time against a given metastore directory,
which is why the Makefile runs seed, ingest and dbt in sequence rather than
in parallel. That is a limitation of the demo harness, not of the pipeline.
"""

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

WORK_DIR = Path(os.environ.get("DEMO_WORK_DIR", "/workspace/.local-run"))
WAREHOUSE_DIR = WORK_DIR / "warehouse"
METASTORE_DIR = WORK_DIR / "metastore"
DELTA_BASE = os.environ.get("DEMO_DELTA_BASE", str(WORK_DIR / "lake"))

MSSQL_JDBC_JAR = os.environ.get("MSSQL_JDBC_JAR", "/opt/jars/mssql-jdbc.jar")


def jdbc_url() -> str:
    host = os.environ["SQLSERVER_HOST"]
    port = os.environ["SQLSERVER_PORT"]
    database = os.environ["SQLSERVER_DB"]
    return (
        f"jdbc:sqlserver://{host}:{port};"
        f"databaseName={database};"
        "encrypt=true;trustServerCertificate=true;loginTimeout=30;"
    )


def jdbc_properties() -> dict:
    return {
        "user": os.environ["SQLSERVER_USER"],
        "password": os.environ["SQLSERVER_PASSWORD"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }


def build_local_spark(app_name: str = "penetration-pipeline-local") -> SparkSession:
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    METASTORE_DIR.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", MSSQL_JDBC_JAR)
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={METASTORE_DIR}/metastore_db;create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={METASTORE_DIR}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", os.environ.get("DEMO_DRIVER_MEMORY", "2g"))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .enableHiveSupport()
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
