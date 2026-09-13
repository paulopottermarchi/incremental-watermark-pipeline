# Databricks notebook source
# bronze/03_incremental_ingest_dialer
# ===========================================================================
# Incremental ingestion of the dialer call log — the highest-volume source
# here, and the reason the pipeline exists: the original report re-scanned
# the full three-month call window on every execution.
#
# This is also where the partitioned JDBC read matters most, particularly on
# the first run, when the watermark is still at its seed value and the
# predicate matches the entire table.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base", "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("repo_path",  "/Repos/data-eng/penetration-pipeline")
dbutils.widgets.text("client_ids", "101, 102, 103")

delta_base = dbutils.widgets.get("delta_base")
repo_path  = dbutils.widgets.get("repo_path")
client_ids = dbutils.widgets.get("client_ids")

import sys

sys.path.insert(0, repo_path)
from utils.ingest import ingest_source

# COMMAND ----------
ingest_source(
    spark,
    delta_base=delta_base,
    table_name="dialer",
    source_table="collections.dialer",
    select_columns="""
        uniqueid, case_id, client_id, telecom_id, phone_number,
        disposition_code, call_date, update_date
    """,
    extra_filter=f"client_id IN ({client_ids})",
    partition_column="case_id",
    num_partitions=16,
)
