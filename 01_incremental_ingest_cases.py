# Databricks notebook source
# bronze/01_incremental_ingest_cases
# ===========================================================================
# Incremental ingestion of the case table, scoped to the clients in the
# report. Runs once daily, which already exceeds the freshness the
# penetration report is consumed at.
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
    table_name="cases",
    source_table="collections.[case]",
    select_columns="""
        case_id, client_id, ref_number, debtor_id,
        original_capital, actual_capital, case_statute_id, update_date
    """,
    extra_filter=f"client_id IN ({client_ids})",
    partition_column="case_id",
    num_partitions=8,
)
