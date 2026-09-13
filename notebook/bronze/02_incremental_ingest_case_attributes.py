# Databricks notebook source
# bronze/02_incremental_ingest_case_attributes
# ===========================================================================
# Incremental ingestion of the case_attribute table, filtered server-side to
# the two attribute types the report reads (provider and days past due).
# The source carries dozens of other types this report never touches.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base",    "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("repo_path",     "/Repos/data-eng/penetration-pipeline")
dbutils.widgets.text("attr_type_ids", "11, 12")

delta_base    = dbutils.widgets.get("delta_base")
repo_path     = dbutils.widgets.get("repo_path")
attr_type_ids = dbutils.widgets.get("attr_type_ids")

import sys

sys.path.insert(0, repo_path)
from utils.ingest import ingest_source

# COMMAND ----------
ingest_source(
    spark,
    delta_base=delta_base,
    table_name="case_attributes",
    source_table="collections.case_attribute",
    select_columns="""
        case_id, case_attribute_type_id, case_attribute_value, update_date
    """,
    extra_filter=f"case_attribute_type_id IN ({attr_type_ids})",
    partition_column="case_id",
    num_partitions=8,
)
