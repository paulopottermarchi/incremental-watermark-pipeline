# Databricks notebook source
# bronze/04_incremental_ingest_contacts
# ===========================================================================
# Incremental ingestion of the contact table, which feeds the Paid / PTP /
# right-party counts in the penetration mart.
#
# The contact table carries no client_id of its own — the source query
# scopes it through a join back to case. We pull it unscoped here and let
# the mart apply the client filter via its join to stg_cases, which is
# already scoped.
# ===========================================================================

# COMMAND ----------
dbutils.widgets.text("delta_base", "dbfs:/mnt/penetration-pipeline")
dbutils.widgets.text("repo_path",  "/Repos/data-eng/penetration-pipeline")

delta_base = dbutils.widgets.get("delta_base")
repo_path  = dbutils.widgets.get("repo_path")

import sys

sys.path.insert(0, repo_path)
from utils.ingest import ingest_source

# COMMAND ----------
ingest_source(
    spark,
    delta_base=delta_base,
    table_name="contacts",
    source_table="collections.contact",
    select_columns="""
        contact_id, case_id, status_type_id, target_type_id,
        contact_date, insert_user, update_date
    """,
    partition_column="contact_id",
    num_partitions=8,
)
