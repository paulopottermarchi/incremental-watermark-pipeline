# terraform/locals.tf
# ===========================================================================
# Shared configuration.
#
# The three hand-written job JSON files this module replaces each carried
# their own copy of the cluster spec, the repo path and the client list.
# Nothing kept them in step: changing the runtime version meant three edits,
# and missing one produced a job running on a different Spark than its
# neighbours — a drift that surfaces as an inexplicable failure months later.
#
# Defined once here, referenced everywhere.
# ===========================================================================

locals {
  name_prefix = "${var.environment}-penetration-pipeline"

  client_ids_csv    = join(", ", [for id in var.client_ids : tostring(id)])
  attr_type_ids_csv = join(", ", [for id in var.attr_type_ids : tostring(id)])

  # Parameters every notebook takes.
  common_parameters = {
    delta_base = var.delta_base
    repo_path  = var.repo_path
  }

  pipeline_cluster = {
    spark_version = var.spark_version
    node_type_id  = var.node_type_id
    spark_conf = {
      "spark.databricks.delta.optimizeWrite.enabled" = "true"
      "spark.databricks.delta.autoCompact.enabled"   = "true"
    }
  }

  # The four Bronze ingestion tasks. They differ only in the notebook they
  # call, the parameters they take and how long they are allowed to run, so
  # they are described as data and expanded with for_each rather than
  # written out four times.
  ingest_tasks = {
    cases = {
      notebook   = "01_incremental_ingest_cases"
      timeout    = 600
      parameters = { client_ids = local.client_ids_csv }
    }
    case_attributes = {
      notebook   = "02_incremental_ingest_case_attributes"
      timeout    = 600
      parameters = { attr_type_ids = local.attr_type_ids_csv }
    }
    # The highest-volume source, and the only one whose first run is a full
    # backfill of the whole table — hence the wider timeout.
    dialer = {
      notebook   = "03_incremental_ingest_dialer"
      timeout    = 1800
      parameters = { client_ids = local.client_ids_csv }
    }
    contacts = {
      notebook   = "04_incremental_ingest_contacts"
      timeout    = 900
      parameters = {}
    }
  }
}
