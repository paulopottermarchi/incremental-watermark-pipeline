# terraform/jobs.tf
# ===========================================================================
# The three jobs.
#
# Replaces dags/*.json. Beyond removing the duplication, declaring them here
# means `terraform plan` shows what a change does to a running job before it
# does it — where `databricks jobs create --json-file` either created a
# duplicate job or silently did nothing, depending on which command you
# reached for.
# ===========================================================================

# ---------------------------------------------------------------------------
# Daily: incremental ingestion, then the dbt build.
# ---------------------------------------------------------------------------
resource "databricks_job" "daily" {
  name        = "${local.name_prefix}-daily"
  description = "Daily incremental refresh of the dialer penetration and phone quality reports."

  max_concurrent_runs = 1

  tags = {
    project     = "penetration-pipeline"
    environment = var.environment
    managed_by  = "terraform"
  }

  # One cluster for the whole run. Every task previously declared its own,
  # which meant five cluster spin-ups of several minutes each for a job
  # whose actual work is short.
  job_cluster {
    job_cluster_key = "pipeline_cluster"

    new_cluster {
      spark_version = local.pipeline_cluster.spark_version
      node_type_id  = local.pipeline_cluster.node_type_id
      spark_conf    = local.pipeline_cluster.spark_conf

      autoscale {
        min_workers = var.autoscale_min_workers
        max_workers = var.autoscale_max_workers
      }
    }
  }

  dynamic "task" {
    for_each = local.ingest_tasks

    content {
      task_key        = "ingest_${task.key}"
      job_cluster_key = "pipeline_cluster"

      notebook_task {
        notebook_path   = "${var.repo_path}/notebooks/bronze/${task.value.notebook}"
        base_parameters = merge(local.common_parameters, task.value.parameters)
      }

      timeout_seconds           = task.value.timeout
      max_retries               = 2
      min_retry_interval_millis = 60000
    }
  }

  task {
    task_key        = "dbt_run"
    job_cluster_key = "pipeline_cluster"

    dynamic "depends_on" {
      for_each = local.ingest_tasks
      content {
        task_key = "ingest_${depends_on.key}"
      }
    }

    dbt_task {
      project_directory = "${var.repo_path}/dbt_project"
      commands = [
        "dbt deps",
        "dbt run",
        "dbt test",
        "dbt source freshness",
      ]
    }

    timeout_seconds = 900
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = var.timezone
    pause_status           = var.schedules_paused ? "PAUSED" : "UNPAUSED"
  }

  email_notifications {
    on_failure = var.alert_emails
  }
}

# ---------------------------------------------------------------------------
# Weekly: reconciliation.
#
# Watermark polling cannot see a row that changed without touching
# update_date. This is what catches that, and it fails the task on drift
# rather than logging it.
# ---------------------------------------------------------------------------
resource "databricks_job" "reconciliation" {
  name        = "${local.name_prefix}-reconciliation"
  description = "Weekly source-to-Bronze drift check."

  max_concurrent_runs = 1

  tags = {
    project     = "penetration-pipeline"
    environment = var.environment
    purpose     = "data-quality"
    managed_by  = "terraform"
  }

  task {
    task_key = "reconciliation_check"

    notebook_task {
      notebook_path = "${var.repo_path}/notebooks/control/01_reconciliation_check"
      base_parameters = merge(local.common_parameters, {
        drift_threshold = tostring(var.drift_threshold)
        client_ids      = local.client_ids_csv
        attr_type_ids   = local.attr_type_ids_csv
      })
    }

    # Counting rows needs a connection, not a cluster.
    new_cluster {
      spark_version = local.pipeline_cluster.spark_version
      node_type_id  = local.pipeline_cluster.node_type_id
      num_workers   = 1
    }

    timeout_seconds = 1800
  }

  schedule {
    quartz_cron_expression = "0 0 7 ? * MON"
    timezone_id            = var.timezone
    pause_status           = var.schedules_paused ? "PAUSED" : "UNPAUSED"
  }

  email_notifications {
    on_failure = var.alert_emails
  }
}

# ---------------------------------------------------------------------------
# Weekly: Delta housekeeping.
#
# Bronze is append-only and written daily. Without compaction the marts get
# slower every week for reasons invisible from the SQL.
# ---------------------------------------------------------------------------
resource "databricks_job" "maintenance" {
  name        = "${local.name_prefix}-maintenance"
  description = "Weekly OPTIMIZE / ZORDER / VACUUM on the Bronze tables."

  max_concurrent_runs = 1

  tags = {
    project     = "penetration-pipeline"
    environment = var.environment
    purpose     = "housekeeping"
    managed_by  = "terraform"
  }

  task {
    task_key = "delta_maintenance"

    notebook_task {
      notebook_path = "${var.repo_path}/notebooks/control/02_delta_maintenance"
      base_parameters = {
        delta_base   = var.delta_base
        vacuum_hours = tostring(var.vacuum_retention_hours)
      }
    }

    new_cluster {
      spark_version = local.pipeline_cluster.spark_version
      node_type_id  = local.pipeline_cluster.node_type_id
      num_workers   = 2
    }

    timeout_seconds = 3600
  }

  # Sunday, ahead of Monday's reconciliation, so the drift check reads
  # compacted tables rather than competing with the rewrite.
  schedule {
    quartz_cron_expression = "0 0 4 ? * SUN"
    timezone_id            = var.timezone
    pause_status           = var.schedules_paused ? "PAUSED" : "UNPAUSED"
  }

  email_notifications {
    on_failure = var.alert_emails
  }
}
