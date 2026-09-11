# terraform/variables.tf

variable "databricks_host" {
  description = "Workspace URL, e.g. https://adb-000000000000.0.azuredatabricks.net"
  type        = string
}

variable "environment" {
  description = "Prefixed onto every job name so dev and prod can share a workspace without colliding."
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "repo_path" {
  description = "Databricks Repos path holding the checked-out project. Use a team path, not a personal one."
  type        = string
  default     = "/Repos/data-eng/penetration-pipeline"

  validation {
    condition     = !can(regex("/Repos/[A-Za-z]+\\.[A-Za-z]+/", var.repo_path))
    error_message = "repo_path looks like a personal workspace path (firstname.lastname). Use a team path such as /Repos/data-eng/."
  }
}

variable "delta_base" {
  description = "Root storage path for the Bronze tables and the watermark control table."
  type        = string
  default     = "dbfs:/mnt/penetration-pipeline"
}

variable "client_ids" {
  description = "Clients in scope for the report. Mirrors the client_ids dbt var."
  type        = list(number)
  default     = [101, 102, 103]
}

variable "attr_type_ids" {
  description = "Case attribute types the pipeline reads (provider, DPD). Mirrors the dbt vars."
  type        = list(number)
  default     = [11, 12]
}

variable "alert_emails" {
  description = "Addresses notified when a job fails."
  type        = list(string)
  default     = ["data-eng-alerts@example.com"]
}

variable "spark_version" {
  description = "Databricks runtime version."
  type        = string
  default     = "14.3.x-scala2.12"
}

variable "node_type_id" {
  description = "Worker node type."
  type        = string
  default     = "Standard_DS3_v2"
}

variable "autoscale_min_workers" {
  type    = number
  default = 2
}

variable "autoscale_max_workers" {
  type    = number
  default = 8
}

variable "timezone" {
  description = "Timezone for the cron schedules."
  type        = string
  default     = "America/Sao_Paulo"
}

variable "drift_threshold" {
  description = "Fraction of row-count drift between source and Bronze that fails the weekly reconciliation."
  type        = number
  default     = 0.005

  validation {
    condition     = var.drift_threshold > 0 && var.drift_threshold < 1
    error_message = "drift_threshold is a fraction between 0 and 1."
  }
}

variable "vacuum_retention_hours" {
  description = "Delta VACUUM retention. Must exceed the longest-running concurrent reader."
  type        = number
  default     = 168

  validation {
    condition     = var.vacuum_retention_hours >= 168
    error_message = "Retention below 168 hours risks deleting files a running query still needs."
  }
}

variable "schedules_paused" {
  description = "Set true to deploy the jobs without arming their schedules — useful on a first apply."
  type        = bool
  default     = false
}

variable "manage_group" {
  description = "Optional Databricks group granted CAN_MANAGE on the jobs. Empty means owner-only."
  type        = string
  default     = ""
}

variable "view_group" {
  description = "Optional Databricks group granted CAN_VIEW on the jobs."
  type        = string
  default     = ""
}
