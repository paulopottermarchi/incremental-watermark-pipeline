# terraform/outputs.tf

output "job_ids" {
  description = "Databricks job IDs, keyed by role."
  value       = local.all_jobs
}

output "job_urls" {
  description = "Direct links to each job in the workspace."
  value = {
    for name, id in local.all_jobs :
    name => "${var.databricks_host}/#job/${id}"
  }
}

output "secret_scope" {
  description = "Scope the pipeline reads its SQL Server credentials from. Values are set out of band — see secrets.tf."
  value       = databricks_secret_scope.pipeline.name
}
