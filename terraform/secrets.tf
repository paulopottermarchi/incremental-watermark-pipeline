# terraform/secrets.tf
# ===========================================================================
# The secret scope is managed here. The secret VALUES deliberately are not.
#
# databricks_secret stores its value in Terraform state in plaintext.
# Marking a variable sensitive suppresses it from console output; it does
# nothing to the state file. Managing the SQL Server password here would
# take a credential that currently lives only in the Databricks secret store
# and copy it into a second place, with weaker controls and a longer
# retention — which is a downgrade, not automation.
#
# So Terraform creates the scope and the access control, and the values go
# in out of band, once:
#
#   databricks secrets put-secret penetration-pipeline sqlserver_host
#   databricks secrets put-secret penetration-pipeline sqlserver_port
#   databricks secrets put-secret penetration-pipeline sqlserver_db
#   databricks secrets put-secret penetration-pipeline sqlserver_user
#   databricks secrets put-secret penetration-pipeline sqlserver_password
#
# Rotating a credential is then a CLI call, not an apply — which is also
# what you want at 2am.
# ===========================================================================

resource "databricks_secret_scope" "pipeline" {
  name                     = "penetration-pipeline"
  initial_manage_principal = "users"
}

resource "databricks_secret_acl" "pipeline_read" {
  count = var.view_group == "" ? 0 : 1

  principal  = var.view_group
  permission = "READ"
  scope      = databricks_secret_scope.pipeline.name
}
