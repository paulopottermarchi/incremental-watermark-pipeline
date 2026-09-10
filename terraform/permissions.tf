# terraform/permissions.tf
# ===========================================================================
# Job access control.
#
# Both blocks are optional — leaving the group variables empty means the
# jobs stay owner-only, which is the right default for a workspace where
# groups have not been set up yet.
# ===========================================================================

locals {
  all_jobs = {
    daily          = databricks_job.daily.id
    reconciliation = databricks_job.reconciliation.id
    maintenance    = databricks_job.maintenance.id
  }
}

resource "databricks_permissions" "jobs" {
  for_each = (var.manage_group == "" && var.view_group == "") ? {} : local.all_jobs

  job_id = each.value

  dynamic "access_control" {
    for_each = var.manage_group == "" ? [] : [var.manage_group]
    content {
      group_name       = access_control.value
      permission_level = "CAN_MANAGE"
    }
  }

  dynamic "access_control" {
    for_each = var.view_group == "" ? [] : [var.view_group]
    content {
      group_name       = access_control.value
      permission_level = "CAN_VIEW"
    }
  }
}
