# terraform/versions.tf
# ===========================================================================
# Version constraints.
#
# Pinned to a minor range rather than left open: the Databricks provider
# changes job schema between minors often enough that an unpinned apply can
# reshape a running job for reasons unrelated to any commit.
# ===========================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}
