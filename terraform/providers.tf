# terraform/providers.tf
# ===========================================================================
# Provider configuration.
#
# No token here, and no token variable anywhere in this module. The provider
# reads DATABRICKS_HOST and DATABRICKS_TOKEN from the environment, which
# keeps the credential out of both the repository and the state file. A
# `token` variable would end up in state in plaintext even when marked
# sensitive — sensitive controls what Terraform prints, not what it stores.
# ===========================================================================

provider "databricks" {
  host = var.databricks_host
}
