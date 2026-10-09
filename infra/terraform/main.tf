terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.47"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

provider "azuread" {}

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

locals {
  suffix = random_string.suffix.result
  tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.project}-${var.environment}"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------- Data lake (ADLS Gen2)
resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}${local.suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

# upstream drop zone that simulates the source system ADF pulls from
resource "azurerm_storage_container" "source" {
  name                  = "source"
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_name  = azurerm_storage_account.lake.name
  container_access_type = "private"
}

# ---------------------------------------------------------------- Databricks workspace
resource "azurerm_databricks_workspace" "dbx" {
  name                = "dbw-${var.project}-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
  tags                = local.tags
}

# ---------------------------------------------------------------- Service principal for ADLS access
resource "azuread_application" "app" {
  display_name = "sp-${var.project}-${var.environment}"
  owners       = [data.azurerm_client_config.current.object_id]
}

resource "azuread_service_principal" "sp" {
  application_id = azuread_application.app.application_id
  owners         = [data.azurerm_client_config.current.object_id]
}

resource "azuread_service_principal_password" "sp_secret" {
  service_principal_id = azuread_service_principal.sp.id
  end_date_relative    = "4320h"
}

resource "azurerm_role_assignment" "sp_blob_contributor" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.sp.object_id
}

# lets the person running terraform upload files with `az ... --auth-mode login`
resource "azurerm_role_assignment" "deployer_blob_contributor" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ---------------------------------------------------------------- Key Vault (secret source of truth)
resource "azurerm_key_vault" "kv" {
  name                      = "kv-${var.project}-${local.suffix}"
  resource_group_name       = azurerm_resource_group.rg.name
  location                  = azurerm_resource_group.rg.location
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  enable_rbac_authorization = false
  tags                      = local.tags

  access_policy {
    tenant_id          = data.azurerm_client_config.current.tenant_id
    object_id          = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
  }
}

resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "sp-client-id"
  value        = azuread_application.app.application_id
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "sp-client-secret"
  value        = azuread_service_principal_password.sp_secret.value
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "sp_tenant_id" {
  name         = "sp-tenant-id"
  value        = data.azurerm_client_config.current.tenant_id
  key_vault_id = azurerm_key_vault.kv.id
}

# ---------------------------------------------------------------- Azure Data Factory (ingestion)
resource "azurerm_data_factory" "adf" {
  name                = "adf-${var.project}-${local.suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

# ADF authenticates to the lake with its managed identity (no keys/secrets)
resource "azurerm_role_assignment" "adf_blob_contributor" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_azure_blob_storage" "lake" {
  name                 = "ls_adls_lake"
  data_factory_id      = azurerm_data_factory.adf.id
  service_endpoint     = azurerm_storage_account.lake.primary_blob_endpoint
  use_managed_identity = true
}

resource "azurerm_data_factory_dataset_binary" "source" {
  name                = "ds_source_raw"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.lake.name

  azure_blob_storage_location {
    container = azurerm_storage_container.source.name
    path      = "retail"
  }
}

resource "azurerm_data_factory_dataset_binary" "landing" {
  name                = "ds_landing_raw"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_azure_blob_storage.lake.name

  azure_blob_storage_location {
    container = azurerm_storage_container.landing.name
    path      = "retail"
  }
}

resource "azurerm_data_factory_pipeline" "ingest" {
  name            = "pl_ingest_raw_to_landing"
  data_factory_id = azurerm_data_factory.adf.id
  description     = "Copies raw CSV/JSON files from source to the ADLS Gen2 landing zone"
  activities_json = file("${path.module}/../adf/pipeline_activities.json")

  depends_on = [
    azurerm_data_factory_dataset_binary.source,
    azurerm_data_factory_dataset_binary.landing,
  ]
}

# Daily schedule trigger. Created deactivated so nothing runs (or costs) until you enable it.
resource "azurerm_data_factory_trigger_schedule" "daily" {
  name            = "tr_daily_0200_ist"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.ingest.name
  frequency       = "Day"
  interval        = 1
  start_time      = var.adf_trigger_start_time
  activated       = false
}
