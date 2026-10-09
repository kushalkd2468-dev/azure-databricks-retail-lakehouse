output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "databricks_workspace_url" {
  value = "https://${azurerm_databricks_workspace.dbx.workspace_url}"
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}

output "data_factory_name" {
  value = azurerm_data_factory.adf.name
}
