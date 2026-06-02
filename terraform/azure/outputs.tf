output "workspace_url" {
  description = "URL do Databricks Workspace"
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  description = "ID do Databricks Workspace"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "storage_account_name" {
  description = "Nome do storage account ADLS Gen2"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_dfs_endpoint" {
  description = "Endpoint DFS do ADLS Gen2 (para path abfss://)"
  value       = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "resource_group_name" {
  description = "Nome do resource group principal"
  value       = azurerm_resource_group.main.name
}

output "unity_catalog_access_connector_id" {
  description = "ID do Access Connector para Unity Catalog"
  value       = azurerm_databricks_access_connector.unity.id
}

output "unity_catalog_managed_identity" {
  description = "Principal ID da Managed Identity do Unity Catalog Access Connector"
  value       = azurerm_databricks_access_connector.unity.identity[0].principal_id
}

output "metastore_id" {
  description = "ID do Unity Catalog Metastore"
  value       = databricks_metastore.main.id
}

output "vnet_id" {
  description = "ID da Virtual Network do Databricks"
  value       = azurerm_virtual_network.main.id
}

output "private_endpoint_databricks_ui_ip" {
  description = "IP do private endpoint da UI do Databricks"
  value       = azurerm_private_endpoint.databricks_ui.private_service_connection[0].private_ip_address
}
