# Outputs for GenAI ETL Databricks infrastructure

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  description = "Databricks workspace ID"
  value       = azurerm_databricks_workspace.this.id
}

output "databricks_cluster_id" {
  description = "Databricks cluster ID"
  value       = databricks_cluster.this.id
}

output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.this.name
}

output "storage_account_primary_key" {
  description = "Storage account primary key"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}

output "storage_account_primary_dfs_endpoint" {
  description = "Storage account primary DFS endpoint"
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "key_vault_name" {
  description = "Key Vault name"
  value       = azurerm_key_vault.this.name
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = azurerm_key_vault.this.vault_uri
}

output "secret_scope_name" {
  description = "Databricks secret scope name"
  value       = databricks_secret_scope.storage.name
}

output "container_names" {
  description = "Storage container names"
  value = {
    raw    = azurerm_storage_container.raw.name
    bronze = azurerm_storage_container.bronze.name
    silver = azurerm_storage_container.silver.name
    gold   = azurerm_storage_container.gold.name
  }
}

output "mount_paths" {
  description = "Databricks mount paths"
  value = {
    raw    = "/mnt/raw"
    bronze = "/mnt/bronze"
    silver = "/mnt/silver"
    gold   = "/mnt/gold"
  }
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

output "resource_group_name" {
  description = "Resource group name"
  value       = var.resource_group_name
}

output "location" {
  description = "Azure region"
  value       = var.location
}



