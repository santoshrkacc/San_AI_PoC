# Terraform configuration for GenAI ETL Databricks infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Configure providers
provider "azurerm" {
  features {}
}

provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}

# Data sources
data "azurerm_client_config" "current" {}

# Local variables
locals {
  common_tags = {
    Project     = "GenAI-ETL-Code-New"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
  
  workspace_name = "dbx-${var.environment}-${var.project_name}"
  storage_name   = "st${var.environment}${var.project_name}${random_string.suffix.result}"
}

# Random string for unique naming
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

