# Variables for GenAI ETL Databricks infrastructure

variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "Environment must be one of: dev, test, prod."
  }
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "genai-etl"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "Databricks SKU must be one of: standard, premium, trial."
  }
}

variable "cluster_node_type" {
  description = "Node type for Databricks cluster"
  type        = string
  default     = "Standard_DS3_v2"
}

variable "cluster_min_workers" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "cluster_max_workers" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 3
}

variable "cluster_autotermination_minutes" {
  description = "Auto-termination time in minutes"
  type        = number
  default     = 60
}

variable "spark_version" {
  description = "Spark version for Databricks cluster"
  type        = string
  default     = "13.3.x-scala2.12"
}

variable "python_version" {
  description = "Python version for Databricks cluster"
  type        = string
  default     = "3"
}

variable "enable_public_ip" {
  description = "Enable public IP for Databricks workspace"
  type        = bool
  default     = true
}

variable "storage_account_tier" {
  description = "Storage account tier"
  type        = string
  default     = "Standard"
  validation {
    condition     = contains(["Standard", "Premium"], var.storage_account_tier)
    error_message = "Storage account tier must be Standard or Premium."
  }
}

variable "storage_replication_type" {
  description = "Storage account replication type"
  type        = string
  default     = "LRS"
  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS"], var.storage_replication_type)
    error_message = "Storage replication type must be one of: LRS, GRS, RAGRS, ZRS."
  }
}

variable "allowed_ip_ranges" {
  description = "List of allowed IP ranges for Databricks workspace"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}

