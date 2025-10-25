# Databricks workspace and cluster configuration

# Databricks workspace
resource "azurerm_databricks_workspace" "this" {
  name                = local.workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.databricks_sku

  custom_parameters {
    no_public_ip        = !var.enable_public_ip
    public_subnet_name  = var.enable_public_ip ? "public-subnet" : null
    private_subnet_name = "private-subnet"
    virtual_network_id  = azurerm_virtual_network.this.id
  }

  tags = merge(local.common_tags, var.tags)
}

# Databricks cluster
resource "databricks_cluster" "this" {
  cluster_name            = "${var.environment}-etl-cluster"
  spark_version           = var.spark_version
  node_type_id            = var.cluster_node_type
  driver_node_type_id     = var.cluster_node_type
  autotermination_minutes = var.cluster_autotermination_minutes
  enable_elastic_disk     = true

  autoscale {
    min_workers = var.cluster_min_workers
    max_workers = var.cluster_max_workers
  }

  spark_conf = {
    "spark.sql.adaptive.enabled"                    = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
    "spark.sql.adaptive.skewJoin.enabled"           = "true"
    "spark.sql.shuffle.partitions"                  = "200"
    "spark.serializer"                              = "org.apache.spark.serializer.KryoSerializer"
    "spark.sql.execution.arrow.pyspark.enabled"     = "true"
  }

  custom_tags = merge(local.common_tags, {
    Environment = var.environment
    Purpose     = "ETL Processing"
  })

  depends_on = [azurerm_databricks_workspace.this]
}

# Databricks cluster policy
resource "databricks_cluster_policy" "this" {
  name = "${var.environment}-etl-policy"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : var.spark_version
    },
    "node_type_id" : {
      "type" : "fixed",
      "value" : var.cluster_node_type
    },
    "driver_node_type_id" : {
      "type" : "fixed",
      "value" : var.cluster_node_type
    },
    "autotermination_minutes" : {
      "type" : "range",
      "maxValue" : 120,
      "minValue" : 10
    },
    "enable_elastic_disk" : {
      "type" : "fixed",
      "value" : true
    },
    "spark_conf.spark.sql.adaptive.enabled" : {
      "type" : "fixed",
      "value" : "true"
    },
    "spark_conf.spark.sql.shuffle.partitions" : {
      "type" : "range",
      "maxValue" : 1000,
      "minValue" : 50
    }
  })

  depends_on = [azurerm_databricks_workspace.this]
}

# Databricks secret scope for storage keys
resource "databricks_secret_scope" "storage" {
  name = "storage-${var.environment}"

  keyvault_metadata {
    resource_id = azurerm_key_vault.this.id
    dns_name    = azurerm_key_vault.this.vault_uri
  }

  depends_on = [azurerm_key_vault_access_policy.databricks]
}

# Databricks init script for storage mounts
resource "databricks_dbfs_file" "init_script" {
  source = "${path.module}/scripts/mount_storage.sh"
  path   = "/databricks/init_scripts/mount_storage.sh"
}

# Databricks job for ETL pipeline
resource "databricks_job" "etl_pipeline" {
  name = "${var.environment}-etl-pipeline"

  new_cluster {
    num_workers   = var.cluster_min_workers
    spark_version = var.spark_version
    node_type_id  = var.cluster_node_type

    spark_conf = {
      "spark.sql.adaptive.enabled"     = "true"
      "spark.sql.shuffle.partitions"   = "200"
    }
  }

  notebook_task {
    notebook_path = "/Repos/GenAI_ETL_Code_New/notebooks/bronze_ingest"
    base_parameters = {
      env         = var.environment
      source_path = "/mnt/raw/sales/"
    }
  }

  depends_on = [azurerm_databricks_workspace.this]
}

