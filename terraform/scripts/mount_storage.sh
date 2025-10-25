#!/bin/bash
# Databricks init script to mount Azure Data Lake Storage

# Get environment from cluster tags
ENVIRONMENT=$(echo $DB_CLUSTER_TAGS | grep -o 'Environment:[^,]*' | cut -d: -f2)

# Storage account details (these will be replaced by Terraform)
STORAGE_ACCOUNT_NAME="st${ENVIRONMENT}genaietl"
CONTAINER_RAW="raw"
CONTAINER_BRONZE="bronze"
CONTAINER_SILVER="silver"
CONTAINER_GOLD="gold"

# Mount raw container
if [ ! -d "/mnt/raw" ]; then
  echo "Mounting raw container..."
  dbutils.fs.mount(
    source = f"abfss://${CONTAINER_RAW}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point = "/mnt/raw",
    extra_configs = {
      "fs.azure.account.key.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": dbutils.secrets.get(scope="storage-${ENVIRONMENT}", key="storage-account-key")
    }
  )
fi

# Mount bronze container
if [ ! -d "/mnt/bronze" ]; then
  echo "Mounting bronze container..."
  dbutils.fs.mount(
    source = f"abfss://${CONTAINER_BRONZE}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = {
      "fs.azure.account.key.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": dbutils.secrets.get(scope="storage-${ENVIRONMENT}", key="storage-account-key")
    }
  )
fi

# Mount silver container
if [ ! -d "/mnt/silver" ]; then
  echo "Mounting silver container..."
  dbutils.fs.mount(
    source = f"abfss://${CONTAINER_SILVER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point = "/mnt/silver",
    extra_configs = {
      "fs.azure.account.key.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": dbutils.secrets.get(scope="storage-${ENVIRONMENT}", key="storage-account-key")
    }
  )
fi

# Mount gold container
if [ ! -d "/mnt/gold" ]; then
  echo "Mounting gold container..."
  dbutils.fs.mount(
    source = f"abfss://${CONTAINER_GOLD}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = {
      "fs.azure.account.key.${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net": dbutils.secrets.get(scope="storage-${ENVIRONMENT}", key="storage-account-key")
    }
  )
fi

echo "Storage mounting completed successfully!"



