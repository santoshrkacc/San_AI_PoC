# Terraform Infrastructure for GenAI ETL Databricks

This directory contains Terraform configurations to provision Azure Databricks workspace, clusters, and supporting infrastructure for the GenAI ETL project.

## Architecture

- **Azure Databricks Workspace** with custom networking
- **Azure Data Lake Storage Gen2** with containers for bronze/silver/gold layers
- **Azure Key Vault** for secrets management
- **Virtual Network** with public/private subnets
- **Databricks Cluster** with optimized Spark configuration
- **Secret Scopes** for secure storage access

## Prerequisites

1. **Azure CLI** installed and authenticated
2. **Terraform** >= 1.0 installed
3. **Azure subscription** with appropriate permissions
4. **Resource group** created in Azure

## Quick Start

1. **Copy and configure variables:**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your values
   ```

2. **Initialize Terraform:**
   ```bash
   terraform init
   ```

3. **Plan deployment:**
   ```bash
   terraform plan
   ```

4. **Apply infrastructure:**
   ```bash
   terraform apply
   ```

## Configuration

### Required Variables

- `environment`: Environment name (dev, test, prod)
- `resource_group_name`: Azure resource group name
- `location`: Azure region

### Key Features

- **Auto-scaling clusters** with min/max worker configuration
- **Optimized Spark settings** for ETL workloads
- **Secure storage mounting** via Key Vault secrets
- **Network security** with configurable IP allowlists
- **Cost optimization** with auto-termination

## Outputs

After deployment, Terraform outputs:
- Databricks workspace URL
- Storage account details
- Key Vault information
- Mount paths for containers

## Environment-Specific Deployment

Deploy to different environments:

```bash
# Development
terraform apply -var="environment=dev" -var="resource_group_name=rg-genai-etl-dev"

# Test
terraform apply -var="environment=test" -var="resource_group_name=rg-genai-etl-test"

# Production
terraform apply -var="environment=prod" -var="resource_group_name=rg-genai-etl-prod"
```

## Post-Deployment

1. **Access Databricks workspace** using the output URL
2. **Verify storage mounts** in `/mnt/` directories
3. **Run table creation** notebook: `notebooks/create_tables.py`
4. **Configure CI/CD** with workspace URLs and tokens

## Cleanup

To destroy infrastructure:
```bash
terraform destroy
```

## Security Notes

- Storage keys are stored in Key Vault
- Databricks uses secret scopes for secure access
- Network security groups restrict access
- Production environments have purge protection enabled