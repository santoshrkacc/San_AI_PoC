# Databricks notebook source
# MAGIC %md
# MAGIC Create all Bronze, Silver, and Gold tables

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Create Bronze tables
# MAGIC -- %run ./sql/bronze_ddl.sql

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: Raw sales data
# MAGIC CREATE TABLE IF NOT EXISTS bronze.sales_raw (
# MAGIC   sale_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   product_id STRING,
# MAGIC   sale_date STRING,
# MAGIC   quantity INT,
# MAGIC   unit_price DOUBLE,
# MAGIC   tax_rate DOUBLE,
# MAGIC   store_id STRING,
# MAGIC   channel STRING,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING,
# MAGIC   _corrupt_record STRING
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (sale_date)
# MAGIC LOCATION '/mnt/bronze/sales_raw';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: Raw customer data
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers_raw (
# MAGIC   customer_id STRING,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   country STRING,
# MAGIC   registration_date STRING,
# MAGIC   is_active BOOLEAN,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING,
# MAGIC   _corrupt_record STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/bronze/customers_raw';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: Raw product data
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products_raw (
# MAGIC   product_id STRING,
# MAGIC   title STRING,
# MAGIC   category STRING,
# MAGIC   isbn STRING,
# MAGIC   brand STRING,
# MAGIC   description STRING,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING,
# MAGIC   _corrupt_record STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/bronze/products_raw';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: Raw pricing data
# MAGIC CREATE TABLE IF NOT EXISTS bronze.pricing_raw (
# MAGIC   product_id STRING,
# MAGIC   currency STRING,
# MAGIC   list_price DOUBLE,
# MAGIC   discount DOUBLE,
# MAGIC   tax_rate DOUBLE,
# MAGIC   effective_date STRING,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING,
# MAGIC   _corrupt_record STRING
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (effective_date)
# MAGIC LOCATION '/mnt/bronze/pricing_raw';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bronze: Raw billing data
# MAGIC CREATE TABLE IF NOT EXISTS bronze.billing_raw (
# MAGIC   billing_id STRING,
# MAGIC   invoice_id STRING,
# MAGIC   payment_method STRING,
# MAGIC   terms STRING,
# MAGIC   status STRING,
# MAGIC   amount DOUBLE,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING,
# MAGIC   _corrupt_record STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/bronze/billing_raw';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Cleaned sales data
# MAGIC CREATE TABLE IF NOT EXISTS silver.sales (
# MAGIC   sale_id STRING NOT NULL,
# MAGIC   customer_id STRING NOT NULL,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   sale_date DATE NOT NULL,
# MAGIC   quantity INT NOT NULL,
# MAGIC   unit_price DOUBLE NOT NULL,
# MAGIC   tax_rate DOUBLE NOT NULL,
# MAGIC   store_id STRING NOT NULL,
# MAGIC   channel STRING NOT NULL,
# MAGIC   net_amount DOUBLE NOT NULL,
# MAGIC   tax_amount DOUBLE NOT NULL,
# MAGIC   gross_amount DOUBLE NOT NULL,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (sale_date)
# MAGIC LOCATION '/mnt/silver/sales';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Cleaned customer data
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC   customer_id STRING NOT NULL,
# MAGIC   name STRING NOT NULL,
# MAGIC   email STRING NOT NULL,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   country STRING NOT NULL,
# MAGIC   registration_date DATE,
# MAGIC   is_active BOOLEAN NOT NULL,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/silver/customers';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Cleaned product data
# MAGIC CREATE TABLE IF NOT EXISTS silver.products (
# MAGIC   product_id STRING NOT NULL,
# MAGIC   title STRING NOT NULL,
# MAGIC   category STRING NOT NULL,
# MAGIC   isbn STRING,
# MAGIC   brand STRING,
# MAGIC   description STRING,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/silver/products';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Cleaned pricing data
# MAGIC CREATE TABLE IF NOT EXISTS silver.pricing (
# MAGIC   product_id STRING NOT NULL,
# MAGIC   currency STRING NOT NULL,
# MAGIC   list_price DOUBLE NOT NULL,
# MAGIC   discount DOUBLE NOT NULL,
# MAGIC   tax_rate DOUBLE NOT NULL,
# MAGIC   price_effective_date DATE NOT NULL,
# MAGIC   effective_from TIMESTAMP,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (price_effective_date)
# MAGIC LOCATION '/mnt/silver/pricing';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Cleaned billing data
# MAGIC CREATE TABLE IF NOT EXISTS silver.billing (
# MAGIC   billing_id STRING NOT NULL,
# MAGIC   invoice_id STRING NOT NULL,
# MAGIC   payment_method STRING NOT NULL,
# MAGIC   terms STRING,
# MAGIC   status STRING NOT NULL,
# MAGIC   amount DOUBLE NOT NULL,
# MAGIC   _ingest_ts TIMESTAMP,
# MAGIC   _source_file STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/silver/billing';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Silver: Data Quality Results
# MAGIC CREATE TABLE IF NOT EXISTS silver.dq_results (
# MAGIC   batch_id STRING NOT NULL,
# MAGIC   table_name STRING NOT NULL,
# MAGIC   rule_name STRING NOT NULL,
# MAGIC   rule_type STRING NOT NULL,
# MAGIC   passed_count BIGINT NOT NULL,
# MAGIC   failed_count BIGINT NOT NULL,
# MAGIC   total_count BIGINT NOT NULL,
# MAGIC   failure_rate DOUBLE NOT NULL,
# MAGIC   execution_ts TIMESTAMP NOT NULL,
# MAGIC   details STRING
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/silver/dq_results';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Date dimension (static calendar)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_date (
# MAGIC   date_key INT NOT NULL,
# MAGIC   date DATE NOT NULL,
# MAGIC   year INT NOT NULL,
# MAGIC   quarter INT NOT NULL,
# MAGIC   month INT NOT NULL,
# MAGIC   day INT NOT NULL,
# MAGIC   dow STRING NOT NULL,
# MAGIC   is_weekend BOOLEAN NOT NULL,
# MAGIC   is_holiday BOOLEAN NOT NULL
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/gold/dim_date';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Customer dimension (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_customer (
# MAGIC   customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   customer_id STRING NOT NULL,
# MAGIC   name STRING NOT NULL,
# MAGIC   email STRING NOT NULL,
# MAGIC   phone STRING,
# MAGIC   country STRING NOT NULL,
# MAGIC   is_active BOOLEAN NOT NULL,
# MAGIC   effective_from TIMESTAMP NOT NULL,
# MAGIC   effective_to TIMESTAMP NOT NULL,
# MAGIC   current_flag BOOLEAN NOT NULL
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/gold/dim_customer';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Product dimension (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_product (
# MAGIC   product_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   title STRING NOT NULL,
# MAGIC   category STRING NOT NULL,
# MAGIC   isbn STRING,
# MAGIC   brand STRING,
# MAGIC   effective_from TIMESTAMP NOT NULL,
# MAGIC   effective_to TIMESTAMP NOT NULL,
# MAGIC   current_flag BOOLEAN NOT NULL
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/gold/dim_product';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Pricing dimension (SCD2)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_pricing (
# MAGIC   pricing_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   currency STRING NOT NULL,
# MAGIC   list_price DOUBLE NOT NULL,
# MAGIC   discount DOUBLE NOT NULL,
# MAGIC   tax_rate DOUBLE NOT NULL,
# MAGIC   effective_from TIMESTAMP NOT NULL,
# MAGIC   effective_to TIMESTAMP NOT NULL,
# MAGIC   current_flag BOOLEAN NOT NULL
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/gold/dim_pricing';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Billing dimension (Type 1)
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_billing (
# MAGIC   billing_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   billing_id STRING NOT NULL,
# MAGIC   payment_method STRING NOT NULL,
# MAGIC   terms STRING,
# MAGIC   status STRING NOT NULL
# MAGIC ) USING delta
# MAGIC LOCATION '/mnt/gold/dim_billing';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Sales fact table
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_sales (
# MAGIC   sale_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   sale_id STRING NOT NULL,
# MAGIC   date_key INT NOT NULL,
# MAGIC   product_key BIGINT NOT NULL,
# MAGIC   customer_key BIGINT NOT NULL,
# MAGIC   pricing_key BIGINT NOT NULL,
# MAGIC   billing_key BIGINT NOT NULL,
# MAGIC   store_id STRING NOT NULL,
# MAGIC   quantity INT NOT NULL,
# MAGIC   net_amount DOUBLE NOT NULL,
# MAGIC   tax_amount DOUBLE NOT NULL,
# MAGIC   gross_amount DOUBLE NOT NULL,
# MAGIC   channel STRING NOT NULL,
# MAGIC   created_ts TIMESTAMP NOT NULL
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (date_key)
# MAGIC LOCATION '/mnt/gold/fact_sales';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Sales summary (aggregated)
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_sales_summary (
# MAGIC   date_key INT NOT NULL,
# MAGIC   product_key BIGINT NOT NULL,
# MAGIC   customer_key BIGINT NOT NULL,
# MAGIC   store_id STRING NOT NULL,
# MAGIC   channel STRING NOT NULL,
# MAGIC   total_quantity BIGINT NOT NULL,
# MAGIC   total_net_amount DOUBLE NOT NULL,
# MAGIC   total_tax_amount DOUBLE NOT NULL,
# MAGIC   total_gross_amount DOUBLE NOT NULL,
# MAGIC   transaction_count BIGINT NOT NULL,
# MAGIC   last_updated TIMESTAMP NOT NULL
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (date_key)
# MAGIC LOCATION '/mnt/gold/fact_sales_summary';

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Gold: Customer metrics
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_customer_metrics (
# MAGIC   customer_key BIGINT NOT NULL,
# MAGIC   date_key INT NOT NULL,
# MAGIC   total_orders BIGINT NOT NULL,
# MAGIC   total_spent DOUBLE NOT NULL,
# MAGIC   avg_order_value DOUBLE NOT NULL,
# MAGIC   last_order_date DATE,
# MAGIC   days_since_last_order INT,
# MAGIC   customer_lifetime_value DOUBLE NOT NULL
# MAGIC ) USING delta
# MAGIC PARTITIONED BY (date_key)
# MAGIC LOCATION '/mnt/gold/fact_customer_metrics';

# COMMAND ----------
# MAGIC %md
# MAGIC All tables created successfully! 
# MAGIC 
# MAGIC Next steps:
# MAGIC 1. Run bronze_ingest.py to load raw data
# MAGIC 2. Run silver_transform.py to clean data
# MAGIC 3. Run SCD2 notebooks to build dimensions
# MAGIC 4. Run fact_sales_merge.py to populate facts

