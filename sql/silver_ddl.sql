-- Silver Layer DDL (Cleansed)
-- These tables store cleaned and standardized data

-- Silver: Cleaned sales data
CREATE TABLE IF NOT EXISTS silver.sales (
  sale_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  sale_date DATE NOT NULL,
  quantity INT NOT NULL,
  unit_price DOUBLE NOT NULL,
  tax_rate DOUBLE NOT NULL,
  store_id STRING NOT NULL,
  channel STRING NOT NULL,
  net_amount DOUBLE NOT NULL,
  tax_amount DOUBLE NOT NULL,
  gross_amount DOUBLE NOT NULL,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING delta
PARTITIONED BY (sale_date)
LOCATION '/mnt/silver/sales';

-- Silver: Cleaned customer data
CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id STRING NOT NULL,
  name STRING NOT NULL,
  email STRING NOT NULL,
  phone STRING,
  address STRING,
  country STRING NOT NULL,
  registration_date DATE,
  is_active BOOLEAN NOT NULL,
  effective_from TIMESTAMP,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING delta
LOCATION '/mnt/silver/customers';

-- Silver: Cleaned product data
CREATE TABLE IF NOT EXISTS silver.products (
  product_id STRING NOT NULL,
  title STRING NOT NULL,
  category STRING NOT NULL,
  isbn STRING,
  brand STRING,
  description STRING,
  effective_from TIMESTAMP,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING delta
LOCATION '/mnt/silver/products';

-- Silver: Cleaned pricing data
CREATE TABLE IF NOT EXISTS silver.pricing (
  product_id STRING NOT NULL,
  currency STRING NOT NULL,
  list_price DOUBLE NOT NULL,
  discount DOUBLE NOT NULL,
  tax_rate DOUBLE NOT NULL,
  price_effective_date DATE NOT NULL,
  effective_from TIMESTAMP,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING delta
PARTITIONED BY (price_effective_date)
LOCATION '/mnt/silver/pricing';

-- Silver: Cleaned billing data
CREATE TABLE IF NOT EXISTS silver.billing (
  billing_id STRING NOT NULL,
  invoice_id STRING NOT NULL,
  payment_method STRING NOT NULL,
  terms STRING,
  status STRING NOT NULL,
  amount DOUBLE NOT NULL,
  _ingest_ts TIMESTAMP,
  _source_file STRING
) USING delta
LOCATION '/mnt/silver/billing';

-- Silver: Data Quality Results
CREATE TABLE IF NOT EXISTS silver.dq_results (
  batch_id STRING NOT NULL,
  table_name STRING NOT NULL,
  rule_name STRING NOT NULL,
  rule_type STRING NOT NULL,
  passed_count BIGINT NOT NULL,
  failed_count BIGINT NOT NULL,
  total_count BIGINT NOT NULL,
  failure_rate DOUBLE NOT NULL,
  execution_ts TIMESTAMP NOT NULL,
  details STRING
) USING delta
LOCATION '/mnt/silver/dq_results';
