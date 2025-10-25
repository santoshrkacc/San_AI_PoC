-- Bronze Layer DDL (Raw Curated)
-- These tables store raw data from ADLS with minimal processing

-- Bronze: Raw sales data
CREATE TABLE IF NOT EXISTS bronze.sales_raw (
  sale_id STRING,
  customer_id STRING,
  product_id STRING,
  sale_date STRING,
  quantity INT,
  unit_price DOUBLE,
  tax_rate DOUBLE,
  store_id STRING,
  channel STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING,
  _corrupt_record STRING
) USING delta
PARTITIONED BY (sale_date)
LOCATION '/mnt/bronze/sales_raw';

-- Bronze: Raw customer data
CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  customer_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  country STRING,
  registration_date STRING,
  is_active BOOLEAN,
  _ingest_ts TIMESTAMP,
  _source_file STRING,
  _corrupt_record STRING
) USING delta
LOCATION '/mnt/bronze/customers_raw';

-- Bronze: Raw product data
CREATE TABLE IF NOT EXISTS bronze.products_raw (
  product_id STRING,
  title STRING,
  category STRING,
  isbn STRING,
  brand STRING,
  description STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING,
  _corrupt_record STRING
) USING delta
LOCATION '/mnt/bronze/products_raw';

-- Bronze: Raw pricing data
CREATE TABLE IF NOT EXISTS bronze.pricing_raw (
  product_id STRING,
  currency STRING,
  list_price DOUBLE,
  discount DOUBLE,
  tax_rate DOUBLE,
  effective_date STRING,
  _ingest_ts TIMESTAMP,
  _source_file STRING,
  _corrupt_record STRING
) USING delta
PARTITIONED BY (effective_date)
LOCATION '/mnt/bronze/pricing_raw';

-- Bronze: Raw billing data
CREATE TABLE IF NOT EXISTS bronze.billing_raw (
  billing_id STRING,
  invoice_id STRING,
  payment_method STRING,
  terms STRING,
  status STRING,
  amount DOUBLE,
  _ingest_ts TIMESTAMP,
  _source_file STRING,
  _corrupt_record STRING
) USING delta
LOCATION '/mnt/bronze/billing_raw';
