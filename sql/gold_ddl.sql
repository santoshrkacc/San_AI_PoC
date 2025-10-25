-- Gold Layer DDL (Star Schema)
-- These tables store the final star schema for analytics

-- Gold: Date dimension (static calendar)
CREATE TABLE IF NOT EXISTS gold.dim_date (
  date_key INT NOT NULL,
  date DATE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  dow STRING NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  is_holiday BOOLEAN NOT NULL
) USING delta
LOCATION '/mnt/gold/dim_date';

-- Gold: Customer dimension (SCD2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING NOT NULL,
  name STRING NOT NULL,
  email STRING NOT NULL,
  phone STRING,
  country STRING NOT NULL,
  is_active BOOLEAN NOT NULL,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP NOT NULL,
  current_flag BOOLEAN NOT NULL
) USING delta
LOCATION '/mnt/gold/dim_customer';

-- Gold: Product dimension (SCD2)
CREATE TABLE IF NOT EXISTS gold.dim_product (
  product_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING NOT NULL,
  title STRING NOT NULL,
  category STRING NOT NULL,
  isbn STRING,
  brand STRING,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP NOT NULL,
  current_flag BOOLEAN NOT NULL
) USING delta
LOCATION '/mnt/gold/dim_product';

-- Gold: Pricing dimension (SCD2)
CREATE TABLE IF NOT EXISTS gold.dim_pricing (
  pricing_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING NOT NULL,
  currency STRING NOT NULL,
  list_price DOUBLE NOT NULL,
  discount DOUBLE NOT NULL,
  tax_rate DOUBLE NOT NULL,
  effective_from TIMESTAMP NOT NULL,
  effective_to TIMESTAMP NOT NULL,
  current_flag BOOLEAN NOT NULL
) USING delta
LOCATION '/mnt/gold/dim_pricing';

-- Gold: Billing dimension (Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_billing (
  billing_key BIGINT GENERATED ALWAYS AS IDENTITY,
  billing_id STRING NOT NULL,
  payment_method STRING NOT NULL,
  terms STRING,
  status STRING NOT NULL
) USING delta
LOCATION '/mnt/gold/dim_billing';

-- Gold: Sales fact table
CREATE TABLE IF NOT EXISTS gold.fact_sales (
  sale_key BIGINT GENERATED ALWAYS AS IDENTITY,
  sale_id STRING NOT NULL,
  date_key INT NOT NULL,
  product_key BIGINT NOT NULL,
  customer_key BIGINT NOT NULL,
  pricing_key BIGINT NOT NULL,
  billing_key BIGINT NOT NULL,
  store_id STRING NOT NULL,
  quantity INT NOT NULL,
  net_amount DOUBLE NOT NULL,
  tax_amount DOUBLE NOT NULL,
  gross_amount DOUBLE NOT NULL,
  channel STRING NOT NULL,
  created_ts TIMESTAMP NOT NULL
) USING delta
PARTITIONED BY (date_key)
LOCATION '/mnt/gold/fact_sales';

-- Gold: Sales summary (aggregated)
CREATE TABLE IF NOT EXISTS gold.fact_sales_summary (
  date_key INT NOT NULL,
  product_key BIGINT NOT NULL,
  customer_key BIGINT NOT NULL,
  store_id STRING NOT NULL,
  channel STRING NOT NULL,
  total_quantity BIGINT NOT NULL,
  total_net_amount DOUBLE NOT NULL,
  total_tax_amount DOUBLE NOT NULL,
  total_gross_amount DOUBLE NOT NULL,
  transaction_count BIGINT NOT NULL,
  last_updated TIMESTAMP NOT NULL
) USING delta
PARTITIONED BY (date_key)
LOCATION '/mnt/gold/fact_sales_summary';

-- Gold: Customer metrics
CREATE TABLE IF NOT EXISTS gold.fact_customer_metrics (
  customer_key BIGINT NOT NULL,
  date_key INT NOT NULL,
  total_orders BIGINT NOT NULL,
  total_spent DOUBLE NOT NULL,
  avg_order_value DOUBLE NOT NULL,
  last_order_date DATE,
  days_since_last_order INT,
  customer_lifetime_value DOUBLE NOT NULL
) USING delta
PARTITIONED BY (date_key)
LOCATION '/mnt/gold/fact_customer_metrics';
