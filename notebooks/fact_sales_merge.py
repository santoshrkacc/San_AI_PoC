# Databricks notebook source
# MAGIC %md
# MAGIC Upsert into gold.fact_sales using MERGE

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ensure spark exists when running outside Databricks (e.g., local/script)
try:
    spark  # type: ignore[name-defined]
except NameError:  # pragma: no cover
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("fact-sales-merge")
        .getOrCreate()
    )

# Assumes dims and silver.sales exist
silver_sales = spark.table("silver.sales")
dim_customer = spark.table("gold.dim_customer").filter("current_flag = true")
dim_product = spark.table("gold.dim_product").filter("current_flag = true")
dim_pricing = spark.table("gold.dim_pricing").filter("current_flag = true")
dim_billing = spark.table("gold.dim_billing")
dim_date = spark.table("gold.dim_date")

staged = (
    silver_sales
    .join(dim_customer, "customer_id", "left")
    .join(dim_product, "product_id", "left")
    .join(dim_pricing, ["product_id"], "left")
    .join(dim_billing, ["billing_id"], "left")
    .join(dim_date, F.to_date("sale_date") == dim_date["date"], "left")
    .selectExpr(
        "sale_id",
        "date_key",
        "product_key",
        "customer_key",
        "pricing_key",
        "billing_key",
        "store_id",
        "quantity",
        "net_amount",
        "tax_amount",
        "gross_amount",
        "channel",
    )
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS gold.fact_sales (
  sale_id STRING,
  date_key INT,
  product_key BIGINT,
  customer_key BIGINT,
  pricing_key BIGINT,
  billing_key BIGINT,
  store_id STRING,
  quantity INT,
  net_amount DOUBLE,
  tax_amount DOUBLE,
  gross_amount DOUBLE,
  channel STRING
) USING delta
"""
)

staged.createOrReplaceTempView("staged_fact")

spark.sql(
    """
MERGE INTO gold.fact_sales t
USING staged_fact s
ON t.sale_id = s.sale_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""
)

display(spark.table("gold.fact_sales").limit(20))

