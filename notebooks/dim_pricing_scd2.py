# Databricks notebook source
# MAGIC %md
# MAGIC Build SCD2 for dim_pricing

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.etl.scd2 import prepare_scd2_updates, scd2_merge

spark: SparkSession = spark

silver_pricing = spark.table("silver.pricing")

staged = (
    silver_pricing
    .withColumn("effective_from", F.coalesce("effective_from", F.current_timestamp()))
    .select(
        "product_id",
        "currency",
        "list_price",
        "discount",
        "tax_rate",
        "effective_from",
    )
)

staged = prepare_scd2_updates(
    staged,
    business_keys=["product_id"],
    change_cols=["currency", "list_price", "discount", "tax_rate"],
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS gold.dim_pricing (
  pricing_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING,
  currency STRING,
  list_price DOUBLE,
  discount DOUBLE,
  tax_rate DOUBLE,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  current_flag BOOLEAN
) USING delta
"""
)

scd2_merge(
    spark,
    staged_updates=staged,
    target_table="gold.dim_pricing",
    business_keys=["product_id"],
)

display(spark.table("gold.dim_pricing").orderBy(F.col("effective_from").desc()).limit(20))

