# Databricks notebook source
# MAGIC %md
# MAGIC Build SCD2 for dim_product

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.etl.scd2 import prepare_scd2_updates, scd2_merge

spark: SparkSession = spark

silver_products = spark.table("silver.products")

staged = (
    silver_products
    .withColumn("effective_from", F.coalesce("effective_from", F.current_timestamp()))
    .select(
        "product_id",
        "title",
        "category",
        "isbn",
        "brand",
        "effective_from",
    )
)

staged = prepare_scd2_updates(
    staged,
    business_keys=["product_id"],
    change_cols=["title", "category", "isbn", "brand"],
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS gold.dim_product (
  product_key BIGINT GENERATED ALWAYS AS IDENTITY,
  product_id STRING,
  title STRING,
  category STRING,
  isbn STRING,
  brand STRING,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  current_flag BOOLEAN
) USING delta
"""
)

scd2_merge(
    spark,
    staged_updates=staged,
    target_table="gold.dim_product",
    business_keys=["product_id"],
)

display(spark.table("gold.dim_product").orderBy(F.col("effective_from").desc()).limit(20))

