# Databricks notebook source
# MAGIC %md
# MAGIC Build SCD2 for dim_customer

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.etl.scd2 import prepare_scd2_updates, scd2_merge

spark: SparkSession = spark

silver_customers = spark.table("silver.customers")

staged = (
    silver_customers
    .withColumn("effective_from", F.coalesce("effective_from", F.current_timestamp()))
    .select(
        "customer_id",
        "name",
        "email",
        "phone",
        "country",
        "is_active",
        "effective_from",
    )
)

staged = prepare_scd2_updates(
    staged,
    business_keys=["customer_id"],
    change_cols=["name", "email", "phone", "country", "is_active"],
)

spark.sql(
    """
CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  country STRING,
  is_active BOOLEAN,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP,
  current_flag BOOLEAN
) USING delta
"""
)

scd2_merge(
    spark,
    staged_updates=staged,
    target_table="gold.dim_customer",
    business_keys=["customer_id"],
)

display(spark.table("gold.dim_customer").orderBy(F.col("effective_from").desc()).limit(20))

