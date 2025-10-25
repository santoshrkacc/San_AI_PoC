# Databricks notebook source
# MAGIC %md
# MAGIC Gold star schema build

# COMMAND ----------
from pyspark.sql import SparkSession
from src.etl.gold_star import build_dim_date, assemble_fact_sales

spark: SparkSession = spark

dim_date = build_dim_date(spark, "2020-01-01", "2030-12-31", "gold.dim_date")

# Example: assuming dim tables already built elsewhere and silver sales ready
silver_sales = spark.table("silver.sales")
dim_customer = spark.table("gold.dim_customer")
dim_product = spark.table("gold.dim_product")
dim_pricing = spark.table("gold.dim_pricing")
dim_billing = spark.table("gold.dim_billing")

fact_sales = assemble_fact_sales(
    silver_sales, dim_customer, dim_product, dim_pricing, dim_billing, dim_date
)
fact_sales.write.mode("overwrite").format("delta").saveAsTable("gold.fact_sales")
display(spark.table("gold.fact_sales").limit(10))

