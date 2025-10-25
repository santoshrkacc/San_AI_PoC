# Databricks notebook source
# MAGIC %md
# MAGIC Bronze ingest (raw → bronze)

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_path", "/mnt/raw/sales/")
dbutils.widgets.text("bronze_table", "bronze.sales_raw")
dbutils.widgets.text("format", "json")

# COMMAND ----------
from pyspark.sql import SparkSession
from src.etl.bronze_ingest import ingest_raw_to_bronze

spark: SparkSession = spark

df = ingest_raw_to_bronze(
    spark,
    source_path=dbutils.widgets.get("source_path"),
    bronze_table=dbutils.widgets.get("bronze_table"),
    format=dbutils.widgets.get("format"),
    read_options={"multiLine": "true", "mode": "PERMISSIVE"},
)

display(df.limit(10))

