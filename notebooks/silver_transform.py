# Databricks notebook source
# MAGIC %md
# MAGIC Silver transform (bronze → silver)

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.etl.silver_transform import standardize_columns, deduplicate_latest, validate_positive

spark: SparkSession = spark

bronze_tbl = "bronze.sales_raw"
silver_tbl = "silver.sales"

df = spark.table(bronze_tbl)
df = standardize_columns(df, to_lower=["email", "channel"])
df = deduplicate_latest(df, keys=["sale_id"])  # assumes `_ingest_ts` is present
df = validate_positive(df, "quantity")
df = df.withColumn("net_amount", F.col("unit_price") * F.col("quantity"))
df = df.withColumn("tax_amount", F.col("net_amount") * F.col("tax_rate"))
df = df.withColumn("gross_amount", F.col("net_amount") + F.col("tax_amount"))

df.write.mode("overwrite").format("delta").saveAsTable(silver_tbl)
display(spark.table(silver_tbl).limit(10))

