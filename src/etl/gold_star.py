from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_dim_date(spark: SparkSession, start: str, end: str, table: str) -> DataFrame:
    df = spark.sql(f"SELECT sequence(to_date('{start}'), to_date('{end}'), interval 1 day) AS d")
    df = df.select(F.explode("d").alias("date")).withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    df = (
        df.withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("dow", F.date_format("date", "E"))
    )
    df.write.mode("overwrite").format("delta").saveAsTable(table)
    return spark.table(table)


def assemble_fact_sales(
    silver_sales: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    dim_pricing: DataFrame,
    dim_billing: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    joined = (
        silver_sales
        .join(dim_customer, "customer_id", "left")
        .join(dim_product, "product_id", "left")
        .join(dim_pricing, ["product_id", "price_effective_date"], "left")
        .join(dim_billing, ["billing_id"], "left")
        .join(dim_date, F.to_date("sale_date") == dim_date["date"], "left")
    )
    result = joined.select(
        F.monotonically_increasing_id().alias("sale_key"),
        dim_date["date_key"],
        dim_product["product_key"],
        dim_customer["customer_key"],
        dim_pricing["pricing_key"],
        dim_billing["billing_key"],
        "store_id",
        "quantity",
        "net_amount",
        "tax_amount",
        "gross_amount",
        "channel",
    )
    return result

