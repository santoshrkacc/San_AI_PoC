from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col


def ingest_raw_to_bronze(
    spark: SparkSession,
    source_path: str,
    bronze_table: str,
    format: str,
    read_options: Dict[str, str] | None = None,
) -> DataFrame:
    options = read_options or {}

    df = (
        spark.read.format(format)
        .options(**options)
        .load(source_path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    (df.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable(bronze_table))
    return spark.table(bronze_table)

