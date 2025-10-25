from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def standardize_columns(df: DataFrame, to_lower: Iterable[str]) -> DataFrame:
    result = df
    for c in to_lower:
        if c in df.columns:
            result = result.withColumn(c, F.lower(F.trim(F.col(c))))
    return result


def deduplicate_latest(df: DataFrame, keys: list[str], ts_col: str = "_ingest_ts") -> DataFrame:
    window_spec = Window.partitionBy(*keys).orderBy(F.col(ts_col).desc())
    ranked = df.withColumn("_rn", F.row_number().over(window_spec))
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def validate_positive(df: DataFrame, column: str) -> DataFrame:
    return df.filter(F.col(column) >= 0)

