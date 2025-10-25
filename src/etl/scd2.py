from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def prepare_scd2_updates(
    incoming: DataFrame,
    business_keys: Iterable[str],
    change_cols: Iterable[str],
    effective_from_col: str = "effective_from",
    effective_to_col: str = "effective_to",
    current_flag_col: str = "current_flag",
) -> DataFrame:
    window_spec = Window.partitionBy(*business_keys).orderBy(F.col(effective_from_col).asc())
    with_next = incoming.withColumn("_next_from", F.lead(F.col(effective_from_col)).over(window_spec))
    result = (
        with_next.withColumn(
            effective_to_col,
            F.coalesce("_next_from", F.to_timestamp(F.lit("9999-12-31 23:59:59"))),
        )
        .withColumn(current_flag_col, F.when(F.col(effective_to_col) >= F.current_timestamp(), F.lit(True)).otherwise(F.lit(False)))
        .drop("_next_from")
    )
    return result


def scd2_merge(
    spark,
    staged_updates: DataFrame,
    target_table: str,
    business_keys: Iterable[str],
    effective_from_col: str = "effective_from",
    effective_to_col: str = "effective_to",
    current_flag_col: str = "current_flag",
):
    staged_updates.createOrReplaceTempView("staged_updates")
    keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])
    non_key_cols = [c for c in staged_updates.columns if c not in list(business_keys)]
    change_pred = " OR ".join([f"t.{c} <> s.{c}" for c in non_key_cols if c not in [effective_from_col, effective_to_col, current_flag_col]]) or "false"

    merge_sql = f"""
MERGE INTO {target_table} t
USING staged_updates s
ON {keys_pred} AND t.{current_flag_col} = true
WHEN MATCHED AND ({change_pred}) THEN UPDATE SET
  {effective_to_col} = s.{effective_from_col},
  {current_flag_col} = false
WHEN NOT MATCHED THEN INSERT *
"""
    spark.sql(merge_sql)

