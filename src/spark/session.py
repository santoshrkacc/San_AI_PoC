from __future__ import annotations

from typing import Dict, Optional

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "ai-exp-pyspark", extra_configs: Optional[Dict[str, str]] = None) -> SparkSession:

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()

