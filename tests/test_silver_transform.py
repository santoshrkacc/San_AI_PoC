from chispa import assert_df_equality
from pyspark.sql import functions as F

from src.etl.silver_transform import standardize_columns, validate_positive


def test_standardize_and_validate(spark):
    input_rows = [(" A@B.com ", " WEB ", 1, 10.0), ("c@d.com", "store", -5, 10.0)]
    df = spark.createDataFrame(input_rows, ["email", "channel", "quantity", "unit_price"])
    out = standardize_columns(df, ["email", "channel"])
    out = validate_positive(out, "quantity")
    out = out.withColumn("net_amount", F.col("quantity") * F.col("unit_price"))

    expected_rows = [("a@b.com", "web", 1, 10.0, 10.0)]
    expected = spark.createDataFrame(expected_rows, ["email", "channel", "quantity", "unit_price", "net_amount"])
    assert_df_equality(out.select(expected.columns), expected, ignore_nullable=True)

