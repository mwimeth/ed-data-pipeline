"""
profile_dates.py
-----------------
Exploratory script: before deciding how to parse date columns, discover
how many distinct DATE FORMATS actually exist in the raw data — not by
reading rows one by one, but by reducing every value to its structural
"shape" (digits -> 9, letters -> X) and counting distinct shapes.

This scales to millions of rows because Spark does the pattern-matching
across the whole dataset in parallel, and the OUTPUT is small (a handful
of distinct patterns) even when the INPUT is huge.

Run from the project root:
    python3 src/profile_dates.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "data/raw/ed_attendances_raw.csv"


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ed-pipeline-profile-dates")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def to_shape(column: str):
    """Turn a date string into its structural shape: every digit -> 9,
    every letter -> X. e.g. '05-Mar-2025 14:30' -> '99-XXX-9999 99:99'."""
    digits_replaced = F.regexp_replace(F.col(column), r"\d", "9")
    letters_replaced = F.regexp_replace(digits_replaced, r"[A-Za-z]", "X")
    return letters_replaced


def profile_column(df, column: str):
    """Show every distinct shape found in a column, with a count and one
    real example of each, so we can confirm what the pattern actually
    represents before writing any parsing logic."""
    shaped = df.withColumn("shape", to_shape(column))

    print(f"\n{'=' * 60}")
    print(f"Distinct shapes found in '{column}'")
    print(f"{'=' * 60}")

    profile = (
        shaped.groupBy("shape")
        .agg(
            F.count("*").alias("row_count"),
            F.first(column).alias("example_value"),
        )
        .orderBy(F.desc("row_count"))
    )
    profile.show(20, truncate=False)


if __name__ == "__main__":
    spark = get_spark()

    df = spark.read.csv(RAW_PATH, header=True, inferSchema=False)
    print("Total rows in raw file:", df.count())

    profile_column(df, "arrival_time")
    profile_column(df, "departure_time")

    spark.stop()