"""
clean.py
--------
Cleaning steps for the raw ED attendance file.
We build this one step at a time. Each step is a small function that takes a
Spark DataFrame in and gives a Spark DataFrame back.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW_PATH = "data/raw/ed_attendances_raw.csv"

# Every way the raw file writes "this value is missing"
NULL_MARKERS = ["", "NULL", "N/A", "n/a", "-"]


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ed-pipeline-clean")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_raw(spark: SparkSession) -> DataFrame:
    """Read the CSV with every column as text (we control the types later)."""
    return spark.read.csv(RAW_PATH, header=True, inferSchema=False)


def trim_and_nullify(df: DataFrame) -> DataFrame:
    """Step 1: strip spaces from every column and turn missing-value
    spellings (empty, NULL, N/A, n/a, -) into real nulls."""
    for column in df.columns:
        trimmed = F.trim(F.col(column))
        df = df.withColumn(
            column,
            F.when(trimmed.isin(NULL_MARKERS), F.lit(None)).otherwise(trimmed),
        )
    return df


def count_nulls(df: DataFrame) -> DataFrame:
    """One row showing how many nulls each column has."""
    return df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )


if __name__ == "__main__":
    spark = get_spark()

    raw = read_raw(spark)
    print("Rows read:", raw.count())

    print("\nNulls BEFORE step 1 (as Spark read the file):")
    count_nulls(raw).show()

    step1 = trim_and_nullify(raw)
    print("Nulls AFTER step 1:")
    count_nulls(step1).show()

    padded_before = raw.filter(F.col("presenting_complaint") != F.trim(F.col("presenting_complaint"))).count()
    padded_after = step1.filter(F.col("presenting_complaint") != F.trim(F.col("presenting_complaint"))).count()
    print("Complaints with stray spaces before:", padded_before, "| after:", padded_after)

    spark.stop()