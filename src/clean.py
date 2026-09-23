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
NULL_MARKERS = ["", "null", "n/a", "na", "-", "none", "nil"]


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
    spellings (any case) into real nulls."""
    for column in df.columns:
        trimmed = F.trim(F.col(column))
        normalised = F.lower(trimmed)
        df = df.withColumn(
            column,
            F.when(normalised.isin(NULL_MARKERS), F.lit(None)).otherwise(trimmed),
        )
    return df


def count_nulls(df: DataFrame) -> DataFrame:
    """One row showing how many nulls each column has."""
    return df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )

COMPLAINTS = [
    "Abdominal pain", "Chest pain", "Shortness of breath",
    "Minor injury - limb", "Head injury", "Fall", "Laceration / wound",
    "Back pain", "Fever", "Headache", "Allergic reaction",
    "Urinary symptoms", "Vomiting / diarrhoea", "Eye problem", "Rash",
    "Palpitations",
]

DISPOSITIONS = [
    "Discharged home", "Admitted to ward", "Transferred to another provider",
    "Discharged with follow-up", "Referred to GP",
    "Left before treatment complete",
]


def _standardise_column(df: DataFrame, column: str, reference_values: list) -> DataFrame:
    mapping = {v.lower(): v for v in reference_values}
    mapping_expr = F.create_map([F.lit(x) for pair in mapping.items() for x in pair])
    lower_col = F.lower(F.col(column))
    df = df.withColumn(
        column,
        F.when(F.col(column).isNull(), F.lit(None))
         .when(mapping_expr[lower_col].isNotNull(), mapping_expr[lower_col])
         .otherwise(F.col(column)),
    )
    return df


def standardise_text(df: DataFrame) -> DataFrame:
    df = _standardise_column(df, "presenting_complaint", COMPLAINTS)
    df = _standardise_column(df, "disposition", DISPOSITIONS)
    return df


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


    step2 = standardise_text(step1)

    print("\nDistinct complaint values (expect 16, or 17 counting null):",
          step2.select("presenting_complaint").distinct().count())
    print("Distinct disposition values (expect 6, or 7 counting null):",
          step2.select("disposition").distinct().count())

    unmatched = step2.filter(
        (~F.col("presenting_complaint").isin(COMPLAINTS)) & F.col("presenting_complaint").isNotNull()
    ).count()
    print("Complaint values that didn't match the reference list (expect 0):", unmatched)

    spark.stop()