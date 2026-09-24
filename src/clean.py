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
############ Milestone 4: standardise text columns ############
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


######### Milestone 5: check that the cleaned data is as expected #########
DATE_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "dd/MM/yyyy HH:mm",
    "dd-MMM-yyyy HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
]


def parse_dates(df: DataFrame) -> DataFrame:
    """Step 3: parse arrival_time and departure_time, trying each known
    format until one works. Coalesce picks the first non-null result."""
    for column in ["arrival_time", "departure_time"]:
        parsed_attempts = [
            F.try_to_timestamp(F.col(column), F.lit(fmt)) for fmt in DATE_FORMATS
        ]
        df = df.withColumn(column, F.coalesce(*parsed_attempts))
    return df


########## Milestone 6:Fix triage code, GP codes and fill missing complaints / dispositions #########
GP_CODE_PATTERN = r"^[A-Z]\d{5}$"  # one capital letter followed by 5 digits


def fix_triage_and_gp_codes(df: DataFrame) -> DataFrame:
    """Step 4:
    - triage_category: strip a trailing '.0' (e.g. '3.0' -> '3'), keep as string for now
      (we'll validate it's a real 1-5 value in the reject-rules step later)
    - gp_practice_code: upper-case it, then check it matches the expected
      pattern; anything that doesn't match becomes null, with a flag column
      recording that it was corrected/rejected
    - presenting_complaint / disposition: fill missing with 'Unknown'
      (these are recoverable - we don't want to reject a whole row just
      because this one field was blank)
    """
    df = df.withColumn(
        "triage_category",
        F.regexp_replace(F.col("triage_category"), r"\.0$", ""),
    )

    upper_gp = F.upper(F.col("gp_practice_code"))
    df = df.withColumn(
        "gp_code_flag",
        F.when(F.col("gp_practice_code").isNull(), F.lit("missing"))
         .when(~upper_gp.rlike(GP_CODE_PATTERN), F.lit("malformed"))
         .otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "gp_practice_code",
        F.when(upper_gp.rlike(GP_CODE_PATTERN), upper_gp).otherwise(F.lit(None)),
    )

    df = df.withColumn(
        "presenting_complaint",
        F.coalesce(F.col("presenting_complaint"), F.lit("Unknown")),
    )
    df = df.withColumn(
        "disposition",
        F.coalesce(F.col("disposition"), F.lit("Unknown")),
    )

    return df

############ Milestone 7: remove exact duplicate rows ############

def remove_duplicates(df: DataFrame) -> DataFrame:
    """Step 5: remove exact duplicate rows, keeping the first occurrence
    of each attendance_id. The raw data has 350 rows that are exact
    copies of another row further up the file."""
    return df.dropDuplicates(["attendance_id"])


############ Milestone 8: reject rules ############

def apply_reject_rules(df: DataFrame):
    """Step 6: check each row against 7 reject rules. Any row that fails
    at least one rule goes to 'rejects' (with the reasons recorded);
    everything else goes to 'clean'. Returns (clean_df, rejects_df)."""

    valid_triage = F.col("triage_category").isin(["1", "2", "3", "4", "5"])
    both_times_present = F.col("arrival_time").isNotNull() & F.col("departure_time").isNotNull()
    length_of_stay_min = (
        F.col("departure_time").cast("long") - F.col("arrival_time").cast("long")
    ) / 60

    all_checks = F.array(
        F.when(F.col("arrival_time").isNull(), F.lit("missing_arrival_time")),
        F.when(F.col("departure_time").isNull(), F.lit("missing_departure_time")),
        F.when(both_times_present & (F.col("departure_time") < F.col("arrival_time")),
               F.lit("departure_before_arrival")),
        F.when(~valid_triage, F.lit("invalid_triage_code")),
        F.when(F.col("arrival_time").isNotNull() &
               ((F.year("arrival_time") < 2025) | (F.year("arrival_time") >= 2026)),
               F.lit("future_dated_attendance")),
        F.when(both_times_present & (length_of_stay_min > 2880),
               F.lit("extreme_length_of_stay")),
        F.when(F.col("pseudo_patient_id").isNull(), F.lit("missing_patient_id")),
    )
    df = df.withColumn(
        "failure_reasons",
        F.filter(all_checks, lambda x: x.isNotNull()),
    )

    clean_df = df.filter(F.size(F.col("failure_reasons")) == 0).drop("failure_reasons")
    rejects_df = df.filter(F.size(F.col("failure_reasons")) > 0)

    return clean_df, rejects_df


def build_dq_report(rejects_df, total_input_rows: int):
    """Step 7a: summarise how many rows failed each rule, and what
    share of the input that represents. This is the report that would
    be escalated to whoever owns the source data."""
    report = (
        rejects_df.select(F.explode("failure_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .withColumn("pct_of_input", F.round(F.col("count") / F.lit(total_input_rows) * 100, 2))
        .orderBy(F.desc("count"))
    )
    return report

########### Milestone 9: check rejects against the answer key ###########
def check_against_answer_key(rejects_df, spark: SparkSession):
    """Step 7b: compare our rejected attendance_ids against the answer
    key that generate_data.py produced, to prove our rules caught
    exactly the errors that were planted (no more, no fewer)."""
    reject_rule_names = [
        "missing_arrival_time", "missing_departure_time",
        "departure_before_arrival", "invalid_triage_code",
        "future_dated_attendance", "extreme_length_of_stay",
        "missing_patient_id",
    ]

    key = spark.read.csv(
        "data/answer_key/injected_errors_log.csv", header=True, inferSchema=False
    )
    key_reject_ids = (
        key.filter(F.col("error_type").isin(reject_rule_names))
        .select("attendance_id").distinct()
    )

    my_reject_ids = rejects_df.select("attendance_id").distinct()

    matched = my_reject_ids.intersect(key_reject_ids).count()
    only_mine = my_reject_ids.subtract(key_reject_ids).count()
    only_key = key_reject_ids.subtract(my_reject_ids).count()

    print(f"Answer key reject IDs: {key_reject_ids.count()}")
    print(f"My reject IDs: {my_reject_ids.count()}")
    print(f"Matched (expect equal to both above): {matched}")
    print(f"I rejected but key didn't (expect 0): {only_mine}")
    print(f"Key rejected but I didn't (expect 0): {only_key}")


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

    step3 = parse_dates(step2)

    print("\nArrival timestamps null after parsing (expect 100, same as before parsing):",
          step3.filter(F.col("arrival_time").isNull()).count())
    print("Departure timestamps null after parsing (expect 200, same as before parsing):",
          step3.filter(F.col("departure_time").isNull()).count())


    step4 = fix_triage_and_gp_codes(step3)

    print("\nTriage values still ending in '.0' (expect 0):",
          step4.filter(F.col("triage_category").rlike(r"\.0$")).count())

    print("GP code flag counts:")
    step4.groupBy("gp_code_flag").count().show()

    print("Presenting complaint nulls after fill (expect 0):",
          step4.filter(F.col("presenting_complaint").isNull()).count())
    print("Disposition nulls after fill (expect 0):",
          step4.filter(F.col("disposition").isNull()).count())

    
    step5 = remove_duplicates(step4)
    print("\nRow count before removing duplicates:", step4.count())
    print("Row count after removing duplicates (expect 20000):", step5.count())
    print("Distinct attendance_ids after dedupe (expect 20000):",
          step5.select("attendance_id").distinct().count())


    clean_df, rejects_df = apply_reject_rules(step5)

    print("\nClean rows (expect 19150):", clean_df.count())
    print("Rejected rows (expect 850):", rejects_df.count())

    print("\nRejects by reason (a row can have more than one reason):")
    rejects_df.select(F.explode("failure_reasons").alias("reason")) \
        .groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False)


    report = build_dq_report(rejects_df, step5.count())
    print("\nData quality report:")
    report.show(truncate=False)

    print("Checking rejects against the answer key:")
    check_against_answer_key(rejects_df, spark)

    
    spark.stop()