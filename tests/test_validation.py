"""
test_validation.py
-------------------
Unit tests for the cleaning and validation logic in src/clean.py.

These use small, hand-built rows (not the full 20,000-row dataset) so
each test is fast and checks exactly one behaviour in isolation.

Run from the project root:
    pytest
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
from pyspark.sql import Row

from clean import get_spark, fix_triage_and_gp_codes, apply_reject_rules


@pytest.fixture(scope="module")
def spark():
    s = get_spark()
    yield s
    s.stop()


def make_row(**overrides):
    """A baseline valid attendance row; pass overrides to break one field
    at a time, so each test only changes what it's actually checking."""
    base = dict(
        attendance_id="ATT000001",
        pseudo_patient_id="PSN0000001",
        arrival_time="2025-06-01 10:00:00",
        departure_time="2025-06-01 12:00:00",
        triage_category="3",
        presenting_complaint="Headache",
        disposition="Discharged home",
        gp_practice_code="E12345",
    )
    base.update(overrides)
    return base


def run_pipeline_on_rows(spark, rows):
    """Parse timestamps on our hand-built rows (they're already valid
    ISO strings, so a direct cast is enough here) and run the reject
    rules, returning (clean_df, rejects_df)."""
    df = spark.createDataFrame([Row(**r) for r in rows])
    df = df.withColumn("arrival_time", df.arrival_time.cast("timestamp"))
    df = df.withColumn("departure_time", df.departure_time.cast("timestamp"))
    return apply_reject_rules(df)


def test_valid_row_is_clean(spark):
    clean_df, rejects_df = run_pipeline_on_rows(spark, [make_row()])
    assert clean_df.count() == 1
    assert rejects_df.count() == 0


def test_departure_before_arrival_is_rejected(spark):
    row = make_row(arrival_time="2025-06-01 12:00:00", departure_time="2025-06-01 10:00:00")
    clean_df, rejects_df = run_pipeline_on_rows(spark, [row])
    assert clean_df.count() == 0
    assert rejects_df.count() == 1
    reasons = rejects_df.collect()[0]["failure_reasons"]
    assert "departure_before_arrival" in reasons


def test_invalid_triage_code_is_rejected(spark):
    row = make_row(triage_category="X")
    clean_df, rejects_df = run_pipeline_on_rows(spark, [row])
    assert clean_df.count() == 0
    reasons = rejects_df.collect()[0]["failure_reasons"]
    assert "invalid_triage_code" in reasons


def test_triage_decimal_format_is_fixed(spark):
    """'3.0' should be stripped down to '3' by fix_triage_and_gp_codes,
    and should then count as VALID (not rejected)."""
    row = make_row(triage_category="3.0")
    df = spark.createDataFrame([Row(**row)])
    df = df.withColumn("arrival_time", df.arrival_time.cast("timestamp"))
    df = df.withColumn("departure_time", df.departure_time.cast("timestamp"))
    df = fix_triage_and_gp_codes(df)
    assert df.collect()[0]["triage_category"] == "3"

    clean_df, rejects_df = apply_reject_rules(df)
    assert clean_df.count() == 1
    assert rejects_df.count() == 0


def test_length_of_stay_boundary(spark):
    """Exactly 2880 minutes (48 hours) should PASS; one minute over
    should FAIL. This checks the rule's edge, not just an obvious case."""
    exactly_48h = make_row(
        attendance_id="ATT000002",
        arrival_time="2025-06-01 00:00:00",
        departure_time="2025-06-03 00:00:00",  # exactly 2880 minutes
    )
    just_over_48h = make_row(
        attendance_id="ATT000003",
        arrival_time="2025-06-01 00:00:00",
        departure_time="2025-06-03 00:01:00",  # 2881 minutes
    )
    clean_df, rejects_df = run_pipeline_on_rows(spark, [exactly_48h, just_over_48h])

    assert clean_df.count() == 1
    assert clean_df.collect()[0]["attendance_id"] == "ATT000002"

    assert rejects_df.count() == 1
    rejected_row = rejects_df.collect()[0]
    assert rejected_row["attendance_id"] == "ATT000003"
    assert "extreme_length_of_stay" in rejected_row["failure_reasons"]