"""
run_pipeline.py
----------------
Runs the whole pipeline end to end, in order:
  read raw -> clean -> validate -> report -> load to DuckDB

Every stage logs what it did (row counts in/out, duration) to both the
console and logs/pipeline.log, so a failure or a surprising result can
be diagnosed from the log rather than by re-running everything.

Run from the project root:
    python3 src/run_pipeline.py
"""

import logging
import time
from pathlib import Path

from clean import (
    get_spark, read_raw, trim_and_nullify, standardise_text, parse_dates,
    fix_triage_and_gp_codes, remove_duplicates, apply_reject_rules,
    build_dq_report, check_against_answer_key,
)
from load import load_clean_to_duckdb

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),                                   # console
        logging.FileHandler(LOG_DIR / "pipeline.log", mode="a"),    # file
    ],
)
log = logging.getLogger("ed_pipeline")


def main():
    log.info("=" * 60)
    log.info("Pipeline run starting")

    spark = get_spark()
    start = time.time()

    raw = read_raw(spark)
    raw_count = raw.count()
    log.info(f"START  read_raw -> {raw_count} rows read")

    df = trim_and_nullify(raw)
    log.info("FINISH trim_and_nullify")

    df = standardise_text(df)
    log.info("FINISH standardise_text")

    df = parse_dates(df)
    log.info("FINISH parse_dates")

    df = fix_triage_and_gp_codes(df)
    log.info("FINISH fix_triage_and_gp_codes")

    df = remove_duplicates(df)
    deduped_count = df.count()
    log.info(f"FINISH remove_duplicates -> {deduped_count} rows "
              f"({raw_count - deduped_count} duplicates removed)")

    clean_df, rejects_df = apply_reject_rules(df)
    clean_count = clean_df.count()
    rejects_count = rejects_df.count()
    log.info(f"FINISH apply_reject_rules -> {clean_count} clean, "
              f"{rejects_count} rejected")

    report = build_dq_report(rejects_df, deduped_count)
    report_pdf = report.toPandas()
    Path("reports").mkdir(exist_ok=True)
    report_pdf.to_csv("reports/dq_report.csv", index=False)
    log.info(f"FINISH build_dq_report -> written to reports/dq_report.csv "
              f"({len(report_pdf)} reasons)")

    log.info("START  check_against_answer_key")
    check_against_answer_key(rejects_df, spark)
    log.info("FINISH check_against_answer_key")

    clean_pdf = clean_df.toPandas()
    loaded_count = load_clean_to_duckdb(clean_pdf)
    log.info(f"FINISH load_clean_to_duckdb -> {loaded_count} rows in "
              f"data/warehouse/ed.duckdb")

    total_duration = time.time() - start
    log.info(f"Pipeline run complete in {total_duration:.2f}s")
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()