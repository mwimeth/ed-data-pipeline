"""
generate_data.py
----------------
Creates a SYNTHETIC Emergency Department (ED) attendance dataset and then
deliberately damages it with realistic data-quality problems.

All data is fictional. No real patient information is used anywhere.

Run from the project root:
    python src/generate_data.py

Outputs:
    data/raw/ed_attendances_raw.csv          <- the messy file you will clean
    data/answer_key/injected_errors_log.csv  <- which raw row got which error
                                                (do NOT use it while cleaning,
                                                 only to check your results)
"""

from pathlib import Path

import numpy as np
import pandas as pd

# ----------------------------------------------------------------------------
# 1. SETTINGS
# ----------------------------------------------------------------------------
SEED = 42                      # same seed = same dataset every run
N_BASE = 20_000                # number of genuine attendances
N_DUPLICATES = 350             # extra exact-duplicate rows added on top
START_DATE = "2025-01-01"
END_DATE = "2025-12-31"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
KEY_DIR = PROJECT_ROOT / "data" / "answer_key"

rng = np.random.default_rng(SEED)

# How many rows get each "structural" error. Each row gets AT MOST ONE of
# these, so the counts below are exact and easy to check.
STRUCTURAL_COUNTS = {
    "missing_arrival_time": 100,
    "missing_departure_time": 200,
    "departure_before_arrival": 200,
    "invalid_triage_code": 200,
    "future_dated_attendance": 30,
    "extreme_length_of_stay": 60,
    "missing_patient_id": 60,
    "malformed_gp_code": 200,
    "missing_gp_code": 600,
    "missing_presenting_complaint": 400,
    "missing_disposition": 200,
}

# Different ways the same "missing" idea gets written in real systems.
NULL_MARKERS = ["", "NULL", "N/A", "n/a", "-"]

# Date formats mixed together (DD/MM/YYYY, never MM/DD/YYYY).
DATE_FORMATS = {
    "iso": "%Y-%m-%d %H:%M:%S",            # 2025-03-05 14:30:00
    "dmy_slash": "%d/%m/%Y %H:%M",         # 05/03/2025 14:30
    "dmy_month_name": "%d-%b-%Y %H:%M",    # 05-Mar-2025 14:30
    "iso_t": "%Y-%m-%dT%H:%M:%S",          # 2025-03-05T14:30:00
}
DATE_FORMAT_PROBS = [0.85, 0.06, 0.05, 0.04]

COMPLAINTS = [
    "Abdominal pain", "Chest pain", "Shortness of breath",
    "Minor injury - limb", "Head injury", "Fall", "Laceration / wound",
    "Back pain", "Fever", "Headache", "Allergic reaction",
    "Urinary symptoms", "Vomiting / diarrhoea", "Eye problem", "Rash",
    "Palpitations",
]
COMPLAINT_WEIGHTS = np.array(
    [10, 8, 7, 12, 6, 7, 8, 5, 6, 4, 2, 4, 4, 3, 2, 3], dtype=float
)
COMPLAINT_PROBS = COMPLAINT_WEIGHTS / COMPLAINT_WEIGHTS.sum()

DISPOSITIONS = [
    "Discharged home", "Admitted to ward", "Transferred to another provider",
    "Discharged with follow-up", "Referred to GP",
    "Left before treatment complete",
]
DISPOSITION_PROBS_BY_TRIAGE = {
    1: [0.10, 0.72, 0.15, 0.03, 0.00, 0.00],
    2: [0.30, 0.45, 0.05, 0.15, 0.03, 0.02],
    3: [0.40, 0.25, 0.02, 0.22, 0.06, 0.05],
    4: [0.55, 0.05, 0.01, 0.25, 0.10, 0.04],
    5: [0.65, 0.01, 0.00, 0.20, 0.10, 0.04],
}

# What a cleaning job is expected to do with each kind of problem.
EXPECTED_ACTION = {
    "duplicate_row": "remove (keep first)",
    "missing_arrival_time": "reject",
    "missing_departure_time": "reject",
    "departure_before_arrival": "reject",
    "invalid_triage_code": "reject",
    "future_dated_attendance": "reject",
    "extreme_length_of_stay": "reject",
    "missing_patient_id": "reject",
    "malformed_gp_code": "set to NULL and flag",
    "missing_gp_code": "set to NULL and flag",
    "missing_presenting_complaint": "fill with 'Unknown'",
    "missing_disposition": "fill with 'Unknown'",
    "triage_decimal_format": "fix (3.0 -> 3)",
    "gp_code_lowercase": "fix (upper-case)",
    "inconsistent_text_format": "fix (trim spaces, standardise case)",
    "mixed_date_format": "fix (parse into one timestamp format)",
}


# ----------------------------------------------------------------------------
# 2. BUILD A CLEAN BASE DATASET
# ----------------------------------------------------------------------------
def make_clean_base(n: int) -> pd.DataFrame:
    """Generate n believable, error-free ED attendances."""
    days = pd.date_range(START_DATE, END_DATE, freq="D")

    # Slightly busier on Mondays (dayofweek 0 = Monday)
    weekday_weights = np.array([1.15, 1.0, 0.97, 0.97, 1.0, 0.95, 0.96])
    day_probs = weekday_weights[days.dayofweek]
    day_probs = day_probs / day_probs.sum()

    # Busy daytime and evening, quiet overnight (index = hour of day)
    hour_weights = np.array(
        [2, 1.5, 1.2, 1, 1, 1.2, 1.8, 3, 4.5, 5.5, 6, 6,
         5.8, 5.5, 5.5, 5.5, 5.5, 5.5, 5, 4.5, 4, 3.5, 3, 2.5]
    )
    hour_probs = hour_weights / hour_weights.sum()

    day_idx = rng.choice(len(days), size=n, p=day_probs)
    hours = rng.choice(24, size=n, p=hour_probs)
    minutes = rng.integers(0, 60, size=n)
    arrival = (
        days[day_idx]
        + pd.to_timedelta(hours, unit="h")
        + pd.to_timedelta(minutes, unit="m")
    )

    df = pd.DataFrame({"arrival": arrival}).sort_values("arrival")
    df = df.reset_index(drop=True)
    df["attendance_id"] = "ATT" + (df.index + 1).astype(str).str.zfill(6)

    # Triage category: 1 = most urgent ... 5 = least urgent
    df["triage"] = rng.choice([1, 2, 3, 4, 5], size=n,
                              p=[0.02, 0.13, 0.38, 0.37, 0.10])

    # Length of stay in minutes: log-normal, longer for more urgent patients
    stay_multiplier = df["triage"].map({1: 1.3, 2: 1.2, 3: 1.0, 4: 0.8, 5: 0.6})
    los = 165 * stay_multiplier.to_numpy() * np.exp(0.55 * rng.standard_normal(n))
    los = np.clip(np.round(los), 20, 900).astype(int)
    df["departure"] = df["arrival"] + pd.to_timedelta(los, unit="m")

    # Pseudonymised patient IDs (some patients attend more than once)
    id_pool = rng.choice(9_000_000, size=14_000, replace=False) + 1_000_000
    chosen = id_pool[rng.integers(0, len(id_pool), size=n)]
    df["pseudo_patient_id"] = ["PSN%07d" % p for p in chosen]

    df["presenting_complaint"] = rng.choice(COMPLAINTS, size=n,
                                            p=COMPLAINT_PROBS)

    disposition = np.empty(n, dtype=object)
    for t, probs in DISPOSITION_PROBS_BY_TRIAGE.items():
        mask = (df["triage"] == t).to_numpy()
        disposition[mask] = rng.choice(DISPOSITIONS, size=int(mask.sum()),
                                       p=probs)
    df["disposition"] = disposition

    # Synthetic GP practice codes: one letter + five digits (e.g. E81234)
    gp_pool = np.array(
        ["E8" + str(x) for x in rng.choice(np.arange(1000, 10000), 60,
                                           replace=False)]
    )
    df["gp_practice_code"] = gp_pool[rng.integers(0, len(gp_pool), size=n)]
    return df


# ----------------------------------------------------------------------------
# 3. DAMAGE THE DATA ON PURPOSE
# ----------------------------------------------------------------------------
def null_markers(size: int) -> np.ndarray:
    """Random mix of ways to write 'missing'."""
    return rng.choice(NULL_MARKERS, size=size)


def make_bad_gp_code(code: str) -> str:
    """Return a GP code that breaks the one-letter-plus-five-digits rule."""
    kind = rng.integers(0, 4)
    if kind == 0:
        return code[:4]                      # too short
    if kind == 1:
        return code + "9"                    # too long
    if kind == 2:
        return str(rng.integers(100000, 999999))   # digits only
    return "GPPRAC"                          # letters only


def mess_text(value: str, kind: int) -> str:
    """Apply inconsistent casing or stray spaces to a text value."""
    if kind == 0:
        return value.upper()
    if kind == 1:
        return value.lower()
    return "  " + value + " "                # leading and trailing spaces


def build_messy_dataset():
    df = make_clean_base(N_BASE)
    n = len(df)
    log = []   # (row uid, column, error_type)

    def record(ix, column, error_type):
        for i in ix:
            log.append((int(i), column, error_type))

    # --- 3a. Choose which rows get which structural error (no overlaps) -----
    order = rng.permutation(n)
    pools, start = {}, 0
    for name, count in STRUCTURAL_COUNTS.items():
        pools[name] = order[start:start + count]
        start += count
    untouched = order[start:]          # rows with no structural error

    # --- 3b. Time-related errors (edit the real timestamps first) -----------
    ix = pools["departure_before_arrival"]
    df.loc[ix, "departure"] = df.loc[ix, "arrival"] - pd.to_timedelta(
        rng.integers(5, 600, size=len(ix)), unit="m")
    record(ix, "departure_time", "departure_before_arrival")

    ix = pools["extreme_length_of_stay"]     # 50 hours to about 14 days
    df.loc[ix, "departure"] = df.loc[ix, "arrival"] + pd.to_timedelta(
        rng.integers(3000, 20000, size=len(ix)), unit="m")
    record(ix, "departure_time", "extreme_length_of_stay")

    ix = pools["future_dated_attendance"]    # year typo: 2025 becomes 2035
    shift = pd.Timedelta(days=3652)
    df.loc[ix, "arrival"] = df.loc[ix, "arrival"] + shift
    df.loc[ix, "departure"] = df.loc[ix, "departure"] + shift
    record(ix, "arrival_time", "future_dated_attendance")

    # --- 3c. Convert everything to raw text, mixing date formats ------------
    raw = pd.DataFrame(index=df.index)
    raw["attendance_id"] = df["attendance_id"]
    raw["pseudo_patient_id"] = df["pseudo_patient_id"]

    fmt_names = list(DATE_FORMATS)
    fmt_choice = rng.choice(len(fmt_names), size=n, p=DATE_FORMAT_PROBS)
    raw["arrival_time"] = ""
    raw["departure_time"] = ""
    for k, name in enumerate(fmt_names):
        mask = fmt_choice == k
        fmt = DATE_FORMATS[name]
        raw.loc[mask, "arrival_time"] = df.loc[mask, "arrival"].dt.strftime(fmt)
        raw.loc[mask, "departure_time"] = df.loc[mask, "departure"].dt.strftime(fmt)

    raw["triage_category"] = df["triage"].astype(str)
    raw["presenting_complaint"] = df["presenting_complaint"]
    raw["disposition"] = df["disposition"]
    raw["gp_practice_code"] = df["gp_practice_code"]

    # --- 3d. Missing values, written in different ways ----------------------
    for pool_name, column in [
        ("missing_arrival_time", "arrival_time"),
        ("missing_departure_time", "departure_time"),
        ("missing_patient_id", "pseudo_patient_id"),
        ("missing_presenting_complaint", "presenting_complaint"),
        ("missing_disposition", "disposition"),
        ("missing_gp_code", "gp_practice_code"),
    ]:
        ix = pools[pool_name]
        raw.loc[ix, column] = null_markers(len(ix))
        record(ix, column, pool_name)

    # --- 3e. Invalid values --------------------------------------------------
    ix = pools["invalid_triage_code"]
    raw.loc[ix, "triage_category"] = rng.choice(
        ["0", "6", "9", "99", "X", "-1"], size=len(ix))
    record(ix, "triage_category", "invalid_triage_code")

    ix = pools["malformed_gp_code"]
    raw.loc[ix, "gp_practice_code"] = [
        make_bad_gp_code(c) for c in raw.loc[ix, "gp_practice_code"]]
    record(ix, "gp_practice_code", "malformed_gp_code")

    # --- 3f. Fixable formatting problems (can overlap with anything) --------
    # Mixed date formats: log every row that is not in the standard format
    not_iso = fmt_choice != 0
    has_date = ~(raw["arrival_time"].isin(NULL_MARKERS)
                 & raw["departure_time"].isin(NULL_MARKERS))
    record(np.where(not_iso & has_date.to_numpy())[0],
           "arrival_time, departure_time", "mixed_date_format")

    # Triage written as "3.0" (only on rows whose triage is otherwise valid)
    candidates = np.setdiff1d(np.arange(n), pools["invalid_triage_code"])
    ix = rng.choice(candidates, size=200, replace=False)
    raw.loc[ix, "triage_category"] = raw.loc[ix, "triage_category"] + ".0"
    record(ix, "triage_category", "triage_decimal_format")

    # GP code written in lower case (only on rows with a valid code)
    bad_gp_rows = np.concatenate([pools["malformed_gp_code"],
                                  pools["missing_gp_code"]])
    candidates = np.setdiff1d(np.arange(n), bad_gp_rows)
    ix = rng.choice(candidates, size=200, replace=False)
    raw.loc[ix, "gp_practice_code"] = raw.loc[ix, "gp_practice_code"].str.lower()
    record(ix, "gp_practice_code", "gp_code_lowercase")

    # Casing and stray spaces in text columns
    for column, pool_name, share in [
        ("presenting_complaint", "missing_presenting_complaint", 0.10),
        ("disposition", "missing_disposition", 0.08),
    ]:
        candidates = np.setdiff1d(np.arange(n), pools[pool_name])
        ix = rng.choice(candidates, size=int(n * share), replace=False)
        kinds = rng.integers(0, 3, size=len(ix))
        raw.loc[ix, column] = [
            mess_text(v, k) for v, k in zip(raw.loc[ix, column], kinds)]
        record(ix, column, "inconsistent_text_format")

    # --- 3g. Exact duplicate rows (copies of otherwise-good rows) -----------
    dup_ix = rng.choice(untouched, size=N_DUPLICATES, replace=False)
    raw["_uid"] = np.arange(n)
    copies = raw.loc[dup_ix].copy()
    new_uids = np.arange(n, n + N_DUPLICATES)
    copy_of = dict(zip(dup_ix.tolist(), new_uids.tolist()))
    copies["_uid"] = new_uids
    # The copy carries the same formatting problems as its original
    log += [(copy_of[u], c, e) for (u, c, e) in log if u in copy_of]

    final = pd.concat([raw, copies], ignore_index=True)

    # --- 3h. Shuffle so duplicates are not sitting next to each other -------
    final = final.sample(frac=1, random_state=SEED).reset_index(drop=True)
    final["raw_row_number"] = final.index + 1     # 1 = first data row

    # The LATER occurrence of a duplicated attendance_id is "the duplicate"
    dup_ids = set(raw.loc[dup_ix, "attendance_id"])
    dups = final[final["attendance_id"].isin(dup_ids)]
    later = dups[dups.duplicated("attendance_id", keep="first")]
    log += [(int(u), "(entire row)", "duplicate_row") for u in later["_uid"]]

    # --- 3i. Build the answer key -------------------------------------------
    uid_to_row = dict(zip(final["_uid"], final["raw_row_number"]))
    uid_to_id = dict(zip(final["_uid"], final["attendance_id"]))
    key = pd.DataFrame(log, columns=["_uid", "column", "error_type"])
    key["raw_row_number"] = key["_uid"].map(uid_to_row)
    key["attendance_id"] = key["_uid"].map(uid_to_id)
    key["expected_action"] = key["error_type"].map(EXPECTED_ACTION)
    key = key[["raw_row_number", "attendance_id", "column",
               "error_type", "expected_action"]]
    key = key.sort_values(["raw_row_number", "error_type"]).reset_index(drop=True)

    out_cols = ["attendance_id", "pseudo_patient_id", "arrival_time",
                "departure_time", "triage_category", "presenting_complaint",
                "disposition", "gp_practice_code"]
    return final[out_cols], key


# ----------------------------------------------------------------------------
# 4. SAVE
# ----------------------------------------------------------------------------
def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    KEY_DIR.mkdir(parents=True, exist_ok=True)

    raw, key = build_messy_dataset()
    raw.to_csv(RAW_DIR / "ed_attendances_raw.csv", index=False, encoding="utf-8")
    key.to_csv(KEY_DIR / "injected_errors_log.csv", index=False, encoding="utf-8")

    print(f"Raw rows written: {len(raw):,}")
    print("\nInjected problems (rows affected):")
    print(key["error_type"].value_counts().to_string())
    print(f"\nSaved to:\n  {RAW_DIR / 'ed_attendances_raw.csv'}"
          f"\n  {KEY_DIR / 'injected_errors_log.csv'}")


if __name__ == "__main__":
    main()
