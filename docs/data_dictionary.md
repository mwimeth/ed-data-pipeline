# Data Dictionary — ED Attendance Pipeline

Describes the columns in the clean table (`ed_attendances_clean` in
DuckDB), the output of the pipeline after Milestones 3–8.

| Column | Type | Description | Notes |
|---|---|---|---|
| `attendance_id` | VARCHAR | Unique ID for the attendance, format `ATT` + 6 digits | Never null; deduplicated on this field |
| `pseudo_patient_id` | VARCHAR | Pseudonymised patient identifier, format `PSN` + 7 digits | Rows with a missing ID are rejected (rule: missing_patient_id) |
| `arrival_time` | TIMESTAMP | When the patient arrived | Parsed from 4 mixed raw formats; rows with a missing/unparseable value are rejected |
| `departure_time` | TIMESTAMP | When the patient left the department | Same as above; also checked against arrival_time (must not precede it) |
| `triage_category` | VARCHAR | Clinical urgency, 1 (most urgent) to 5 (least urgent) | `.0` suffix stripped (e.g. "3.0" -> "3"); values outside 1–5 are rejected |
| `presenting_complaint` | VARCHAR | Reason for attendance, one of 16 standard categories | Missing values filled with "Unknown" rather than rejected |
| `disposition` | VARCHAR | Outcome, one of 6 standard categories | Missing values filled with "Unknown" rather than rejected |
| `gp_practice_code` | VARCHAR | Registered GP practice code, one letter + 5 digits | Malformed codes are set to null and flagged (see gp_code_flag) |
| `gp_code_flag` | VARCHAR | `NULL` (valid), `"missing"`, or `"malformed"` | Records why gp_practice_code is null, for reporting |

## Rejected rows (`failure_reasons` column)

Rows that fail one or more rules are kept separately (not in the clean
table), with an array column `failure_reasons` listing every rule that
failed. Possible values:

| Reason | Meaning |
|---|---|
| `missing_arrival_time` | arrival_time is null |
| `missing_departure_time` | departure_time is null |
| `departure_before_arrival` | departure_time is earlier than arrival_time |
| `invalid_triage_code` | triage_category is not one of 1–5 |
| `future_dated_attendance` | arrival_time falls outside the year 2025 |
| `extreme_length_of_stay` | stay exceeds 2,880 minutes (48 hours) |
| `missing_patient_id` | pseudo_patient_id is null |