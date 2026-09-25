-- views.sql
-- ---------
-- Three KPI views built on top of the clean ED attendance table.
-- Run this after the pipeline has loaded data/warehouse/ed.duckdb.

-- View 1: what share of attendances breached the 4-hour standard?
CREATE OR REPLACE VIEW v_four_hour_breach AS
SELECT
    COUNT(*) AS total_attendances,
    SUM(CASE WHEN los_minutes > 240 THEN 1 ELSE 0 END) AS breaches,
    ROUND(100.0 * SUM(CASE WHEN los_minutes > 240 THEN 1 ELSE 0 END) / COUNT(*), 1) AS breach_rate_pct
FROM (
    SELECT DATEDIFF('minute', arrival_time, departure_time) AS los_minutes
    FROM ed_attendances_clean
);

-- View 2: average and median length of stay, by triage category
CREATE OR REPLACE VIEW v_los_by_triage AS
SELECT
    triage_category,
    COUNT(*) AS attendances,
    ROUND(AVG(los_minutes), 0) AS avg_los_minutes,
    ROUND(MEDIAN(los_minutes), 0) AS median_los_minutes
FROM (
    SELECT triage_category,
           DATEDIFF('minute', arrival_time, departure_time) AS los_minutes
    FROM ed_attendances_clean
)
GROUP BY triage_category
ORDER BY triage_category;

-- View 3: arrivals by hour of day
CREATE OR REPLACE VIEW v_arrivals_by_hour AS
SELECT
    EXTRACT(HOUR FROM arrival_time) AS arrival_hour,
    COUNT(*) AS attendances
FROM ed_attendances_clean
GROUP BY arrival_hour
ORDER BY arrival_hour;