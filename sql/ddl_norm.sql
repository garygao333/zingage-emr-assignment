/*
 * Normalized schema (3rd normal form) for caregivers and carelogs
 * Also MART views --> contains logic used for the later analytical queries
 */

-- Normalized model (3NF)

DROP TABLE IF EXISTS model_carevisit CASCADE;
DROP TABLE IF EXISTS model_caregiver CASCADE;
DROP TABLE IF EXISTS model_applicant_status CASCADE;
DROP TABLE IF EXISTS model_employment_status CASCADE;
DROP TABLE IF EXISTS model_external_identifier CASCADE;
DROP TABLE IF EXISTS model_profile CASCADE;
DROP TABLE IF EXISTS model_locations CASCADE;
DROP TABLE IF EXISTS model_agency CASCADE;
DROP TABLE IF EXISTS model_franchisor CASCADE;

CREATE TABLE IF NOT EXISTS model_franchisor (
    franchisor_id text PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS model_agency (
    agency_id text PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS model_locations (
    locations_id text PRIMARY KEY,
    name text
);

CREATE TABLE IF NOT EXISTS model_profile (
  profile_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS model_external_identifier (
  external_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS model_employment_status (
  employment_status_id smallserial PRIMARY KEY,
  employment_status text UNIQUE
);

CREATE TABLE IF NOT EXISTS model_applicant_status (
  applicant_status_id smallserial PRIMARY KEY,
  applicant_status text UNIQUE
);

CREATE TABLE IF NOT EXISTS model_caregiver (
    caregiver_id text PRIMARY KEY,
    franchisor_id text,
    agency_id text,
    locations_id text,
    profile_id text,
    external_id text,
    applicant_status_id smallint,
    employment_status_id smallint,
    is_active boolean,
    constraint fk_caregiver_franchisor FOREIGN KEY (franchisor_id) REFERENCES model_franchisor(franchisor_id),
    constraint fk_caregiver_agency FOREIGN KEY (agency_id) REFERENCES model_agency(agency_id),
    constraint fk_caregiver_location FOREIGN KEY (locations_id) REFERENCES model_locations(locations_id),
    constraint fk_caregiver_profile FOREIGN KEY (profile_id) REFERENCES model_profile(profile_id),
    constraint fk_caregiver_external FOREIGN KEY (external_id)  REFERENCES model_external_identifier(external_id),
    constraint fk_caregiver_app_status FOREIGN KEY (applicant_status_id) REFERENCES model_applicant_status(applicant_status_id),
    constraint fk_caregiver_emp_status FOREIGN KEY (employment_status_id)REFERENCES model_employment_status(employment_status_id)
);

-- Carelogs/care visits table
-- References caregiver_id as foreign key to ensure that the visit points to a valid caregiver
CREATE TABLE IF NOT EXISTS model_carevisit (
    carelog_id text PRIMARY KEY,
    caregiver_id text,
    agency_id text,
    franchisor_id text,
    parent_id text,
    start_at timestamp,
    end_at timestamptz,
    in_at timestamptz,
    out_at timestamptz,
    clock_in_method text,
    clock_out_method text,
    status_code text,
    is_split boolean,
    comment_chars int,
    constraint fk_visit_caregiver FOREIGN KEY (caregiver_id) REFERENCES model_caregiver(caregiver_id),
    constraint fk_visit_agency FOREIGN KEY (agency_id) REFERENCES model_agency(agency_id),
    constraint fk_visit_franchisor FOREIGN KEY (franchisor_id) REFERENCES model_franchisor(franchisor_id)
);

-- Indexes to speed up common queries
CREATE INDEX IF NOT EXISTS ix_visit_caregiver_time ON model_carevisit (caregiver_id, start_at);
CREATE INDEX IF NOT EXISTS ix_visit_inout ON model_carevisit (in_at, out_at);

-- MART views/tables for analytical queries
CREATE OR REPLACE VIEW mart_visit_base AS
SELECT
    v.*,
    EXTRACT(EPOCH FROM (out_at - in_at))/60.0 AS actual_mins,
    EXTRACT(EPOCH FROM (end_at - start_at))/60.0 AS scheduled_mins,
    GREATEST(0, EXTRACT(EPOCH FROM (in_at - start_at))/60.0) AS late_by_mins
FROM model_carevisit v;

-- Completed visits are those with both clock in and clock out times and with duration greater than 5 minutes
CREATE OR REPLACE VIEW mart_completed_visits AS
SELECT * FROM mart_visit_base
WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND out_at > in_at AND actual_mins >= 5;

-- Reliability issues per caregiver
-- For each caregiver, count total visits, completed visits, missed visits (no clock in/out), late arrivals (>10 mins), and short worked (<25% of scheduled)
-- Then compute a weighted reliability issue rate: (2*missed + 1*late_arrivals + 1*short_worked) / total_visits
CREATE OR REPLACE VIEW mart_reliability_by_caregiver AS
WITH f AS (
    SELECT caregiver_id,
            COUNT(*) AS total_visits,
            COUNT(*) FILTER (WHERE in_at IS NULL AND out_at IS NULL AND start_at IS NOT NULL) AS missed,
            COUNT(*) FILTER (WHERE late_by_mins > 10) AS late_arrivals,
            COUNT(*) FILTER (
                WHERE in_at IS NOT NULL AND out_at IS NOT NULL
                    AND actual_mins < 0.25 * NULLIF(scheduled_mins,0)
         ) AS short_worked,
         COUNT(*) FILTER (WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND out_at > in_at AND actual_mins >= 5) AS completed
  FROM mart_visit_base
  GROUP BY caregiver_id
)
SELECT caregiver_id, total_visits, completed, missed, late_arrivals, short_worked,
       (COALESCE(missed,0)*2 + COALESCE(late_arrivals,0) + COALESCE(short_worked,0))::numeric
        / NULLIF(total_visits,0) AS reliability_issue_rate
FROM f;

-- Duration stats (avg, p50, p90) excluding outliers (<6 mins or >16 hours)
CREATE OR REPLACE VIEW mart_duration_stats AS
SELECT
    AVG(actual_mins) FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS avg_mins,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_mins)
    FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS p50_mins,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY actual_mins)
    FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS p90_mins
FROM mart_visit_base;

-- Finding the outliers via IQR (interquartile range)
-- Outliers are those outside the interquartile range (Q3(75%)-Q1(25%)) with actual_mins < Q1 - 1.5*IQR or > Q3 + 1.5*IQR
CREATE OR REPLACE VIEW mart_duration_outliers AS
WITH d AS (
    SELECT carelog_id, caregiver_id, actual_mins
    FROM mart_visit_base
    WHERE actual_mins IS NOT NULL AND actual_mins > 0
), b AS (
    SELECT
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY actual_mins) AS q1,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY actual_mins) AS q3
    FROM d
)
SELECT d.*, b.q1, b.q3, (b.q3 - b.q1) AS iqr
FROM d CROSS JOIN b
WHERE d.actual_mins < q1 - 1.5*(q3-q1) OR d.actual_mins > q3 + 1.5*(q3-q1);

-- Documentation consistency
-- Out of the completed visits, how many have detailed notes (>=200 chars)
-- Consistency rate = detailed / completed
-- Also compute median comment length for context
CREATE OR REPLACE VIEW mart_documentation_consistency AS
WITH per_cg AS (
    SELECT caregiver_id,
            COUNT(*) FILTER (WHERE in_at IS NOT NULL AND out_at IS NOT NULL) AS completed,
            COUNT(*) FILTER (WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND comment_chars >= 200) AS detailed,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY comment_chars)
            FILTER (WHERE in_at IS NOT NULL AND out_at IS NOT NULL) AS median_chars
    FROM mart_visit_base
    GROUP BY caregiver_id
)
SELECT caregiver_id, completed, detailed, median_chars,
       (detailed::numeric / NULLIF(completed,0)) AS detailed_rate
FROM per_cg;

-- Calculates overtime by week per caregiver
-- Overtime defined as >40 hours (2400 minutes) in a week
CREATE OR REPLACE VIEW mart_overtime_by_week AS
WITH wk AS (
    SELECT caregiver_id,
            DATE_TRUNC('week', COALESCE(in_at, start_at)) AS week_start,
            SUM(actual_mins) AS mins
    FROM mart_visit_base
    WHERE actual_mins IS NOT NULL
    GROUP BY caregiver_id, DATE_TRUNC('week', COALESCE(in_at, start_at)) 
    HAVING sum(actual_mins) > 2400
)
SELECT caregiver_id, week_start, mins, (mins > 2400) AS is_overtime
FROM wk;
