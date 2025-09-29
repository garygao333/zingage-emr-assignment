-- Normalized model (3NF)
CREATE TABLE IF NOT EXISTS model_caregiver (
  caregiver_id text PRIMARY KEY,
  agency_id text,
  profile_id text,
  applicant_status text,
  employment_status text,
  is_active boolean GENERATED ALWAYS AS (employment_status = 'active') STORED
);

CREATE TABLE IF NOT EXISTS model_carevisit (
  carelog_id text PRIMARY KEY,
  caregiver_id text REFERENCES model_caregiver(caregiver_id),
  parent_id text,
  start_at timestamptz,
  end_at timestamptz,
  in_at timestamptz,
  out_at timestamptz,
  clock_in_method text,
  clock_out_method text,
  status_code text,
  is_split boolean,
  comment_chars int
);

CREATE INDEX IF NOT EXISTS ix_visit_caregiver_time ON model_carevisit (caregiver_id, start_at);
CREATE INDEX IF NOT EXISTS ix_visit_inout ON model_carevisit (in_at, out_at);

-- MART base with derived fields (used by all analytics)
CREATE OR REPLACE VIEW mart_visit_base AS
SELECT
  v.*,
  EXTRACT(EPOCH FROM (out_at - in_at))/60.0 AS actual_mins,
  EXTRACT(EPOCH FROM (end_at - start_at))/60.0 AS scheduled_mins,
  GREATEST(0, EXTRACT(EPOCH FROM (in_at - start_at))/60.0) AS late_by_mins
FROM model_carevisit v;

-- Completed visits: both actuals present, >5 minutes
CREATE OR REPLACE VIEW mart_completed_visits AS
SELECT * FROM mart_visit_base
WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND out_at > in_at AND actual_mins >= 5;

-- Reliability by caregiver (missed / late / short-worked)
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

-- Duration stats (robust)
CREATE OR REPLACE VIEW mart_duration_stats AS
SELECT
  AVG(actual_mins)  FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS avg_mins,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_mins)
    FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS p50_mins,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY actual_mins)
    FILTER (WHERE actual_mins BETWEEN 6 AND 16*60) AS p90_mins
FROM mart_visit_base;

-- Outliers via IQR
CREATE OR REPLACE VIEW mart_duration_outliers AS
WITH d AS (
  SELECT carelog_id, caregiver_id, actual_mins
  FROM mart_visit_base
  WHERE actual_mins IS NOT NULL AND actual_mins > 0
), b AS (
  SELECT
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY actual_mins) AS q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY actual_mins) AS q3
  FROM d
)
SELECT d.*, b.q1, b.q3, (b.q3 - b.q1) AS iqr
FROM d CROSS JOIN b
WHERE d.actual_mins < q1 - 1.5*(q3-q1) OR d.actual_mins > q3 + 1.5*(q3-q1);

-- Documentation consistency
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

-- Overtime per week
CREATE OR REPLACE VIEW mart_overtime_by_week AS
WITH wk AS (
  SELECT caregiver_id,
         DATE_TRUNC('week', COALESCE(in_at, start_at)) AS week_start,
         SUM(actual_mins) AS mins
  FROM mart_visit_base
  WHERE actual_mins IS NOT NULL
  GROUP BY caregiver_id, DATE_TRUNC('week', COALESCE(in_at, start_at))
)
SELECT caregiver_id, week_start, mins, (mins > 2400) AS is_overtime
FROM wk;
