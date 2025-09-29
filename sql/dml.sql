/*
 * DATA MIGRATION & ANALYTICS QUERIES
 *
 * This script:
 * 1. Migrates data from staging to normalized model
 * 2. Runs all analytical queries for business insights
 */

-- Step 1: Migrate caregivers (must be first for foreign key constraint)
INSERT INTO model_caregiver (caregiver_id, agency_id, profile_id, applicant_status, employment_status)
SELECT caregiver_id, agency_id, profile_id, applicant_status, status
FROM stage_caregivers
WHERE caregiver_id IS NOT NULL
ON CONFLICT (caregiver_id) DO UPDATE
  SET agency_id = EXCLUDED.agency_id,
      profile_id = EXCLUDED.profile_id,
      applicant_status = EXCLUDED.applicant_status,
      employment_status = EXCLUDED.employment_status;

-- Step 2: Migrate care visits (references caregivers)
INSERT INTO model_carevisit (
  carelog_id, caregiver_id, parent_id,
  start_at, end_at, in_at, out_at,
  clock_in_method, clock_out_method, status_code, is_split, comment_chars
)
SELECT
  carelog_id, caregiver_id, parent_id,
  start_datetime, end_datetime, clock_in_actual_datetime, clock_out_actual_datetime,
  clock_in_method, clock_out_method, status, split, general_comment_char_count
FROM stage_carelogs
WHERE carelog_id IS NOT NULL AND caregiver_id IS NOT NULL
ON CONFLICT (carelog_id) DO UPDATE
  SET caregiver_id = EXCLUDED.caregiver_id,
      parent_id    = EXCLUDED.parent_id,
      start_at     = EXCLUDED.start_at,
      end_at       = EXCLUDED.end_at,
      in_at        = EXCLUDED.in_at,
      out_at       = EXCLUDED.out_at,
      clock_in_method  = EXCLUDED.clock_in_method,
      clock_out_method = EXCLUDED.clock_out_method,
      status_code  = EXCLUDED.status_code,
      is_split     = EXCLUDED.is_split,
      comment_chars= EXCLUDED.comment_chars;

/*
 * ANALYTICAL QUERIES - Business Intelligence Results
 */

-- Question 1a: Top Performers (Most Completed Visits)
-- "Completed" = has both clock in/out times, duration >5 minutes
SELECT caregiver_id, COUNT(*) AS completed
FROM mart_completed_visits
GROUP BY caregiver_id
ORDER BY completed DESC
LIMIT 20;

-- Question 1b: Reliability Issues
-- Flags caregivers with frequent problems (late/missed/short visits)
-- Only shows caregivers with 10+ visits for statistical significance
SELECT caregiver_id, total_visits, missed, late_arrivals, short_worked,
       ROUND(100*reliability_issue_rate,1) AS issue_rate_pct
FROM mart_reliability_by_caregiver
WHERE total_visits >= 10
ORDER BY reliability_issue_rate DESC
LIMIT 20;

-- Question 2a: Visit Duration Statistics
-- Shows average, median (P50), and 90th percentile durations
SELECT * FROM mart_duration_stats;

-- Question 2b: Duration Outliers
-- Uses IQR method to identify unusually short/long visits
SELECT * FROM mart_duration_outliers ORDER BY actual_mins;

-- Question 3a: Consistent Documentation Providers
-- Shows caregivers who leave detailed notes (≥200 chars) on ≥70% of visits
SELECT caregiver_id, completed, detailed,
       ROUND(100*detailed_rate,1) AS detailed_pct, median_chars
FROM mart_documentation_consistency
WHERE completed >= 10 AND detailed_rate >= 0.70
ORDER BY detailed_pct DESC, median_chars DESC;

-- Question 3b: Data Quality Check - Negative Durations
-- Identifies impossible timestamps (clock out before clock in)
SELECT carelog_id, caregiver_id, in_at, out_at
FROM mart_visit_base
WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND out_at <= in_at;

-- Question 3b: Data Quality Check - Overlapping Visits
-- Finds caregivers in two places at once (data integrity issue)
WITH v AS (
  SELECT caregiver_id, carelog_id, in_at, out_at,
         LAG(out_at) OVER (PARTITION BY caregiver_id ORDER BY in_at) AS prev_out
  FROM mart_visit_base
  WHERE in_at IS NOT NULL AND out_at IS NOT NULL
)
SELECT * FROM v WHERE prev_out IS NOT NULL AND in_at < prev_out;

-- Question 4: Overtime Analysis
-- Shows weeks where caregivers exceeded 40 hours (2400 minutes)
SELECT caregiver_id, week_start, ROUND(mins/60.0,1) AS hours
FROM mart_overtime_by_week
WHERE is_overtime
ORDER BY week_start DESC, hours DESC;
