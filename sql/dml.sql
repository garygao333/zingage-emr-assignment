/*
 * (Data Migration Language) 
 * Migrate data from stage to normalized model created in ddl_norm.sql
 * Run analytic queries to answer questions
 */
-- Migrate caregiver data from stage to model

-- Technically transformation, but just cleaning out the NONE.
-- INSERT INTO model_locations(locations_id, name)
-- SELECT distinct locations_id, location_name FROM stage_caregivers ORDER BY 1;
-- UPDATE model_locations SET name = NULL WHERE locations_id = '0'; -- Updating by primary key

TRUNCATE model_caregiver CASCADE;
TRUNCATE model_carevisit CASCADE;
TRUNCATE model_applicant_status CASCADE;
TRUNCATE model_employment_status CASCADE;
TRUNCATE model_external_identifier CASCADE;
TRUNCATE model_profile CASCADE;
TRUNCATE model_agency CASCADE;
TRUNCATE model_franchisor CASCADE;
TRUNCATE model_locations CASCADE;

INSERT INTO model_locations (locations_id, name)
SELECT DISTINCT locations_id, NULLIF(location_name,'') AS name FROM stage_caregivers WHERE locations_id IS NOT NULL;

INSERT INTO model_franchisor(franchisor_id)
SELECT DISTINCT franchisor_id FROM stage_caregivers WHERE franchisor_id IS NOT NULL ORDER BY 1;

INSERT INTO model_agency( agency_id)
SELECT DISTINCT agency_id FROM stage_caregivers WHERE franchisor_id IS NOT NULL ORDER BY 1;

INSERT INTO model_profile(profile_id)
SELECT DISTINCT profile_id FROM stage_caregivers ORDER BY 1;

INSERT INTO model_external_identifier (external_id)
SELECT DISTINCT external_id FROM stage_caregivers WHERE external_id IS NOT NULL AND external_id <> '' ORDER BY 1;

INSERT INTO model_employment_status (employment_status)
SELECT DISTINCT status FROM stage_caregivers WHERE status IS NOT NULL ORDER BY 1;

INSERT INTO model_applicant_status (applicant_status)
SELECT DISTINCT applicant_status FROM stage_caregivers WHERE applicant_status IS NOT NULL ORDER BY 1;

INSERT INTO model_caregiver (
  caregiver_id,
  franchisor_id,
  agency_id,
  locations_id,
  profile_id,
  external_id,
  applicant_status_id,
  employment_status_id,
  is_active
)
SELECT
  sc.caregiver_id,
  sc.franchisor_id,
  sc.agency_id,
  sc.locations_id,
  sc.profile_id,
  NULLIF(sc.external_id,'') AS external_id,
  mas.applicant_status_id,
  mes.employment_status_id,
  CASE WHEN mes.employment_status = 'active' THEN TRUE ELSE FALSE END AS is_active
FROM stage_caregivers sc
LEFT JOIN model_applicant_status mas
  ON mas.applicant_status = sc.applicant_status
LEFT JOIN model_employment_status mes
  ON mes.employment_status = sc.status
WHERE sc.caregiver_id IS NOT NULL
ON CONFLICT (caregiver_id) DO UPDATE
SET franchisor_id        = EXCLUDED.franchisor_id,
    agency_id            = EXCLUDED.agency_id,
    locations_id         = EXCLUDED.locations_id,
    profile_id           = EXCLUDED.profile_id,
    external_id          = EXCLUDED.external_id,
    applicant_status_id  = EXCLUDED.applicant_status_id,
    employment_status_id = EXCLUDED.employment_status_id,
    is_active            = EXCLUDED.is_active;

\echo === caregiver count ===
SELECT COUNT(*) AS model_caregiver FROM model_caregiver;

--Migratae carevisit data from stage to model
INSERT INTO model_carevisit (
  carelog_id,
  caregiver_id,
  agency_id,
  franchisor_id,
  parent_id,
  start_at,
  end_at,
  in_at,
  out_at,
  clock_in_method,
  clock_out_method,
  status_code,
  is_split,
  comment_chars
)
SELECT
  cl.carelog_id,
  cl.caregiver_id,
  cl.agency_id,
  cl.franchisor_id,
  cl.parent_id,
  cl.start_datetime,
  cl.end_datetime,
  cl.clock_in_actual_datetime,
  cl.clock_out_actual_datetime,
  cl.clock_in_method,
  cl.clock_out_method,
  cl.status,
  cl.split,
  cl.general_comment_char_count
FROM stage_carelogs cl
WHERE cl.carelog_id   IS NOT NULL
  AND cl.caregiver_id IS NOT NULL
ON CONFLICT (carelog_id) DO UPDATE
SET caregiver_id     = EXCLUDED.caregiver_id,
    agency_id        = EXCLUDED.agency_id,
    franchisor_id    = EXCLUDED.franchisor_id,
    parent_id        = EXCLUDED.parent_id,
    start_at         = EXCLUDED.start_at,
    end_at           = EXCLUDED.end_at,
    in_at            = EXCLUDED.in_at,
    out_at           = EXCLUDED.out_at,
    clock_in_method  = EXCLUDED.clock_in_method,
    clock_out_method = EXCLUDED.clock_out_method,
    status_code      = EXCLUDED.status_code,
    is_split         = EXCLUDED.is_split,
    comment_chars    = EXCLUDED.comment_chars;

\echo === model counts ===
SELECT (SELECT COUNT(*) FROM model_caregiver)  AS model_caregiver,
       (SELECT COUNT(*) FROM model_carevisit) AS model_carevisit;



-- Top performers: caregivers with the highest number of completed vistis
-- Completed means that the caregiver has both clock in/out times with duration greater than 5 minutes
SELECT caregiver_id, COUNT(*) AS completed
FROM mart_completed_visits
GROUP BY caregiver_id
ORDER BY completed DESC
LIMIT 20;

-- Reliability Issues: highlight caregivers showing frequent reliability issues
-- For each caregiver, count total visits, completed visits, missed visits (no clock in/out), late arrivals (>10 mins), and short worked (<25% of scheduled)
-- Then compute a weighted reliability issue rate: (2*missed + 1*late_arrivals + 1*short_worked) / total_visits
-- Only shows caregivers with 10+ visits for statistical significance
SELECT caregiver_id, total_visits, missed, late_arrivals, short_worked,
       ROUND(100*reliability_issue_rate,1) AS issue_rate_pct
FROM mart_reliability_by_caregiver
WHERE total_visits >= 10
ORDER BY reliability_issue_rate DESC
LIMIT 20;

-- Visit Duration Analysis: Calculate and clearly present the average actual duration of caregiver visits.
-- Shows average, median (P50), and 90th percentile durations
SELECT * FROM mart_duration_stats;

-- Identifying outliers: Identify and clearly present visits significantly shorter or longer than typical durations. 
-- Use interquartile range, or IQR, to identify outliers. 
-- Outliers are those outside the interquartile range (Q3(75%)-Q1(25%)) with actual_mins < Q1 - 1.5*IQR or > Q3 + 1.5*IQR
SELECT * FROM mart_duration_outliers ORDER BY actual_mins;

-- Detailed Documentation Providers: Clearly identify caregivers consistently leaving detailed comments.
-- Shows caregivers who leave detailed notes (≥200 chars) on ≥70% of visits
-- Out of the completed visits, how many have detailed notes (>=200 chars)
-- Consistency rate = detailed / completed
-- Also compute median comment length for context
SELECT caregiver_id, completed, detailed,
        ROUND(100*detailed_rate,1) AS detailed_pct, median_chars
FROM mart_documentation_consistency
WHERE completed >= 10 AND detailed_rate >= 0.70
ORDER BY detailed_pct DESC, median_chars DESC;

-- Data Quality Check: Clearly highlight any unusual or suspicious patterns in documentation data.
-- Finds visits where clock out is before clock in (data integrity issue)
-- Also finds caregivers in two places at once (data integrity issue)
SELECT carelog_id, caregiver_id, in_at, out_at
FROM mart_visit_base
WHERE in_at IS NOT NULL AND out_at IS NOT NULL AND out_at <= in_at;

-- Caregiver Overtime Analysis: Clearly identify caregivers regularly incurring overtime hours. 
-- Shows weeks where caregivers exceeded 40 hours (2400 minutes)
SELECT caregiver_id, week_start, ROUND(mins/60.0,1) AS hours
FROM mart_overtime_by_week
WHERE is_overtime AND mins > 3600
ORDER BY week_start DESC, hours DESC;

-- Who drives the most overtime?
-- Caregivers with the most weeks of overtime (4+ weeks)
-- Shows total weeks observed, weeks with overtime, and overtime rate
SELECT caregiver_id,
        COUNT(*) AS weeks_observed,
        COUNT(*) FILTER (WHERE is_overtime) AS weeks_overtime,
        ROUND(
            COUNT(*) FILTER (WHERE is_overtime)::numeric
            / NULLIF(COUNT(*),0), 2
        ) AS ot_rate
FROM mart_overtime_by_week
GROUP BY caregiver_id
HAVING COUNT(*) >= 4 
ORDER BY weeks_overtime DESC, ot_rate DESC, weeks_observed DESC
LIMIT 20;

-- Are specific agencies driving more overtime?
-- Shows total caregivers in agency, total weeks observed, weeks with overtime, and overtime rate
-- Also shows total and average overtime hours per OT week for context
WITH ot AS (
    SELECT caregiver_id, week_start, mins, is_overtime
    FROM mart_overtime_by_week
),
cg AS (
    SELECT caregiver_id, agency_id
    FROM model_caregiver
)
SELECT
    cg.agency_id,
    COUNT(DISTINCT cg.caregiver_id) AS caregivers_in_agency,
    COUNT(*) AS agency_weeks_observed,
    COUNT(*) FILTER (WHERE ot.is_overtime) AS agency_ot_weeks,
    ROUND(
        COUNT(*) FILTER (WHERE ot.is_overtime)::numeric
        / NULLIF(COUNT(*),0), 3
    ) AS agency_ot_week_rate,
    ROUND(SUM(ot.mins) FILTER (WHERE ot.is_overtime) / 60.0, 1) AS ot_hours_sum,
    ROUND(AVG(ot.mins) FILTER (WHERE ot.is_overtime) / 60.0, 1) AS ot_hours_avg_per_ot_week
FROM ot
JOIN cg USING (caregiver_id)
GROUP BY cg.agency_id
ORDER BY agency_ot_weeks DESC, agency_ot_week_rate DESC;

-- Are certain shifts correlated with overtime? 
-- Break down visits by shift (day, evening, night), weekend vs weekday, overnight vs same-day, and whether in an overtime week
-- For each segment, show total visits, average duration, and split visit rate
-- Day: 06:00–17:59, Evening: 18:00–21:59, Night: 22:00–05:59
-- Weekend: Saturday and Sunday
-- Overnight: end date > start date
-- In OT week: whether the visit falls in a week where the caregiver had >40 hours

-- If user is overtime many weeks - are there any specific caregivers who keep repeating
-- Threshold - 4+ weeks of overtime
-- Are specific caregivers or agencies disproportionately responsible for overtime? Output -> day -> # of overtime, night -> # of overtime 
-- 2 columsn - shift and count of overtime shifts
-- Do differnet shifts (day, evening, night) are more likely to have incurred overtime shifts?
WITH ot_weeks AS (
    SELECT caregiver_id, week_start
    FROM mart_overtime_by_week
    WHERE is_overtime
),
v AS (
    SELECT
        caregiver_id,
        DATE_TRUNC('week', COALESCE(in_at, start_at)) AS week_start,
        COALESCE(in_at, start_at) AS start_ts,
        COALESCE(out_at, end_at) AS end_ts,
        is_split,
        actual_mins
    FROM mart_visit_base
    WHERE actual_mins IS NOT NULL
),
labeled AS (
    SELECT
        v.*,
        CASE
        WHEN EXTRACT(HOUR FROM start_ts) BETWEEN 6 AND 17 THEN 'day' 
        WHEN EXTRACT(HOUR FROM start_ts) BETWEEN 18 AND 21 THEN 'evening'
        ELSE 'night' 
        END AS shift_bucket,
        CASE
        WHEN EXTRACT(ISODOW FROM start_ts) IN (6,7) THEN TRUE ELSE FALSE
        END AS is_weekend,
        (DATE(v.end_ts) > DATE(v.start_ts)) AS is_overnight,
        (v.caregiver_id, v.week_start) IN (SELECT caregiver_id, week_start FROM ot_weeks) AS in_ot_week
    FROM v
)
SELECT
    shift_bucket,
    is_weekend,
    is_overnight,
    in_ot_week,
    COUNT(*) AS visits,
    ROUND(AVG(actual_mins),1) AS avg_duration_mins,
    ROUND(100.0 * AVG(CASE WHEN is_split THEN 1 ELSE 0 END),1) AS split_rate_pct
FROM labeled
GROUP BY shift_bucket, is_weekend, is_overnight, in_ot_week
ORDER BY in_ot_week DESC, shift_bucket, is_weekend, is_overnight;
