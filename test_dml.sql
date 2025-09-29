-- Test what's happening with the DML
\echo '=== Stage table counts ==='
SELECT 'stage_caregivers' as table, COUNT(*) FROM stage_caregivers
UNION ALL
SELECT 'stage_carelogs', COUNT(*) FROM stage_carelogs;

\echo ''
\echo '=== Testing the INSERT statements directly ==='

-- Test inserting into model_caregiver
\echo 'Inserting into model_caregiver...'
INSERT INTO model_caregiver (caregiver_id, agency_id, profile_id, applicant_status, employment_status)
SELECT caregiver_id, agency_id, profile_id, applicant_status, status
FROM stage_caregivers
WHERE caregiver_id IS NOT NULL
LIMIT 10  -- Just test with 10 rows first
ON CONFLICT (caregiver_id) DO UPDATE
  SET agency_id = EXCLUDED.agency_id,
      profile_id = EXCLUDED.profile_id,
      applicant_status = EXCLUDED.applicant_status,
      employment_status = EXCLUDED.employment_status;

\echo 'Checking model_caregiver count...'
SELECT COUNT(*) as model_caregiver_count FROM model_caregiver;

-- Test inserting into model_carevisit
\echo ''
\echo 'Inserting into model_carevisit...'
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
LIMIT 10  -- Just test with 10 rows first
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

\echo 'Checking model_carevisit count...'
SELECT COUNT(*) as model_carevisit_count FROM model_carevisit;

\echo ''
\echo '=== Checking for foreign key issues ==='
-- Check if caregiver_ids in carelogs exist in caregivers
SELECT COUNT(*) as carelogs_with_missing_caregiver
FROM stage_carelogs cl
WHERE cl.caregiver_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM model_caregiver mc
    WHERE mc.caregiver_id = cl.caregiver_id
  )
LIMIT 10;