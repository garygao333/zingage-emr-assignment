-- Check model table structures and constraints
\echo '=== model_caregiver structure ==='
\d model_caregiver

\echo ''
\echo '=== model_carevisit structure ==='
\d model_carevisit

\echo ''
\echo '=== Checking for any existing data in model tables ==='
SELECT 'model_caregiver' as table_name, COUNT(*) as count FROM model_caregiver
UNION ALL
SELECT 'model_carevisit', COUNT(*) FROM model_carevisit;

\echo ''
\echo '=== Sample caregivers to be inserted ==='
SELECT caregiver_id, agency_id, status
FROM stage_caregivers
WHERE caregiver_id IS NOT NULL
LIMIT 5;

\echo ''
\echo '=== Sample carelogs to be inserted ==='
SELECT carelog_id, caregiver_id, start_datetime
FROM stage_carelogs
WHERE carelog_id IS NOT NULL AND caregiver_id IS NOT NULL
LIMIT 5;