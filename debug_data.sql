-- Check where the data actually is
\echo '=== Checking all tables ==='
SELECT 'stage_caregivers' as table_name, COUNT(*) as count FROM stage_caregivers
UNION ALL
SELECT 'stage_carelogs', COUNT(*) FROM stage_carelogs
UNION ALL
SELECT 'model_caregiver', COUNT(*) FROM model_caregiver
UNION ALL
SELECT 'model_carevisit', COUNT(*) FROM model_carevisit;

\echo ''
\echo '=== Sample data from stage_caregivers ==='
SELECT * FROM stage_caregivers LIMIT 3;

\echo ''
\echo '=== Sample data from stage_carelogs ==='
SELECT * FROM stage_carelogs LIMIT 3;

\echo ''
\echo '=== Checking for NULL caregiver_ids in stage_caregivers ==='
SELECT COUNT(*) as null_caregiver_ids FROM stage_caregivers WHERE caregiver_id IS NULL;

\echo ''
\echo '=== Checking for NULL caregiver_ids in stage_carelogs ==='
SELECT COUNT(*) as null_caregiver_ids FROM stage_carelogs WHERE caregiver_id IS NULL;

\echo ''
\echo '=== Checking caregiver_ids that exist in carelogs but not in caregivers ==='
SELECT COUNT(DISTINCT cl.caregiver_id) as orphan_caregiver_ids
FROM stage_carelogs cl
WHERE cl.caregiver_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM stage_caregivers cg WHERE cg.caregiver_id = cl.caregiver_id);