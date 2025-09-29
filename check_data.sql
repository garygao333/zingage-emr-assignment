-- Check if data exists in staging tables
SELECT 'stage_caregivers' as table_name, COUNT(*) as row_count FROM stage_caregivers
UNION ALL
SELECT 'stage_carelogs' as table_name, COUNT(*) as row_count FROM stage_carelogs;

-- Sample some data
SELECT * FROM stage_caregivers LIMIT 5;
SELECT * FROM stage_carelogs LIMIT 5;