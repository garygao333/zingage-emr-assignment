-- Find all tables named stage_caregivers or stage_carelogs in any schema
SELECT
    schemaname,
    tablename,
    (SELECT COUNT(*) FROM information_schema.tables t2
     WHERE t2.table_schema = t.schemaname
     AND t2.table_name = t.tablename) as exists
FROM pg_tables t
WHERE tablename IN ('stage_caregivers', 'stage_carelogs')
ORDER BY schemaname;

-- Check what's in each schema
SELECT
    n.nspname as schema_name,
    c.relname as table_name,
    pg_size_pretty(pg_relation_size(c.oid)) as size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname IN ('stage_caregivers', 'stage_carelogs')
AND c.relkind = 'r'
ORDER BY n.nspname, c.relname;

-- Check row counts in all possible locations
DO $$
DECLARE
    rec RECORD;
    count_val INTEGER;
BEGIN
    FOR rec IN
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE tablename IN ('stage_caregivers', 'stage_carelogs')
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', rec.schemaname, rec.tablename) INTO count_val;
        RAISE NOTICE '%.% has % rows', rec.schemaname, rec.tablename, count_val;
    END LOOP;
END $$;