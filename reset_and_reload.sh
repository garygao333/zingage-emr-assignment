#!/bin/bash
set -e

echo "🔄 Resetting database and reloading data..."

# Drop and recreate all schemas
psql $DATABASE_URL <<EOF
DROP SCHEMA IF EXISTS stage CASCADE;
DROP SCHEMA IF EXISTS model CASCADE;
DROP SCHEMA IF EXISTS mart CASCADE;
DROP TABLE IF EXISTS public.stage_caregivers CASCADE;
DROP TABLE IF EXISTS public.stage_carelogs CASCADE;
DROP TABLE IF EXISTS public.model_caregiver CASCADE;
DROP TABLE IF EXISTS public.model_carevisit CASCADE;
EOF

echo "✅ Cleaned up existing tables"

# Recreate everything fresh
npm run db:ddl:init
echo "✅ Created staging tables in stage schema"

npm run db:ddl:norm
echo "✅ Created normalized tables in model schema"

npm run etl
echo "✅ Loaded data into stage schema"

npm run db:dml
echo "✅ Migrated data to normalized tables and ran queries"

# Verify
echo ""
echo "📊 Final verification:"
psql $DATABASE_URL <<EOF
SELECT 'stage.stage_caregivers' as table_name, COUNT(*) as rows FROM stage.stage_caregivers
UNION ALL
SELECT 'stage.stage_carelogs', COUNT(*) FROM stage.stage_carelogs
UNION ALL
SELECT 'model.model_caregiver', COUNT(*) FROM model.model_caregiver
UNION ALL
SELECT 'model.model_carevisit', COUNT(*) FROM model.model_carevisit;
EOF