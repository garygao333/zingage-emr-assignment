#!/bin/bash
echo "=== Checking with direct psql command ==="
psql postgres://garygao@localhost:5432/zingage -c "SELECT COUNT(*) FROM stage_caregivers;"

echo ""
echo "=== Checking with DATABASE_URL ==="
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM stage_caregivers;"

echo ""
echo "=== Checking database name ==="
psql postgres://garygao@localhost:5432/zingage -c "SELECT current_database();"
psql "$DATABASE_URL" -c "SELECT current_database();"

echo ""
echo "=== Running Node.js check ==="
node -e "
const pg = require('pg');
require('dotenv/config');
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
pool.query('SELECT COUNT(*) FROM stage_caregivers').then(r => {
  console.log('Node.js sees:', r.rows[0].count, 'rows');
  pool.end();
});
"