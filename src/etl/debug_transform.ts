import { Pool } from "pg";
import "dotenv/config";

const { DATABASE_URL } = process.env;
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL in .env");
  process.exit(1);
}

async function debug() {
  const pool = new Pool({ connectionString: DATABASE_URL });

  console.log("🔍 Debug ETL - Finding where data is");
  console.log("DATABASE_URL:", DATABASE_URL);

  // Check connection
  const dbInfo = await pool.query("SELECT current_database(), current_user, current_schema()");
  console.log("Connected to:", dbInfo.rows[0]);

  // Check search path
  const searchPath = await pool.query("SHOW search_path");
  console.log("Search path:", searchPath.rows[0].search_path);

  // Find all tables named stage_caregivers or stage_carelogs
  const tables = await pool.query(`
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE tablename IN ('stage_caregivers', 'stage_carelogs')
    ORDER BY schemaname
  `);
  console.log("\n📋 Found these tables:");
  console.table(tables.rows);

  // Check counts in each
  for (const table of tables.rows) {
    const countQuery = `SELECT COUNT(*) as count FROM ${table.schemaname}.${table.tablename}`;
    const result = await pool.query(countQuery);
    console.log(`${table.schemaname}.${table.tablename}: ${result.rows[0].count} rows`);
  }

  // Test insert
  const client = await pool.connect();
  try {
    console.log("\n🧪 Testing insert into stage_caregivers (no schema prefix)...");
    await client.query("BEGIN");

    // Try insert without schema
    await client.query(
      `INSERT INTO stage_caregivers (caregiver_id, status)
       VALUES ('TEST123', 'active')
       ON CONFLICT (caregiver_id) DO UPDATE SET status = 'active'`
    );

    // Check where it went
    const check1 = await client.query("SELECT COUNT(*) FROM stage_caregivers WHERE caregiver_id = 'TEST123'");
    console.log("Found in stage_caregivers:", check1.rows[0].count);

    // Check public schema explicitly
    const check2 = await client.query("SELECT COUNT(*) FROM public.stage_caregivers WHERE caregiver_id = 'TEST123'");
    console.log("Found in public.stage_caregivers:", check2.rows[0].count);

    await client.query("ROLLBACK");
    console.log("Rolled back test insert");
  } finally {
    client.release();
  }

  await pool.end();
}

debug().catch(console.error);