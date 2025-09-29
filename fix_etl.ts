import { Pool } from "pg";
import "dotenv/config";

async function fixAndReload() {
  // Use exact same connection string
  const DATABASE_URL = process.env.DATABASE_URL || "postgres://garygao@localhost:5432/zingage";
  console.log("Using DATABASE_URL:", DATABASE_URL);

  const pool = new Pool({ connectionString: DATABASE_URL });

  try {
    // First, check what we have
    const checkResult = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM stage_caregivers) as stage_caregivers,
        (SELECT COUNT(*) FROM stage_carelogs) as stage_carelogs,
        (SELECT COUNT(*) FROM model_caregiver) as model_caregivers,
        (SELECT COUNT(*) FROM model_carevisit) as model_carevisits
    `);

    console.log("Current state:", checkResult.rows[0]);

    // If stage tables have data but model tables don't, migrate
    if (checkResult.rows[0].stage_caregivers > 0 && checkResult.rows[0].model_caregivers == 0) {
      console.log("\nMigrating caregivers to model...");

      const result1 = await pool.query(`
        INSERT INTO model_caregiver (caregiver_id, agency_id, profile_id, applicant_status, employment_status)
        SELECT caregiver_id, agency_id, profile_id, applicant_status, status
        FROM stage_caregivers
        WHERE caregiver_id IS NOT NULL
        ON CONFLICT (caregiver_id) DO UPDATE
          SET agency_id = EXCLUDED.agency_id,
              profile_id = EXCLUDED.profile_id,
              applicant_status = EXCLUDED.applicant_status,
              employment_status = EXCLUDED.employment_status
      `);

      console.log(`Inserted/updated ${result1.rowCount} caregivers`);

      console.log("\nMigrating carelogs to model...");

      const result2 = await pool.query(`
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
              comment_chars= EXCLUDED.comment_chars
      `);

      console.log(`Inserted/updated ${result2.rowCount} visits`);
    }

    // Check final state
    const finalResult = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM model_caregiver) as model_caregivers,
        (SELECT COUNT(*) FROM model_carevisit) as model_carevisits
    `);

    console.log("\nFinal state:", finalResult.rows[0]);

    // Run a sample query
    console.log("\n=== Sample Query Results ===");
    const topPerformers = await pool.query(`
      SELECT caregiver_id, COUNT(*) AS completed
      FROM mart_completed_visits
      GROUP BY caregiver_id
      ORDER BY completed DESC
      LIMIT 5
    `);

    console.log("Top 5 performers:");
    console.table(topPerformers.rows);

  } finally {
    await pool.end();
  }
}

fixAndReload().catch(console.error);