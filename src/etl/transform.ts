/**
 * ETL Pipeline for Zingage Home Care EMR Data
 *
 * This script processes two CSV files:
 * 1. Caregiver profiles (~1M records)
 * 2. Care visit logs (~300K records)
 *
 * Features:
 * - Streaming processing (memory efficient)
 * - Transaction safety (all or nothing)
 * - Idempotent (can re-run safely)
 */

import fs from "node:fs";
import { parse } from "csv-parse";
import { Pool } from "pg";
import "dotenv/config";

// Database connection from environment
const { DATABASE_URL } = process.env;
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL in .env");
  process.exit(1);
}
const pool = new Pool({ connectionString: DATABASE_URL });

/**
 * Data transformation utilities
 * Empty strings become NULL (preserves meaning: "not provided")
 */

// Convert various boolean representations to true/false/null
function toBool(v: any): boolean | null {
  if (v === undefined || v === null || v === "") return null;
  const s = String(v).toLowerCase();
  if (["true", "t", "1", "yes", "y"].includes(s)) return true;
  if (["false", "f", "0", "no", "n"].includes(s)) return false;
  return null;
}

// Convert to number, preserving NULL for missing data
function toInt(v: any): number | null {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// Convert to string, treating empty as NULL
function toNullable(v: any): string | null {
  return (v === undefined || v === null || v === "") ? null : String(v);
}

/**
 * Load caregiver profiles from CSV
 * Only skips rows without caregiver_id (primary key)
 * Uses UPSERT to handle duplicates gracefully
 */
async function loadCaregivers(csvPath: string) {
  const client = await pool.connect();
  await client.query("BEGIN"); // Start transaction
  let ok = 0, skipped = 0;
  try {
    const parser = fs.createReadStream(csvPath).pipe(parse({ columns: true, trim: true }));
    for await (const r of parser) {
      const caregiver_id = toNullable(r.caregiver_id);
      if (!caregiver_id) { skipped++; continue; }             // only skip if key is missing
      await client.query(
        `INSERT INTO stage_caregivers (
           franchisor_id, agency_id, profile_id, caregiver_id, applicant_status, status
         ) VALUES ($1,$2,$3,$4,$5,$6)
         ON CONFLICT (caregiver_id) DO UPDATE
           SET franchisor_id=$1, agency_id=$2, profile_id=$3, applicant_status=$5, status=$6`,
        [
          toNullable(r.franchisor_id),
          toNullable(r.agency_id),
          toNullable(r.profile_id),
          caregiver_id,
          toNullable(r.applicant_status),
          toNullable(r.status),
        ]
      );
      ok++;
    }
    await client.query("COMMIT");
    console.log(`caregivers: inserted/updated=${ok}, skipped_missing_key=${skipped}`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("caregivers load failed:", e);
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Load care visit logs from CSV
 * Only skips rows without carelog_id (primary key)
 * Links to caregivers via caregiver_id foreign key
 */
async function loadCarelogs(csvPath: string) {
  const client = await pool.connect();
  await client.query("BEGIN"); // Start transaction
  let ok = 0, skipped = 0;
  try {
    const parser = fs.createReadStream(csvPath).pipe(parse({ columns: true, trim: true }));
    for await (const r of parser) {
      const carelog_id = toNullable(r.carelog_id);
      if (!carelog_id) { skipped++; continue; }               // only skip if key is missing
      await client.query(
        `INSERT INTO stage_carelogs (
           carelog_id, parent_id, caregiver_id,
           start_datetime, end_datetime,
           clock_in_actual_datetime, clock_out_actual_datetime,
           clock_in_method, clock_out_method, status, split, general_comment_char_count
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT (carelog_id) DO UPDATE
           SET parent_id=$2, caregiver_id=$3, start_datetime=$4, end_datetime=$5,
               clock_in_actual_datetime=$6, clock_out_actual_datetime=$7,
               clock_in_method=$8, clock_out_method=$9, status=$10, split=$11,
               general_comment_char_count=$12`,
        [
          carelog_id,
          toNullable(r.parent_id),
          toNullable(r.caregiver_id),
          toNullable(r.start_datetime),
          toNullable(r.end_datetime),
          toNullable(r.clock_in_actual_datetime),
          toNullable(r.clock_out_actual_datetime),
          toNullable(r.clock_in_method),
          toNullable(r.clock_out_method),
          toNullable(r.status),
          toBool(r.split),
          toInt(r.general_comment_char_count)
        ]
      );
      ok++;
    }
    await client.query("COMMIT");
    console.log(`carelogs: inserted/updated=${ok}, skipped_missing_key=${skipped}`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("carelogs load failed:", e);
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Main ETL execution
 * Processes both CSV files sequentially
 * Reports final counts for verification
 */
async function main() {
  // Allow custom CSV paths via command line arguments
  const caregiverCsv = process.argv[2] ?? "data/caregiver_data_20250415_sanitized.csv";
  const carelogCsv = process.argv[3] ?? "data/carelog_data_20250415_sanitized.csv";

  // Verify database connection
  console.log("Connecting to:", DATABASE_URL);
  const testResult = await pool.query("SELECT current_database(), current_user");
  console.log("Connected to database:", testResult.rows[0]);

  // Load data in order (caregivers first, then their visits)
  await loadCaregivers(caregiverCsv);
  await loadCarelogs(carelogCsv);

  // Verify final counts
  const countResult = await pool.query(
    "SELECT (SELECT COUNT(*) FROM stage_caregivers) as caregivers, (SELECT COUNT(*) FROM stage_carelogs) as carelogs"
  );
  console.log("Final counts in database:", countResult.rows[0]);

  await pool.end();
}

main().catch(err => { console.error(err); process.exit(1); });
