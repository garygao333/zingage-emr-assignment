/**
 * Extract, Transform, Load (ETL) pipeline
 *
 * Loads caregiver and carelog data from CSV files into stage tables. 
 * It does so by streaming the CSV data row by row to avoid blowing up RAM. 
 */

import fs from "node:fs";
import { parse } from "csv-parse";
import { Pool } from "pg";
import "dotenv/config";
import { skip } from "node:test";

// Connect to database
const { DATABASE_URL } = process.env;
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL in .env");
  process.exit(1);
}
const pool = new Pool({ connectionString: DATABASE_URL });

// Maps boolean strings to boolean values. Returns NULL if empty or null. 
// THINK ABOUT: Should we convert status codes into booleans? 
function toBool(v: any): boolean | null {
    if (v === undefined || v === null || v === "") {
        return null;
    }
    const s = String(v).toLowerCase();
    if (["true", "TRUE"].includes(s)) {
        return true;
    }
    if (["false", "FALSE"].includes(s)) {
        return false;
    }
    return null;
}

// Converts numerical strings to numbers. Returns NULL if empty, null, or invalid (like infinity). 
function toInt(v: any): number | null {
    if (v === undefined || v === null || v === "") {
        return null;
    }
    const n = Number(v);
    if (Number.isFinite(n)) {
        return n;
    } else {
        return null;
    }
}

// Convert a value to string. Converts undefined, null, or empty string to null. 
function toNullable(v: any): string | null {
  if (v === undefined || v === null || v === "") {
    return null;
  }
  return String(v);
}

//Read caregiver CSV row by row and write each row into the caregiver stage table. 
// Skips rows without caregiver_id (primary key).
// If a row with the same caregiver_id already exists, update it instead of inserting a new row (UPSERT).
async function loadCaregivers(csvPath: string) {
  const client = await pool.connect();
  await client.query("BEGIN"); 
  let ok = 0, skipped = 0;
  try {
    const parser = fs.createReadStream(csvPath).pipe(parse({ columns: true, trim: true }));
    for await (const r of parser) {
        const caregiver_id = toNullable(r.caregiver_id);
        if (!caregiver_id)  {
            skipped++;
            continue;
        }
        await client.query(
            `INSERT INTO stage_caregivers (
                franchisor_id, agency_id, subdomain, profile_id, caregiver_id,
                external_id, first_name, last_name, email, phone_number, gender,
                applicant, birthday_date, onboarding_date, location_name, locations_id,
                applicant_status, status
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            ON CONFLICT (caregiver_id) DO UPDATE
            SET franchisor_id=$1, agency_id=$2, subdomain=$3, profile_id=$4,
                external_id=$6, first_name=$7, last_name=$8, email=$9, phone_number=$10, gender=$11,
                applicant=$12, birthday_date=$13, onboarding_date=$14, location_name=$15, locations_id=$16,
                applicant_status=$17, status=$18`,
          [
              toNullable(r.franchisor_id),
              toNullable(r.agency_id),
              toNullable(r.subdomain),
              toNullable(r.profile_id),
              caregiver_id,
              toNullable(r.external_id),
              toNullable(r.first_name),
              toNullable(r.last_name),
              toNullable(r.email),
              toNullable(r.phone_number),
              toNullable(r.gender),
              toBool(r.applicant),
              toNullable(r.birthday_date),
              toNullable(r.onboarding_date),
              toNullable(r.location_name),
              toNullable(r.locations_id),
              toNullable(r.applicant_status),
              toNullable(r.status)
          ]
        );
        ok++;
    }
    await client.query("COMMIT");
    console.log(`caregivers table: inserted/updated: ${ok}, skipped(due to missing primary key): ${skipped}`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("caregivers load failed:", e);
    throw e;
  } finally {
    client.release();
  }
}

// Load carelog CSV row by row into carelog stage table.
// Skips rows without carelog_id (primary key).
// If a row with the same carelog_id already exists, update it instead of inserting a new row (UPSERT).
async function loadCarelogs(csvPath: string) {
  const client = await pool.connect();
  await client.query("BEGIN");
  let ok = 0, skipped = 0;
  try {
    const parser = fs.createReadStream(csvPath).pipe(parse({ columns: true, trim: true }));
    for await (const r of parser) {
        const carelog_id = toNullable(r.carelog_id);
        if (!carelog_id) {
            skipped++; 
            continue; 
        }
        await client.query(
        `INSERT INTO stage_carelogs (
            franchisor_id, agency_id,
            carelog_id, caregiver_id, parent_id,
            start_datetime, end_datetime,
            clock_in_actual_datetime, clock_out_actual_datetime,
            clock_in_method, clock_out_method, status, split,
            documentation, general_comment_char_count
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (carelog_id) DO UPDATE
        SET franchisor_id=$1, agency_id=$2,
            caregiver_id=$4, parent_id=$5,
            start_datetime=$6, end_datetime=$7,
            clock_in_actual_datetime=$8, clock_out_actual_datetime=$9,
            clock_in_method=$10, clock_out_method=$11, status=$12, split=$13,
            documentation=$14, general_comment_char_count=$15`,
        [
            toNullable(r.franchisor_id),
            toNullable(r.agency_id),
            carelog_id,
            toNullable(r.caregiver_id),
            toNullable(r.parent_id),
            toNullable(r.start_datetime),
            toNullable(r.end_datetime),
            toNullable(r.clock_in_actual_datetime),
            toNullable(r.clock_out_actual_datetime),
            toNullable(r.clock_in_method),
            toNullable(r.clock_out_method),
            toNullable(r.status),
            toBool(r.split),
            toNullable(r.documentation),
            toInt(r.general_comment_char_count)
        ]
        );
        ok++;
    }
    await client.query("COMMIT");
    console.log(`carelogs: inserted/updated: ${ok}, skipped (due to missing primary key): ${skipped}`);
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("carelogs load failed:", e);
    throw e;
  } finally {
    client.release();
  }
}

// Main ETL function
async function main() {
    const caregiverCsv = "data/caregiver_data_20250415_sanitized.csv";
    const carelogCsv = "data/carelog_data_20250415_sanitized.csv";

    // verify database connection
    console.log("connecting to:", DATABASE_URL);
    const testResult = await pool.query("SELECT current_database(), current_user");
    console.log("connected to database:", testResult.rows[0]);

    await loadCaregivers(caregiverCsv);
    await loadCarelogs(carelogCsv);

    const countResult = await pool.query(
    "SELECT (SELECT COUNT(*) FROM stage_caregivers) AS caregivers, (SELECT COUNT(*) FROM stage_carelogs) AS carelogs"
    );
    console.log("count in database:", countResult.rows[0]);

    await pool.end();
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
