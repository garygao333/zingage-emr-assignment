/**
 * Extract, Transform, Load (ETL) pipeline
 *
 * Migrate data from stage to normalized model created in ddl_norm.sql
 * Loads caregiver and carelog data from stage tables into normalized model tables defined in ddl_norm.sql.
 * Truncates normalized tables first. 
 */


import { Pool } from "pg";
import "dotenv/config";

// Connect to database
const { DATABASE_URL, RESET_MODEL } = process.env;
if (!DATABASE_URL) {
    console.error("Missing DATABASE_URL in .env");
    process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL });

async function one(sql: string, params: any[] = []) {
    return pool.query(sql, params);
}

// Main ETL function
async function main() {
    console.log("starting transform norm");
    const info = await one("SELECT current_database() AS db, current_user AS usr");
    console.log("DB info:", info.rows[0]);

    // Basic sanity
    const stageCounts = await one(`
        SELECT (SELECT COUNT(*) FROM stage_caregivers) AS stage_caregivers,
            (SELECT COUNT(*) FROM stage_carelogs)   AS stage_carelogs
    `);
    console.log("Stage counts:", stageCounts.rows[0]);

    //Reset model tables
    console.log("Resetting model tables");
    await one(`TRUNCATE model_carevisit CASCADE`);
    await one(`TRUNCATE model_caregiver CASCADE`);
    await one(`TRUNCATE model_applicant_status CASCADE`);
    await one(`TRUNCATE model_employment_status CASCADE`);
    await one(`TRUNCATE model_external_identifier CASCADE`);
    await one(`TRUNCATE model_profile CASCADE`);
    await one(`TRUNCATE model_agency CASCADE`);
    await one(`TRUNCATE model_franchisor CASCADE`);
    await one(`TRUNCATE model_locations CASCADE`);

    // locations from caregivers
    await one(`
        INSERT INTO model_locations (locations_id, name)
        SELECT DISTINCT locations_id, NULLIF(location_name,'') AS name
        FROM stage_caregivers
        WHERE locations_id IS NOT NULL
        ON CONFLICT (locations_id) DO UPDATE
        SET name = COALESCE(EXCLUDED.name, model_locations.name)
    `);

    // franchisor from both caregivers and carelogs
    await one(`
        INSERT INTO model_franchisor (franchisor_id)
        SELECT DISTINCT franchisor_id
        FROM stage_caregivers
        WHERE franchisor_id IS NOT NULL
        ON CONFLICT DO NOTHING
    `);
    await one(`
        INSERT INTO model_franchisor (franchisor_id)
        SELECT DISTINCT franchisor_id
        FROM stage_carelogs
        WHERE franchisor_id IS NOT NULL
        ON CONFLICT DO NOTHING
    `);

    // agency from both caregivers and carelogs
    await one(`
        INSERT INTO model_agency (agency_id)
        SELECT DISTINCT agency_id
        FROM stage_caregivers
        WHERE agency_id IS NOT NULL
        ON CONFLICT DO NOTHING
    `);
    await one(`
        INSERT INTO model_agency (agency_id)
        SELECT DISTINCT agency_id
        FROM stage_carelogs
        WHERE agency_id IS NOT NULL
        ON CONFLICT DO NOTHING
    `);

    // profile/external_id from caregivers
    await one(`
        INSERT INTO model_profile (profile_id)
        SELECT DISTINCT profile_id
        FROM stage_caregivers
        WHERE profile_id IS NOT NULL
        ON CONFLICT DO NOTHING
    `);
    await one(`
        INSERT INTO model_external_identifier (external_id)
        SELECT DISTINCT external_id
        FROM stage_caregivers
        WHERE external_id IS NOT NULL AND external_id <> ''
        ON CONFLICT DO NOTHING
    `);

    // status/applicant status from caregivers
    await one(`
        INSERT INTO model_employment_status (employment_status)
        SELECT DISTINCT status
        FROM stage_caregivers
        WHERE status IS NOT NULL
        ON CONFLICT (employment_status) DO NOTHING
    `);
    await one(`
        INSERT INTO model_applicant_status (applicant_status)
        SELECT DISTINCT applicant_status
        FROM stage_caregivers
        WHERE applicant_status IS NOT NULL
        ON CONFLICT (applicant_status) DO NOTHING
    `);

    // Insert/Upsert caregivers
    await one(`
        INSERT INTO model_caregiver (
        caregiver_id,
        franchisor_id,
        agency_id,
        locations_id,
        profile_id,
        external_id,
        applicant_status_id,
        employment_status_id,
        is_active
        )
        SELECT
        sc.caregiver_id,
        sc.franchisor_id,
        sc.agency_id,
        sc.locations_id,
        sc.profile_id,
        NULLIF(sc.external_id,'') AS external_id,
        mas.applicant_status_id,
        mes.employment_status_id,
        CASE WHEN mes.employment_status = 'active' THEN TRUE ELSE FALSE END AS is_active
        FROM stage_caregivers sc
        LEFT JOIN model_applicant_status mas
        ON mas.applicant_status = sc.applicant_status
        LEFT JOIN model_employment_status mes
        ON mes.employment_status = sc.status
        WHERE sc.caregiver_id IS NOT NULL
        ON CONFLICT (caregiver_id) DO UPDATE
        SET franchisor_id        = EXCLUDED.franchisor_id,
            agency_id            = EXCLUDED.agency_id,
            locations_id         = EXCLUDED.locations_id,
            profile_id           = EXCLUDED.profile_id,
            external_id          = EXCLUDED.external_id,
            applicant_status_id  = EXCLUDED.applicant_status_id,
            employment_status_id = EXCLUDED.employment_status_id,
            is_active            = EXCLUDED.is_active
    `);

    const cgCount = await one(`SELECT COUNT(*) AS model_caregiver FROM model_caregiver`);
    console.log("model_caregiver count:", cgCount.rows[0].model_caregiver);

    // This ensures parent rows can be inserted in the same transaction as children.
    console.log("Inserting carevisits (deferred parent FK)...");
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        await client.query("SET CONSTRAINTS ALL DEFERRED");

        // Insert/Upsert carevisits
        await client.query(`
        INSERT INTO model_carevisit (
            carelog_id,
            caregiver_id,
            agency_id,
            franchisor_id,
            parent_id,
            start_at,
            end_at,
            in_at,
            out_at,
            clock_in_method,
            clock_out_method,
            status_code,
            is_split,
            comment_chars
        )
        SELECT
            cl.carelog_id,
            cl.caregiver_id,
            cl.agency_id,
            cl.franchisor_id,
            NULLIF(cl.parent_id,'') AS parent_id,
            cl.start_datetime,
            cl.end_datetime,
            cl.clock_in_actual_datetime,
            cl.clock_out_actual_datetime,
            cl.clock_in_method,
            cl.clock_out_method,
            cl.status,
            cl.split,
            cl.general_comment_char_count
        FROM stage_carelogs cl
        WHERE cl.carelog_id IS NOT NULL
            AND cl.caregiver_id IS NOT NULL
        ON CONFLICT (carelog_id) DO UPDATE
            SET caregiver_id     = EXCLUDED.caregiver_id,
                agency_id        = EXCLUDED.agency_id,
                franchisor_id    = EXCLUDED.franchisor_id,
                parent_id        = EXCLUDED.parent_id,
                start_at         = EXCLUDED.start_at,
                end_at           = EXCLUDED.end_at,
                in_at            = EXCLUDED.in_at,
                out_at           = EXCLUDED.out_at,
                clock_in_method  = EXCLUDED.clock_in_method,
                clock_out_method = EXCLUDED.clock_out_method,
                status_code      = EXCLUDED.status_code,
                is_split         = EXCLUDED.is_split,
                comment_chars    = EXCLUDED.comment_chars
        `);

        await client.query("COMMIT");
    } catch (e) {
        await client.query("ROLLBACK");
        console.error("carevisit load failed:", e);
        throw e;
    } finally {
        client.release();
    }

    // Final counts
    const mvCount = await one(`
        SELECT 
        (SELECT COUNT(*) FROM model_caregiver)  AS caregivers,
        (SELECT COUNT(*) FROM model_carevisit) AS carevisits
    `);
    console.log("model counts:", mvCount.rows[0]);

    await pool.end();
    console.log("transform norm done");
}

// Run main
main().catch(err => {
  console.error(err);
  process.exit(1);
});
