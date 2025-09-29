/*
 * STAGE LAYER: Raw Data Ingestion
 *
 * Purpose: Mirror CSV structure exactly for initial load
 * No complex constraints to allow all data to load
 * Only primary keys enforced to prevent duplicates
 */

-- Create logical schemas (for future use)
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS model;
CREATE SCHEMA IF NOT EXISTS mart;

-- Caregiver profiles staging table
-- Matches: caregiver_data_20250415_sanitized.csv
CREATE TABLE IF NOT EXISTS stage_caregivers (
  franchisor_id text,
  agency_id text,
  profile_id text,
  caregiver_id text PRIMARY KEY,
  applicant_status text,
  status text,                       -- Employment status: 'active' or 'deactivated'
  _ingested_at timestamptz DEFAULT now()  -- Audit timestamp
);

-- Care visit logs staging table
-- Matches: carelog_data_20250415_sanitized.csv
CREATE TABLE IF NOT EXISTS stage_carelogs (
  carelog_id text PRIMARY KEY,
  parent_id text,
  caregiver_id text,
  start_datetime timestamptz,
  end_datetime timestamptz,
  clock_in_actual_datetime timestamptz,
  clock_out_actual_datetime timestamptz,
  clock_in_method text,
  clock_out_method text,
  status text,                       -- Visit status code (kept as-is from EMR)
  split boolean,                     -- TRUE if shift was split into multiple
  general_comment_char_count int,    -- Length of caregiver's visit notes
  _ingested_at timestamptz DEFAULT now()  -- Audit timestamp
);
