-- Create schemas for logical organization (tables still in public)
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS model;
CREATE SCHEMA IF NOT EXISTS mart;

-- Stage tables mirror CSVs (typed, minimal constraints)
CREATE TABLE IF NOT EXISTS stage_caregivers (
  franchisor_id text,
  agency_id text,
  profile_id text,
  caregiver_id text PRIMARY KEY,
  applicant_status text,
  status text,                       -- 'active' | 'deactivated'
  _ingested_at timestamptz DEFAULT now()
);

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
  status text,                       -- keep as text; we won't decode numeric codes
  split boolean,
  general_comment_char_count int,
  _ingested_at timestamptz DEFAULT now()
);
