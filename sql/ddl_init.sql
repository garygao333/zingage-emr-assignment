/*
 * (Data Definition Language Initialization) Creating schemas and initial stage tables to mirror CSV files
 * We will use this to load in the raw CSV data before transforming to normalized tables
 * Enforces primary keys to avoid duplicates
 */

-- Create schemas if not exist
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS model;
CREATE SCHEMA IF NOT EXISTS mart;

-- Caregiver stage table
CREATE TABLE IF NOT EXISTS stage_caregivers (
    franchisor_id text,
    agency_id text,
    subdomain text,
    profile_id text,
    caregiver_id text PRIMARY KEY,
    external_id text,
    first_name text,
    last_name text,
    email text,
    phone_number text,
    gender text,
    applicant boolean,
    birthday_date date,
    onboarding_date date,
    location_name text,
    locations_id text,
    applicant_status text,
    status text,
    _ingested_at timestamptz DEFAULT now()
);

-- Carelogs stage table
CREATE TABLE IF NOT EXISTS stage_carelogs(
    franchisor_id text,
    agency_id text,
    carelog_id text PRIMARY KEY,
    caregiver_id text,
    parent_id text,
    start_datetime timestamptz,
    end_datetime timestamptz,
    clock_in_actual_datetime timestamptz,
    clock_out_actual_datetime timestamptz,
    clock_in_method text,
    clock_out_method text,
    status text,
    split boolean,
    documentation text,
    general_comment_char_count int,
    _ingested_at timestamptz DEFAULT now()
);
