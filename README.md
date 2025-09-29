# Zingage EMR Data Pipeline

A PostgreSQL-based ETL pipeline for analyzing home care EMR data, processing 1M+ caregiver records to identify performance, reliability, and operational insights.

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Create database
createdb zingage

# 3. Set environment
export DATABASE_URL="postgres://username@localhost:5432/zingage"

# 4. Run complete pipeline
npm run all
```

This executes:
1. Creates staging tables
2. Loads CSV data (~1M caregivers, ~300K visits)
3. Creates normalized schema
4. Runs analytical queries

## Project Structure

```
zingage-project/
├── data/                         # CSV data files (not in git)
│   ├── caregiver_data_*.csv     # 1M caregiver profiles
│   └── carelog_data_*.csv       # 300K visit logs
├── sql/
│   ├── ddl_init.sql             # Staging tables
│   ├── ddl_norm.sql             # Normalized schema + views
│   └── dml.sql                  # Data migration + analytics
├── src/etl/
│   └── transform.ts             # ETL pipeline
└── package.json                 # NPM scripts
```

## Key Features

- **Scalable**: Streams large CSV files without memory issues
- **Safe**: Transaction-based, can re-run without duplicates
- **Normalized**: 3NF schema design prevents redundancy
- **Performant**: Indexed foreign keys and materialized views

## Database Schema

```
┌─────────────────┐       ┌──────────────────┐
│ model_caregiver │───────│ model_carevisit  │
├─────────────────┤   1:N ├──────────────────┤
│ caregiver_id PK │◄──────│ carelog_id    PK │
│ agency_id       │       │ caregiver_id  FK │
│ employment_status│       │ actual times    │
└─────────────────┘       └──────────────────┘
```

## Analytical Insights

The pipeline answers four key business questions:

1. **Caregiver Performance**: Who are top/bottom performers?
2. **Visit Durations**: What are normal vs outlier visit lengths?
3. **Documentation Quality**: Who provides detailed care notes?
4. **Overtime Patterns**: Which caregivers exceed 40 hours/week?

See `ZINGAGE_ASSIGNMENT_ANSWERS.md` for detailed results and interpretation.

## Scripts

- `npm run db:ddl:init` - Create staging tables
- `npm run etl` - Load CSV data
- `npm run db:ddl:norm` - Create normalized model
- `npm run db:dml` - Run analytics

## Requirements

- PostgreSQL 14+
- Node.js 18+
- ~500MB disk space for data