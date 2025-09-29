# Zingage Caregiver Analytics Project

## 🚀 Quick Start Guide

### Prerequisites
- **PostgreSQL 16+** installed and running
- **Node.js 18+** and npm
- **Git** (optional, for version control)

### Installation

```bash
# 1. Install PostgreSQL (if not installed)
brew install postgresql@16
brew services start postgresql@16

# 2. Clone/navigate to project directory
cd zingage-project

# 3. Install Node dependencies
npm install

# 4. Create database
createdb zingage

# 5. Configure environment
# Edit .env file to match your PostgreSQL setup:
DATABASE_URL=postgres://[username]@localhost:5432/zingage
```

### Running the Pipeline

Execute these commands in order:

```bash
# 1. Create staging tables
npm run db:ddl:init
# Creates empty tables to receive CSV data

# 2. Load CSV data
npm run etl
# Loads ~1M caregiver records and ~300K visit logs

# 3. Create normalized schema
npm run db:ddl:norm
# Sets up analytical data model and views

# 4. Migrate and analyze
npm run db:dml
# Populates normalized tables and runs all queries
```

Or run everything at once:
```bash
npm run all
```

### Viewing Results

```bash
# Run analytical queries individually
psql $DATABASE_URL -c "SELECT * FROM mart_duration_stats;"
psql $DATABASE_URL -c "SELECT * FROM mart_reliability_by_caregiver LIMIT 10;"
psql $DATABASE_URL -c "SELECT * FROM mart_overtime_by_week WHERE is_overtime;"
```

## 📋 Project Overview

### What It Does
This ETL pipeline analyzes caregiver service data to identify:
- **Performance metrics**: Who completes the most visits
- **Reliability issues**: Caregivers with attendance problems
- **Operational efficiency**: Visit duration patterns and anomalies
- **Documentation quality**: Who provides detailed care notes
- **Overtime patterns**: Weekly hour tracking for labor compliance

### How It Works
1. **Extract**: Reads two sanitized CSV files containing caregiver profiles and visit logs
2. **Transform**: Cleans data (removes invalid records), calculates derived metrics
3. **Load**: Stores in PostgreSQL using a staged approach
4. **Analyze**: Materializes views for business intelligence queries

### Architecture Design
- **3-Layer Architecture**:
  - **Stage Layer**: Raw data matching CSV structure
  - **Model Layer**: Normalized 3NF schema
  - **Mart Layer**: Analytical views with pre-calculated metrics
- **Idempotent Design**: All operations can be re-run safely
- **Incremental Updates**: Uses UPSERT (ON CONFLICT) for data refresh

## 📁 Project Structure

### Configuration Files

#### `.env`
```env
DATABASE_URL=postgres://username@localhost:5432/zingage
LATE_THRESHOLD_MIN=10        # Minutes to consider "late"
MIN_COMPLETED_MIN=5          # Minimum visit duration
OVERTIME_WEEKLY_MIN=2400     # 40 hours in minutes
```
Environment variables for database connection and business rules.

#### `package.json`
Defines npm scripts and dependencies:
- `db:ddl:init`: Initialize staging tables
- `db:ddl:norm`: Create normalized schema
- `etl`: Run data extraction and loading
- `db:dml`: Migrate to model and run analytics

#### `tsconfig.json`
TypeScript compiler configuration for Node.js compatibility.

### Data Files (`/data`)

#### `caregiver_data_20250415_sanitized.csv`
- ~1M rows of caregiver profiles
- Fields: `caregiver_id`, `agency_id`, `status` (active/deactivated)

#### `carelog_data_20250415_sanitized.csv`
- ~300K visit records
- Fields: `carelog_id`, `caregiver_id`, scheduled times, actual clock times, comments

### SQL Scripts (`/sql`)

#### `ddl_init.sql`
Creates staging tables that mirror CSV structure:
```sql
CREATE TABLE stage_caregivers (
  caregiver_id PRIMARY KEY,
  agency_id, status, ...
)
CREATE TABLE stage_carelogs (
  carelog_id PRIMARY KEY,
  caregiver_id, start_datetime, clock_in_actual_datetime, ...
)
```

#### `ddl_norm.sql`
Creates normalized 3NF schema:
- `model_caregiver`: Caregiver master data
- `model_carevisit`: Visit transactions with foreign key to caregiver
- Multiple analytical views (`mart_*`) for reporting

Key views:
- `mart_completed_visits`: Filters valid completed visits
- `mart_reliability_by_caregiver`: Calculates missed/late/short visits
- `mart_duration_stats`: Aggregate duration statistics
- `mart_overtime_by_week`: Weekly hours per caregiver

#### `dml.sql`
1. Migrates data from stage → model tables
2. Runs analytical queries for all business questions
3. Outputs results directly

### Source Code (`/src/etl`)

#### `transform.ts`
Node.js ETL script that:
1. Connects to PostgreSQL using connection pool
2. Streams CSV files row-by-row (memory efficient)
3. Transforms data:
   - Converts strings to appropriate types
   - Handles NULL values (empty strings → NULL)
   - Skips rows missing primary keys
4. Performs UPSERT operations with transactions
5. Reports success/failure counts

Key features:
- Transaction safety (COMMIT/ROLLBACK)
- Error handling with detailed logging
- Idempotent (ON CONFLICT DO UPDATE)

## 📊 Interpreting Results

### Question 1: Caregiver Performance

**Top Performers Query**
```
caregiver_id | completed
56f5cc4b85   | 294
```
- Caregiver `56f5cc4b85` successfully completed 294 visits
- "Completed" = has both clock in/out times, duration >5 minutes

**Reliability Issues Query**
```
caregiver_id | missed | late | short_worked | issue_rate_pct
d92f7f9b1e   | 0      | 12   | 12          | 171.4
```
- Issue rate >100% means multiple problems per visit
- This caregiver was late AND worked too short on most visits
- Requires immediate intervention

### Question 2: Visit Duration Analysis

**Duration Statistics**
```
avg_mins | p50_mins | p90_mins
319.7    | 249.0    | 626.1
```
- Average visit: 5.3 hours
- Median visit: 4.1 hours
- 90th percentile: 10.4 hours
- Right-skewed distribution (some very long visits)

**Duration Outliers**
```
carelog_id | actual_mins
f186bbae33 | 913.9 (15+ hours!)
```
- Visits >15 hours are flagged as outliers
- Likely data quality issues (forgot to clock out)
- Used IQR method: outside Q1-1.5×IQR or Q3+1.5×IQR

### Question 3: Documentation Quality

**Consistent Documenters**
```
caregiver_id | detailed_rate | median_chars
abc123      | 0.85         | 245
```
- 85% of their visits have detailed notes (>200 chars)
- Consistently provides quality documentation

### Question 4: Overtime Analysis

**Weekly Overtime**
```
caregiver_id | week_start | hours
xyz789      | 2024-01-01 | 52.3
```
- Worked 52.3 hours in one week
- Exceeds 40-hour threshold
- Potential labor law compliance issue

## 🎯 Business Insights

1. **Staffing Quality**: Wide performance gap between top (294 visits) and problem caregivers
2. **Scheduling Issues**: High late arrival rates suggest unrealistic travel time between visits
3. **Data Quality**: 15+ hour visits indicate clock-out process needs improvement
4. **Compliance Risk**: Multiple caregivers exceeding 40 hours/week
5. **Training Needs**: Caregivers with >100% issue rates need immediate retraining

## 🔧 Troubleshooting

### Common Issues

1. **"relation does not exist"**: Run `npm run db:ddl:init` first
2. **Empty query results**: Ensure `DATABASE_URL` is correct in both .env and shell
3. **Connection refused**: Start PostgreSQL with `brew services start postgresql@16`
4. **Slow ETL**: Normal - processing 1M+ records takes 2-3 minutes

### Verifying Data

```bash
# Check row counts
psql $DATABASE_URL -c "SELECT COUNT(*) FROM stage_caregivers;"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM model_caregiver;"

# Test a view
psql $DATABASE_URL -c "SELECT * FROM mart_duration_stats;"
```

## 📈 Next Steps

1. **Add indexes** for caregiver_id lookups if queries are slow
2. **Implement incremental updates** for daily data refreshes
3. **Add data validation** rules for business constraints
4. **Create dashboard** using Grafana or Tableau connected to views
5. **Schedule automated runs** using cron or Apache Airflow