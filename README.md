# Zingage Data Challenge - Gary Gao

### To start

Install dependencies:

npm install

Create database:

createdb zingage

Configure the postgres environment by editing .env file: 

DATABASE_URL=postgres://[username]@localhost:5432/zingage

### To run the pipeline

Run ddl_init.sql to create the stage tables used to load the CSV data:

npm run db:ddl:init

Run etl/transform.ts to load in the CSV data into the empty stage tables: 

npm run etl

Run ddl_norm.sql to create the normalized schemas and mart schemas w/ analytical columns: 

npm run db:ddl:norm

Run dml.sql to migrate data to the normalized views/tables and run analytical queries: 

npm run db:dml

### File structure: 

zingage-project/
├─ data/
│  ├─ caregiver_data_20250415_sanitized.csv
│  └─ carelog_data_20250415_sanitized.csv
├─ sql/
│  ├─ ddl_init.sql (creates initial database schemas and stage tables matching CSV)
│  ├─ ddl_norm.sql (normalized database schema according to ERD)
│  └─ dml.sql (the CRUD operations, or the actual sql)
├─ src (or app)/
│  └─ etl/
│     ├─ transform.ts (read, transform, and load row by row with psql transform. )
├─ README (contains ERD)
├─ .env
└─ package.json

### Workflow

1. Run ddl_init.sql to create initial database schemes and tables to put the CSV information in. 
2. Run ETL sequentially, row by row to copy over data into the created database. 
3. Run ddl_norm.sql to normalize the database schema and transform it into the ERD format, possible 3NF, that we’re looking for. 
4. Run dml.sql to query the database and obtain the answers.  

### Key assumption: 

I assumed that all the datatime fields are already in the correct business time zone and thereby did no conversion. 

Since clock_in_method, clock_out_method, and status are some type of EMR code, I just stored them as text and passed them through the model without any decoding. 

I handeled empty strings as NULL values in order to preserve the 'not provided' sentiment. 

I used the character count as an indication of documentation quality. 

I defined a completed visit as one with both in_at and out_at, out_at > in_at, and actual_mins ≥ 5. 

I treated late arrival as one greater than 10 minutes. 

I defined short-worked as one where actual_mins < 25% * scheduled_mins.

For the duration stat model, I used ignore values < 6 min or > 16 h for summary statistics.

I used interquartile range (IQR) to identify outliers, where they're outside the 25th and 75th percentile. 

I defined working overtime as one where weekly minutes per caregiver in a week is greater than 2400 (40 hours). 

### Schema design and normalization: 

Throughout the pipeline, I used 3 main 'stages' of schemas: stage, model, and mart. Stage is a schema that mirrors the CSV columns and is used to load in the CSV data. Model is the 3rd Normal Form (3NF) normalized version of stage. Mart has additional derived attributes that provides analytics views used in the later queries to answer questions. 

I designed it as such so that it is robust, stable, and also very clearly traceble. Each stage is clearly separated - making it very easy to debug. I decided to put the analytical derivations in the form of MART views so that it is cleaner and easier to debug. 

### Tradeoffs

One tradeoff that I made was keeping the EMR codes as text. While it allowed faster development, it lost potentially helpful attributes. Another tradeoff was that I decided not to do data imputation on in_at and out_at. While I have fewer completed visits to do analysis on, I gained valuable information on caregiver reliability. Using ETL streaming and upserts is also a tradeoff. While I added complexity by parsing CSVs, upserting with transactions, and transforming rows by skipping those wth primary keys relative to a simple insert, I was able to get data of better quality while using less memory. This is especially important with the large dataset that we are dealing with. 

### Entity Relation Diagram (ERD)

##### SCHEMAS: 
stage: direct CSV mirrors to load in the raw data
model: 3NF normalized form of stage tables
mart: analytics views with derived metrics to answer questions. 


##### ENTITIES (for the model schema)

Caregiver model
- caregiver_id (text) 
- agency_id (text)
- profile_id (text)
- applicant_status (text)
- employment_status (text)
- is_active (bool)

Carelog model
- carelog_id (text)  
- caregiver_id (text)
- parent_id (text) 
- start_at (timestamptz) 
- end_at (timestamptz) 
- in_at (timestamptz) 
- out_at (timestamptz) 
- clock_in_method (text) 
- clock_out_method (text)
- status_code (text) 
- is_split (bool)
- comment_chars (int)

##### RELATIONSHIPS
MODEL_CAREGIVER 1 ──> N MODEL_CAREVISIT (one caregiver can have many care visits)
- They are joined by the caregiver_id key


##### NOTES
- stage tables are not shown, but they essentially mirror the CSVs columns for data loading.
- mart views are not shown, but they derive additional analytical attributes such as actual_mins, late_by_mins, reliability rollups, duration stats, outliers, documentation consistency, and overtime. 


THINK ABOUT --> Do I still need a database schema if I already have an ERD? 

