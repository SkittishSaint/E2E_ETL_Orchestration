# E2E ETL Orchestration

An end-to-end ETL orchestration project built with Apache Airflow and Snowflake.
The system extracts data from REST APIs and PostgreSQL, transforms it with Python
and SQL, loads it into Snowflake raw tables, and publishes staging and analytics
layers for reporting.

## Project Scope

- Daily full ETL DAG for users, products, and orders
- Hourly incremental DAG with Snowflake watermark tracking
- Python transformation layer for source normalization
- Snowflake raw, staging, analytics, and control schemas
- Data quality validation before raw loads
- Snowflake SQL transformation scripts with clustering keys
- Retry, timeout, logging, and failure callback handling in Airflow
- Persisted pipeline monitoring and performance metrics in Snowflake

## Architecture

### Sources

- `DummyJSON API`
  - `users`
  - `products`
- `PostgreSQL`
  - `orders`

### Processing Layers

1. `Extract`
   - API extraction via `etl/extractors/api_extractor.py`
   - Database extraction via `etl/extractors/db_extractor.py`
2. `Transform`
   - Python normalization in `etl/transformers/transformer.py`
3. `Load`
   - Raw Snowflake loads and merges in `etl/loaders/snowflake_loader.py`
4. `Warehouse SQL`
   - Staging transforms in `sql/transformations/staging_transforms.sql`
   - Analytics transforms in `sql/transformations/analytics_transforms.sql`

### Snowflake Layers

- `ETL_DB.RAW`
- `ETL_DB.STAGING`
- `ETL_DB.ANALYTICS`
- `CONTROL_DB.CONTROL`

## DAGs

### Main DAG

File: `dags/etl_main_dag.py`

Schedule:
- Daily at `02:00 UTC`

Flow:
- Extract products, users, and orders
- Transform each dataset in Python
- Run dataset-level validation checks
- Load raw tables in Snowflake
- Log raw row-count metrics
- Build staging tables in Snowflake
- Build analytics tables in Snowflake

Key characteristics:
- Parallel extraction / transform / load branches
- Validation gates before raw loads
- Snowflake SQL scripts executed through the shared Snowflake loader

### Incremental DAG

File: `dags/incremental_load_dag.py`

Schedule:
- Hourly at the top of the hour

Flow:
- Read per-pipeline watermarks from Snowflake
- Extract incremental candidates from APIs and PostgreSQL
- Transform only extracted rows
- Upsert raw Snowflake tables
- Validate incremental row counts
- Advance watermarks only for branches that actually loaded rows

Key characteristics:
- Product, user, and order watermarks tracked independently
- No-op hours do not advance watermarks
- Products use merge/upsert instead of truncate-reload

## Repo Structure

```text
config/
  dag_config.py
  snowflake_config.py
dags/
  etl_main_dag.py
  incremental_load_dag.py
etl/
  extractors/
  transformers/
  loaders/
sql/
  transformations/
monitoring/
docker-compose.yml
requirements.txt
snowflake_setup.sql
setup_sample_database.py
ORDERS_README.md
```

## Setup

### 1. Start services

```bash
docker compose up -d
```

This starts:
- Airflow metadata PostgreSQL
- Source PostgreSQL database for orders
- Airflow webserver
- Airflow scheduler
- Airflow init job

The Airflow containers install the runtime packages listed in
`requirements.txt` through `_PIP_ADDITIONAL_REQUIREMENTS`.

### 2. Configure environment variables

Create or update `.env` with:

```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ETL_DB
SNOWFLAKE_WAREHOUSE=ETL_WH
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ETL_ROLE

API_BASE_URL=https://dummyjson.com
API_KEY=dummy

DB_HOST=ecommerce-db
DB_PORT=5432
DB_NAME=ecommerce
DB_USER=postgres
DB_PASSWORD=postgres

ALERT_EMAIL=your_email
ALERT_EMAIL_PASSWORD=your_email_password
ALERT_RECIPIENTS=recipient@example.com
```

### 3. Prepare Snowflake

Run:

```sql
snowflake_setup.sql
```

This script creates:
- Databases and schemas
- Raw / staging / analytics tables
- Control watermark table
- Pipeline run metrics table
- Warehouse
- Role grants

### 4. Seed the source orders database

```bash
python setup_sample_database.py
```

## Airflow Access

- URL: `http://localhost:8080`
- Username: `admin`
- Password: `admin`

## Scheduling and Control Settings

Shared DAG defaults are defined in `config/dag_config.py`.

Included controls:
- `retries = 2`
- `retry_exponential_backoff = True`
- `max_retry_delay = 60 minutes`
- `execution_timeout = 2 hours`
- failure callback with email attempt and protected logging

## Data Quality Checks

The main DAG validates transformed datasets before raw loads.

Checks implemented:
- dataset is not empty
- required columns exist
- required fields are not null

Current required fields:
- Orders: `order_id`, `user_id`, `order_date`
- Users: `user_id`, `email`
- Products: `product_id`, `product_name`

## Monitoring and Logging

Implemented monitoring today:
- Airflow task logs
- raw load row-count metrics in the main DAG
- incremental row-count metrics in the hourly DAG
- successful run metrics in `CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS`
- failure records from the shared callback in `config/dag_config.py`
- reusable monitoring helpers in `monitoring/pipeline_monitor.py`

Current limitation:
- SMTP alert delivery is configured but may still require valid reachable SMTP
  infrastructure before email notifications work in runtime.

## Snowflake Optimization

Optimization choices already applied:
- clustering keys on raw, staging, and analytics tables
- merge-based upserts for incremental raw tables
- schema separation for raw, staging, analytics, and control workloads

## Security and RBAC

`snowflake_setup.sql` includes:
- `ETL_ROLE`
- warehouse grants
- schema usage grants
- table privileges for raw, staging, analytics, and control schemas
- create privileges for Snowflake staging tables, file formats, and metrics

## Known Limitations

- API incrementality depends on whether the upstream source truly honors the
  `updated_since` filter; if it does not, those API branches behave like
  idempotent full pulls with upsert protection.
- The failure callback is hardened against SMTP errors, but successful email
  delivery still depends on working SMTP connectivity.
- A dashboard over `PIPELINE_RUN_METRICS` can be added later, but the metrics
  store needed for project evidence is implemented.

## Evidence of Completion

See `PROJECT_COMPLETION.md` for a mapped assessment of each project objective
and task against the current implementation.
