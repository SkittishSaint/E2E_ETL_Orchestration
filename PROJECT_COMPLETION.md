# Project Completion Report

## Objective Statement

Develop an automated, scalable ETL orchestration system using Apache Airflow for
workflow scheduling and Snowflake as a cloud data warehouse, ensuring reliable
pipeline execution, monitoring, and optimized data transformations for
analytical reporting.

## Overall Status

Status: `Completed for MCA final semester submission`

The repository implements a working end-to-end ETL orchestration project with:

- Apache Airflow DAGs for daily full loads and hourly incremental loads.
- REST API and PostgreSQL extraction.
- Python-based cleaning, normalization, masking, and enrichment.
- Snowflake raw, staging, analytics, and control layers.
- Merge/upsert loading, watermark tracking, data quality gates, retry controls,
  failure callbacks, and persisted monitoring metrics.
- SQL transformations with clustered reporting tables for analytical queries.

Operational caveats are documented at the end of this report. They do not block
the core project objectives.

## Project Tasks Assessment

| No. | Project Task | Status | Repository Evidence |
| --- | --- | --- | --- |
| 1 | Study DAG-based workflow orchestration in Airflow | Completed | `dags/etl_main_dag.py`, `dags/incremental_load_dag.py` define scheduled DAGs, task dependencies, callbacks, and execution control. |
| 2 | Install and configure Apache Airflow environment | Completed | `docker-compose.yml` defines Airflow webserver, scheduler, metadata PostgreSQL, init job, volumes, environment variables, and runtime package installation. |
| 3 | Design DAGs for multi-stage ETL pipelines | Completed | The main DAG has extract, transform, validate, load, metrics, staging SQL, and analytics SQL stages. The incremental DAG has watermark, extract, transform, upsert, validate, metrics, and watermark update stages. |
| 4 | Extract data from APIs and relational databases | Completed | `etl/extractors/api_extractor.py` extracts users/products from DummyJSON; `etl/extractors/db_extractor.py` extracts orders from PostgreSQL. |
| 5 | Transform datasets using Python and SQL | Completed | `etl/transformers/transformer.py` handles Python transforms; `sql/transformations/staging_transforms.sql` and `sql/transformations/analytics_transforms.sql` handle Snowflake SQL transforms. |
| 6 | Load processed data into Snowflake warehouse | Completed | `etl/loaders/snowflake_loader.py` implements Snowflake connections, truncate-load, merge/upsert, SQL execution, and watermark writes. |
| 7 | Implement incremental loading strategies | Completed | `dags/incremental_load_dag.py` reads Snowflake watermarks, extracts since watermark, upserts raw tables, validates row counts, and advances watermarks only after successful loads. |
| 8 | Schedule automated daily and hourly jobs | Completed | `config/dag_config.py` sets `DAILY_SCHEDULE = "0 2 * * *"` and `HOURLY_SCHEDULE = "0 * * * *"`. |
| 9 | Set up task dependencies and retry mechanisms | Completed | DAG dependency graphs are explicit; `DEFAULT_ARGS` configures retries, retry delay, exponential backoff, max retry delay, timeout, and failure callback. |
| 10 | Monitor pipeline logs and failures | Completed | Airflow logs, shared failure callback, email alert attempt, and `monitoring/pipeline_monitor.py` records failures in `CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS`. |
| 11 | Optimize Snowflake queries using clustering keys | Completed | `snowflake_setup.sql`, staging SQL, and analytics SQL define clustered raw, staging, analytics, watermark, and metrics tables. |
| 12 | Apply role-based access controls | Completed | `snowflake_setup.sql` creates `ETL_ROLE`, grants database/schema/table/warehouse privileges, and grants required create privileges for ETL operations. |
| 13 | Implement data quality validation tasks | Completed | Main DAG validation tasks check empty datasets, required columns, and nulls. Incremental DAG validates row-count results before watermark updates. |
| 14 | Document workflow design and performance metrics | Completed | `README.md`, `ORDERS_README.md`, DAG docstrings, this completion report, and `PIPELINE_RUN_METRICS` provide workflow and performance evidence. |

## Architecture Summary

Sources:

- DummyJSON API: users and products.
- PostgreSQL ecommerce database: orders.

Pipeline layers:

- Extract: API pagination/retry/rate-limit logic and PostgreSQL timestamp-based
  extraction.
- Transform: pandas normalization, deduplication, PII masking, surrogate keys,
  JSON-safe nested field handling, and audit columns.
- Load: Snowflake raw table writes using upsert or truncate-load strategies.
- Warehouse SQL: staging and analytics tables built inside Snowflake.
- Control: watermarks and pipeline run metrics stored in Snowflake.

## Monitoring and Metrics

Monitoring evidence is now implemented in both runtime logs and Snowflake:

- `log_load_metrics` records daily raw load counts in Airflow logs and XCom.
- `validate_incremental_loads` records hourly incremental row counts.
- `persist_pipeline_metrics` records successful full-pipeline metrics.
- `persist_incremental_metrics` records successful incremental-pipeline metrics.
- `on_failure_callback` records failed task details.
- `CONTROL_DB.CONTROL.PIPELINE_RUN_METRICS` stores DAG ID, task ID, run ID,
  run type, status, rows loaded, duration seconds, error message, and timestamp.

## Final Deliverables

- `docker-compose.yml`
- `requirements.txt`
- `snowflake_setup.sql`
- `dags/etl_main_dag.py`
- `dags/incremental_load_dag.py`
- `etl/extractors/api_extractor.py`
- `etl/extractors/db_extractor.py`
- `etl/transformers/transformer.py`
- `etl/loaders/snowflake_loader.py`
- `monitoring/pipeline_monitor.py`
- `sql/transformations/staging_transforms.sql`
- `sql/transformations/analytics_transforms.sql`
- `setup_sample_database.py`
- `README.md`
- `ORDERS_README.md`

## Operational Notes

- API incrementality depends on upstream API support for the `updated_since`
  parameter. If the API ignores it, the branch still remains idempotent because
  Snowflake merge/upsert logic protects the raw tables from duplicate rows.
- SMTP alert delivery requires valid SMTP credentials and reachable SMTP
  infrastructure. The failure callback is protected so SMTP issues are logged
  without hiding the original pipeline failure.
- Generated CSV/text outputs under `etl_output/` are runtime artifacts, not
  required source deliverables.

## Final Assessment

The repository satisfies the stated MCA project objective: it demonstrates an
automated Airflow-based ETL orchestration system that extracts from API and
relational sources, transforms data with Python and SQL, loads Snowflake,
supports incremental processing, applies validation and RBAC, monitors runtime
results, and produces analytics-ready tables for reporting.
