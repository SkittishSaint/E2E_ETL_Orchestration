"""
dags/etl_main_dag.py
──────────────────────
Primary ETL DAG: daily full-pipeline execution.

Flow:
  start
    ├── extract_orders (API)
    ├── extract_users (API)
    └── extract_products (DB)
          ↓ (all joined)
    ├── transform_orders
    ├── transform_users
    └── transform_products
          ↓
    ├── load_raw_orders
    ├── load_raw_users
    └── load_raw_products
          ↓
    run_snowflake_transformations   (SQL-level staging→analytics)
          ↓
    build_daily_aggregations
          ↓
    end
"""
import logging
import sys
from datetime import datetime



sys.path.append('/opt/airflow')

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from config.dag_config import DEFAULT_ARGS, DAILY_SCHEDULE, PIPELINE_START_DATE, CATCHUP, TAGS
from etl.extractors.api_extractor import extract_products, extract_users
from etl.extractors.db_extractor import extract_orders
from etl.transformers.transformer import (
    transform_users,
    transform_products,
    transform_orders,
)
from etl.loaders.snowflake_loader import load_raw_orders, load_raw_users, load_raw_products

logger = logging.getLogger(__name__)

# ── XCom keys ─────────────────────────────────────────────────────────────
XCOM_PRODUCTS = "products_df"
XCOM_USERS    = "users_df"
XCOM_ORDERS   = "orders_df"


# ── Task functions ────────────────────────────────────────────────────────

def _extract_products(**ctx):
    import os
    os.environ.setdefault("API_BASE_URL", "https://dummyjson.com")
    df = extract_products()
    ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=df.to_json())
    logger.info("Extracted %d products", len(df))


def _extract_users(**ctx):
    import os
    os.environ.setdefault("API_BASE_URL", "https://dummyjson.com")
    df = extract_users()
    ctx["ti"].xcom_push(key=XCOM_USERS, value=df.to_json())
    logger.info("Extracted %d users", len(df))


def _extract_orders(**ctx):
    df = extract_orders()
    ctx["ti"].xcom_push(key=XCOM_ORDERS, value=df.to_json())
    logger.info("Extracted %d orders", len(df))
#     df = extract_products()
#     ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=df.to_json())
#     logger.info("Extracted %d products", len(df))


def _transform_orders(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
    transformed = transform_orders(raw)
    ctx["ti"].xcom_push(key=XCOM_ORDERS, value=transformed.to_json())


def _transform_users(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_USERS))
    transformed = transform_users(raw)
    ctx["ti"].xcom_push(key=XCOM_USERS, value=transformed.to_json())


def _transform_products(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_PRODUCTS))
    transformed = transform_products(raw)
    ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=transformed.to_json())


def _load_orders(**ctx):
    df = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
    rows = load_raw_orders(df)
    logger.info("Loaded %d order rows to Snowflake", rows)


def _load_users(**ctx):
    df = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_USERS))
    rows = load_raw_users(df)
    logger.info("Loaded %d user rows to Snowflake", rows)


def _load_products(**ctx):
    df = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_PRODUCTS))
    rows = load_raw_products(df)
    logger.info("Loaded %d product rows to Snowflake", rows)


# def _build_daily_aggregations(**ctx):
#     orders_df = pd.read_json(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
#     agg_df = build_daily_sales_agg(orders_df)
#     from etl.loaders.snowflake_loader import SnowflakeLoader
#     from config.snowflake_config import SnowflakeTargets
#     with SnowflakeLoader() as loader:
#         rows = loader.load_truncate_replace(
#             df=agg_df,
#             table=SnowflakeTargets.AGG_DAILY_SALES,
#             schema="ANALYTICS",
#         )
#     logger.info("Daily aggregation: %d rows loaded.", rows)


# ── DAG definition ────────────────────────────────────────────────────────

with DAG(
    dag_id="etl_main_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily full ETL: API + DB → Snowflake",
    schedule_interval=DAILY_SCHEDULE,
    start_date=PIPELINE_START_DATE,
    catchup=CATCHUP,
    max_active_runs=1,
    tags=TAGS,
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ── Extract ────────────────────────────────────────────────────────────
    t_extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=_extract_products,
    )
    t_extract_users=PythonOperator(
        task_id="extract_users",
        python_callable=_extract_users,
    )
    t_extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=_extract_orders,
    )

    # # ── Transform ──────────────────────────────────────────────────────────
    t_transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=_transform_orders,
    )
    t_transform_users = PythonOperator(
        task_id="transform_users",
        python_callable=_transform_users,
    )
    t_transform_products = PythonOperator(
        task_id="transform_products",
        python_callable=_transform_products,
    )

    # ── Load (raw layer) ───────────────────────────────────────────────────
    t_load_orders = PythonOperator(
        task_id="load_raw_orders",
        python_callable=_load_orders,
    )
    t_load_users = PythonOperator(
        task_id="load_raw_users",
        python_callable=_load_users,
    )
    t_load_products = PythonOperator(
        task_id="load_raw_products",
        python_callable=_load_products,
    )

    # # ── SQL transformations via Snowflake operator ─────────────────────────
    # t_snowflake_staging = SnowflakeOperator(
    #     task_id="run_snowflake_staging_transforms",
    #     snowflake_conn_id="snowflake_default",
    #     sql="sql/transformations/staging_transforms.sql",
    #     autocommit=True,
    # )

    # t_snowflake_analytics = SnowflakeOperator(
    #     task_id="run_snowflake_analytics_transforms",
    #     snowflake_conn_id="snowflake_default",
    #     sql="sql/transformations/analytics_transforms.sql",
    #     autocommit=True,
    # )

    # # ── Aggregations ───────────────────────────────────────────────────────
    # t_daily_agg = PythonOperator(
    #     task_id="build_daily_aggregations",
    #     python_callable=_build_daily_aggregations,
    # )

    # ── Task dependency graph ──────────────────────────────────────────────
    start >> [t_extract_orders, t_extract_users, t_extract_products]

    t_extract_orders    >> t_transform_orders    >> t_load_orders
    t_extract_users     >> t_transform_users     >> t_load_users
    t_extract_products  >> t_transform_products  >> t_load_products

    [t_load_orders, t_load_users, t_load_products] >> end
