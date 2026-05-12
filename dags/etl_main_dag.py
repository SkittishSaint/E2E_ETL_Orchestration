"""
dags/etl_main_dag.py
──────────────────────
Primary ETL DAG: daily full-pipeline execution.

Flow:
  start
    ├── extract_products (API)
    ├── extract_users (API)
    └── extract_orders (DB)
          ↓
    ├── transform_products
    ├── transform_users
    └── transform_orders
          ↓
    ├── validate_products
    ├── validate_users
    └── validate_orders
          ↓
    ├── load_raw_products
    ├── load_raw_users
    └── load_raw_orders
          ↓
    log_load_metrics
          ↓
    run_snowflake_staging_transforms
          ↓
    run_snowflake_analytics_transforms
          ↓
    end
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import List

import numpy as np

sys.path.append('/opt/airflow')

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from config.dag_config import DEFAULT_ARGS, DAILY_SCHEDULE, PIPELINE_START_DATE, CATCHUP, TAGS
from etl.extractors.api_extractor import extract_products, extract_users
from etl.extractors.db_extractor import extract_orders
from etl.transformers.transformer import (
    transform_users,
    transform_products,
    transform_orders,
)
from etl.loaders.snowflake_loader import (
    SnowflakeLoader,
    load_raw_orders,
    load_raw_users,
    load_raw_products,
)
from monitoring.pipeline_monitor import duration_seconds_from_context, record_pipeline_metric

logger = logging.getLogger(__name__)

# ── XCom keys ─────────────────────────────────────────────────────────────
XCOM_PRODUCTS = "products_df"
XCOM_USERS    = "users_df"
XCOM_ORDERS   = "orders_df"
XCOM_ROWS_ORDERS = "orders_loaded_rows"
XCOM_ROWS_USERS = "users_loaded_rows"
XCOM_ROWS_PRODUCTS = "products_loaded_rows"

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql", "transformations")


def _normalize_xcom_df(df: pd.DataFrame):
    if df is None:
        return []
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].astype(str)

    records = df.to_dict(orient="records")

    def _normalize_value(value):
        # Handle null/NaN for scalar types
        try:
            if pd.isna(value):
                return None
        except (ValueError, TypeError):
            pass
        
        # Handle nested structures: lists, dicts, arrays
        if isinstance(value, (list, dict, np.ndarray)):
            return json.dumps(value, default=str)
        
        # Handle pandas/numpy scalars
        if isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        if isinstance(value, np.generic):
            return value.item()
        
        return value

    return [{k: _normalize_value(v) for k, v in rec.items()} for rec in records]


def _deserialize_xcom_df(payload):
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return pd.DataFrame([payload])
    if isinstance(payload, list):
        # Try to parse JSON strings in list items (nested structures)
        parsed_records = []
        for rec in payload:
            if isinstance(rec, dict):
                parsed_rec = {}
                for k, v in rec.items():
                    if isinstance(v, str) and (v.startswith('[') or v.startswith('{')):
                        try:
                            parsed_rec[k] = json.loads(v)
                        except (json.JSONDecodeError, ValueError):
                            parsed_rec[k] = v
                    else:
                        parsed_rec[k] = v
                parsed_records.append(parsed_rec)
            else:
                parsed_records.append(rec)
        return pd.DataFrame.from_records(parsed_records)
    if isinstance(payload, dict):
        return pd.DataFrame.from_dict(payload)
    return pd.DataFrame(payload)


# ── Task functions ────────────────────────────────────────────────────────

def _extract_products(**ctx):
    import os
    os.environ.setdefault("API_BASE_URL", "https://dummyjson.com")
    df = extract_products()
    ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=_normalize_xcom_df(df))
    logger.info("Extracted %d products", len(df))


def _extract_users(**ctx):
    import os
    os.environ.setdefault("API_BASE_URL", "https://dummyjson.com")
    df = extract_users()
    ctx["ti"].xcom_push(key=XCOM_USERS, value=_normalize_xcom_df(df))
    logger.info("Extracted %d users", len(df))


def _extract_orders(**ctx):
    df = extract_orders()
    ctx["ti"].xcom_push(key=XCOM_ORDERS, value=_normalize_xcom_df(df))
    logger.info("Extracted %d orders", len(df))
#     df = extract_products()
#     ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=_normalize_xcom_df(df))
#     logger.info("Extracted %d products", len(df))


def _transform_orders(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
    transformed = transform_orders(raw)
    ctx["ti"].xcom_push(key=XCOM_ORDERS, value=_normalize_xcom_df(transformed))


def _transform_users(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_USERS))
    transformed = transform_users(raw)
    ctx["ti"].xcom_push(key=XCOM_USERS, value=_normalize_xcom_df(transformed))


def _transform_products(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_PRODUCTS))
    transformed = transform_products(raw)
    ctx["ti"].xcom_push(key=XCOM_PRODUCTS, value=_normalize_xcom_df(transformed))


def _load_orders(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
    rows = load_raw_orders(df)
    ctx["ti"].xcom_push(key=XCOM_ROWS_ORDERS, value=rows)
    logger.info("Loaded %d order rows to Snowflake", rows)
    return rows


def _load_users(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_USERS))
    rows = load_raw_users(df)
    ctx["ti"].xcom_push(key=XCOM_ROWS_USERS, value=rows)
    logger.info("Loaded %d user rows to Snowflake", rows)
    return rows


def _load_products(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_PRODUCTS))
    rows = load_raw_products(df)
    ctx["ti"].xcom_push(key=XCOM_ROWS_PRODUCTS, value=rows)
    logger.info("Loaded %d product rows to Snowflake", rows)
    return rows


def _validate_transformed_dataset(df: pd.DataFrame, dataset: str, required_columns: List[str]) -> None:
    if df.empty:
        raise ValueError(f"{dataset} transform produced an empty DataFrame")

    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise ValueError(f"{dataset} missing required columns: {missing_columns}")

    null_counts = df[required_columns].isna().sum()
    null_issues = {col: int(null_counts[col]) for col in required_columns if null_counts[col] > 0}
    if null_issues:
        raise ValueError(f"{dataset} has null values in required columns: {null_issues}")

    logger.info("Data quality check passed for %s: %d rows", dataset, len(df))


def _validate_orders(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_ORDERS))
    _validate_transformed_dataset(df, "orders", ["order_id", "user_id", "order_date"])
    ctx["ti"].xcom_push(key="orders_validated", value=True)


def _validate_users(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_USERS))
    _validate_transformed_dataset(df, "users", ["user_id", "email"])
    ctx["ti"].xcom_push(key="users_validated", value=True)


def _validate_products(**ctx):
    df = _deserialize_xcom_df(ctx["ti"].xcom_pull(key=XCOM_PRODUCTS))
    _validate_transformed_dataset(df, "products", ["product_id", "product_name"])
    ctx["ti"].xcom_push(key="products_validated", value=True)


def _log_load_metrics(**ctx):
    orders = int(ctx["ti"].xcom_pull(key=XCOM_ROWS_ORDERS) or 0)
    users = int(ctx["ti"].xcom_pull(key=XCOM_ROWS_USERS) or 0)
    products = int(ctx["ti"].xcom_pull(key=XCOM_ROWS_PRODUCTS) or 0)

    logger.info(
        "Raw load metrics: orders=%d, users=%d, products=%d",
        orders,
        users,
        products,
    )
    ctx["ti"].xcom_push(key="raw_load_metrics", value={
        "orders": orders,
        "users": users,
        "products": products,
    })


def _persist_pipeline_metrics(**ctx):
    metrics = ctx["ti"].xcom_pull(
        key="raw_load_metrics",
        task_ids="log_load_metrics",
    ) or {}
    rows_loaded = sum(int(metrics.get(name) or 0) for name in ("orders", "users", "products"))

    record_pipeline_metric(
        dag_id=ctx["dag"].dag_id,
        task_id=ctx["ti"].task_id,
        run_id=ctx["run_id"],
        run_type="daily_full",
        status="SUCCESS",
        rows_loaded=rows_loaded,
        duration_seconds=duration_seconds_from_context(ctx),
    )


def _run_snowflake_sql_file(sql_filename: str) -> None:
    sql_path = os.path.join(SQL_DIR, sql_filename)
    with SnowflakeLoader() as loader:
        loader.execute_sql_file(sql_path)
    logger.info("Completed Snowflake SQL script: %s", sql_filename)


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
    template_searchpath=[SQL_DIR],
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

    t_validate_orders = PythonOperator(
        task_id="validate_orders",
        python_callable=_validate_orders,
    )
    t_validate_users = PythonOperator(
        task_id="validate_users",
        python_callable=_validate_users,
    )
    t_validate_products = PythonOperator(
        task_id="validate_products",
        python_callable=_validate_products,
    )

    t_log_load_metrics = PythonOperator(
        task_id="log_load_metrics",
        python_callable=_log_load_metrics,
    )

    # ── SQL transformations via Snowflake operator ─────────────────────────
    t_snowflake_staging = PythonOperator(
        task_id="run_snowflake_staging_transforms",
        python_callable=_run_snowflake_sql_file,
        op_kwargs={"sql_filename": "staging_transforms.sql"},
    )

    t_snowflake_analytics = PythonOperator(
        task_id="run_snowflake_analytics_transforms",
        python_callable=_run_snowflake_sql_file,
        op_kwargs={"sql_filename": "analytics_transforms.sql"},
    )

    t_persist_pipeline_metrics = PythonOperator(
        task_id="persist_pipeline_metrics",
        python_callable=_persist_pipeline_metrics,
    )

    # ── Task dependency graph ──────────────────────────────────────────────
    start >> [t_extract_products, t_extract_users, t_extract_orders]

    t_extract_products  >> t_transform_products  >> t_validate_products >> t_load_products
    t_extract_users     >> t_transform_users     >> t_validate_users    >> t_load_users
    t_extract_orders    >> t_transform_orders    >> t_validate_orders   >> t_load_orders

    [t_load_orders, t_load_users, t_load_products] >> t_log_load_metrics >> t_snowflake_staging >> t_snowflake_analytics >> t_persist_pipeline_metrics >> end
