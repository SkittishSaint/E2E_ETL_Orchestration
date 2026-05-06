"""
dags/incremental_load_dag.py
─────────────────────────────
Hourly incremental pipeline — loads only records changed
since the last successful run using a watermark table in Snowflake.

Flow:
  start
    ├── get_watermarks          (reads last success timestamps)
    ├── incremental_orders      (API, since watermark)
    └── incremental_customers   (API, since watermark)
          ↓
    ├── transform_orders
    └── transform_customers
          ↓
    ├── upsert_orders
    └── upsert_customers
          ↓
    update_watermarks
          ↓
    end
"""
import logging
import sys
from datetime import datetime, timezone

sys.path.append('/opt/airflow')

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

from config.dag_config import DEFAULT_ARGS, HOURLY_SCHEDULE, PIPELINE_START_DATE, CATCHUP, TAGS
from etl.extractors.api_extractor import extract_products, extract_users
from etl.extractors.db_extractor import extract_orders
from etl.transformers.transformer import transform_products, transform_users, transform_orders
from etl.loaders.snowflake_loader import SnowflakeLoader, load_raw_products, load_raw_users, load_raw_orders

logger = logging.getLogger(__name__)

PIPELINE_PRODUCTS = "incremental_products"
PIPELINE_USERS    = "incremental_users"
PIPELINE_ORDERS   = "incremental_orders"


# ── Watermark helpers ─────────────────────────────────────────────────────

def _get_watermarks(**ctx):
    with SnowflakeLoader() as loader:
        products_wm = loader.get_watermark(PIPELINE_PRODUCTS)
        users_wm    = loader.get_watermark(PIPELINE_USERS)
        orders_wm   = loader.get_watermark(PIPELINE_ORDERS)

    ctx["ti"].xcom_push(key="products_watermark",
                        value=products_wm.isoformat() if products_wm else None)
    ctx["ti"].xcom_push(key="users_watermark",
                        value=users_wm.isoformat() if users_wm else None)
    ctx["ti"].xcom_push(key="orders_watermark",
                        value=orders_wm.isoformat() if orders_wm else None)
    logger.info("Watermarks — products: %s | users: %s | orders: %s", products_wm, users_wm, orders_wm)


def _update_watermarks(**ctx):
    now = datetime.now(timezone.utc)
    products_rows = int(ctx["ti"].xcom_pull(key="products_rows_loaded") or 0)
    users_rows    = int(ctx["ti"].xcom_pull(key="users_rows_loaded") or 0)
    orders_rows   = int(ctx["ti"].xcom_pull(key="orders_rows_loaded") or 0)

    with SnowflakeLoader() as loader:
        loader.set_watermark(PIPELINE_PRODUCTS, now, rows_loaded=products_rows)
        loader.set_watermark(PIPELINE_USERS,    now, rows_loaded=users_rows)
        loader.set_watermark(PIPELINE_ORDERS,   now, rows_loaded=orders_rows)

    logger.info("Watermarks updated to %s (products: %d rows, users: %d rows, orders: %d rows)",
                now, products_rows, users_rows, orders_rows)


# ── Extract ───────────────────────────────────────────────────────────────

def _incremental_extract_products(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="products_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_products(since=since)
    ctx["ti"].xcom_push(key="products_raw", value=df.to_json())
    logger.info("Incremental extract: %d products (since %s)", len(df), since)
    return len(df) > 0   # ShortCircuit: skip rest if nothing new


def _incremental_extract_users(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="users_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_users(since=since)
    ctx["ti"].xcom_push(key="users_raw", value=df.to_json())
    logger.info("Incremental extract: %d users (since %s)", len(df), since)
    return len(df) > 0


def _incremental_extract_orders(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="orders_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_orders(watermark=since)
    ctx["ti"].xcom_push(key="orders_raw", value=df.to_json())
    logger.info("Incremental extract: %d orders (since %s)", len(df), since)
    return len(df) > 0


# ── Transform ─────────────────────────────────────────────────────────────

def _transform_products(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key="products_raw"))
    ctx["ti"].xcom_push(key="products_transformed", value=transform_products(raw).to_json())


def _transform_users(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key="users_raw"))
    ctx["ti"].xcom_push(key="users_transformed", value=transform_users(raw).to_json())


def _transform_orders(**ctx):
    raw = pd.read_json(ctx["ti"].xcom_pull(key="orders_raw"))
    ctx["ti"].xcom_push(key="orders_transformed", value=transform_orders(raw).to_json())


# ── Load ──────────────────────────────────────────────────────────────────

def _upsert_products(**ctx):
    df   = pd.read_json(ctx["ti"].xcom_pull(key="products_transformed"))
    rows = load_raw_products(df)
    ctx["ti"].xcom_push(key="products_rows_loaded", value=rows)
    logger.info("Upserted %d products", rows)


def _upsert_users(**ctx):
    df   = pd.read_json(ctx["ti"].xcom_pull(key="users_transformed"))
    rows = load_raw_users(df)
    ctx["ti"].xcom_push(key="users_rows_loaded", value=rows)
    logger.info("Upserted %d users", rows)


def _upsert_orders(**ctx):
    df   = pd.read_json(ctx["ti"].xcom_pull(key="orders_transformed"))
    rows = load_raw_orders(df)
    ctx["ti"].xcom_push(key="orders_rows_loaded", value=rows)
    logger.info("Upserted %d orders", rows)


# ── DAG definition ────────────────────────────────────────────────────────

with DAG(
    dag_id="etl_incremental_pipeline",
    default_args=DEFAULT_ARGS,
    description="Hourly incremental load using watermark-based CDC",
    schedule_interval=HOURLY_SCHEDULE,
    start_date=PIPELINE_START_DATE,
    catchup=CATCHUP,
    max_active_runs=1,
    tags=TAGS + ["incremental"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end",
                          trigger_rule="none_failed_min_one_success")

    t_get_watermarks = PythonOperator(
        task_id="get_watermarks",
        python_callable=_get_watermarks,
    )

    # ShortCircuitOperator skips downstream tasks if no new records
    t_extract_products = ShortCircuitOperator(
        task_id="incremental_extract_products",
        python_callable=_incremental_extract_products,
    )
    t_extract_users = ShortCircuitOperator(
        task_id="incremental_extract_users",
        python_callable=_incremental_extract_users,
    )
    t_extract_orders = ShortCircuitOperator(
        task_id="incremental_extract_orders",
        python_callable=_incremental_extract_orders,
    )

    t_transform_products = PythonOperator(task_id="transform_products", python_callable=_transform_products)
    t_transform_users    = PythonOperator(task_id="transform_users",    python_callable=_transform_users)
    t_transform_orders   = PythonOperator(task_id="transform_orders",   python_callable=_transform_orders)

    t_upsert_products = PythonOperator(task_id="upsert_products", python_callable=_upsert_products)
    t_upsert_users    = PythonOperator(task_id="upsert_users",    python_callable=_upsert_users)
    t_upsert_orders   = PythonOperator(task_id="upsert_orders",   python_callable=_upsert_orders)

    t_update_watermarks = PythonOperator(
        task_id="update_watermarks",
        python_callable=_update_watermarks,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Dependency graph ───────────────────────────────────────────────────
    start >> t_get_watermarks >> [t_extract_products, t_extract_users, t_extract_orders]

    t_extract_products >> t_transform_products >> t_upsert_products
    t_extract_users    >> t_transform_users    >> t_upsert_users
    t_extract_orders   >> t_transform_orders   >> t_upsert_orders

    [t_upsert_products, t_upsert_users, t_upsert_orders] >> t_update_watermarks >> end
