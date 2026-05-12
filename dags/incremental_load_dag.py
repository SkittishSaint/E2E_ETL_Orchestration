"""
dags/incremental_load_dag.py
─────────────────────────────
Hourly incremental pipeline — loads only records changed
since the last successful run using a watermark table in Snowflake.

Flow:
  start
    ├── get_watermarks          (reads last success timestamps)
    ├── incremental_products    (API, since watermark)
    ├── incremental_users       (API, since watermark)
    └── incremental_orders      (DB, since watermark)
          ↓
    ├── transform_products
    ├── transform_users
    └── transform_orders
          ↓
    ├── upsert_products
    ├── upsert_users
    └── upsert_orders
          ↓
    validate_incremental_loads
          ↓
    update_watermarks
          ↓
    end
"""
import json
import logging
import sys
from datetime import datetime, timezone

import numpy as np

sys.path.append('/opt/airflow')

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from config.dag_config import DEFAULT_ARGS, HOURLY_SCHEDULE, PIPELINE_START_DATE, CATCHUP, TAGS
from etl.extractors.api_extractor import extract_products, extract_users
from etl.extractors.db_extractor import extract_orders
from etl.transformers.transformer import transform_products, transform_users, transform_orders
from etl.loaders.snowflake_loader import SnowflakeLoader, upsert_raw_products, load_raw_users, load_raw_orders
from monitoring.pipeline_monitor import duration_seconds_from_context, record_pipeline_metric

logger = logging.getLogger(__name__)

PIPELINE_PRODUCTS = "incremental_products"
PIPELINE_USERS    = "incremental_users"
PIPELINE_ORDERS   = "incremental_orders"
XCOM_PRODUCTS_WATERMARK = "products_next_watermark"
XCOM_USERS_WATERMARK = "users_next_watermark"
XCOM_ORDERS_WATERMARK = "orders_next_watermark"


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


def _max_timestamp_value(df: pd.DataFrame, candidates) -> str:
    for col in candidates:
        if col in df.columns:
            series = pd.to_datetime(df[col], utc=True, errors="coerce").dropna()
            if not series.empty:
                return series.max().isoformat()
    return None


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
    products_rows = int(ctx["ti"].xcom_pull(key="products_rows_loaded") or 0)
    users_rows    = int(ctx["ti"].xcom_pull(key="users_rows_loaded") or 0)
    orders_rows   = int(ctx["ti"].xcom_pull(key="orders_rows_loaded") or 0)
    products_wm = ctx["ti"].xcom_pull(key=XCOM_PRODUCTS_WATERMARK)
    users_wm = ctx["ti"].xcom_pull(key=XCOM_USERS_WATERMARK)
    orders_wm = ctx["ti"].xcom_pull(key=XCOM_ORDERS_WATERMARK)

    updates = []

    with SnowflakeLoader() as loader:
        if products_rows > 0 and products_wm:
            loader.set_watermark(
                PIPELINE_PRODUCTS,
                datetime.fromisoformat(products_wm),
                rows_loaded=products_rows,
            )
            updates.append(f"products={products_wm}")
        if users_rows > 0 and users_wm:
            loader.set_watermark(
                PIPELINE_USERS,
                datetime.fromisoformat(users_wm),
                rows_loaded=users_rows,
            )
            updates.append(f"users={users_wm}")
        if orders_rows > 0 and orders_wm:
            loader.set_watermark(
                PIPELINE_ORDERS,
                datetime.fromisoformat(orders_wm),
                rows_loaded=orders_rows,
            )
            updates.append(f"orders={orders_wm}")

    if updates:
        logger.info("Updated incremental watermarks: %s", ", ".join(updates))
    else:
        logger.info("No branch produced new rows; watermarks unchanged.")


# ── Extract ───────────────────────────────────────────────────────────────

def _incremental_extract_products(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="products_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_products(since=since)
    ctx["ti"].xcom_push(key="products_raw", value=_normalize_xcom_df(df))
    ctx["ti"].xcom_push(key=XCOM_PRODUCTS_WATERMARK, value=_max_timestamp_value(df, ["updated_at", "_extracted_at"]))
    logger.info("Incremental extract: %d products (since %s)", len(df), since)
    if since is not None:
        logger.warning(
            "Products incremental mode relies on source-side filtering support; "
            "if the API ignores 'updated_since', this branch behaves like a full pull."
        )


def _incremental_extract_users(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="users_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_users(since=since)
    ctx["ti"].xcom_push(key="users_raw", value=_normalize_xcom_df(df))
    ctx["ti"].xcom_push(key=XCOM_USERS_WATERMARK, value=_max_timestamp_value(df, ["updated_at", "_extracted_at"]))
    logger.info("Incremental extract: %d users (since %s)", len(df), since)
    if since is not None:
        logger.warning(
            "Users incremental mode relies on source-side filtering support; "
            "if the API ignores 'updated_since', this branch behaves like a full pull."
        )


def _incremental_extract_orders(**ctx):
    wm_str = ctx["ti"].xcom_pull(key="orders_watermark")
    since  = datetime.fromisoformat(wm_str) if wm_str else None
    df     = extract_orders(watermark=since)
    ctx["ti"].xcom_push(key="orders_raw", value=_normalize_xcom_df(df))
    ctx["ti"].xcom_push(key=XCOM_ORDERS_WATERMARK, value=_max_timestamp_value(df, ["updated_at", "created_at", "_extracted_at"]))
    logger.info("Incremental extract: %d orders (since %s)", len(df), since)


# ── Transform ─────────────────────────────────────────────────────────────

def _transform_products(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="products_raw"))
    if raw.empty:
        logger.info("No product rows extracted; skipping product transform.")
        ctx["ti"].xcom_push(key="products_transformed", value=[])
        return
    ctx["ti"].xcom_push(key="products_transformed", value=_normalize_xcom_df(transform_products(raw)))


def _transform_users(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="users_raw"))
    if raw.empty:
        logger.info("No user rows extracted; skipping user transform.")
        ctx["ti"].xcom_push(key="users_transformed", value=[])
        return
    ctx["ti"].xcom_push(key="users_transformed", value=_normalize_xcom_df(transform_users(raw)))


def _transform_orders(**ctx):
    raw = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="orders_raw"))
    if raw.empty:
        logger.info("No order rows extracted; skipping order transform.")
        ctx["ti"].xcom_push(key="orders_transformed", value=[])
        return
    ctx["ti"].xcom_push(key="orders_transformed", value=_normalize_xcom_df(transform_orders(raw)))


# ── Load ──────────────────────────────────────────────────────────────────

def _upsert_products(**ctx):
    df   = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="products_transformed"))
    rows = upsert_raw_products(df)
    ctx["ti"].xcom_push(key="products_rows_loaded", value=rows)
    logger.info("Upserted %d products", rows)


def _upsert_users(**ctx):
    df   = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="users_transformed"))
    rows = load_raw_users(df)
    ctx["ti"].xcom_push(key="users_rows_loaded", value=rows)
    logger.info("Upserted %d users", rows)


def _upsert_orders(**ctx):
    df   = _deserialize_xcom_df(ctx["ti"].xcom_pull(key="orders_transformed"))
    rows = load_raw_orders(df)
    ctx["ti"].xcom_push(key="orders_rows_loaded", value=rows)
    logger.info("Upserted %d orders", rows)


def _validate_incremental_loads(**ctx):
    products_rows = int(ctx["ti"].xcom_pull(key="products_rows_loaded") or 0)
    users_rows = int(ctx["ti"].xcom_pull(key="users_rows_loaded") or 0)
    orders_rows = int(ctx["ti"].xcom_pull(key="orders_rows_loaded") or 0)

    logger.info(
        "Incremental load metrics: products=%d, users=%d, orders=%d",
        products_rows,
        users_rows,
        orders_rows,
    )

    if products_rows < 0 or users_rows < 0 or orders_rows < 0:
        raise ValueError("Incremental load returned invalid row counts")

    if products_rows == 0 and users_rows == 0 and orders_rows == 0:
        logger.warning("No incremental rows loaded in this run; watermarks will remain unchanged.")

    ctx["ti"].xcom_push(key="incremental_validation_passed", value=True)


def _persist_incremental_metrics(**ctx):
    products_rows = int(ctx["ti"].xcom_pull(key="products_rows_loaded", task_ids="upsert_products") or 0)
    users_rows = int(ctx["ti"].xcom_pull(key="users_rows_loaded", task_ids="upsert_users") or 0)
    orders_rows = int(ctx["ti"].xcom_pull(key="orders_rows_loaded", task_ids="upsert_orders") or 0)

    record_pipeline_metric(
        dag_id=ctx["dag"].dag_id,
        task_id=ctx["ti"].task_id,
        run_id=ctx["run_id"],
        run_type="hourly_incremental",
        status="SUCCESS",
        rows_loaded=products_rows + users_rows + orders_rows,
        duration_seconds=duration_seconds_from_context(ctx),
    )


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

    t_extract_products = PythonOperator(
        task_id="incremental_extract_products",
        python_callable=_incremental_extract_products,
    )
    t_extract_users = PythonOperator(
        task_id="incremental_extract_users",
        python_callable=_incremental_extract_users,
    )
    t_extract_orders = PythonOperator(
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

    t_validate_incremental_loads = PythonOperator(
        task_id="validate_incremental_loads",
        python_callable=_validate_incremental_loads,
    )

    t_persist_incremental_metrics = PythonOperator(
        task_id="persist_incremental_metrics",
        python_callable=_persist_incremental_metrics,
    )

    # ── Dependency graph ───────────────────────────────────────────────────
    start >> t_get_watermarks >> [t_extract_products, t_extract_users, t_extract_orders]

    t_extract_products >> t_transform_products >> t_upsert_products
    t_extract_users    >> t_transform_users    >> t_upsert_users
    t_extract_orders   >> t_transform_orders   >> t_upsert_orders

    [t_upsert_products, t_upsert_users, t_upsert_orders] >> t_validate_incremental_loads >> t_update_watermarks >> t_persist_incremental_metrics >> end
