"""
etl/transformers/transformer.py
────────────────────────────────
Pure-Python / pandas transformation layer.
Each transform function takes a raw DataFrame and returns
a cleaned, enriched, schema-aligned DataFrame ready for Snowflake.
"""
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Generic utilities ─────────────────────────────────────────────────────

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_pk(*values) -> str:
    """Create a stable surrogate key from natural key components."""
    raw = "|".join(str(v) for v in values)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _to_json_string(value) -> Optional[str]:
    """Serialize nested source values so Snowflake receives VARCHAR-safe data."""
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def cast_timestamps(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def strip_and_upper(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    return df


def drop_duplicates_on(df: pd.DataFrame, subset: List[str]) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep="last")
    logger.info("Deduplication on %s: %d → %d rows", subset, before, len(df))
    return df


# ── Domain transforms ─────────────────────────────────────────────────────

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich raw orders data from database.

    Business rules:
    - Rename id → order_id, created_at → order_date
    - Drop orders with null order_id or user_id.
    - Parse monetary columns as float; replace negatives with 0.
    - Add derived columns: order_year, order_month.
    - Attach a surrogate key (sk_order).
    """
    logger.info("Transforming orders — input rows: %d", len(df))

    # Rename columns from database schema to target schema
    df = df.rename(columns={
        'id': 'order_id',
        'created_at': 'order_date',
    })

    # Handle products - store as JSON string
    if 'products' in df.columns:
        df['products_json'] = df['products'].apply(_to_json_string)
        df = df.drop(columns=['products'])

    # Required field guard
    df = df.dropna(subset=["order_id", "user_id"])

    # Timestamps
    df = cast_timestamps(df, ["order_date", "updated_at"])

    # Monetary cleaning
    for col in ["total", "discounted_total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).clip(lower=0)

    # Derived date parts
    df["order_year"] = df["order_date"].dt.year
    df["order_month"] = df["order_date"].dt.month
    df["order_day_of_week"] = df["order_date"].dt.dayofweek

    # Deduplication
    df = drop_duplicates_on(df, subset=["order_id"])

    # Surrogate key
    df["sk_order"] = df["order_id"].apply(lambda x: _hash_pk("order", x))

    # Audit columns
    df["_transformed_at"] = _now_utc()
    df["_pipeline_version"] = "1.0"

    # Select only columns that match the ORDERS_RAW schema
    schema_columns = [
        'order_id', 'user_id', 'products_json', 'total', 'discounted_total',
        'total_products', 'total_quantity', 'order_date', 'updated_at',
        'sk_order', '_transformed_at', '_pipeline_version'
    ]
    df = df[[col for col in schema_columns if col in df.columns]]

    logger.info("Transform complete — output rows: %d", len(df))
    return df.reset_index(drop=True)


def transform_users(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich raw user data from DummyJSON or similar API schema.

    Business rules:
    - Normalize user identifiers and email.
    - Flatten nested name and address objects.
    - Mask PII in password and phone.
    - Build stable surrogate key sk_user.
    """
    logger.info("Transforming users — input rows: %d", len(df))

    if "id" in df.columns and "user_id" not in df.columns:
        df["user_id"] = df["id"]

    df = df.dropna(subset=["user_id", "email"])

    if "email" in df.columns:
        df["email"] = df["email"].astype(str).str.strip().str.lower()

    if "username" in df.columns:
        df["username"] = df["username"].astype(str).str.strip()

    if "password" in df.columns:
        df["password_masked"] = df["password"].astype(str).str.replace(
            r".", "*", regex=True
        )
        df.drop(columns=["password"], inplace=True)

    if "firstName" in df.columns or "lastName" in df.columns:
        df["first_name"] = df["firstName"].astype(str).str.strip().replace({"nan": None})
        df["last_name"] = df["lastName"].astype(str).str.strip().replace({"nan": None})
        df["full_name"] = (
            df["first_name"].fillna("") + " " + df["last_name"].fillna("")
        ).str.strip()
        # drop original camelCase name fields
        df.drop(columns=[c for c in ("firstName", "lastName") if c in df.columns], inplace=True)

    if "name" in df.columns and ("first_name" not in df.columns or "last_name" not in df.columns):
        df["first_name"] = df["name"].apply(
            lambda x: x.get("firstname") if isinstance(x, dict) else None
        )
        df["last_name"] = df["name"].apply(
            lambda x: x.get("lastname") if isinstance(x, dict) else None
        )
        df["full_name"] = (
            df["first_name"].fillna("") + " " + df["last_name"].fillna("")
        ).str.strip()
        df.drop(columns=["name"], inplace=True)

    if "address" in df.columns:
        def _addr_field(value, field):
            return value.get(field) if isinstance(value, dict) else None

        df["address_line1"] = df["address"].apply(lambda x: _addr_field(x, "address"))
        df["address_city"] = df["address"].apply(lambda x: _addr_field(x, "city"))
        df["address_state"] = df["address"].apply(lambda x: _addr_field(x, "state"))
        df["address_postal_code"] = df["address"].apply(lambda x: _addr_field(x, "postalCode"))

        if df["address"].apply(lambda x: isinstance(x, dict) and "coordinates" in x).any():
            df["address_lat"] = df["address"].apply(
                lambda x: x.get("coordinates", {}).get("lat") if isinstance(x, dict) else None
            )
            df["address_lng"] = df["address"].apply(
                lambda x: x.get("coordinates", {}).get("lng") if isinstance(x, dict) else None
            )
        df.drop(columns=["address"], inplace=True)

    if "phone" in df.columns:
        df["phone_masked"] = df["phone"].astype(str).str.replace(
            r"\d(?=\d{4})", "*", regex=True
        )
        # drop original phone column
        df.drop(columns=["phone"], inplace=True)

    df = drop_duplicates_on(df, subset=["user_id"])
    df["sk_user"] = df["user_id"].apply(lambda x: _hash_pk("user", x))
    df["_transformed_at"] = _now_utc()
    df["_pipeline_version"] = "1.0"

    # Select only columns that match the USERS_RAW schema to avoid passing
    # camelCase or unexpected columns into Snowflake staging table.
    schema_columns = [
        'user_id', 'email', 'username', 'password_masked', 'first_name', 'last_name',
        'full_name', 'address_line1', 'address_city', 'address_state', 'address_postal_code',
        'address_lat', 'address_lng', 'phone_masked', 'sk_user', '_transformed_at', '_pipeline_version'
    ]
    df = df[[col for col in schema_columns if col in df.columns]]

    logger.info("Transform complete — output rows: %d", len(df))
    return df.reset_index(drop=True)


# Backwards compatibility alias for older customer-focused naming
transform_customers = transform_users


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich raw product catalogue data from DummyJSON or similar schema.
    """
    logger.info("Transforming products — input rows: %d", len(df))

    if "id" in df.columns and "product_id" not in df.columns:
        df["product_id"] = df["id"]
    if "title" in df.columns and "product_name" not in df.columns:
        df["product_name"] = df["title"]

    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    if "reviews" in df.columns:
        def _normalize_reviews(value):
            if isinstance(value, (list, tuple)):
                return len(value)
            if isinstance(value, dict):
                return 1
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, (list, tuple)):
                        return len(parsed)
                    if isinstance(parsed, dict):
                        return 1
                except (ValueError, TypeError):
                    pass
            return pd.to_numeric(value, errors="coerce")

        df["reviews"] = df["reviews"].apply(_normalize_reviews).fillna(0).astype(int)

    if "discountPercentage" in df.columns:
        df["discount_percentage"] = pd.to_numeric(df["discountPercentage"], errors="coerce")
    if "discountedPrice" in df.columns and "discounted_price" not in df.columns:
        df["discounted_price"] = pd.to_numeric(df["discountedPrice"], errors="coerce")
    if "returnPolicy" in df.columns and "return_policy" not in df.columns:
        df["return_policy"] = df["returnPolicy"].astype(str).str.strip()
    if "warrantyInformation" in df.columns and "warranty_months" not in df.columns:
        df["warranty_months"] = (
            pd.to_numeric(
                df["warrantyInformation"].astype(str).str.extract(r"(\d+)", expand=False),
                errors="coerce",
            )
            .astype("Int64")
        )
    if "dimensions" in df.columns:
        df["dimensions"] = df["dimensions"].apply(_to_json_string)

    df = df.dropna(subset=["product_id", "product_name"])

    # Price normalisation
    for col in ["price", "cost", "list_price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").clip(lower=0)

    if "price" in df.columns and "cost" in df.columns:
        df["margin_pct"] = (
            ((df["price"] - df["cost"]) / df["price"].replace(0, np.nan)) * 100
        ).round(2)

    df = strip_and_upper(df, ["category", "brand", "status"])
    df = cast_timestamps(df, ["created_at", "updated_at"])

    if "status" not in df.columns:
        df["status"] = "ACTIVE"
    df["is_active"] = df["status"] == "ACTIVE"

    df = drop_duplicates_on(df, subset=["product_id"])

    df["sk_product"] = df["product_id"].apply(lambda x: _hash_pk("product", x))
    df["_transformed_at"] = _now_utc()
    df["_pipeline_version"] = "1.0"

    # Select only columns that match the PRODUCTS_RAW schema to avoid passing
    # API-specific or unexpected columns into Snowflake.
    schema_columns = [
        'id', 'product_id', 'product_name', 'description', 'rating', 'reviews', 'price',
        'discount_percentage', 'discounted_price', 'stock', 'category', 'thumbnail', 'sku',
        'weight', 'dimensions', 'warranty_months', 'return_policy', 'sk_product',
        '_transformed_at', '_pipeline_version', '_extracted_at', '_source'
    ]
    df = df[[col for col in schema_columns if col in df.columns]]

    logger.info("Transform complete — output rows: %d", len(df))
    return df.reset_index(drop=True)


def build_daily_sales_agg(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate transformed orders into a daily sales summary.
    Used to populate ANALYTICS.AGG_DAILY_SALES.
    """
    logger.info("Building daily sales aggregation.")

    if "order_date" not in orders_df.columns:
        raise ValueError("orders_df must contain order_date for aggregation")

    orders_df = cast_timestamps(orders_df, ["order_date"])
    if "status" not in orders_df.columns:
        orders_df["status"] = "COMPLETED"

    agg = (
        orders_df.groupby(pd.Grouper(key="order_date", freq="D"))
        .agg(
            order_count=("order_id", "count"),
            total_revenue_usd=("total", "sum"),
            avg_order_value_usd=("total", "mean"),
            unique_customers=("user_id", "nunique"),
            cancelled_orders=("status", lambda s: (s == "CANCELLED").sum()),
        )
        .reset_index()
        .rename(columns={"order_date": "sale_date"})
    )
    agg["cancellation_rate_pct"] = (
        (agg["cancelled_orders"] / agg["order_count"]) * 100
    ).round(2)
    agg["_transformed_at"] = _now_utc()
    logger.info("Daily agg complete — %d rows", len(agg))
    return agg
