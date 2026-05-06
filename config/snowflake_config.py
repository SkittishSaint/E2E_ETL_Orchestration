"""
config/snowflake_config.py
──────────────────────────
Central Snowflake connection configuration.
Reads from environment variables; falls back to Airflow connections.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str
    warehouse: str
    schema: str
    role: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "ETL_DB"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ETL_ROLE"),
        )

    def as_dict(self) -> dict:
        """Return as a dict suitable for snowflake.connector.connect(**cfg)."""
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "warehouse": self.warehouse,
            "schema": self.schema,
            "role": self.role,
        }

    def get_sqlalchemy_uri(self) -> str:
        return (
            f"snowflake://{self.user}:{self.password}@{self.account}/"
            f"{self.database}/{self.schema}?warehouse={self.warehouse}&role={self.role}"
        )


# ── Table & schema constants ──────────────────────────────────────────────
class SnowflakeTargets:
    # Raw / staging layer
    RAW_SCHEMA = "RAW"
    RAW_ORDERS = "RAW.ORDERS_RAW"
    RAW_CUSTOMERS = "RAW.CUSTOMERS_RAW"
    RAW_USERS = "RAW.USERS_RAW"
    RAW_PRODUCTS = "RAW.PRODUCTS_RAW"
    RAW_API_EVENTS = "RAW.API_EVENTS_RAW"

    # Staging / transformed layer
    STAGING_SCHEMA = "STAGING"
    STG_ORDERS = "STAGING.STG_ORDERS"
    STG_CUSTOMERS = "STAGING.STG_CUSTOMERS"
    STG_USERS = "STAGING.STG_USERS"
    STG_PRODUCTS = "STAGING.STG_PRODUCTS"

    # Analytics / reporting layer
    ANALYTICS_SCHEMA = "ANALYTICS"
    DIM_CUSTOMERS = "ANALYTICS.DIM_CUSTOMERS"
    DIM_PRODUCTS = "ANALYTICS.DIM_PRODUCTS"
    FACT_ORDERS = "ANALYTICS.FACT_ORDERS"
    AGG_DAILY_SALES = "ANALYTICS.AGG_DAILY_SALES"

    # Watermark table for incremental loads
    WATERMARK_TABLE = "CONTROL.ETL_WATERMARKS"
