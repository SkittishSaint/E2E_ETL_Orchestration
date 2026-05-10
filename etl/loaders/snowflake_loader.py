"""
etl/loaders/snowflake_loader.py
────────────────────────────────
Handles all writes to Snowflake:
  • Truncate-and-load (full replace)
  • Merge / upsert (incremental, SCD-1)
  • Watermark read & write for incremental pipelines
  • Supports schema-aligned loading and Snowflake clustering optimization
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from config.snowflake_config import SnowflakeConfig, SnowflakeTargets

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Manages all Snowflake write operations."""

    def __init__(self, config: Optional[SnowflakeConfig] = None):
        self.config = config or SnowflakeConfig.from_env()
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ── Connection management ─────────────────────────────────────────────
    def connect(self) -> "SnowflakeLoader":
        self._conn = snowflake.connector.connect(**self.config.as_dict())
        logger.info("Connected to Snowflake account: %s", self.config.account)
        return self

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("Snowflake connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.close()

    @property
    def conn(self) -> snowflake.connector.SnowflakeConnection:
        if not self._conn:
            raise RuntimeError("Call connect() or use as a context manager first.")
        return self._conn

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            logger.debug("Executed: %s", sql[:120])

    def execute_script(self, sql: str) -> None:
        """Execute a multi-statement SQL script."""
        for statement in self.conn.execute_string(sql):
            logger.debug("Executed statement: %s", statement.query[:120] if statement.query else "<unknown>")

    def execute_sql_file(self, sql_file: str) -> None:
        """Load and execute a SQL file from disk."""
        sql_path = Path(sql_file)
        sql = sql_path.read_text(encoding="utf-8")
        logger.info("Executing Snowflake SQL file: %s", sql_path.name)
        self.execute_script(sql)

    # ── Load strategies ───────────────────────────────────────────────────
    def load_truncate_replace(
        self,
        df: pd.DataFrame,
        table: str,        # e.g. "RAW.ORDERS_RAW"
        schema: str,
    ) -> int:
        """Full replace: TRUNCATE then bulk insert."""
        if df.empty:
            logger.warning("Empty DataFrame — skipping load to %s", table)
            return 0

        table_name = table.split(".")[-1]
        logger.info("Truncating %s before full load.", table)
        self.execute(f"TRUNCATE TABLE IF EXISTS {table}")

        df = df.reset_index(drop=True)
        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name=table_name,
            schema=schema,
            database=self.config.database,
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
            use_logical_type=True,
        )
        logger.info("Loaded %d rows into %s (%d chunks)", nrows, table, nchunks)
        return nrows

    def load_upsert(
        self,
        df: pd.DataFrame,
        target_table: str,   # fully qualified, e.g. "STAGING.STG_ORDERS"
        staging_table: str,  # temp staging table
        merge_keys: List[str],
        schema: str,
    ) -> int:
        """
        MERGE-based upsert (SCD Type 1).
        1. Write df to a transient staging table.
        2. MERGE staging → target on merge_keys.
        3. Drop staging table.
        """
        if df.empty:
            logger.warning("Empty DataFrame — skipping upsert to %s", target_table)
            return 0

        staging_name = staging_table.split(".")[-1]
        target_name_qualified = target_table

        # Step 1: load into staging
        logger.info("Loading %d rows into staging table %s", len(df), staging_table)
        self.execute(f"CREATE OR REPLACE TRANSIENT TABLE {staging_table} LIKE {target_table}")
        df = df.reset_index(drop=True)
        write_pandas(
            conn=self.conn,
            df=df,
            table_name=staging_name,
            schema=schema,
            database=self.config.database,
            auto_create_table=False,
            overwrite=True,
            quote_identifiers=False,
            use_logical_type=True,
        )

        # Step 2: build MERGE statement
        all_cols = [c for c in df.columns if c not in merge_keys]
        on_clause = " AND ".join(
            [f"tgt.{k} = src.{k}" for k in merge_keys]
        )
        update_clause = ", ".join([f"tgt.{c} = src.{c}" for c in all_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"src.{c}" for c in df.columns])

        merge_sql = f"""
        MERGE INTO {target_name_qualified} AS tgt
        USING {staging_table} AS src
            ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        logger.info("Executing MERGE into %s", target_table)
        self.execute(merge_sql)

        # Step 3: cleanup
        self.execute(f"DROP TABLE IF EXISTS {staging_table}")

        row_count = len(df)
        logger.info("Upsert complete — %d rows processed to %s", row_count, target_table)
        return row_count

    # ── Watermark management ──────────────────────────────────────────────
    def get_watermark(self, pipeline_name: str) -> Optional[datetime]:
        """Retrieve the last successful watermark for an incremental pipeline."""
        sql = f"""
        SELECT MAX(watermark_value)
        FROM {SnowflakeTargets.WATERMARK_TABLE}
        WHERE pipeline_name = %s
          AND status = 'SUCCESS'
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (pipeline_name,))
            row = cur.fetchone()
            if row and row[0]:
                logger.info("Watermark for '%s': %s", pipeline_name, row[0])
                return row[0]
        logger.info("No watermark found for '%s' — defaulting to full load.", pipeline_name)
        return None

    def set_watermark(
        self,
        pipeline_name: str,
        watermark_value: datetime,
        status: str = "SUCCESS",
        rows_loaded: int = 0,
    ) -> None:
        """Record a new watermark after a successful pipeline run."""
        sql = f"""
        INSERT INTO {SnowflakeTargets.WATERMARK_TABLE}
            (pipeline_name, watermark_value, status, rows_loaded, recorded_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        self.execute(sql, (pipeline_name, watermark_value, status, rows_loaded))
        logger.info("Watermark set for '%s': %s (%s)", pipeline_name, watermark_value, status)


# ── Task-level helpers for Airflow operators ─────────────────────────────
def load_raw_orders(df: pd.DataFrame) -> int:
    with SnowflakeLoader() as loader:
        return loader.load_upsert(
            df=df,
            target_table=SnowflakeTargets.RAW_ORDERS,
            staging_table="RAW.ORDERS_STAGING_TMP",
            merge_keys=["order_id"],
            schema="RAW",
        )


def load_raw_customers(df: pd.DataFrame) -> int:
    with SnowflakeLoader() as loader:
        return loader.load_upsert(
            df=df,
            target_table=SnowflakeTargets.RAW_CUSTOMERS,
            staging_table="RAW.CUSTOMERS_STAGING_TMP",
            merge_keys=["customer_id"],
            schema="RAW",
        )


def load_raw_users(df: pd.DataFrame) -> int:
    with SnowflakeLoader() as loader:
        return loader.load_upsert(
            df=df,
            target_table=SnowflakeTargets.RAW_USERS,
            staging_table="RAW.USERS_STAGING_TMP",
            merge_keys=["user_id"],
            schema="RAW",
        )


def load_raw_products(df: pd.DataFrame) -> int:
    with SnowflakeLoader() as loader:
        return loader.load_truncate_replace(
            df=df,
            table=SnowflakeTargets.RAW_PRODUCTS,
            schema="RAW",
        )


def upsert_raw_products(df: pd.DataFrame) -> int:
    with SnowflakeLoader() as loader:
        return loader.load_upsert(
            df=df,
            target_table=SnowflakeTargets.RAW_PRODUCTS,
            staging_table="RAW.PRODUCTS_STAGING_TMP",
            merge_keys=["product_id"],
            schema="RAW",
        )
