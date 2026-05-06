"""
etl/extractors/db_extractor.py
──────────────────────────────
Extracts data from databases (PostgreSQL, Oracle, etc.) with
incremental loading support using timestamps.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql

logger = logging.getLogger(__name__)


class DatabaseExtractor:
    """Generic database extractor with incremental loading support."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        db_type: str = "postgresql",  # or "oracle"
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.db_type = db_type.lower()
        self._conn = None

    def connect(self):
        """Establish database connection."""
        try:
            if self.db_type == "postgresql":
                self._conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                )
            elif self.db_type == "oracle":
                # Note: Would need cx_Oracle for Oracle
                raise NotImplementedError("Oracle support not yet implemented")
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            logger.info(f"Connected to {self.db_type} database: {self.database}")
            return self
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            logger.info("Database connection closed.")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def conn(self):
        if not self._conn:
            raise RuntimeError("Call connect() or use as a context manager first.")
        return self._conn

    def extract_orders(
        self,
        watermark: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract orders data with optional incremental filtering.

        Args:
            watermark: Only extract records newer than this timestamp
            limit: Maximum number of records to extract
        """
        query = """
        SELECT
            id,
            user_id,
            products,
            total,
            discounted_total,
            total_products,
            total_quantity,
            created_at,
            updated_at
        FROM orders
        """

        params = []
        if watermark:
            query += " WHERE updated_at > %s"
            params.append(watermark)

        query += " ORDER BY updated_at ASC"

        if limit:
            query += f" LIMIT {limit}"

        logger.info(f"Executing query: {query}")
        if params:
            logger.info(f"With parameters: {params}")

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            df = pd.DataFrame(rows)
            logger.info(f"Extracted {len(df)} orders from database")

            # Convert timestamp columns to datetime
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])

            return df

        except Exception as e:
            logger.error(f"Failed to extract orders: {e}")
            raise


# ── Convenience functions ────────────────────────────────────────────────
def extract_orders(
    watermark: Optional[datetime] = None,
    limit: Optional[int] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> pd.DataFrame:
    """
    Convenience function to extract orders from database.

    Uses environment variables if connection details not provided.
    When running from host machine (outside Docker), use localhost:5433
    When running inside Docker Airflow, use ecommerce-db:5432
    """
    import os

    # Detect if running inside Docker or outside
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("AIRFLOW_HOME") is not None

    default_host = "ecommerce-db" if is_docker else "localhost"
    default_port = 5432 if is_docker else 5433

    db_config = {
        "host": host or os.environ.get("DB_HOST", default_host),
        "port": port or int(os.environ.get("DB_PORT", default_port)),
        "database": database or os.environ.get("DB_NAME", "ecommerce"),
        "user": user or os.environ.get("DB_USER", "postgres"),
        "password": password or os.environ.get("DB_PASSWORD", "postgres"),
    }

    with DatabaseExtractor(**db_config) as extractor:
        return extractor.extract_orders(watermark=watermark, limit=limit)