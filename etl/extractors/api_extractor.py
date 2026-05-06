"""
etl/extractors/api_extractor.py
────────────────────────────────
Extracts data from REST APIs with pagination, rate-limiting,
and retry support. Returns a pandas DataFrame.
"""
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables from .env file
load_dotenv()
logger = logging.getLogger(__name__)


class APIExtractor:
    """Generic paginated REST API extractor."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        # api_key: Optional[str] = None,
        rate_limit_per_minute: int = 60,
        timeout: int = 30,
    ):
        self.base_url = base_url or os.environ.get("API_BASE_URL", "https://dummyjson.com")
        # self.api_key = api_key or os.environ.get("API_KEY", "")
        self.rate_limit_delay = 60.0 / rate_limit_per_minute
        self.timeout = timeout
        self.session = self._build_session()

    # ── Session setup ─────────────────────────────────────────────────────
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {"Accept": "application/json"}
        )
        return session

    # ── Core extraction ───────────────────────────────────────────────────
    def extract(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Paginate through an endpoint and return all records as a DataFrame.

        Args:
            endpoint:        API path relative to base_url (e.g. '/orders')
            params:          Extra query parameters
            page_param:      Query param name for page number
            page_size_param: Query param name for page size
            page_size:       Records per page
            max_pages:       Safety cap on pages (None = unlimited)
        """
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        all_records: List[Dict] = []
        page = 1
        params = params or {}

        logger.info("Starting extraction from %s", url)

        while True:
            if page_param == "skip":
                offset = (page - 1) * page_size
                query = {**params, page_param: offset, page_size_param: page_size}
            else:
                query = {**params, page_param: page, page_size_param: page_size}
            response = self.session.get(url, params=query, timeout=self.timeout)
            response.raise_for_status()

            data = response.json()
            records = self._parse_response(data)

            if not records:
                logger.info("No more records at page %d — extraction complete", page)
                break

            all_records.extend(records)
            logger.info("Page %d: fetched %d records (total so far: %d)", page, len(records), len(all_records))

            # Stop if API returns fewer records than requested (indicates last page)
            if len(records) < page_size:
                logger.info("Received fewer records than page_size — last page reached")
                break

            if max_pages and page >= max_pages:
                logger.warning("Reached max_pages cap (%d)", max_pages)
                break

            page += 1
            time.sleep(self.rate_limit_delay)   # respect rate limit

        df = pd.DataFrame(all_records)
        df["_extracted_at"] = datetime.now(timezone.utc).isoformat()
        df["_source"] = url
        logger.info("Extraction complete — %d total records", len(df))
        return df

    def _parse_response(self, data: Any) -> List[Dict]:
        """Handle both list and paginated envelope responses."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "results", "items", "records", "users", "products", "carts", "posts"):
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    # ── Incremental helpers ───────────────────────────────────────────────
    def extract_since(
        self,
        endpoint: str,
        since: datetime,
        date_param: str = "updated_since",
        **kwargs,
    ) -> pd.DataFrame:
        """Extract records updated after a given watermark timestamp."""
        params = kwargs.pop("params", {})
        params[date_param] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info("Incremental extract from %s since %s", endpoint, since)
        return self.extract(endpoint, params=params, **kwargs)


# ── Convenience wrapper used directly in Airflow tasks ───────────────────
def extract_products(since: Optional[datetime] = None) -> pd.DataFrame:
    extractor = APIExtractor()
    if since:
        return extractor.extract_since(
            "/products",
            since=since,
            page_param="skip",
            page_size_param="limit",
            page_size=100,
        )
    return extractor.extract(
        "/products",
        page_param="skip",
        page_size_param="limit",
        page_size=100,
    )


def extract_users(since: Optional[datetime] = None) -> pd.DataFrame:
    extractor = APIExtractor()
    if since:
        return extractor.extract_since(
            "/users",
            since=since,
            page_param="skip",
            page_size_param="limit",
            page_size=100,
        )
    return extractor.extract(
        "/users",
        page_param="skip",
        page_size_param="limit",
        page_size=100,
    )


def extract_api_events(since: Optional[datetime] = None) -> pd.DataFrame:
    extractor = APIExtractor()
    if since:
        return extractor.extract_since("/events", since=since, date_param="from_time")
    return extractor.extract("/events")
