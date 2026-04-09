"""Database connection and API helpers for E-Commerce Data Pipeline."""

import os
import time
import logging
import requests
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecommerce_pipeline")

API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")
API_PAGE_LIMIT = 100


def get_db_connection_string() -> str:
    """Build PostgreSQL connection string from environment variables."""
    user = os.getenv("POSTGRES_USER", "pipeline_user")
    password = os.getenv("POSTGRES_PASSWORD", "pipeline_pass_2024")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "ecommerce")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_engine():
    """Create and return SQLAlchemy engine."""
    conn_str = get_db_connection_string()
    engine = create_engine(conn_str, pool_pre_ping=True, pool_size=5)
    logger.info("Database engine created successfully")
    return engine


def fetch_api_data(endpoint: str, max_retries: int = 3) -> list:
    """Fetch all paginated data from a DummyJSON endpoint with retry."""
    url = f"{API_BASE_URL}/{endpoint}"
    all_items = []
    skip = 0
    total = 0

    while True:
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    params={"limit": API_PAGE_LIMIT, "skip": skip},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                total = data.get("total", 0)
                items = data.get(endpoint, [])
                all_items.extend(items)
                logger.info(f"Fetched {len(items)} {endpoint} (skip={skip}, total={total})")
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"All {max_retries} attempts failed for {url}")
                    return all_items if all_items else []

        skip += API_PAGE_LIMIT
        if skip >= total:
            break

    logger.info(f"Total {endpoint} fetched: {len(all_items)}")
    return all_items
