"""Getting ticker data from Polygon API and loading into Parquet files."""

from pathlib import Path

import polars as pl

from tickerlake.schemas import TICKERS_RAW_SCHEMA
from tickerlake.clients import setup_polygon_api_client
from tickerlake.config import settings
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def get_tickers() -> pl.DataFrame:
    """Retrieve list of tickers from Polygon API."""
    polygon_client = setup_polygon_api_client()
    logger.info("Retrieving list of tickers from Polygon API...")

    tickers = [
        t
        for t in polygon_client.list_tickers(
            market="stocks",
            active=True,
            order="asc",
            sort="ticker",
            limit=1000,
        )
    ]

    logger.info(f"Fetched {len(tickers)} active tickers")
    return pl.DataFrame(tickers, schema_overrides=TICKERS_RAW_SCHEMA)


def load_tickers() -> None:
    """Load tickers data into Parquet files."""
    tickers_df = get_tickers()

    # Ensure directory exists before writing
    tickers_path = Path(f"{settings.bronze_storage_path}/tickers")
    tickers_path.mkdir(parents=True, exist_ok=True)

    tickers_df.write_parquet(
        f"{settings.bronze_storage_path}/tickers/tickers.parquet",
    )
    logger.info("Tickers data written to Parquet files.")
