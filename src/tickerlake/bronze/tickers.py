"""Getting ticker data from Polygon API and loading into Parquet files."""

import polars as pl

from tickerlake.bronze.clients import setup_polygon_api_client
from tickerlake.bronze.schemas import TICKERS_SCHEMA
from tickerlake.config import s3_storage_options, settings
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
    return pl.DataFrame(tickers, schema_overrides=TICKERS_SCHEMA)


def load_tickers() -> None:
    """Load tickers data into Parquet files."""
    tickers_df = get_tickers()
    tickers_df.write_parquet(
        f"{settings.bronze_unified_storage_path}/tickers/tickers.parquet",
        storage_options=s3_storage_options,
    )
    logger.info("Tickers data written to Parquet files.")
