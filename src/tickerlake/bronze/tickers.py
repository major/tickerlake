"""Getting ticker data from Polygon API."""

import polars as pl

from tickerlake.clients import setup_polygon_api_client
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import TICKERS_RAW_SCHEMA

setup_logging()
logger = get_logger(__name__)


def get_tickers() -> pl.DataFrame:
    """Retrieve list of tickers from Polygon API.

    Returns:
        DataFrame containing ticker data from Polygon API.
    """
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
