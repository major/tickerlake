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

    # Helper to get field from either dict or object
    def get_field(item: dict | object, field: str, default=None):  # type: ignore[misc]
        """Get field from dict or object attribute."""
        if isinstance(item, dict):
            return item.get(field, default)
        return getattr(item, field, default)

    # Manually extract fields from API objects (Polars can't auto-extract from these objects)
    tickers = [
        {
            "ticker": get_field(t, "ticker"),
            "name": get_field(t, "name"),
            "type": get_field(t, "type"),
            "active": get_field(t, "active", True),
            "locale": get_field(t, "locale"),
            "market": get_field(t, "market", "stocks"),
            "primary_exchange": get_field(t, "primary_exchange"),
            "currency_name": get_field(t, "currency_name"),
            "currency_symbol": get_field(t, "currency_symbol"),
            "cik": get_field(t, "cik"),
            "composite_figi": get_field(t, "composite_figi"),
            "share_class_figi": get_field(t, "share_class_figi"),
            "base_currency_name": get_field(t, "base_currency_name"),
            "base_currency_symbol": get_field(t, "base_currency_symbol"),
            "delisted_utc": get_field(t, "delisted_utc"),
            "last_updated_utc": get_field(t, "last_updated_utc"),
        }
        for t in polygon_client.list_tickers(
            market="stocks",
            order="asc",
            sort="ticker",
            limit=1000,
        )
    ]

    logger.info(f"Fetched {len(tickers)} active tickers")
    return pl.DataFrame(tickers, schema_overrides=TICKERS_RAW_SCHEMA)
