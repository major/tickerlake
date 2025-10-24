"""Getting data about stock splits from Polygon API."""

from datetime import datetime

import polars as pl

from tickerlake.clients import setup_polygon_api_client
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import SPLITS_RAW_SCHEMA

setup_logging()
logger = get_logger(__name__)


def get_splits() -> pl.DataFrame:
    """Retrieve stock splits data from Polygon API.

    Returns:
        DataFrame containing stock split data from Polygon API.
    """
    polygon_client = setup_polygon_api_client()
    logger.info("Retrieving stock splits data from Polygon API...")

    splits = [
        {
            "ticker": s.ticker,  # type: ignore
            "execution_date": datetime.strptime(s.execution_date, "%Y-%m-%d").date(),  # type: ignore
            "split_from": s.split_from,  # type: ignore
            "split_to": s.split_to,  # type: ignore
        }
        for s in polygon_client.list_splits(
            execution_date_gte="2020-01-01",
            order="asc",
            sort="execution_date",
            limit=1000,
        )
    ]

    logger.info(f"Retrieved {len(splits)} stock splits.")
    return pl.DataFrame(splits, schema_overrides=SPLITS_RAW_SCHEMA)
