"""Postgres storage backend for bronze layer. 🥉

Uses shared database utilities from tickerlake.db for common operations.
Bronze-specific functions remain here (upserts, queries, etc.).
"""

from datetime import date

import polars as pl
from sqlalchemy import delete, func, select

from tickerlake.bronze.models import metadata, splits, stocks, tickers
from tickerlake.db import get_engine, init_schema  # noqa: F401
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


# Re-export for backward compatibility (convenience)
def init_bronze_schema() -> None:
    """Initialize bronze layer database schema (idempotent). ✨"""
    init_schema(metadata, "bronze")


def get_existing_dates() -> list[str]:
    """Get sorted list of dates already stored in Postgres.

    Returns:
        List of date strings in YYYY-MM-DD format.
    """
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            select(stocks.c.date).distinct().order_by(stocks.c.date)
        ).fetchall()

    return [row[0].strftime("%Y-%m-%d") for row in result]


def upsert_tickers(df: pl.DataFrame) -> None:
    """Replace all tickers (full refresh each run).

    Args:
        df: Polars DataFrame with ticker data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No ticker data to load")
        return

    # Rename 'type' column to 'ticker_type' to match database schema
    df = df.rename({"type": "ticker_type"})

    # Convert to list of dictionaries for bulk insert
    records = df.to_dicts()

    engine = get_engine()

    # Delete and insert in the same transaction to avoid conflicts
    with engine.begin() as conn:
        # Delete all existing tickers
        conn.execute(delete(tickers))

        # Bulk insert in chunks
        chunk_size = 10000
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            conn.execute(tickers.insert(), chunk)

    logger.info(f"✅ Loaded {len(df):,} tickers")


def upsert_splits(df: pl.DataFrame) -> None:
    """Replace all splits (full refresh each run).

    Args:
        df: Polars DataFrame with split data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No split data to load")
        return

    # Convert to list of dictionaries for bulk insert
    records = df.to_dicts()

    engine = get_engine()

    # Delete and insert in the same transaction to avoid conflicts
    with engine.begin() as conn:
        # Delete all existing splits
        conn.execute(delete(splits))

        # Bulk insert in chunks
        chunk_size = 10000
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]
            conn.execute(splits.insert(), chunk)

    logger.info(f"✅ Loaded {len(df):,} splits")


def get_validation_stats() -> list[tuple[date, int]]:
    """Get record counts per date for validation.

    Returns:
        List of (date, count) tuples.
    """
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            select(stocks.c.date, func.count().label("record_count"))
            .group_by(stocks.c.date)
            .order_by(stocks.c.date)
        ).fetchall()

    return [(row[0], row[1]) for row in result]
