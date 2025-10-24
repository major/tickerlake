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

    logger.info(f"💾 Loading {len(df):,} tickers...")

    engine = get_engine()

    with engine.begin() as conn:
        # Delete all existing tickers
        conn.execute(delete(tickers))

        # Bulk insert via raw COPY
        raw_conn = conn.connection
        with raw_conn.cursor().copy(
            "COPY tickers (ticker, name, ticker_type, active, locale, market, "
            "primary_exchange, currency_name, currency_symbol, cik, composite_figi, "
            "share_class_figi, base_currency_name, base_currency_symbol, "
            "delisted_utc, last_updated_utc) FROM STDIN (FORMAT BINARY)"
        ) as copy:
            copy.write_arrow(df.to_arrow())

    logger.info(f"✅ Loaded {len(df):,} tickers")


def upsert_splits(df: pl.DataFrame) -> None:
    """Replace all splits (full refresh each run).

    Args:
        df: Polars DataFrame with split data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No split data to load")
        return

    logger.info(f"💾 Loading {len(df):,} splits...")

    engine = get_engine()

    with engine.begin() as conn:
        # Delete all existing splits
        conn.execute(delete(splits))

        # Bulk insert via raw COPY
        raw_conn = conn.connection
        with raw_conn.cursor().copy(
            "COPY splits (ticker, execution_date, split_from, split_to) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            copy.write_arrow(df.to_arrow())

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
