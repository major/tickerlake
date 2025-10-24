"""Postgres storage backend using SQLAlchemy + psycopg COPY for bronze layer."""

from datetime import date
from typing import Optional

import polars as pl
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from tickerlake.bronze.models import metadata, splits, stocks, tickers
from tickerlake.config import settings
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)

# Singleton engine with connection pooling
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Get or create SQLAlchemy engine with connection pooling."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.postgres_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            echo=False,  # Set to True for SQL debugging
        )
        logger.info(f"🔗 Connected to Postgres at {settings.postgres_host}:{settings.postgres_port}")
    return _engine


def init_schema() -> None:
    """Create all tables if they don't exist (idempotent)."""
    engine = get_engine()
    metadata.create_all(engine)
    logger.info("✅ Database schema initialized")


def bulk_load_stocks(df: pl.DataFrame) -> None:
    """Fast bulk load using raw psycopg COPY BINARY (1-2M rows/sec).

    Args:
        df: Polars DataFrame with stocks data to load.
    """
    if len(df) == 0:
        logger.warning("⚠️  No stock data to load")
        return

    logger.info(f"💾 Loading {len(df):,} stock records...")

    engine = get_engine()

    # Access raw psycopg connection from SQLAlchemy
    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY stocks (ticker, date, open, high, low, close, volume, transactions) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            # Stream Arrow batches for memory efficiency
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} stock records")


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
