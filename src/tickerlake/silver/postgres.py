"""Postgres storage backend using SQLAlchemy + psycopg COPY for silver layer."""

from typing import Optional

import polars as pl
from sqlalchemy import create_engine, delete
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from tickerlake.bronze.postgres import get_engine as get_bronze_engine
from tickerlake.config import settings
from tickerlake.logging_config import get_logger
from tickerlake.silver.models import (
    daily_aggregates,
    daily_indicators,
    metadata,
    monthly_aggregates,
    monthly_indicators,
    ticker_metadata,
    weekly_aggregates,
    weekly_indicators,
)

logger = get_logger(__name__)

# Singleton engine (reuse bronze engine since same database)
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Get or create SQLAlchemy engine (reuses bronze connection pool)."""
    global _engine
    if _engine is None:
        # Reuse the bronze engine since we're using the same database
        _engine = get_bronze_engine()
        logger.info(f"🔗 Reusing Postgres connection for silver layer")
    return _engine


def init_schema() -> None:
    """Create all silver layer tables if they don't exist (idempotent)."""
    engine = get_engine()
    metadata.create_all(engine)
    logger.info("✅ Silver layer schema initialized")


def clear_all_tables() -> None:
    """Delete all data from silver layer tables (for full rebuild).

    ⚠️  WARNING: This deletes all processed data!
    """
    engine = get_engine()

    logger.warning("⚠️  Clearing all silver layer tables...")

    with engine.begin() as conn:
        # Delete in reverse dependency order
        conn.execute(delete(daily_indicators))
        conn.execute(delete(daily_aggregates))
        conn.execute(delete(weekly_indicators))
        conn.execute(delete(weekly_aggregates))
        conn.execute(delete(monthly_indicators))
        conn.execute(delete(monthly_aggregates))
        conn.execute(delete(ticker_metadata))

    logger.info("✅ All silver layer tables cleared")


def bulk_load_ticker_metadata(df: pl.DataFrame) -> None:
    """Fast bulk load ticker metadata using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with ticker metadata (ticker, name, type, primary_exchange, active, cik).
    """
    if len(df) == 0:
        logger.warning("⚠️  No ticker metadata to load")
        return

    logger.info(f"💾 Loading {len(df):,} ticker metadata records...")

    engine = get_engine()

    with engine.begin() as conn:
        # Clear existing metadata first (full refresh)
        conn.execute(delete(ticker_metadata))

        # Bulk insert via raw COPY
        raw_conn = conn.connection
        with raw_conn.cursor().copy(
            "COPY silver_ticker_metadata (ticker, name, ticker_type, primary_exchange, active, cik) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            copy.write_arrow(df.to_arrow())

    logger.info(f"✅ Loaded {len(df):,} ticker metadata records")


def bulk_load_daily_aggregates(df: pl.DataFrame) -> None:
    """Fast bulk load daily aggregates using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with daily OHLCV data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No daily aggregates to load")
        return

    logger.info(f"💾 Loading {len(df):,} daily aggregate records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_daily_aggregates (ticker, date, open, high, low, close, volume, transactions) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            # Stream Arrow batches for memory efficiency
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} daily aggregate records")


def bulk_load_daily_indicators(df: pl.DataFrame) -> None:
    """Fast bulk load daily indicators using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with daily technical indicators.
    """
    if len(df) == 0:
        logger.warning("⚠️  No daily indicators to load")
        return

    logger.info(f"💾 Loading {len(df):,} daily indicator records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_daily_indicators (ticker, date, sma_20, sma_50, sma_200, atr_14, "
            "volume_ma_20, volume_ratio, is_hvc) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} daily indicator records")


def bulk_load_weekly_aggregates(df: pl.DataFrame) -> None:
    """Fast bulk load weekly aggregates using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with weekly OHLCV data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No weekly aggregates to load")
        return

    logger.info(f"💾 Loading {len(df):,} weekly aggregate records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_weekly_aggregates (ticker, date, open, high, low, close, volume, transactions) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} weekly aggregate records")


def bulk_load_weekly_indicators(df: pl.DataFrame) -> None:
    """Fast bulk load weekly indicators using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with weekly technical indicators (includes Weinstein stages).
    """
    if len(df) == 0:
        logger.warning("⚠️  No weekly indicators to load")
        return

    logger.info(f"💾 Loading {len(df):,} weekly indicator records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_weekly_indicators (ticker, date, sma_20, sma_50, sma_200, atr_14, "
            "volume_ma_20, volume_ratio, is_hvc, ma_30, price_vs_ma_pct, ma_slope_pct, "
            "raw_stage, stage, stage_changed, weeks_in_stage) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} weekly indicator records")


def bulk_load_monthly_aggregates(df: pl.DataFrame) -> None:
    """Fast bulk load monthly aggregates using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with monthly OHLCV data.
    """
    if len(df) == 0:
        logger.warning("⚠️  No monthly aggregates to load")
        return

    logger.info(f"💾 Loading {len(df):,} monthly aggregate records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_monthly_aggregates (ticker, date, open, high, low, close, volume, transactions) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} monthly aggregate records")


def bulk_load_monthly_indicators(df: pl.DataFrame) -> None:
    """Fast bulk load monthly indicators using raw psycopg COPY BINARY.

    Args:
        df: Polars DataFrame with monthly technical indicators.
    """
    if len(df) == 0:
        logger.warning("⚠️  No monthly indicators to load")
        return

    logger.info(f"💾 Loading {len(df):,} monthly indicator records...")

    engine = get_engine()

    with engine.raw_connection() as raw_conn:
        with raw_conn.cursor().copy(
            "COPY silver_monthly_indicators (ticker, date, sma_20, sma_50, sma_200, atr_14, "
            "volume_ma_20, volume_ratio, is_hvc) "
            "FROM STDIN (FORMAT BINARY)"
        ) as copy:
            for batch in df.to_arrow().to_batches(max_chunksize=50000):
                copy.write_arrow(batch)

        raw_conn.commit()

    logger.info(f"✅ Loaded {len(df):,} monthly indicator records")
