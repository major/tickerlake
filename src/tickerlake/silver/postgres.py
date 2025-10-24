"""Postgres storage backend for silver layer. 🥈

Uses shared database utilities from tickerlake.db for common operations.
Silver-specific functions remain here (clear_all_tables, etc.).
"""

import polars as pl
from sqlalchemy import delete

from tickerlake.db import get_engine, init_schema  # noqa: F401
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


# Re-export for backward compatibility (convenience)
def init_silver_schema() -> None:
    """Initialize silver layer database schema (idempotent). ✨"""
    init_schema(metadata, "silver")


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


