"""Postgres storage backend for gold layer. 🥇

Uses shared database utilities from tickerlake.db for common operations.
Gold-specific functions remain here (clear_all_tables, etc.).
"""

from sqlalchemy import delete

from tickerlake.db import get_engine, init_schema  # noqa: F401
from tickerlake.db.schema import drop_schema
from tickerlake.gold.models import (
    hvc_returns_daily,
    hvc_returns_weekly,
    hvc_streak_breakers_daily,
    hvc_streak_breakers_weekly,
    hvc_streaks_daily,
    hvc_streaks_weekly,
    hvcs_daily,
    hvcs_weekly,
    metadata,
)
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


# Re-export for backward compatibility (convenience)
def init_gold_schema() -> None:
    """Initialize gold layer database schema (idempotent). ✨"""
    init_schema(metadata, "gold")


def reset_schema() -> None:
    """Drop and recreate all gold layer tables (⚠️ DESTRUCTIVE!). 🔄

    This is useful when the table schema changes and you need to rebuild
    the database structure from scratch.

    ⚠️  WARNING: This drops all tables and data!
    """
    logger.warning("🔄 Resetting gold layer schema...")
    drop_schema(metadata, "gold")
    init_gold_schema()
    logger.info("✅ Gold layer schema reset complete")


def clear_all_tables() -> None:
    """Delete all data from gold layer tables (for full rebuild). 🧹

    ⚠️  WARNING: This deletes all analytics data!
    """
    engine = get_engine()

    logger.warning("⚠️  Clearing all gold layer tables...")

    with engine.begin() as conn:
        # Delete HVC cache tables (must clear first - other tables depend on these)
        conn.execute(delete(hvcs_daily))
        conn.execute(delete(hvcs_weekly))
        # Delete HVC streak tables
        conn.execute(delete(hvc_streaks_daily))
        conn.execute(delete(hvc_streaks_weekly))
        # Delete HVC streak breaker tables
        conn.execute(delete(hvc_streak_breakers_daily))
        conn.execute(delete(hvc_streak_breakers_weekly))
        # Delete HVC returns tables
        conn.execute(delete(hvc_returns_daily))
        conn.execute(delete(hvc_returns_weekly))
        # Future tables will be added here

    logger.info("✅ All gold layer tables cleared")
