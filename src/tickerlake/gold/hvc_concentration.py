"""HVC (High Volume Close) concentration analysis - market-wide high-volume days/weeks. 📅

This module identifies dates with the highest concentration of HVCs across all tickers,
which can indicate significant market-wide events or volatility.
"""

from datetime import datetime, timezone

import polars as pl
from sqlalchemy import func, select

from tickerlake.config import settings
from tickerlake.db import bulk_load, get_engine
from tickerlake.gold.models import (
    hvc_concentration_daily,
    hvc_concentration_weekly,
)
from tickerlake.logging_config import get_logger
from tickerlake.silver.models import daily_indicators, weekly_indicators

logger = get_logger(__name__)


def calculate_hvc_concentration_for_timeframe(
    timeframe: str, volume_threshold: float
) -> pl.DataFrame:
    """Calculate HVC concentration (count per date) for a specific timeframe. 📊

    Queries silver layer indicators directly to count how many tickers had
    volume_ratio >= threshold on each date.

    Args:
        timeframe: Either 'daily' or 'weekly'.
        volume_threshold: Minimum volume_ratio to count as HVC (e.g., 3.0).

    Returns:
        DataFrame with columns: date, hvc_count, calculated_at.
    """
    logger.info(f"📊 Calculating {timeframe} HVC concentration by date...")

    # Select appropriate silver layer indicators table
    if timeframe == "daily":
        indicators_table = daily_indicators
    elif timeframe == "weekly":
        indicators_table = weekly_indicators
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    # Query to count HVCs per date
    engine = get_engine()
    with engine.connect() as conn:
        query = (
            select(
                indicators_table.c.date,
                func.count().label("hvc_count"),
            )
            .where(indicators_table.c.volume_ratio >= volume_threshold)
            .group_by(indicators_table.c.date)
            .order_by(indicators_table.c.date.desc())
        )

        result = conn.execute(query)
        rows = result.fetchall()

    if not rows:
        logger.warning(f"⚠️ No {timeframe} HVCs found!")
        return pl.DataFrame(
            {
                "date": pl.Series([], dtype=pl.Date),
                "hvc_count": pl.Series([], dtype=pl.Int64),
                "calculated_at": pl.Series([], dtype=pl.Datetime),
            }
        )

    # Convert to Polars DataFrame
    calculated_at = datetime.now(timezone.utc)
    df = pl.DataFrame(
        {
            "date": [row[0] for row in rows],
            "hvc_count": [row[1] for row in rows],
            "calculated_at": [calculated_at] * len(rows),
        }
    )

    logger.info(
        f"✅ Found {len(df)} {timeframe} dates with HVCs "
        f"(max: {df['hvc_count'].max()} HVCs on a single date)"
    )

    return df


def run_hvc_concentration_analysis() -> None:
    """Run complete HVC concentration analysis for all timeframes. 🚀

    This function:
    1. Queries silver layer for HVC counts per date (daily + weekly)
    2. Writes results to gold layer concentration tables

    Tables written:
    - gold_hvc_concentration_daily
    - gold_hvc_concentration_weekly
    """
    logger.info("📅 Starting HVC Concentration Analysis...")

    for timeframe in settings.hvc_timeframes:
        logger.info(f"📊 Processing {timeframe} concentration...")

        # Calculate concentration
        df = calculate_hvc_concentration_for_timeframe(
            timeframe=timeframe,
            volume_threshold=settings.hvc_volume_threshold,
        )

        if len(df) == 0:
            logger.warning(f"⚠️ No {timeframe} HVC concentration data to write!")
            continue

        # Select target table
        if timeframe == "daily":
            target_table = hvc_concentration_daily
        elif timeframe == "weekly":
            target_table = hvc_concentration_weekly
        else:
            logger.warning(f"⚠️ Unknown timeframe: {timeframe}, skipping...")
            continue

        # Write to gold layer
        bulk_load(target_table, df)
        logger.info(
            f"✅ Wrote {len(df)} {timeframe} concentration records to {target_table.name}"
        )

    logger.info("✅ HVC Concentration Analysis complete! 🎉")
