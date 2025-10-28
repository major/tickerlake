"""HVC (High Volume Close) identification and caching for gold layer. 🔥

This module identifies all High Volume Close periods from the silver layer and
caches them in gold layer tables. These cached HVCs are then reused by all
downstream analyses (streaks, breakers, returns) for efficiency.

A High Volume Close (HVC) occurs when a period's volume is significantly above
the 20-period moving average (default: 3x or more) AND the volume_ma_20 is at
least 250K shares (to filter out low-liquidity stocks).
"""

from datetime import datetime, timezone

import polars as pl
from sqlalchemy import select

from tickerlake.config import settings
from tickerlake.db import bulk_load, get_engine
from tickerlake.gold.models import hvcs_daily, hvcs_weekly
from tickerlake.logging_config import get_logger
from tickerlake.silver.models import (
    daily_aggregates,
    daily_indicators,
    weekly_aggregates,
    weekly_indicators,
)

logger = get_logger(__name__)

# Minimum average daily volume to qualify (filters out illiquid stocks) 📊
MIN_VOLUME_MA_20 = 250_000


def identify_hvcs_for_timeframe(
    timeframe: str, volume_threshold: float
) -> pl.DataFrame:
    """Identify all HVCs for a specific timeframe from silver layer. 🔍

    Queries the silver layer to find all periods where:
    1. volume_ratio >= volume_threshold (e.g., 3x the 20-period MA)
    2. volume_ma_20 >= MIN_VOLUME_MA_20 (250K shares minimum)

    Args:
        timeframe: Either 'daily' or 'weekly'.
        volume_threshold: Minimum volume multiplier to qualify as HVC.

    Returns:
        DataFrame with all HVC periods ready for bulk loading.
    """
    logger.info(
        f"🔍 Identifying {timeframe} HVCs "
        f"(threshold={volume_threshold}x, min_volume_ma={MIN_VOLUME_MA_20:,})..."
    )

    # Select appropriate silver tables
    if timeframe == "daily":
        agg_table = daily_aggregates
        ind_table = daily_indicators
    elif timeframe == "weekly":
        agg_table = weekly_aggregates
        ind_table = weekly_indicators
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    # Read HVC data from silver layer with JOIN
    engine = get_engine()
    with engine.connect() as conn:
        # Query: SELECT agg.*, ind.volume_ratio, ind.volume_ma_20
        # FROM aggregates agg
        # JOIN indicators ind ON agg.ticker = ind.ticker AND agg.date = ind.date
        # WHERE ind.volume_ratio >= threshold AND ind.volume_ma_20 >= MIN_VOLUME_MA_20
        query = (
            select(
                agg_table.c.ticker,
                agg_table.c.date,
                agg_table.c.open,
                agg_table.c.high,
                agg_table.c.low,
                agg_table.c.close,
                agg_table.c.volume,
                ind_table.c.volume_ratio,
                ind_table.c.volume_ma_20,
            )
            .select_from(
                agg_table.join(
                    ind_table,
                    (agg_table.c.ticker == ind_table.c.ticker)
                    & (agg_table.c.date == ind_table.c.date),
                )
            )
            .where(
                (ind_table.c.volume_ratio >= volume_threshold)
                & (ind_table.c.volume_ma_20 >= MIN_VOLUME_MA_20)
            )
            .order_by(agg_table.c.ticker, agg_table.c.date)
        )

        result = conn.execute(query)
        rows = result.fetchall()

    if not rows:
        logger.info(f"ℹ️  No {timeframe} HVC periods found")
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "volume_ratio": pl.Float64,
                "volume_ma_20": pl.Float64,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to Polars DataFrame
    df = pl.DataFrame(
        rows,
        schema=[
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "volume_ratio",
            "volume_ma_20",
        ],
        orient="row",
    )

    # Add calculated_at timestamp
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("calculated_at"))

    logger.info(
        f"✅ Identified {len(df):,} {timeframe} HVC periods "
        f"across {df['ticker'].n_unique():,} tickers"
    )

    return df


def run_hvc_identification() -> None:
    """Main entry point for HVC identification. 🎯

    Identifies all HVCs from silver layer and caches them in gold layer tables.
    This should run FIRST in the gold pipeline, before any downstream analyses.
    """
    logger.info("🔥 Starting HVC Identification...")

    # Process each timeframe
    for timeframe in settings.hvc_timeframes:
        if timeframe == "daily":
            results = identify_hvcs_for_timeframe(
                timeframe="daily",
                volume_threshold=settings.hvc_volume_threshold,
            )
            if len(results) > 0:
                bulk_load(hvcs_daily, results)
                logger.info(f"💾 Cached {len(results):,} daily HVCs to database")
            else:
                logger.info("ℹ️  No daily HVCs to cache")

        elif timeframe == "weekly":
            results = identify_hvcs_for_timeframe(
                timeframe="weekly",
                volume_threshold=settings.hvc_volume_threshold,
            )
            if len(results) > 0:
                bulk_load(hvcs_weekly, results)
                logger.info(f"💾 Cached {len(results):,} weekly HVCs to database")
            else:
                logger.info("ℹ️  No weekly HVCs to cache")

        else:
            logger.warning(f"⚠️  Unknown timeframe: {timeframe}")

    logger.info("✅ HVC Identification Complete!")
