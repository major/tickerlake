"""High Volume Close (HVC) streak analysis for gold layer. 📊

A High Volume Close (HVC) occurs when a period's volume is significantly above
the 20-period moving average (default: 3x or more). This module identifies and
tracks consecutive HVC periods moving in the same price direction.
"""

from datetime import datetime, timezone

import polars as pl
from sqlalchemy import select

from tickerlake.config import settings
from tickerlake.db import bulk_load, get_engine
from tickerlake.gold.models import (
    hvc_streaks_daily,
    hvc_streaks_weekly,
    hvcs_daily,
    hvcs_weekly,
)
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def detect_most_recent_streak(ticker: str, hvc_df: pl.DataFrame) -> dict | None:
    """Detect the most recent HVC streak for a single ticker. 📈

    Walks backwards from the most recent HVC, counting consecutive periods
    moving in the same price direction. Equal prices continue the current
    direction.

    Args:
        ticker: Stock ticker symbol.
        hvc_df: DataFrame of HVC periods for this ticker, sorted by date.

    Returns:
        Dictionary with streak metadata, or None if no valid streak exists.
        Keys: ticker, streak_length, direction, streak_start_date,
              streak_end_date, latest_close, streak_price_change_pct
    """
    if len(hvc_df) == 0:
        return None

    # Ensure sorted by date ascending (oldest to newest)
    hvc_df = hvc_df.sort("date")

    # Extract as lists for easier backward iteration
    dates = hvc_df["date"].to_list()
    closes = hvc_df["close"].to_list()

    if len(closes) == 1:
        # Single HVC period - streak of 1, direction is neutral (mark as 'up')
        return {
            "ticker": ticker,
            "streak_length": 1,
            "direction": "up",
            "streak_start_date": dates[0],
            "streak_end_date": dates[0],
            "latest_close": round(closes[0], 2),
            "streak_price_change_pct": 0.0,
        }

    # Walk backwards from most recent HVC
    streak_length = 1
    direction = None  # Will be set on first comparison
    streak_start_date = dates[-1]
    streak_end_date = dates[-1]
    first_close = closes[-1]
    last_close = closes[-1]

    for i in range(len(closes) - 1, 0, -1):
        current_close = closes[i]
        prev_close = closes[i - 1]

        # Determine direction of this step
        if current_close > prev_close:
            step_direction = "up"
        elif current_close < prev_close:
            step_direction = "down"
        else:
            # Equal prices continue current direction
            step_direction = direction if direction else "up"

        # Set initial direction on first iteration
        if direction is None:
            direction = step_direction

        # Check if streak continues
        if step_direction != direction:
            # Direction changed - streak ends here
            break

        # Extend streak
        streak_length += 1
        streak_start_date = dates[i - 1]
        first_close = prev_close

    # Calculate price change percentage
    price_change_pct = ((last_close - first_close) / first_close) * 100.0

    return {
        "ticker": ticker,
        "streak_length": streak_length,
        "direction": direction or "up",  # Fallback if somehow still None
        "streak_start_date": streak_start_date,
        "streak_end_date": streak_end_date,
        "latest_close": round(last_close, 2),
        "streak_price_change_pct": round(price_change_pct, 2),
    }


def calculate_hvc_streaks_for_timeframe(
    timeframe: str, min_streak_length: int
) -> pl.DataFrame:
    """Calculate HVC streaks for a specific timeframe. ⏱️

    Reads pre-identified HVCs from gold layer cache and detects the most recent
    streak for each ticker.

    Args:
        timeframe: Either 'daily' or 'weekly'.
        min_streak_length: Minimum consecutive HVCs to include in results.

    Returns:
        DataFrame ready for bulk loading into gold layer tables.
    """
    logger.info(f"🔍 Analyzing {timeframe} HVC streaks...")

    # Select appropriate HVC cache table
    if timeframe == "daily":
        hvc_table = hvcs_daily
    elif timeframe == "weekly":
        hvc_table = hvcs_weekly
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    # Read pre-identified HVCs from gold layer cache
    engine = get_engine()
    with engine.connect() as conn:
        query = select(
            hvc_table.c.ticker,
            hvc_table.c.date,
            hvc_table.c.close,
            hvc_table.c.volume_ratio,
        ).order_by(hvc_table.c.ticker, hvc_table.c.date)

        result = conn.execute(query)
        rows = result.fetchall()

    if not rows:
        logger.info(f"ℹ️  No {timeframe} HVCs found in cache")
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "streak_length": pl.Int32,
                "direction": pl.String,
                "streak_start_date": pl.Date,
                "streak_end_date": pl.Date,
                "latest_close": pl.Float64,
                "streak_price_change_pct": pl.Float64,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to Polars DataFrame
    df = pl.DataFrame(
        rows, schema=["ticker", "date", "close", "volume_ratio"], orient="row"
    )
    logger.info(
        f"📊 Processing {len(df):,} {timeframe} HVCs across {df['ticker'].n_unique():,} tickers"
    )

    # Process each ticker individually
    streaks = []
    calculated_at = datetime.now(timezone.utc)

    for ticker_symbol in df["ticker"].unique():
        ticker_hvcs = df.filter(pl.col("ticker") == ticker_symbol)
        streak = detect_most_recent_streak(ticker_symbol, ticker_hvcs)

        if streak and streak["streak_length"] >= min_streak_length:
            streak["calculated_at"] = calculated_at
            streaks.append(streak)

    if not streaks:
        logger.info(
            f"ℹ️  No {timeframe} streaks met minimum length threshold ({min_streak_length})"
        )
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "streak_length": pl.Int32,
                "direction": pl.String,
                "streak_start_date": pl.Date,
                "streak_end_date": pl.Date,
                "latest_close": pl.Float64,
                "streak_price_change_pct": pl.Float64,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to DataFrame
    results_df = pl.DataFrame(streaks)

    logger.info(
        f"✅ Detected {len(results_df):,} {timeframe} HVC streaks "
        f"(avg length: {results_df['streak_length'].mean():.1f})"
    )

    return results_df


def run_hvc_streaks_analysis() -> None:
    """Main entry point for HVC streaks report. 🎯

    Calculates HVC streaks for all configured timeframes and writes results
    to gold layer tables.
    """
    logger.info("📊 Starting HVC Streaks Analysis...")

    # Process each timeframe (HVCs must already be cached in gold layer)
    for timeframe in settings.hvc_timeframes:
        if timeframe == "daily":
            results = calculate_hvc_streaks_for_timeframe(
                timeframe="daily",
                min_streak_length=settings.hvc_min_streak_length,
            )
            if len(results) > 0:
                bulk_load(hvc_streaks_daily, results)
                logger.info(f"💾 Loaded {len(results):,} daily HVC streaks to database")
            else:
                logger.info("ℹ️  No daily HVC streaks to load")

        elif timeframe == "weekly":
            results = calculate_hvc_streaks_for_timeframe(
                timeframe="weekly",
                min_streak_length=settings.hvc_min_streak_length,
            )
            if len(results) > 0:
                bulk_load(hvc_streaks_weekly, results)
                logger.info(
                    f"💾 Loaded {len(results):,} weekly HVC streaks to database"
                )
            else:
                logger.info("ℹ️  No weekly HVC streaks to load")

        else:
            logger.warning(f"⚠️  Unknown timeframe: {timeframe}")

    logger.info("✅ HVC Streaks Analysis Complete!")
