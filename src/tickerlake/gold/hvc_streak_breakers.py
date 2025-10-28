"""HVC Streak Breaker analysis - detects momentum reversals. 🔔

Identifies when a streak of 3+ consecutive HVCs moving in one direction
is broken by an HVC moving in the opposite direction. These are potential
trend reversal signals.
"""

from datetime import datetime, timezone

import polars as pl
from sqlalchemy import select

from tickerlake.config import settings
from tickerlake.db import bulk_load, get_engine
from tickerlake.gold.models import (
    hvc_streak_breakers_daily,
    hvc_streak_breakers_weekly,
    hvcs_daily,
    hvcs_weekly,
)
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def detect_streak_breakers(
    ticker: str, hvc_df: pl.DataFrame, min_streak_length: int = 3
) -> list[dict]:
    """Detect all streak breakers for a single ticker. 🔔

    Walks forward through HVC periods, tracking streaks. When a streak of
    min_streak_length or more is broken by opposite direction, records it.

    Note: This returns ALL historical breakers for analysis. The calling function
    filters to only the most recent breaker per ticker. 🎯

    Args:
        ticker: Stock ticker symbol.
        hvc_df: DataFrame of HVC periods for this ticker, sorted by date.
        min_streak_length: Minimum streak length to qualify as breakable (default: 3).

    Returns:
        List of ALL streak breaker dictionaries, each containing:
        - ticker, streak_direction, streak_length
        - streak_start_date, streak_end_date, breaker_date
        - streak_start_close, streak_end_close, breaker_close
        - streak_price_change_pct
    """
    if len(hvc_df) < min_streak_length + 1:
        # Need at least min_streak_length + 1 breaker
        return []

    # Ensure sorted by date ascending (oldest to newest)
    hvc_df = hvc_df.sort("date")

    # Extract as lists for iteration
    dates = hvc_df["date"].to_list()
    closes = hvc_df["close"].to_list()

    breakers = []
    current_streak_direction = None
    current_streak_start_idx = 0

    for i in range(1, len(dates)):
        prev_close = closes[i - 1]
        curr_close = closes[i]

        # Determine price direction
        if curr_close > prev_close:
            step_direction = "up"
        elif curr_close < prev_close:
            step_direction = "down"
        else:
            # Equal prices continue current direction
            step_direction = (
                current_streak_direction if current_streak_direction else "up"
            )

        # Initialize streak on first step
        if current_streak_direction is None:
            current_streak_direction = step_direction
            continue

        # Check if direction changed (streak broken!)
        if step_direction != current_streak_direction:
            # Calculate streak length (number of movements, not periods)
            # Streak goes from current_streak_start_idx to i-1 (before the breaker)
            # Number of movements = (last_index - first_index)
            streak_length = (i - 1) - current_streak_start_idx

            # Only record if streak was long enough
            if streak_length >= min_streak_length:
                streak_start_close = closes[current_streak_start_idx]
                streak_end_close = closes[i - 1]
                breaker_close = closes[i]

                streak_price_change_pct = (
                    (streak_end_close - streak_start_close) / streak_start_close
                ) * 100.0

                breakers.append({
                    "ticker": ticker,
                    "streak_direction": current_streak_direction,
                    "streak_length": streak_length,
                    "streak_start_date": dates[current_streak_start_idx],
                    "streak_end_date": dates[i - 1],
                    "breaker_date": dates[i],
                    "streak_start_close": round(streak_start_close, 2),
                    "streak_end_close": round(streak_end_close, 2),
                    "breaker_close": round(breaker_close, 2),
                    "streak_price_change_pct": round(streak_price_change_pct, 2),
                })

            # Start new streak at i-1 (where the direction change begins)
            current_streak_direction = step_direction
            current_streak_start_idx = i - 1

    return breakers


def calculate_streak_breakers_for_timeframe(
    timeframe: str, min_streak_length: int
) -> pl.DataFrame:
    """Calculate HVC streak breakers for a specific timeframe. 🔔

    Reads pre-identified HVCs from gold layer cache and detects streak breakers
    for each ticker. Returns only the MOST RECENT breaker per ticker. 🎯

    Args:
        timeframe: Either 'daily' or 'weekly'.
        min_streak_length: Minimum consecutive HVCs before break is significant.

    Returns:
        DataFrame with most recent breaker per ticker, ready for bulk loading.
    """
    logger.info(
        f"🔍 Analyzing {timeframe} HVC streak breakers (min_streak={min_streak_length})..."
    )

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
                "streak_direction": pl.String,
                "streak_length": pl.Int32,
                "streak_start_date": pl.Date,
                "streak_end_date": pl.Date,
                "breaker_date": pl.Date,
                "streak_start_close": pl.Float64,
                "streak_end_close": pl.Float64,
                "breaker_close": pl.Float64,
                "streak_price_change_pct": pl.Float64,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to Polars DataFrame
    df = pl.DataFrame(
        rows, schema=["ticker", "date", "close", "volume_ratio"], orient="row"
    )
    logger.info(
        f"📊 Processing {len(df):,} {timeframe} HVCs across "
        f"{df['ticker'].n_unique():,} tickers"
    )

    # Process each ticker individually
    all_breakers = []
    calculated_at = datetime.now(timezone.utc)

    for ticker_symbol in df["ticker"].unique():
        ticker_hvcs = df.filter(pl.col("ticker") == ticker_symbol)
        breakers = detect_streak_breakers(ticker_symbol, ticker_hvcs, min_streak_length)

        # Add calculated_at timestamp to each breaker
        for breaker in breakers:
            breaker["calculated_at"] = calculated_at
            all_breakers.append(breaker)

    if not all_breakers:
        logger.info(
            f"ℹ️  No {timeframe} streak breakers found "
            f"(min_streak_length={min_streak_length})"
        )
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "streak_direction": pl.String,
                "streak_length": pl.Int32,
                "streak_start_date": pl.Date,
                "streak_end_date": pl.Date,
                "breaker_date": pl.Date,
                "streak_start_close": pl.Float64,
                "streak_end_close": pl.Float64,
                "breaker_close": pl.Float64,
                "streak_price_change_pct": pl.Float64,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to DataFrame
    results_df = pl.DataFrame(all_breakers)

    # Filter to only most recent breaker per ticker 🎯
    results_df = (
        results_df.sort("breaker_date", descending=True)
        .group_by("ticker", maintain_order=True)
        .first()
    )

    logger.info(
        f"✅ Detected {len(results_df):,} most recent {timeframe} HVC streak breakers "
        f"(avg streak length: {results_df['streak_length'].mean():.1f})"
    )

    return results_df


def run_streak_breaker_analysis() -> None:
    """Main entry point for HVC streak breaker report. 🎯

    Calculates streak breakers for all configured timeframes and writes results
    to gold layer tables.
    """
    logger.info("🔔 Starting HVC Streak Breaker Analysis...")

    # Process each timeframe (HVCs must already be cached in gold layer)
    for timeframe in settings.hvc_timeframes:
        if timeframe == "daily":
            results = calculate_streak_breakers_for_timeframe(
                timeframe="daily",
                min_streak_length=settings.hvc_min_streak_length,
            )
            if len(results) > 0:
                bulk_load(hvc_streak_breakers_daily, results)
                logger.info(
                    f"💾 Loaded {len(results):,} daily streak breakers to database"
                )
            else:
                logger.info("ℹ️  No daily streak breakers to load")

        elif timeframe == "weekly":
            results = calculate_streak_breakers_for_timeframe(
                timeframe="weekly",
                min_streak_length=settings.hvc_min_streak_length,
            )
            if len(results) > 0:
                bulk_load(hvc_streak_breakers_weekly, results)
                logger.info(
                    f"💾 Loaded {len(results):,} weekly streak breakers to database"
                )
            else:
                logger.info("ℹ️  No weekly streak breakers to load")

        else:
            logger.warning(f"⚠️  Unknown timeframe: {timeframe}")

    logger.info("✅ HVC Streak Breaker Analysis Complete!")
