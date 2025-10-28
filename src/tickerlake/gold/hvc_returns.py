"""HVC Returns analysis for gold layer. 🔄

Identifies when current price returns to (falls within) the range of a previous
High Volume Close (HVC) period. This helps identify revisits to significant
volume zones that could act as support/resistance.
"""

from datetime import datetime, timezone

import polars as pl
from sqlalchemy import select

from tickerlake.config import settings
from tickerlake.db import bulk_load, get_engine
from tickerlake.gold.models import (
    hvc_returns_daily,
    hvc_returns_weekly,
    hvcs_daily,
    hvcs_weekly,
)
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def detect_hvc_returns_for_ticker(
    ticker_df: pl.DataFrame, min_periods_ago: int, timeframe: str
) -> list[dict]:
    """Detect HVC returns for a single ticker. 🎯

    For each period in the dataframe, checks if the current close price falls
    within the range [low, high] of any previous HVC that occurred at least
    min_periods_ago periods earlier.

    Args:
        ticker_df: DataFrame with columns: date, close, low, high, volume_ratio.
                   Must be sorted by date ascending and filtered to HVCs only.
        min_periods_ago: Minimum gap required (5 for daily, 4 for weekly).
        timeframe: Either 'daily' or 'weekly' (for logging/labeling).

    Returns:
        List of dictionaries, each representing one HVC return event.
        Keys: ticker, return_date, current_close, hvc_date, hvc_low, hvc_high,
              hvc_close, hvc_volume_ratio, days_since_hvc/weeks_since_hvc
    """
    if len(ticker_df) == 0:
        return []

    # Ensure sorted by date
    ticker_df = ticker_df.sort("date")

    returns = []
    ticker = ticker_df["ticker"][0]  # All rows have same ticker

    # Convert to lists for easier iteration
    dates = ticker_df["date"].to_list()
    lows = ticker_df["low"].to_list()
    highs = ticker_df["high"].to_list()
    closes = ticker_df["close"].to_list()
    volume_ratios = ticker_df["volume_ratio"].to_list()

    # For each HVC period (the "current" period)
    for i in range(len(ticker_df)):
        current_date = dates[i]
        current_close = closes[i]

        # Check all previous HVCs that are far enough back
        for j in range(i):
            hvc_date = dates[j]
            hvc_low = lows[j]
            hvc_high = highs[j]
            hvc_close = closes[j]
            hvc_volume_ratio = volume_ratios[j]

            # Calculate time gap (in days or weeks depending on timeframe)
            time_gap = (current_date - hvc_date).days
            if timeframe == "weekly":
                # For weekly, convert days to weeks
                time_gap = time_gap // 7

            # Check if current close is within previous HVC range
            # AND the time gap is sufficient
            if time_gap >= min_periods_ago and hvc_low <= current_close <= hvc_high:
                if timeframe == "daily":
                    period_key = "days_since_hvc"
                else:
                    period_key = "weeks_since_hvc"

                returns.append({
                    "ticker": ticker,
                    "return_date": current_date,
                    "current_close": round(current_close, 2),
                    "hvc_date": hvc_date,
                    "hvc_low": round(hvc_low, 2),
                    "hvc_high": round(hvc_high, 2),
                    "hvc_close": round(hvc_close, 2),
                    "hvc_volume_ratio": round(hvc_volume_ratio, 2),
                    period_key: time_gap,
                })

    return returns


def calculate_hvc_returns_for_timeframe(
    timeframe: str, min_periods_ago: int
) -> pl.DataFrame:
    """Calculate HVC returns for a specific timeframe. ⏱️

    Reads pre-identified HVCs from gold layer cache and detects when price
    returns to previous HVC ranges.

    Args:
        timeframe: Either 'daily' or 'weekly'.
        min_periods_ago: Minimum gap (5 for daily, 4 for weekly).

    Returns:
        DataFrame ready for bulk loading into gold layer tables.
    """
    logger.info(
        f"🔍 Analyzing {timeframe} HVC returns (min_gap={min_periods_ago})..."
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
            hvc_table.c.low,
            hvc_table.c.high,
            hvc_table.c.close,
            hvc_table.c.volume_ratio,
        ).order_by(hvc_table.c.ticker, hvc_table.c.date)

        result = conn.execute(query)
        rows = result.fetchall()

    if not rows:
        logger.info(f"ℹ️  No {timeframe} HVCs found in cache")
        # Return empty DataFrame with correct schema
        if timeframe == "daily":
            period_col = "days_since_hvc"
        else:
            period_col = "weeks_since_hvc"

        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "return_date": pl.Date,
                "current_close": pl.Float64,
                "hvc_date": pl.Date,
                "hvc_low": pl.Float64,
                "hvc_high": pl.Float64,
                "hvc_close": pl.Float64,
                "hvc_volume_ratio": pl.Float64,
                period_col: pl.Int32,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to Polars DataFrame
    df = pl.DataFrame(
        rows,
        schema=["ticker", "date", "low", "high", "close", "volume_ratio"],
        orient="row",
    )
    logger.info(
        f"📊 Processing {len(df):,} {timeframe} HVCs across {df['ticker'].n_unique():,} tickers"
    )

    # Process each ticker individually
    all_returns = []
    calculated_at = datetime.now(timezone.utc)

    for ticker_symbol in df["ticker"].unique():
        ticker_hvcs = df.filter(pl.col("ticker") == ticker_symbol)
        ticker_returns = detect_hvc_returns_for_ticker(
            ticker_hvcs, min_periods_ago, timeframe
        )

        # Add calculated_at timestamp to each return
        for ret in ticker_returns:
            ret["calculated_at"] = calculated_at

        all_returns.extend(ticker_returns)

    if not all_returns:
        logger.info(
            f"ℹ️  No {timeframe} HVC returns detected (no price returned to previous HVCs)"
        )
        # Return empty DataFrame with correct schema
        if timeframe == "daily":
            period_col = "days_since_hvc"
        else:
            period_col = "weeks_since_hvc"

        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "return_date": pl.Date,
                "current_close": pl.Float64,
                "hvc_date": pl.Date,
                "hvc_low": pl.Float64,
                "hvc_high": pl.Float64,
                "hvc_close": pl.Float64,
                "hvc_volume_ratio": pl.Float64,
                period_col: pl.Int32,
                "calculated_at": pl.Datetime,
            }
        )

    # Convert to DataFrame
    results_df = pl.DataFrame(all_returns)

    logger.info(
        f"✅ Detected {len(results_df):,} {timeframe} HVC returns "
        f"across {results_df['ticker'].n_unique():,} tickers"
    )

    return results_df


def run_hvc_returns_analysis() -> None:
    """Main entry point for HVC returns report. 🎯

    Calculates HVC returns for all configured timeframes and writes results
    to gold layer tables.
    """
    logger.info("🔄 Starting HVC Returns Analysis...")

    # Process each timeframe (HVCs must already be cached in gold layer)
    for timeframe in settings.hvc_timeframes:
        if timeframe == "daily":
            results = calculate_hvc_returns_for_timeframe(
                timeframe="daily",
                min_periods_ago=5,  # Must be 5+ days old
            )
            if len(results) > 0:
                bulk_load(hvc_returns_daily, results)
                logger.info(f"💾 Loaded {len(results):,} daily HVC returns to database")
            else:
                logger.info("ℹ️  No daily HVC returns to load")

        elif timeframe == "weekly":
            results = calculate_hvc_returns_for_timeframe(
                timeframe="weekly",
                min_periods_ago=4,  # Must be 4+ weeks old
            )
            if len(results) > 0:
                bulk_load(hvc_returns_weekly, results)
                logger.info(
                    f"💾 Loaded {len(results):,} weekly HVC returns to database"
                )
            else:
                logger.info("ℹ️  No weekly HVC returns to load")

        else:
            logger.warning(f"⚠️  Unknown timeframe: {timeframe}")

    logger.info("✅ HVC Returns Analysis Complete!")
