"""Aggregate daily stock data to weekly and monthly timeframes."""

import polars as pl

from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def aggregate_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily OHLCV data to weekly bars.

    Uses Sunday-Saturday weeks. OHLC aggregation:
    - Open: First open of the week
    - High: Maximum high of the week
    - Low: Minimum low of the week
    - Close: Last close of the week
    - Volume: Sum of daily volumes
    - Transactions: Sum of daily transactions

    Args:
        df: DataFrame with ticker, date, open, high, low, close, volume, transactions.

    Returns:
        DataFrame with weekly aggregated OHLCV data.
    """
    logger.info("Aggregating daily data to weekly timeframe...")

    # Sort to ensure proper first/last aggregation
    df_sorted = df.sort(["ticker", "date"])

    # Group by ticker and week (Sunday start)
    weekly = (
        df_sorted.group_by_dynamic(
            index_column="date",
            every="1w",
            period="1w",
            offset="0d",  # Weeks start on Sunday
            by="ticker",  # type: ignore[call-arg]
            start_by="monday",  # Anchor to Monday to get Sunday-Saturday weeks
        )
        .agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("transactions").sum().alias("transactions"),
        ])
        .sort(["ticker", "date"])
    )

    logger.info(f"Created {weekly.height:,} weekly bars from {df.height:,} daily bars")
    return weekly


def aggregate_to_monthly(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate daily OHLCV data to monthly bars.

    Uses calendar months. OHLC aggregation:
    - Open: First open of the month
    - High: Maximum high of the month
    - Low: Minimum low of the month
    - Close: Last close of the month
    - Volume: Sum of daily volumes
    - Transactions: Sum of daily transactions

    Args:
        df: DataFrame with ticker, date, open, high, low, close, volume, transactions.

    Returns:
        DataFrame with monthly aggregated OHLCV data.
    """
    logger.info("Aggregating daily data to monthly timeframe...")

    # Sort to ensure proper first/last aggregation
    df_sorted = df.sort(["ticker", "date"])

    # Group by ticker and month
    monthly = (
        df_sorted.group_by_dynamic(
            index_column="date",
            every="1mo",
            period="1mo",
            by="ticker",  # type: ignore[call-arg]
        )
        .agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("transactions").sum().alias("transactions"),
        ])
        .sort(["ticker", "date"])
    )

    logger.info(f"Created {monthly.height:,} monthly bars from {df.height:,} daily bars")
    return monthly
