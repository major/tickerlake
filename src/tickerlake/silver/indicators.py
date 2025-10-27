"""Technical indicator calculations for stock data."""

import polars as pl

from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def calculate_sma(df: pl.DataFrame, periods: int) -> pl.DataFrame:
    """Calculate Simple Moving Average for closing prices.

    Args:
        df: DataFrame with ticker, date, and close columns.
        periods: Number of periods for the moving average.

    Returns:
        DataFrame with added sma_{periods} column.
    """
    return df.with_columns(
        pl.col("close")
        .rolling_mean(window_size=periods)
        .over("ticker")
        .alias(f"sma_{periods}")
    )


def calculate_atr(df: pl.DataFrame, periods: int = 14) -> pl.DataFrame:
    """Calculate Average True Range.

    ATR measures volatility by calculating the average of true ranges over a period.
    True Range = max(high - low, |high - prev_close|, |low - prev_close|)

    Args:
        df: DataFrame with ticker, date, high, low, and close columns.
        periods: Number of periods for ATR calculation (default: 14).

    Returns:
        DataFrame with added atr_{periods} column.
    """
    # Calculate previous close for each ticker
    df_with_prev = df.with_columns(
        pl.col("close").shift(1).over("ticker").alias("prev_close")
    )

    # Calculate true range components
    true_range = df_with_prev.with_columns(
        pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("prev_close")).abs(),
            (pl.col("low") - pl.col("prev_close")).abs(),
        ).alias("true_range")
    )

    # Calculate ATR as rolling mean of true range
    result = true_range.with_columns(
        pl.col("true_range")
        .rolling_mean(window_size=periods)
        .over("ticker")
        .alias(f"atr_{periods}")
    ).drop(["prev_close", "true_range"])

    return result


def calculate_volume_indicators(df: pl.DataFrame, periods: int = 20) -> pl.DataFrame:
    """Calculate volume-based indicators.

    Calculates:
    - volume_ma_{periods}: Moving average of volume (includes current period)
    - volume_ratio: Current volume / volume MA

    Note: This matches TradingView's calculation method which includes the
    current bar in the moving average calculation.

    Args:
        df: DataFrame with ticker, date, and volume columns.
        periods: Number of periods for volume moving average (default: 20).

    Returns:
        DataFrame with added volume_ma_{periods} and volume_ratio columns.
    """
    # Calculate volume MA including current period (matches TradingView)
    df_with_ma = df.with_columns(
        pl.col("volume")
        .rolling_mean(window_size=periods)
        .over("ticker")
        .cast(pl.UInt64)
        .alias(f"volume_ma_{periods}")
    )

    # Calculate volume ratio
    result = df_with_ma.with_columns(
        # Volume ratio: current volume / average volume
        pl.when(pl.col(f"volume_ma_{periods}").is_not_null())
        .then(pl.col("volume") / pl.col(f"volume_ma_{periods}"))
        .otherwise(None)
        .alias("volume_ratio")
    )

    return result


def calculate_all_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate all technical indicators for stock data.

    Calculates:
    - SMA 20, 50, 200
    - ATR 14
    - Volume MA 20
    - Volume Ratio

    Args:
        df: DataFrame with ticker, date, open, high, low, close, volume columns.

    Returns:
        DataFrame with ticker, date, and all indicator columns.
    """
    # Sort by ticker and date to ensure proper rolling calculations
    df_sorted = df.sort(["ticker", "date"])

    # Calculate SMAs
    df_with_sma = df_sorted
    for periods in [20, 50, 200]:
        df_with_sma = calculate_sma(df_with_sma, periods)

    # Calculate ATR
    df_with_atr = calculate_atr(df_with_sma, periods=14)

    # Calculate volume indicators
    df_with_all = calculate_volume_indicators(df_with_atr, periods=20)

    # Select only indicator columns (plus ticker and date)
    indicators = df_with_all.select([
        "ticker",
        "date",
        "sma_20",
        "sma_50",
        "sma_200",
        "atr_14",
        "volume_ma_20",
        "volume_ratio",
    ])

    return indicators


