"""Technical indicator calculations for stock data."""

import polars as pl
import polars_talib as plta

from tickerlake.config import settings
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


def calculate_volume_indicators(
    df: pl.DataFrame, periods: int = 20
) -> pl.DataFrame:
    """Calculate volume-based indicators.

    Calculates:
    - volume_ma_{periods}: Moving average of volume (includes current period)
    - volume_ratio: Current volume / volume MA
    - is_hvc: Boolean flag for high volume closes (volume ratio >= threshold)

    Note: This matches TradingView's calculation method which includes the
    current bar in the moving average calculation.

    Args:
        df: DataFrame with ticker, date, and volume columns.
        periods: Number of periods for volume moving average (default: 20).

    Returns:
        DataFrame with added volume_ma_{periods}, volume_ratio, and is_hvc columns.
    """
    # Calculate volume MA including current period (matches TradingView)
    df_with_ma = df.with_columns(
        pl.col("volume")
        .rolling_mean(window_size=periods)
        .over("ticker")
        .cast(pl.UInt64)
        .alias(f"volume_ma_{periods}")
    )

    # Calculate volume ratio and HVC flag
    result = df_with_ma.with_columns([
        # Volume ratio: current volume / average volume
        pl.when(pl.col(f"volume_ma_{periods}").is_not_null())
        .then(pl.col("volume") / pl.col(f"volume_ma_{periods}"))
        .otherwise(None)
        .alias("volume_ratio"),
    ]).with_columns([
        # High Volume Close flag
        pl.when(pl.col("volume_ratio").is_not_null())
        .then(pl.col("volume_ratio") >= settings.hvc_volume_ratio_threshold)
        .otherwise(False)
        .alias("is_hvc")
    ])

    return result


def calculate_all_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate all technical indicators for stock data.

    Calculates:
    - SMA 20, 50, 200
    - ATR 14
    - Volume MA 20
    - Volume Ratio
    - High Volume Close flag

    Args:
        df: DataFrame with ticker, date, open, high, low, close, volume columns.

    Returns:
        DataFrame with ticker, date, and all indicator columns.
    """
    logger.info("Calculating all technical indicators...")

    # Sort by ticker and date to ensure proper rolling calculations
    df_sorted = df.sort(["ticker", "date"])

    # Calculate SMAs
    logger.info("Calculating SMAs (20, 50, 200)...")
    df_with_sma = df_sorted
    for periods in [20, 50, 200]:
        df_with_sma = calculate_sma(df_with_sma, periods)

    # Calculate ATR
    logger.info("Calculating ATR (14)...")
    df_with_atr = calculate_atr(df_with_sma, periods=14)

    # Calculate volume indicators
    logger.info("Calculating volume indicators (MA 20, ratio, HVC flag)...")
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
        "is_hvc",
    ])

    logger.info(f"Calculated indicators for {indicators.height:,} rows")
    return indicators


def calculate_weinstein_stage(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate Weinstein Stage Analysis (stages 1-4) for weekly stock data.

    Weinstein's Stage Analysis classifies stocks into 4 stages based on price
    position relative to a 30-week moving average and the MA's slope:

    - Stage 1 (Basing): Price below MA, MA flat or rising
    - Stage 2 (Advancing): Price >2% above MA, MA rising >0.5%
    - Stage 3 (Topping): Price above MA, MA flat or falling
    - Stage 4 (Declining): Price <2% below MA, MA falling >0.5%

    Anti-whipsaw logic: Stages must persist for minimum 2 weeks before
    confirming a stage change. Otherwise, the previous confirmed stage is held.

    Args:
        df: DataFrame with ticker, date, and close columns.

    Returns:
        DataFrame with added Weinstein stage columns:
        - ma_30: 30-period simple moving average
        - price_vs_ma_pct: Price distance from MA as percentage
        - ma_slope_pct: MA slope over 4 periods as percentage
        - raw_stage: Raw stage before anti-whipsaw filter
        - stage: Final confirmed stage (1-4, guaranteed no nulls)
        - stage_changed: Boolean flag for stage transitions
        - weeks_in_stage: Counter for weeks in current stage
    """
    logger.info("Calculating Weinstein stage analysis...")

    # Sort by ticker and date for proper time-series calculations
    df_sorted = df.sort(["ticker", "date"])

    # Calculate 30-period MA using polars-talib
    logger.info("Calculating 30-period moving average...")
    df_with_ma = df_sorted.with_columns([
        pl.col("close").ta.sma(timeperiod=30).over("ticker").alias("ma_30")
    ])

    # Calculate price distance from MA as percentage
    df_with_pct = df_with_ma.with_columns([
        pl.when(pl.col("ma_30").is_not_null())
        .then(((pl.col("close") - pl.col("ma_30")) / pl.col("ma_30") * 100.0))
        .otherwise(None)
        .alias("price_vs_ma_pct")
    ])

    # Calculate MA slope over 4 periods as percentage
    logger.info("Calculating MA slope over 4 periods...")
    df_with_slope = df_with_pct.with_columns([
        pl.when(
            (pl.col("ma_30").is_not_null()) &
            (pl.col("ma_30").shift(4).over("ticker").is_not_null())
        )
        .then(
            ((pl.col("ma_30") - pl.col("ma_30").shift(4).over("ticker")) /
             pl.col("ma_30").shift(4).over("ticker") * 100.0)
        )
        .otherwise(None)
        .alias("ma_slope_pct")
    ])

    # Classify raw stage based on Weinstein rules
    logger.info("Classifying Weinstein stages...")
    df_with_raw_stage = _classify_raw_stage(df_with_slope)

    # Apply anti-whipsaw logic per ticker
    logger.info("Applying anti-whipsaw filter (2-week minimum)...")
    df_with_stage = df_with_raw_stage.group_by("ticker").map_groups(
        _apply_anti_whipsaw
    )

    # Calculate stage change flag and weeks in stage
    df_final = df_with_stage.with_columns([
        # Stage changed flag
        (pl.col("stage") != pl.col("stage").shift(1).over("ticker"))
        .fill_null(True)  # First row is always a "change"
        .alias("stage_changed")
    ])

    # Calculate weeks in stage using run-length encoding per ticker
    df_final = (
        df_final.group_by("ticker", maintain_order=True)
        .map_groups(lambda ticker_df: ticker_df.with_columns([
            pl.col("stage").rle_id().alias("_stage_group_id")
        ]).with_columns([
            pl.col("_stage_group_id")
            .cum_count()
            .over("_stage_group_id")
            .alias("weeks_in_stage")
        ]).drop("_stage_group_id"))
    )

    # Log stage distribution
    stage_counts = df_final.group_by("stage").agg(pl.count().alias("count"))
    logger.info(f"Stage distribution: {stage_counts.to_dicts()}")

    logger.info(f"Completed Weinstein stage analysis for {df_final.height:,} rows")
    return df_final


def _classify_raw_stage(df: pl.DataFrame) -> pl.DataFrame:
    """Classify Weinstein stages based on price vs MA and MA slope.

    Classification rules:
    - Stage 2: Price >2% above MA AND MA rising >0.5%
    - Stage 4: Price <2% below MA AND MA falling >0.5%
    - Stage 1: Price below MA AND (MA flat or rising)
    - Stage 3: Price above MA AND (MA flat or falling)

    Args:
        df: DataFrame with price_vs_ma_pct and ma_slope_pct columns.

    Returns:
        DataFrame with added raw_stage column.
    """
    df_with_stage = df.with_columns([
        pl.when(
            # Stage 2: Price >2% above MA AND MA rising >0.5%
            (pl.col("price_vs_ma_pct") > 2.0) &
            (pl.col("ma_slope_pct") > 0.5)
        )
        .then(pl.lit(2, dtype=pl.UInt8))
        .when(
            # Stage 4: Price <-2% below MA AND MA falling <-0.5%
            (pl.col("price_vs_ma_pct") < -2.0) &
            (pl.col("ma_slope_pct") < -0.5)
        )
        .then(pl.lit(4, dtype=pl.UInt8))
        .when(
            # Stage 1: Price below MA (but not <-2% with falling MA)
            pl.col("price_vs_ma_pct") < 0.0
        )
        .then(pl.lit(1, dtype=pl.UInt8))
        .when(
            # Stage 3: Price above MA (but not >2% with rising MA)
            pl.col("price_vs_ma_pct") >= 0.0
        )
        .then(pl.lit(3, dtype=pl.UInt8))
        .otherwise(None)
        .alias("raw_stage")
    ])

    return df_with_stage


def _apply_anti_whipsaw(ticker_df: pl.DataFrame) -> pl.DataFrame:
    """Apply anti-whipsaw logic to stage classifications.

    Uses hysteresis: only confirm stage change after it persists for 2+ weeks.
    If a new stage doesn't persist, hold the previous confirmed stage.

    This prevents rapid whipsawing between stages due to short-term noise.

    Args:
        ticker_df: DataFrame for a single ticker with raw_stage column.

    Returns:
        DataFrame with added stage column (confirmed stage after anti-whipsaw).
    """
    # Use run-length encoding to find consecutive runs of the same raw stage
    df_with_rle = ticker_df.with_columns([
        pl.col("raw_stage").rle_id().alias("_rle_id")
    ])

    # Count the length of each run
    df_with_run_length = df_with_rle.with_columns([
        pl.col("_rle_id")
        .cum_count()
        .over("_rle_id")
        .alias("_run_length")
    ])

    # Only confirm stage if run length >= 2, otherwise use previous confirmed stage
    confirmed_stages = []
    last_confirmed_stage = None

    for row in df_with_run_length.iter_rows(named=True):
        raw_stage = row["raw_stage"]
        run_length = row["_run_length"]

        # If we don't have a raw stage yet (early in data), wait
        if raw_stage is None:
            if last_confirmed_stage is None:
                # Default to Stage 1 if we have no data yet
                confirmed_stages.append(1)
                last_confirmed_stage = 1
            else:
                confirmed_stages.append(last_confirmed_stage)
            continue

        # If this is a new stage and hasn't persisted for 2 weeks yet
        if run_length < 2:
            if last_confirmed_stage is None:
                # First stage seen, use it
                confirmed_stages.append(raw_stage)
                last_confirmed_stage = raw_stage
            else:
                # Hold previous confirmed stage
                confirmed_stages.append(last_confirmed_stage)
        else:
            # Stage has persisted for 2+ weeks, confirm it
            confirmed_stages.append(raw_stage)
            last_confirmed_stage = raw_stage

    # Add confirmed stage column
    result = df_with_run_length.with_columns([
        pl.Series("stage", confirmed_stages, dtype=pl.UInt8)
    ]).drop(["_rle_id", "_run_length"])

    return result
