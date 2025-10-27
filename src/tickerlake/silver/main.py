"""Silver layer processing to adjust historical stock data for splits. 🥈"""

import polars as pl
from sqlalchemy import select

from tickerlake.bronze.models import splits as bronze_splits
from tickerlake.bronze.models import stocks as bronze_stocks
from tickerlake.bronze.models import tickers as bronze_tickers
from tickerlake.db import bulk_load, get_engine
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import validate_daily_aggregates, validate_indicators
from tickerlake.silver.aggregates import aggregate_to_monthly, aggregate_to_weekly
from tickerlake.silver.indicators import calculate_all_indicators
from tickerlake.silver.models import (
    daily_aggregates as daily_aggregates_table,
    daily_indicators as daily_indicators_table,
    monthly_aggregates as monthly_aggregates_table,
    monthly_indicators as monthly_indicators_table,
    ticker_metadata as ticker_metadata_table,
    weekly_aggregates as weekly_aggregates_table,
    weekly_indicators as weekly_indicators_table,
)
from tickerlake.silver.postgres import clear_all_tables, init_silver_schema

setup_logging()
logger = get_logger(__name__)

pl.Config.set_verbose(False)


def read_splits() -> pl.DataFrame:
    """Read splits data from bronze Postgres layer."""
    logger.info("📥 Reading splits from bronze Postgres...")
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(select(bronze_splits))
        df = pl.DataFrame(
            result.fetchall(),
            schema=["ticker", "execution_date", "split_from", "split_to"],
            orient="row"
        )

    logger.info(f"✅ Loaded {len(df):,} splits")
    return df


def read_tickers() -> pl.DataFrame:
    """Read tickers data from bronze Postgres layer."""
    logger.info("📥 Reading tickers from bronze Postgres...")
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(select(bronze_tickers))
        # Map column names from bronze to expected schema
        # Note: bronze table has 'ticker_type' but we rename to 'type' for processing
        rows = result.fetchall()
        df = pl.DataFrame(
            rows,
            schema=[
                "ticker", "name", "ticker_type", "active", "locale", "market",
                "primary_exchange", "currency_name", "currency_symbol", "cik",
                "composite_figi", "share_class_figi", "base_currency_name",
                "base_currency_symbol", "delisted_utc", "last_updated_utc"
            ],
            orient="row"
        ).rename({"ticker_type": "type"})

    logger.info(f"✅ Loaded {len(df):,} tickers")
    return df


def read_stocks(ticker_list: list[str] | None = None) -> pl.DataFrame:
    """Read stock data from bronze Postgres layer.

    Args:
        ticker_list: Optional list of tickers to filter. If None, reads all stocks.

    Returns:
        DataFrame with stock data for the specified tickers.
    """
    if ticker_list is not None:
        logger.info(f"📥 Reading stocks from bronze Postgres for {len(ticker_list):,} tickers...")
    else:
        logger.info("📥 Reading stocks from bronze Postgres...")

    engine = get_engine()

    with engine.connect() as conn:
        # Build query with optional ticker filter
        query = select(bronze_stocks)
        if ticker_list is not None:
            query = query.where(bronze_stocks.c.ticker.in_(ticker_list))

        result = conn.execute(query)
        df = pl.DataFrame(
            result.fetchall(),
            schema=["ticker", "date", "open", "high", "low", "close", "volume", "transactions"],
            orient="row"
        )

    logger.info(f"✅ Loaded {len(df):,} stock records")
    return df


def apply_splits(
    stocks_df: pl.DataFrame,
    splits_df: pl.DataFrame,
) -> pl.DataFrame:
    """Apply split adjustments to stock data.

    For each stock date, multiplies prices by the ratio (split_from/split_to) for
    all splits that occurred AFTER that date. This adjusts historical prices to
    match current split-adjusted prices.

    Args:
        stocks_df: DataFrame with stock data.
        splits_df: DataFrame with splits data.

    Returns:
        DataFrame with split-adjusted OHLCV data.
    """
    logger.info("⚙️  Applying split adjustments...")

    # Join stocks with all splits for that ticker
    # Then calculate adjustment factor: for each date, apply split_from/split_to
    # if the split occurred AFTER that date
    adjusted_df = (
        stocks_df.lazy()
        .join(
            splits_df.select(["ticker", "execution_date", "split_from", "split_to"]).lazy(),
            on="ticker",
            how="left",
        )
        # Calculate adjustment factor for each split
        .with_columns([
            pl.when(pl.col("date") < pl.col("execution_date"))
            .then(pl.col("split_from") / pl.col("split_to"))
            .otherwise(1.0)
            .alias("adjustment_factor")
        ])
        # Calculate total adjustment by multiplying all factors for this ticker+date
        .group_by(["ticker", "date"])
        .agg([
            pl.col("adjustment_factor").product().alias("total_adjustment"),
            pl.col("open").first(),
            pl.col("high").first(),
            pl.col("low").first(),
            pl.col("close").first(),
            pl.col("volume").first(),
            pl.col("transactions").first(),
        ])
        # Apply adjustments
        .with_columns([
            (pl.col("open") * pl.col("total_adjustment")).alias("open"),
            (pl.col("high") * pl.col("total_adjustment")).alias("high"),
            (pl.col("low") * pl.col("total_adjustment")).alias("low"),
            (pl.col("close") * pl.col("total_adjustment")).alias("close"),
            (pl.col("volume") / pl.col("total_adjustment"))
            .cast(pl.UInt64)
            .alias("volume"),
            (pl.col("transactions") / pl.col("total_adjustment"))
            .cast(pl.UInt64)
            .alias("transactions"),
        ])
        .drop("total_adjustment")
        .select(["ticker", "date", "open", "high", "low", "close", "volume", "transactions"])
        .collect()
    )

    logger.info(f"✅ Applied split adjustments to {len(adjusted_df):,} records")
    return adjusted_df


def create_ticker_metadata(tickers_df: pl.DataFrame) -> pl.DataFrame:
    """Create ticker metadata dimension table in silver layer.

    Args:
        tickers_df: DataFrame with ticker data from bronze layer.

    Returns:
        DataFrame with ticker metadata for tradeable securities (CS, ETF, PFD, WARRANT, ADRC, ADRP, ETN).
    """
    logger.info("📊 Creating ticker metadata dimension table...")

    # Debug: Show what types we have and the dtype
    if len(tickers_df) > 0:
        logger.info(f"🔍 Type column dtype: {tickers_df['type'].dtype}")
        # Cast to string to ensure consistent comparison
        type_counts = (
            tickers_df
            .with_columns(pl.col("type").cast(pl.Utf8))
            .group_by("type")
            .agg(pl.count())
            .sort("count", descending=True)
        )
        logger.info(f"📋 Ticker types found:\n{type_counts.head(10)}")

    # Filter to tradeable securities - cast type to string for reliable filtering
    # (categorical columns from Postgres might have different encoding)
    # Include: CS (Common Stock), ETF, PFD (Preferred), WARRANT, ADRC/ADRP (ADRs), ETN
    allowed_types = ["CS", "ETF", "PFD", "WARRANT", "ADRC", "ADRP", "ETN"]
    ticker_metadata = (
        tickers_df
        .with_columns(pl.col("type").cast(pl.Utf8))
        .filter(pl.col("type").is_in(allowed_types))
        .select([
            "ticker",
            "name",
            "type",
            "primary_exchange",
            "active",
            "cik",
        ])
    )

    logger.info(f"✅ Filtered to {len(ticker_metadata):,} tradeable tickers ({', '.join(allowed_types)})")

    # Write to silver Postgres 🚀
    bulk_load(ticker_metadata_table, ticker_metadata)

    return ticker_metadata


def process_ticker_batch(
    ticker_batch: list[str],
    splits_df: pl.DataFrame,
    batch_num: int,
    total_batches: int,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Process a batch of tickers through the silver pipeline.

    Args:
        ticker_batch: List of ticker symbols to process in this batch.
        splits_df: DataFrame with all splits data (filtered to tradeable securities).
        batch_num: Current batch number (1-indexed for logging).
        total_batches: Total number of batches (for progress logging).

    Returns:
        Tuple of (daily_df, weekly_df, monthly_df) for this batch.
    """
    logger.info(
        f"🔄 Processing batch {batch_num}/{total_batches} "
        f"({len(ticker_batch):,} tickers)..."
    )

    # Read stocks for this batch only 📊
    stocks_df = read_stocks(ticker_list=ticker_batch)

    if len(stocks_df) == 0:
        logger.warning(f"⚠️  No stock data found for batch {batch_num}")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

    # Apply split adjustments to create daily aggregates
    daily_df = apply_splits(stocks_df=stocks_df, splits_df=splits_df)
    daily_df = validate_daily_aggregates(daily_df)

    # Create weekly and monthly aggregates
    weekly_df = aggregate_to_weekly(daily_df)
    weekly_df = validate_daily_aggregates(weekly_df)

    monthly_df = aggregate_to_monthly(daily_df)
    monthly_df = validate_daily_aggregates(monthly_df)

    logger.info(
        f"✅ Batch {batch_num}/{total_batches} complete: "
        f"{len(daily_df):,} daily, {len(weekly_df):,} weekly, "
        f"{len(monthly_df):,} monthly records"
    )

    return daily_df, weekly_df, monthly_df


def main(batch_size: int = 250) -> None:  # pragma: no cover
    """Main function to adjust historical stock data for splits using batch processing.

    Args:
        batch_size: Number of tickers to process in each batch (default: 250).
                   Lower values use less RAM but take slightly longer.
    """
    logger.info("🚀 Starting silver layer processing (memory-efficient batch mode)...")
    logger.info(f"⚙️  Batch size: {batch_size:,} tickers per batch")

    # Initialize silver schema
    init_silver_schema()

    # Clear all existing data for full rebuild
    clear_all_tables()

    # Read tickers and filter to tradeable securities
    tickers_df = read_tickers()

    # Create ticker metadata dimension table
    ticker_metadata = create_ticker_metadata(tickers_df)

    valid_tickers_df = ticker_metadata.select("ticker")
    valid_ticker_list = valid_tickers_df["ticker"].to_list()

    logger.info(
        f"🎯 Processing {len(valid_ticker_list):,} tradeable securities "
        f"(from {len(tickers_df):,} total tickers)"
    )

    # Read splits (small dataset, fits in memory easily)
    splits_df = read_splits()

    # Filter splits to only tradeable securities
    original_splits_count = len(splits_df)
    splits_df = splits_df.join(valid_tickers_df, on="ticker", how="semi")
    logger.info(
        f"✂️  Filtered splits to {len(splits_df):,} splits for tradeable securities "
        f"(from {original_splits_count:,} total splits)"
    )

    # Process stocks in batches to avoid loading millions of rows into RAM 🧠
    total_batches = (len(valid_ticker_list) + batch_size - 1) // batch_size
    logger.info(f"📦 Splitting into {total_batches:,} batches of ~{batch_size:,} tickers each")

    # Accumulate aggregates for indicator calculation
    all_daily_dfs = []
    all_weekly_dfs = []
    all_monthly_dfs = []

    for i in range(0, len(valid_ticker_list), batch_size):
        batch_num = (i // batch_size) + 1
        ticker_batch = valid_ticker_list[i : i + batch_size]

        daily_df, weekly_df, monthly_df = process_ticker_batch(
            ticker_batch=ticker_batch,
            splits_df=splits_df,
            batch_num=batch_num,
            total_batches=total_batches,
        )

        # Write aggregates immediately to Postgres (frees memory) 💾
        if len(daily_df) > 0:
            bulk_load(daily_aggregates_table, daily_df)
            all_daily_dfs.append(daily_df)

        if len(weekly_df) > 0:
            bulk_load(weekly_aggregates_table, weekly_df)
            all_weekly_dfs.append(weekly_df)

        if len(monthly_df) > 0:
            bulk_load(monthly_aggregates_table, monthly_df)
            all_monthly_dfs.append(monthly_df)

    logger.info("✅ All batches processed! Now calculating technical indicators...")

    # Calculate indicators on concatenated data (still memory-efficient per timeframe)
    # Daily indicators
    if all_daily_dfs:
        logger.info("📈 Calculating daily technical indicators...")
        daily_combined = pl.concat(all_daily_dfs, how="vertical")
        daily_indicators = calculate_all_indicators(daily_combined)
        daily_indicators = validate_indicators(daily_indicators)
        bulk_load(daily_indicators_table, daily_indicators)
        del daily_combined, daily_indicators, all_daily_dfs  # Free memory 🗑️

    # Weekly indicators
    if all_weekly_dfs:
        logger.info("📊 Calculating weekly technical indicators...")
        weekly_combined = pl.concat(all_weekly_dfs, how="vertical")
        weekly_indicators = calculate_all_indicators(weekly_combined)
        weekly_indicators = validate_indicators(weekly_indicators)
        bulk_load(weekly_indicators_table, weekly_indicators)
        del weekly_combined, weekly_indicators, all_weekly_dfs  # Free memory 🗑️

    # Monthly indicators
    if all_monthly_dfs:
        logger.info("📊 Calculating monthly technical indicators...")
        monthly_combined = pl.concat(all_monthly_dfs, how="vertical")
        monthly_indicators = calculate_all_indicators(monthly_combined)
        monthly_indicators = validate_indicators(monthly_indicators)
        bulk_load(monthly_indicators_table, monthly_indicators)
        del monthly_combined, monthly_indicators, all_monthly_dfs  # Free memory 🗑️

    logger.info("✅ Silver layer processing complete! 🎉")


if __name__ == "__main__":  # pragma: no cover
    main()
