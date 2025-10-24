"""Silver layer processing to adjust historical stock data for splits."""

import polars as pl
from sqlalchemy import select

from tickerlake.bronze.models import splits as bronze_splits
from tickerlake.bronze.models import stocks as bronze_stocks
from tickerlake.bronze.models import tickers as bronze_tickers
from tickerlake.bronze.postgres import get_engine as get_bronze_engine
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import validate_daily_aggregates, validate_indicators
from tickerlake.silver.aggregates import aggregate_to_monthly, aggregate_to_weekly
from tickerlake.silver.indicators import (
    calculate_all_indicators,
    calculate_weinstein_stage,
)
from tickerlake.silver.postgres import (
    bulk_load_daily_aggregates,
    bulk_load_daily_indicators,
    bulk_load_monthly_aggregates,
    bulk_load_monthly_indicators,
    bulk_load_ticker_metadata,
    bulk_load_weekly_aggregates,
    bulk_load_weekly_indicators,
    clear_all_tables,
    init_schema,
)

setup_logging()
logger = get_logger(__name__)

pl.Config.set_verbose(False)


def read_splits() -> pl.DataFrame:
    """Read splits data from bronze Postgres layer."""
    logger.info("📥 Reading splits from bronze Postgres...")
    engine = get_bronze_engine()

    with engine.connect() as conn:
        result = conn.execute(select(bronze_splits))
        df = pl.DataFrame(result.fetchall(), schema=["ticker", "execution_date", "split_from", "split_to"])

    logger.info(f"✅ Loaded {len(df):,} splits")
    return df


def read_tickers() -> pl.DataFrame:
    """Read tickers data from bronze Postgres layer."""
    logger.info("📥 Reading tickers from bronze Postgres...")
    engine = get_bronze_engine()

    with engine.connect() as conn:
        result = conn.execute(select(bronze_tickers))
        # Map column names from bronze to expected schema
        rows = result.fetchall()
        df = pl.DataFrame(
            rows,
            schema=[
                "ticker", "name", "type", "active", "locale", "market",
                "primary_exchange", "currency_name", "currency_symbol", "cik",
                "composite_figi", "share_class_figi", "base_currency_name",
                "base_currency_symbol", "delisted_utc", "last_updated_utc"
            ]
        )

    logger.info(f"✅ Loaded {len(df):,} tickers")
    return df


def read_stocks() -> pl.DataFrame:
    """Read stock data from bronze Postgres layer."""
    logger.info("📥 Reading stocks from bronze Postgres...")
    engine = get_bronze_engine()

    with engine.connect() as conn:
        result = conn.execute(select(bronze_stocks))
        df = pl.DataFrame(
            result.fetchall(),
            schema=["ticker", "date", "open", "high", "low", "close", "volume", "transactions"]
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
        DataFrame with ticker metadata for CS/ETF tickers.
    """
    logger.info("📊 Creating ticker metadata dimension table...")

    # Filter to CS/ETF and select relevant columns
    ticker_metadata = tickers_df.filter(
        pl.col("type").is_in(["CS", "ETF"])
    ).select([
        "ticker",
        "name",
        "type",
        "primary_exchange",
        "active",
        "cik",
    ])

    # Write to silver Postgres
    bulk_load_ticker_metadata(ticker_metadata)

    return ticker_metadata


def main() -> None:  # pragma: no cover
    """Main function to adjust historical stock data for splits."""
    logger.info("🚀 Starting silver layer processing...")

    # Initialize silver schema
    init_schema()

    # Clear all existing data for full rebuild
    clear_all_tables()

    # Read tickers and filter to only CS (Common Stock) and ETF types
    tickers_df = read_tickers()

    # Create ticker metadata dimension table
    ticker_metadata = create_ticker_metadata(tickers_df)

    valid_tickers = ticker_metadata.select("ticker")

    logger.info(
        f"🎯 Filtering to {len(valid_tickers):,} tickers with type CS or ETF "
        f"(from {len(tickers_df):,} total tickers)"
    )

    # Read splits and stocks
    splits_df = read_splits()

    # Filter splits to only CS/ETF tickers to avoid applying incorrect splits
    # from warrants, delisted tickers, or other instrument types
    original_splits_count = len(splits_df)
    splits_df = splits_df.join(valid_tickers, on="ticker", how="semi")
    logger.info(
        f"✂️  Filtered splits to {len(splits_df):,} splits for CS/ETF tickers "
        f"(from {original_splits_count:,} total splits)"
    )

    stocks_df = read_stocks()

    # Filter stocks to only valid ticker types using semi-join
    stocks_df = stocks_df.join(valid_tickers, on="ticker", how="semi")
    logger.info(f"📊 Filtered to {len(stocks_df):,} stock records for CS/ETF tickers")

    # Apply split adjustments to create daily aggregates
    daily_df = apply_splits(stocks_df=stocks_df, splits_df=splits_df)
    daily_df = validate_daily_aggregates(daily_df)

    # Write daily aggregates to Postgres
    bulk_load_daily_aggregates(daily_df)

    # Calculate and write daily indicators
    logger.info("📈 Calculating daily technical indicators...")
    daily_indicators = calculate_all_indicators(daily_df)
    daily_indicators = validate_indicators(daily_indicators)
    bulk_load_daily_indicators(daily_indicators)

    # Create weekly aggregates
    logger.info("📅 Creating weekly aggregates...")
    weekly_df = aggregate_to_weekly(daily_df)
    weekly_df = validate_daily_aggregates(weekly_df)
    bulk_load_weekly_aggregates(weekly_df)

    # Calculate and write weekly indicators
    logger.info("📊 Calculating weekly technical indicators...")
    weekly_indicators = calculate_all_indicators(weekly_df)

    # Add Weinstein Stage Analysis for weekly data (needs close prices)
    logger.info("🎯 Adding Weinstein Stage Analysis to weekly indicators...")
    # Join close prices back for Weinstein calculation
    weekly_with_close = weekly_df.select(["ticker", "date", "close", "volume"]).join(
        weekly_indicators, on=["ticker", "date"], how="inner"
    )
    weekly_with_stages = calculate_weinstein_stage(weekly_with_close)

    # Drop the close and volume columns (they're in aggregates already)
    weekly_indicators = weekly_with_stages.drop(["close", "volume"])

    weekly_indicators = validate_indicators(weekly_indicators, include_stages=True)
    bulk_load_weekly_indicators(weekly_indicators)

    # Create monthly aggregates
    logger.info("📆 Creating monthly aggregates...")
    monthly_df = aggregate_to_monthly(daily_df)
    monthly_df = validate_daily_aggregates(monthly_df)
    bulk_load_monthly_aggregates(monthly_df)

    # Calculate and write monthly indicators
    logger.info("📊 Calculating monthly technical indicators...")
    monthly_indicators = calculate_all_indicators(monthly_df)
    monthly_indicators = validate_indicators(monthly_indicators)
    bulk_load_monthly_indicators(monthly_indicators)

    logger.info("✅ Silver layer processing complete! 🎉")


if __name__ == "__main__":  # pragma: no cover
    main()
