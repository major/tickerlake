"""Smart incremental processing logic for silver layer.

Replaces 400+ lines of watermark management code with simple checkpoint-based logic.

Strategy:
- Check for new splits since last silver update
- If splits found: Full rewrite (prices need retroactive adjustment)
- If no splits: Append new data only (fast!)
"""

import logging

import polars as pl

from tickerlake.storage import get_max_date, get_table_path, read_table, table_exists

logger = logging.getLogger(__name__)


def should_do_full_rewrite() -> bool:
    """
    Check if full rewrite is needed (new splits detected).

    Returns:
        True if full rewrite needed, False for incremental append

    Example:
        >>> if should_do_full_rewrite():
        ...     process_full_rewrite_silver()
        ... else:
        ...     process_append_silver()
    """
    # Get last silver update date
    silver_agg_table = get_table_path("silver", "daily_aggregates")

    if not table_exists(silver_agg_table):
        logger.info("📊 First run detected - will do full rewrite")
        return True

    last_silver_date = get_max_date(silver_agg_table)

    if not last_silver_date:
        logger.info("📊 No silver data found - will do full rewrite")
        return True

    # Check for splits since last silver update
    splits_table = get_table_path("bronze", "splits")

    if not table_exists(splits_table):
        logger.info("✅ No splits table - incremental append mode")
        return False

    # Use pure Polars to filter and count
    splits_df = read_table(splits_table)
    new_splits_count = len(
        splits_df.filter(pl.col("execution_date") > last_silver_date)
    )

    if new_splits_count > 0:
        logger.warning(
            f"⚠️  {new_splits_count} new split(s) detected since {last_silver_date} - will do FULL rewrite"
        )
        return True
    else:
        logger.info(f"✅ No new splits since {last_silver_date} - incremental append mode")
        return False


def get_new_stocks_data(last_silver_date: str | None = None) -> pl.DataFrame:
    """
    Fetch only new bronze stocks since last silver update.

    Args:
        last_silver_date: Last date processed in silver (None for full load)

    Returns:
        Polars DataFrame with new bronze stocks data

    Example:
        >>> last_date = get_max_date("s3://bucket/silver/daily_aggregates")
        >>> new_stocks = get_new_stocks_data(last_date)
        >>> len(new_stocks)
        50000  # Only new data since last run!
    """
    stocks_table = get_table_path("bronze", "stocks", partitioned=True)

    if not table_exists(stocks_table):
        logger.warning("⚠️  No bronze stocks table found!")
        return pl.DataFrame()

    # Load all data from partitioned dataset with pure Polars
    stocks_df = read_table(stocks_table)

    if last_silver_date:
        # Incremental: Filter to only new data
        logger.info(f"📊 Loading stocks since {last_silver_date}")
        stocks_df = stocks_df.filter(pl.col("date") > last_silver_date)
        logger.info(f"📊 Loaded {len(stocks_df)} new rows")
    else:
        # Full load: Load all data (WARNING: memory intensive!)
        logger.info("📊 Loading all stocks (full rewrite mode)")
        logger.info(f"📊 Loaded {len(stocks_df)} rows")

    # Sort by ticker and date
    stocks_df = stocks_df.sort(["ticker", "date"])

    return stocks_df


def get_stocks_for_tickers(tickers: list[str]) -> pl.DataFrame:
    """
    Load bronze stocks data for specific tickers only (memory-efficient).

    Args:
        tickers: List of ticker symbols to load

    Returns:
        Polars DataFrame with stocks data for specified tickers

    Example:
        >>> tickers = ['AAPL', 'MSFT', 'GOOGL']
        >>> stocks = get_stocks_for_tickers(tickers)
        >>> stocks['ticker'].unique().sort().to_list()
        ['AAPL', 'GOOGL', 'MSFT']
    """
    stocks_table = get_table_path("bronze", "stocks", partitioned=True)

    if not table_exists(stocks_table):
        logger.warning("⚠️  No bronze stocks table found!")
        return pl.DataFrame()

    # Use pure Polars for filtering
    stocks_df = read_table(stocks_table)
    stocks_df = stocks_df.filter(pl.col("ticker").is_in(tickers)).sort(["ticker", "date"])

    logger.debug(f"📊 Loaded {len(stocks_df)} rows for {len(tickers)} tickers")

    return stocks_df


def get_aggregates_for_tickers(timeframe: str, tickers: list[str]) -> pl.DataFrame:
    """
    Load silver aggregates for specific tickers only (memory-efficient).

    Args:
        timeframe: 'daily', 'weekly', or 'monthly'
        tickers: List of ticker symbols to load

    Returns:
        Polars DataFrame with aggregates for specified tickers

    Example:
        >>> tickers = ['AAPL', 'MSFT']
        >>> daily_aggs = get_aggregates_for_tickers('daily', tickers)
        >>> daily_aggs['ticker'].unique().sort().to_list()
        ['AAPL', 'MSFT']
    """
    agg_table = get_table_path("silver", f"{timeframe}_aggregates")

    if not table_exists(agg_table):
        logger.debug(f"⚠️  No {timeframe} aggregates table found!")
        return pl.DataFrame()

    # Use pure Polars for filtering
    aggs_df = read_table(agg_table)
    aggs_df = aggs_df.filter(pl.col("ticker").is_in(tickers)).sort(["ticker", "date"])

    logger.debug(f"📊 Loaded {len(aggs_df)} {timeframe} aggregates for {len(tickers)} tickers")

    return aggs_df


def get_all_splits() -> pl.DataFrame:
    """
    Load all stock splits from bronze layer.

    Returns:
        Polars DataFrame with all splits (small table, always load fully)

    Example:
        >>> splits = get_all_splits()
        >>> len(splits)
        500  # All historical splits
    """
    splits_table = get_table_path("bronze", "splits")

    if not table_exists(splits_table):
        logger.warning("⚠️  No splits table found - returning empty DataFrame")
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "execution_date": pl.Date,
                "split_from": pl.Float32,
                "split_to": pl.Float32,
            }
        )

    # Use pure Polars to read and sort
    splits_df = read_table(splits_table).sort(["ticker", "execution_date"])

    logger.info(f"📊 Loaded {len(splits_df)} splits")
    return splits_df


def get_filtered_tickers() -> pl.DataFrame:
    """
    Load filtered ticker metadata (CS and ETF only).

    Returns:
        Polars DataFrame with filtered tickers

    Example:
        >>> tickers = get_filtered_tickers()
        >>> tickers['type'].unique()
        ['CS', 'ETF']  # Only common stocks and ETFs
    """
    tickers_table = get_table_path("bronze", "tickers")

    if not table_exists(tickers_table):
        logger.warning("⚠️  No tickers table found!")
        return pl.DataFrame()

    # Filter to only CS (common stock) and ETF using pure Polars
    # Exclude: PFD (preferred), WARRANT, ADRC/ADRP (ADRs), ETN, etc.
    tickers_df = read_table(tickers_table)
    filtered_df = (
        tickers_df
        .filter(pl.col("type").is_in(["CS", "ETF"]))
        .select(["ticker", "name", "type", "primary_exchange", "active", "cik"])
        .sort("ticker")
    )

    logger.info(
        f"📊 Filtered {len(filtered_df)} tickers (CS and ETF only)"
    )
    return filtered_df
