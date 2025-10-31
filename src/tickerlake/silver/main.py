"""Silver layer processing - split-adjusted OHLCV with technical indicators. 🥈

Major simplification from PostgreSQL version:
- 981 lines → ~250 lines (74% reduction!)
- Removed 400+ lines of watermark tracking complexity
- Smart incremental logic via checkpoints
- Parquet files for storage (no database!)
"""

import argparse
from datetime import date

import polars as pl

from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import validate_daily_aggregates, validate_indicators
from tickerlake.silver.aggregates import aggregate_to_monthly, aggregate_to_weekly
from tickerlake.silver.incremental import (
    get_aggregates_for_tickers,
    get_all_splits,
    get_filtered_tickers,
    get_new_stocks_data,
    get_stocks_for_tickers,
    should_do_full_rewrite,
)
from tickerlake.silver.indicators import calculate_all_indicators
from tickerlake.silver.splits import apply_splits
from tickerlake.storage import (
    get_max_date,
    get_table_path,
    load_checkpoints,
    save_checkpoints,
    write_table,
)
from tickerlake.utils.batch_processing import batch_generator

setup_logging()
logger = get_logger(__name__)

pl.Config.set_verbose(False)


def process_append_silver(batch_size: int = 250, indicator_batch_size: int = 500) -> None:
    """
    Append only new data (daily fast path).

    This runs when no new splits have been detected - we can just append
    new data without reprocessing history.

    Args:
        batch_size: Number of tickers to process per batch (aggregation)
        indicator_batch_size: Number of tickers per batch (indicators)
    """
    logger.info("⚡ Starting incremental append mode")

    # Get last silver date
    silver_agg_table = get_table_path("silver", "daily_aggregates")
    last_date = get_max_date(silver_agg_table)

    # Load new stocks data
    stocks = get_new_stocks_data(last_date)

    if len(stocks) == 0:
        logger.info("✅ No new data to process")
        return

    logger.info(f"📊 Processing {len(stocks)} new rows")

    # Load splits (full table - it's small)
    splits = get_all_splits()

    # Apply split adjustments
    adjusted = apply_splits(stocks, splits)

    # Calculate aggregates
    daily_aggs = validate_daily_aggregates(adjusted)
    weekly_aggs = aggregate_to_weekly(adjusted)
    monthly_aggs = aggregate_to_monthly(adjusted)

    # Write aggregates to Parquet
    write_table(get_table_path("silver", "daily_aggregates"), daily_aggs, mode="append")
    write_table(get_table_path("silver", "weekly_aggregates"), weekly_aggs, mode="append")
    write_table(get_table_path("silver", "monthly_aggregates"), monthly_aggs, mode="append")

    logger.info(f"✅ Appended {len(daily_aggs)} daily, {len(weekly_aggs)} weekly, {len(monthly_aggs)} monthly aggregates")

    # Phase 2: Calculate indicators in batches
    logger.info("📊 Calculating indicators...")

    # Get unique tickers from new aggregates
    tickers = daily_aggs["ticker"].unique().to_list()

    # Process indicators in batches
    all_daily_indicators = []
    all_weekly_indicators = []
    all_monthly_indicators = []

    for batch_num, ticker_batch in enumerate(batch_generator(tickers, indicator_batch_size), 1):
        logger.info(f"📊 Processing indicator batch {batch_num} ({len(ticker_batch)} tickers)")

        # Filter aggregates for this batch
        batch_daily = daily_aggs.filter(pl.col("ticker").is_in(ticker_batch))
        batch_weekly = weekly_aggs.filter(pl.col("ticker").is_in(ticker_batch))
        batch_monthly = monthly_aggs.filter(pl.col("ticker").is_in(ticker_batch))

        # Calculate indicators
        daily_inds = calculate_all_indicators(batch_daily)
        weekly_inds = calculate_all_indicators(batch_weekly)
        monthly_inds = calculate_all_indicators(batch_monthly)

        # Validate schemas
        daily_inds = validate_indicators(daily_inds)
        weekly_inds = validate_indicators(weekly_inds)
        monthly_inds = validate_indicators(monthly_inds)

        all_daily_indicators.append(daily_inds)
        all_weekly_indicators.append(weekly_inds)
        all_monthly_indicators.append(monthly_inds)

    # Combine all batches
    combined_daily_inds = pl.concat(all_daily_indicators)
    combined_weekly_inds = pl.concat(all_weekly_indicators)
    combined_monthly_inds = pl.concat(all_monthly_indicators)

    # Write indicators to Parquet
    write_table(get_table_path("silver", "daily_indicators"), combined_daily_inds, mode="append")
    write_table(get_table_path("silver", "weekly_indicators"), combined_weekly_inds, mode="append")
    write_table(get_table_path("silver", "monthly_indicators"), combined_monthly_inds, mode="append")

    logger.info(f"✅ Appended {len(combined_daily_inds)} daily, {len(combined_weekly_inds)} weekly, {len(combined_monthly_inds)} monthly indicators")


def process_full_rewrite_silver(batch_size: int = 250, indicator_batch_size: int = 500) -> None:
    """
    Full reprocess (when splits detected) - MEMORY EFFICIENT VERSION.

    When new splits are detected, we need to reprocess all historical data
    because prices need retroactive adjustment.

    This version loads data in batches to avoid memory issues:
    - Phase 1: Load stocks by ticker batch → process → write immediately
    - Phase 2: Load aggregates by ticker batch → calculate indicators → write

    Args:
        batch_size: Number of tickers to process per batch (aggregation)
        indicator_batch_size: Number of tickers per batch (indicators)
    """
    logger.warning("🔄 Starting FULL REWRITE mode (splits detected)")

    # Get list of tickers to process (small query, just ticker list)
    filtered_tickers = get_filtered_tickers()
    if len(filtered_tickers) == 0:
        logger.warning("⚠️  No tickers found!")
        return

    all_tickers = filtered_tickers["ticker"].to_list()
    logger.info(f"📊 Processing {len(all_tickers)} tickers in batches")

    # Load splits once (small table, can keep in memory)
    splits = get_all_splits()

    # Phase 1: Process aggregates in batches (stream-like processing)
    logger.info(f"📊 Phase 1: Processing aggregates in batches of {batch_size} tickers")

    total_batches = (len(all_tickers) + batch_size - 1) // batch_size
    for batch_num, ticker_batch in enumerate(batch_generator(all_tickers, batch_size), 1):
        logger.info(f"📊 Aggregation batch {batch_num}/{total_batches} ({len(ticker_batch)} tickers)")

        # Load ONLY this batch's stocks (S3 filter pushdown!)
        batch_stocks = get_stocks_for_tickers(ticker_batch)

        if len(batch_stocks) == 0:
            logger.warning(f"⚠️  No stocks data for batch {batch_num}")
            continue

        # Apply splits
        adjusted = apply_splits(batch_stocks, splits)

        # Calculate aggregates
        daily_aggs = validate_daily_aggregates(adjusted)
        weekly_aggs = aggregate_to_weekly(adjusted)
        monthly_aggs = aggregate_to_monthly(adjusted)

        # Write immediately (overwrite first batch, append rest)
        write_mode = "overwrite" if batch_num == 1 else "append"
        write_table(get_table_path("silver", "daily_aggregates"), daily_aggs, mode=write_mode)
        write_table(get_table_path("silver", "weekly_aggregates"), weekly_aggs, mode=write_mode)
        write_table(get_table_path("silver", "monthly_aggregates"), monthly_aggs, mode=write_mode)

        logger.info(f"✅ Wrote {len(daily_aggs)} daily, {len(weekly_aggs)} weekly, {len(monthly_aggs)} monthly aggregates")

        # Memory cleanup happens automatically when variables go out of scope

    logger.info("✅ Phase 1 complete - all aggregates written to Parquet")

    # Phase 2: Calculate indicators in batches (read from Parquet)
    logger.info(f"📊 Phase 2: Calculating indicators in batches of {indicator_batch_size} tickers")

    total_batches = (len(all_tickers) + indicator_batch_size - 1) // indicator_batch_size
    for batch_num, ticker_batch in enumerate(batch_generator(all_tickers, indicator_batch_size), 1):
        logger.info(f"📊 Indicator batch {batch_num}/{total_batches} ({len(ticker_batch)} tickers)")

        # Load aggregates from Parquet for just this batch
        batch_daily = get_aggregates_for_tickers("daily", ticker_batch)
        batch_weekly = get_aggregates_for_tickers("weekly", ticker_batch)
        batch_monthly = get_aggregates_for_tickers("monthly", ticker_batch)

        if len(batch_daily) == 0:
            logger.warning(f"⚠️  No aggregates for batch {batch_num}")
            continue

        # Calculate indicators
        daily_inds = calculate_all_indicators(batch_daily)
        weekly_inds = calculate_all_indicators(batch_weekly)
        monthly_inds = calculate_all_indicators(batch_monthly)

        # Validate schemas
        daily_inds = validate_indicators(daily_inds)
        weekly_inds = validate_indicators(weekly_inds)
        monthly_inds = validate_indicators(monthly_inds)

        # Write immediately (overwrite first batch, append rest)
        write_mode = "overwrite" if batch_num == 1 else "append"
        write_table(get_table_path("silver", "daily_indicators"), daily_inds, mode=write_mode)
        write_table(get_table_path("silver", "weekly_indicators"), weekly_inds, mode=write_mode)
        write_table(get_table_path("silver", "monthly_indicators"), monthly_inds, mode=write_mode)

        logger.info(f"✅ Wrote {len(daily_inds)} daily, {len(weekly_inds)} weekly, {len(monthly_inds)} monthly indicators")

        # Memory cleanup happens automatically when variables go out of scope

    logger.info("✅ Phase 2 complete - all indicators written to Parquet")
    logger.info("🎉 Full rewrite complete!")


def main(batch_size: int = 250, indicator_batch_size: int = 500) -> None:  # pragma: no cover
    """
    Main silver layer processing function.

    Smart incremental logic:
    - If new splits detected: Full rewrite (15-30 min)
    - If no splits: Append new data only (2-5 min)

    Args:
        batch_size: Number of tickers per batch for aggregation (default: 250)
        indicator_batch_size: Number of tickers per batch for indicators (default: 500)
    """
    logger.info("🥈 Starting silver layer processing")

    # Write ticker metadata (full refresh for this small table)
    tickers = get_filtered_tickers()
    write_table(get_table_path("silver", "ticker_metadata"), tickers, mode="overwrite")
    logger.info(f"✅ Wrote {len(tickers)} tickers to metadata table")

    # Smart incremental decision
    if should_do_full_rewrite():
        process_full_rewrite_silver(batch_size, indicator_batch_size)

        # Update checkpoint
        checkpoints = load_checkpoints()
        checkpoints["silver_last_full_rewrite"] = str(date.today())
        save_checkpoints(checkpoints)
    else:
        process_append_silver(batch_size, indicator_batch_size)

    logger.info("✅ Silver layer complete!")


def cli() -> None:  # pragma: no cover
    """CLI entry point with argument parsing."""
    parser = argparse.ArgumentParser(description="Silver layer processing")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Number of tickers to process per batch for aggregation (default: 250)",
    )
    parser.add_argument(
        "--indicator-batch-size",
        type=int,
        default=500,
        help="Number of tickers to process per batch for indicators (default: 500)",
    )

    args = parser.parse_args()

    main(batch_size=args.batch_size, indicator_batch_size=args.indicator_batch_size)


if __name__ == "__main__":  # pragma: no cover
    cli()
