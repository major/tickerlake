"""Batch processing utilities for memory-efficient data processing. 🚀

This module provides reusable batch processing functions used across the data pipeline,
particularly in the silver layer for handling large datasets efficiently.
"""

from typing import Callable

import polars as pl

from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def batch_generator(items: list, batch_size: int):
    """Generate batches from a list of items. 📦

    Yields consecutive slices of the input list with the specified batch size.
    The last batch may be smaller if len(items) is not evenly divisible by batch_size.

    Args:
        items: List of items to batch.
        batch_size: Number of items per batch.

    Yields:
        List slices of size batch_size (or smaller for the final batch).

    Example:
        >>> tickers = ["AAPL", "MSFT", "GOOGL", "AMZN"]
        >>> for batch in batch_generator(tickers, 2):
        ...     print(batch)
        ['AAPL', 'MSFT']
        ['GOOGL', 'AMZN']
    """
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def process_in_batches(
    tickers: list[str],
    data: pl.DataFrame,
    processor: Callable[[pl.DataFrame], pl.DataFrame],
    batch_size: int,
    stage_name: str,
) -> list[pl.DataFrame]:
    """Process DataFrame in batches by ticker for memory efficiency. ⚙️

    This pattern is used throughout the silver layer to process large datasets
    without loading everything into memory at once.

    Args:
        tickers: List of unique ticker symbols to process.
        data: Full DataFrame containing all tickers (will be filtered per batch).
        processor: Function that takes a batch DataFrame and returns processed DataFrame.
        batch_size: Number of tickers to process per batch.
        stage_name: Name of processing stage (for logging).

    Returns:
        List of processed DataFrames (one per batch).

    Example:
        >>> def calculate_indicators(df: pl.DataFrame) -> pl.DataFrame:
        ...     return df.with_columns(pl.col("close").rolling_mean(20).alias("sma_20"))
        ...
        >>> results = process_in_batches(
        ...     tickers=all_tickers,
        ...     data=stocks_df,
        ...     processor=calculate_indicators,
        ...     batch_size=250,
        ...     stage_name="indicator calculation"
        ... )
    """
    results = []
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for batch_num, ticker_batch in enumerate(batch_generator(tickers, batch_size), 1):
        logger.info(
            f"📊 Processing {stage_name} batch {batch_num}/{total_batches} "
            f"({len(ticker_batch)} tickers)..."
        )

        # Filter data to current batch of tickers
        batch_data = data.filter(pl.col("ticker").is_in(ticker_batch))

        # Process batch
        result = processor(batch_data)
        results.append(result)

        logger.debug(
            f"✅ Batch {batch_num}/{total_batches} complete "
            f"({len(result):,} rows processed)"
        )

    logger.info(
        f"✅ All {total_batches} batches of {stage_name} complete! "
        f"({sum(len(r) for r in results):,} total rows)"
    )

    return results
