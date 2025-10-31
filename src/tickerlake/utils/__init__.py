"""Shared utilities across all layers. 🛠️

This module provides reusable functions and patterns used throughout
the TickerLake data pipeline, organized by functionality:

- **batch_processing**: Memory-efficient batch processing for large datasets
- **calendar**: NYSE trading calendar and market status checks
- **dataframe**: Common Polars DataFrame operations
- **timestamps**: UTC timestamp utilities for metadata tracking
- **validation**: Data quality validation patterns
"""

from tickerlake.utils.batch_processing import batch_generator, process_in_batches
from tickerlake.utils.calendar import (
    get_trading_days,
    is_data_available_for_today,
    is_market_open,
)
from tickerlake.utils.dataframe import (
    convert_to_categorical_ticker,
    ensure_sorted_by_date,
    filter_by_ticker,
    filter_by_tickers,
    get_unique_tickers,
    sort_by_date,
    sort_by_ticker_and_date,
)
from tickerlake.utils.timestamps import add_timestamp, get_utc_timestamp
from tickerlake.utils.validation import (
    get_anomaly_reasons,
    is_anomalous_count,
    validate_no_nulls,
    validate_positive_values,
    validate_record_counts,
)

__all__ = [
    # Batch processing 📦
    "batch_generator",
    "process_in_batches",
    # Calendar 📅
    "get_trading_days",
    "is_market_open",
    "is_data_available_for_today",
    # DataFrame operations 🐻‍❄️
    "filter_by_ticker",
    "filter_by_tickers",
    "sort_by_ticker_and_date",
    "sort_by_date",
    "convert_to_categorical_ticker",
    "get_unique_tickers",
    "ensure_sorted_by_date",
    # Timestamps ⏰
    "get_utc_timestamp",
    "add_timestamp",
    # Validation ✅
    "is_anomalous_count",
    "get_anomaly_reasons",
    "validate_record_counts",
    "validate_no_nulls",
    "validate_positive_values",
]
