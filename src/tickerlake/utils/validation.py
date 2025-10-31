"""Data quality validation utilities used across all layers. ✅

This module provides reusable validation patterns for detecting anomalies
and ensuring data quality across bronze, silver, and gold layers.
"""

from typing import Any

import polars as pl

from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def is_anomalous_count(
    count: int, mean_count: float, absolute_min: int = 5000
) -> bool:
    """Check if record count is anomalous. 📊

    A count is considered anomalous if it:
    - Falls below 50% of the mean count
    - Exceeds 200% of the mean count
    - Falls below the absolute minimum threshold

    Args:
        count: Daily record count to check.
        mean_count: Mean count across all days.
        absolute_min: Absolute minimum threshold (default: 5000).

    Returns:
        True if count is anomalous, False otherwise.

    Example:
        >>> is_anomalous_count(3000, 10000)  # 30% of mean
        True
        >>> is_anomalous_count(9000, 10000)  # 90% of mean
        False
        >>> is_anomalous_count(25000, 10000)  # 250% of mean
        True
    """
    relative_min = mean_count * 0.50
    relative_max = mean_count * 2.00
    return count < relative_min or count > relative_max or count < absolute_min


def get_anomaly_reasons(
    count: int, mean_count: float, absolute_min: int = 5000
) -> list[str]:
    """Get human-readable reasons why a count is anomalous. 🔍

    Args:
        count: Daily record count.
        mean_count: Mean count across all days.
        absolute_min: Absolute minimum threshold (default: 5000).

    Returns:
        List of reason strings explaining why the count is anomalous.
        Empty list if count is not anomalous.

    Example:
        >>> reasons = get_anomaly_reasons(3000, 10000)
        >>> print(reasons)
        ['below absolute minimum of 5000', 'only 30% of mean']
    """
    reasons = []
    pct_of_mean = (count / mean_count) * 100
    relative_min = mean_count * 0.50
    relative_max = mean_count * 2.00

    if count < absolute_min:
        reasons.append(f"below absolute minimum of {absolute_min}")
    if count < relative_min:
        reasons.append(f"only {pct_of_mean:.0f}% of mean")
    if count > relative_max:
        reasons.append(f"{pct_of_mean:.0f}% of mean (unusually high)")

    return reasons


def validate_record_counts(
    stats_df: pl.DataFrame, table_name: str, date_col: str = "date"
) -> list[tuple[Any, int]]:
    """Validate record counts using statistical thresholds. ✅

    Checks each date's record count against the mean to detect anomalies.
    Logs warnings for any anomalous dates found.

    Args:
        stats_df: DataFrame with columns: date_col, record_count
        table_name: Name of table being validated (for logging)
        date_col: Name of date column (default: "date")

    Returns:
        List of tuples (date, count) for anomalous dates.
        Empty list if all dates pass validation.

    Example:
        >>> stats = pl.DataFrame({
        ...     "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        ...     "record_count": [10000, 9500, 3000]
        ... })
        >>> anomalies = validate_record_counts(stats, "stocks")
        >>> # Logs warning for 2024-01-03 (only 30% of mean)
    """
    if len(stats_df) == 0:
        logger.warning(f"⚠️  No data found for validation of {table_name}")
        return []

    # Calculate statistics
    counts = stats_df["record_count"].to_list()
    mean_count = sum(counts) / len(counts)

    # Find anomalies
    anomalies = []
    for row in stats_df.iter_rows(named=True):
        date_val = row[date_col]
        count = row["record_count"]

        if is_anomalous_count(count, mean_count):
            anomalies.append((date_val, count))

    # Log warnings for anomalies
    if anomalies:
        logger.warning(
            f"⚠️  Found {len(anomalies)} anomalous date(s) in {table_name}:"
        )
        for date_val, count in anomalies:
            if hasattr(date_val, "strftime"):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                date_str = str(date_val)

            reasons = get_anomaly_reasons(count, mean_count)
            logger.warning(
                f"   📅 {date_str}: {count:,} records ({', '.join(reasons)})"
            )

    return anomalies


def validate_no_nulls(df: pl.DataFrame, columns: list[str] | None = None) -> bool:
    """Validate that specified columns have no null values. 🚫

    Args:
        df: DataFrame to validate.
        columns: List of column names to check. If None, checks all columns.

    Returns:
        True if validation passes (no nulls), False otherwise.

    Example:
        >>> df = pl.DataFrame({"a": [1, 2, None], "b": [4, 5, 6]})
        >>> validate_no_nulls(df, ["b"])  # True
        >>> validate_no_nulls(df, ["a"])  # False
    """
    if columns is None:
        columns = df.columns

    for col in columns:
        null_count = df[col].null_count()
        if null_count > 0:
            logger.warning(
                f"⚠️  Column '{col}' has {null_count:,} null values "
                f"({null_count / len(df) * 100:.1f}%)"
            )
            return False

    return True


def validate_positive_values(
    df: pl.DataFrame, columns: list[str]
) -> bool:
    """Validate that specified numeric columns have only positive values. ➕

    Args:
        df: DataFrame to validate.
        columns: List of numeric column names to check.

    Returns:
        True if validation passes (all positive), False otherwise.

    Example:
        >>> df = pl.DataFrame({"price": [100.0, 150.0, -50.0], "volume": [1000, 2000, 3000]})
        >>> validate_positive_values(df, ["volume"])  # True
        >>> validate_positive_values(df, ["price"])  # False
    """
    for col in columns:
        min_val = df[col].min()
        # Handle empty DataFrame (min() returns None)
        if min_val is None:
            continue

        # Type narrowing: check if value is numeric (int or float)
        if not isinstance(min_val, (int, float)):
            logger.warning(
                f"⚠️  Column '{col}' contains non-numeric values (type: {type(min_val).__name__}), "
                "skipping positive value check"
            )
            continue

        # Now pyright knows min_val is int | float, safe to compare
        if min_val < 0:
            negative_count = (df[col] < 0).sum()
            logger.warning(
                f"⚠️  Column '{col}' has {negative_count:,} negative values "
                f"(min: {min_val})"
            )
            return False

    return True
