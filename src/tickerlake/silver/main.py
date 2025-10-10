"""Silver medallion layer for TickerLake."""

import logging
from datetime import date, datetime, time, timedelta
from typing import Literal

import polars as pl
import structlog

from tickerlake.config import s3_storage_options, settings
from tickerlake.delta_utils import (
    delta_table_exists,
    merge_to_delta_table,
    scan_delta_table,
    write_delta_table,
)
from tickerlake.schemas import (
    validate_daily_aggregates,
    validate_etf_holdings,
    validate_splits,
    validate_ticker_details,
)
from tickerlake.silver.layer import SilverLayer

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def read_ticker_details() -> pl.DataFrame:
    """Read ticker details from bronze layer and filter for stocks and ETFs.

    Returns:
        pl.DataFrame: DataFrame with ticker, name, and ticker_type columns for CS and ETF types.
    """
    logger.info("Reading ticker details")
    df = (
        pl.scan_parquet(
            f"s3://{settings.s3_bucket_name}/bronze/tickers/data.parquet",
            storage_options=s3_storage_options,
        )
        .select(["ticker", "name", "type"])
        .filter(pl.col("type").is_in(["CS", "ETF"]))
        .collect()
        .select(["ticker", "name", pl.col("type").alias("ticker_type")])
        .sort("ticker")
    )
    return validate_ticker_details(df)


def write_ticker_details(df: pl.DataFrame) -> None:
    """Write ticker details to silver layer Delta table.

    Args:
        df: DataFrame containing ticker details to write.
    """
    logger.info("Writing ticker details to Delta table")
    if delta_table_exists("tickers"):
        merge_to_delta_table(df, "tickers", merge_keys=["ticker"])
    else:
        write_delta_table(df, "tickers", mode="overwrite")


def read_split_details(valid_tickers: list = []) -> pl.DataFrame:
    """Read split details from bronze layer for specified tickers.

    Args:
        valid_tickers: List of ticker symbols to filter for.

    Returns:
        pl.DataFrame: DataFrame with split details for valid tickers.
    """
    logger.info("Reading split details")
    df = (
        pl.scan_parquet(
            f"s3://{settings.s3_bucket_name}/bronze/splits/data.parquet",
            storage_options=s3_storage_options,
        )
        .filter(pl.col("ticker").is_in(valid_tickers))
        .select(["ticker", "execution_date", "split_from", "split_to"])
        .collect()
        .sort("execution_date")
    )
    return validate_splits(df)


def write_split_details(df: pl.DataFrame) -> None:
    """Write split details to silver layer Delta table.

    Args:
        df: DataFrame containing split details to write.
    """
    logger.info("Writing split details to Delta table")
    if delta_table_exists("splits"):
        merge_to_delta_table(df, "splits", merge_keys=["ticker", "execution_date"])
    else:
        write_delta_table(df, "splits", mode="overwrite")


def read_etf_holdings(etf_ticker: str) -> pl.DataFrame:
    """Read ETF holdings data from bronze layer.

    Args:
        etf_ticker: ETF ticker symbol to read holdings for.

    Returns:
        pl.DataFrame: DataFrame with ETF holdings including ticker and weight.
    """
    path = f"s3://{settings.s3_bucket_name}/bronze/holdings/{etf_ticker.lower()}/data.parquet"
    df = (
        pl.read_parquet(path, storage_options=s3_storage_options)
        .sort("ticker")
        .with_columns(
            pl.lit(etf_ticker.lower()).alias("etf"),
        )
    )
    return validate_etf_holdings(df)


def read_all_etf_holdings() -> pl.DataFrame:
    """Read and aggregate holdings for all configured ETFs.

    Returns:
        pl.DataFrame: DataFrame with ticker and list of ETFs holding each ticker.
    """
    all_etfs = pl.concat([read_etf_holdings(x) for x in settings.etfs])

    etf_membership = all_etfs.group_by("ticker").agg(pl.col("etf").alias("etfs"))
    return etf_membership


def add_volume_ratio(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a 'volume_avg_ratio' column to the given DataFrame, representing the ratio of the current 'volume' to its 20-period rolling mean per 'ticker'.

    Parameters:
        df (pl.DataFrame): Input DataFrame containing at least 'volume' and 'ticker' columns.

    Returns:
        pl.DataFrame: DataFrame with two additional columns:
            - 'volume_avg': 20-period rolling mean of 'volume' per 'ticker' (excluding current day).
            - 'volume_avg_ratio': Ratio of 'volume' to 'volume_avg'.
    """
    logger.info("Calculating volume average ratio")
    result = df.with_columns(
        pl.col("volume")
        .rolling_mean(window_size=20)
        .shift(1)  # Use previous 20 days, not including current day
        .over("ticker")
        .cast(pl.UInt64)
        .alias("volume_avg")
    ).with_columns(
        pl.when(pl.col("volume_avg").is_not_null())
        .then(pl.col("volume") / pl.col("volume_avg"))
        .otherwise(None)
        .alias("volume_avg_ratio")
    )
    return validate_daily_aggregates(result, include_volume_ratio=True)


def read_daily_aggs(
    valid_tickers: list = [],
    ticker_details: pl.DataFrame | None = None,
    start_date: date | None = None,
) -> pl.DataFrame:
    """Read daily aggregates from bronze layer for specified tickers.

    Args:
        valid_tickers: List of ticker symbols to filter for.
        ticker_details: DataFrame with ticker details including ticker_type.
        start_date: Optional start date to filter data (for incremental loading).

    Returns:
        pl.DataFrame: DataFrame with daily OHLCV data, date column, and ticker_type.
    """
    if start_date:
        logger.info(f"Reading daily aggregates from {start_date}")
    else:
        logger.info("Reading all daily aggregates")

    path = f"s3://{settings.s3_bucket_name}/bronze/daily/*/data.parquet"
    lazy_frame = (
        pl.scan_parquet(path, storage_options=s3_storage_options)
        .filter(pl.col("ticker").is_in(valid_tickers))
    )

    if start_date:
        cutoff_ts = int(datetime.combine(start_date, time.min).timestamp() * 1000)
        lazy_frame = lazy_frame.filter(pl.col("timestamp") > cutoff_ts)

    df = lazy_frame.collect().with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
        .cast(pl.Date)
        .alias("date"),
        pl.col("volume").cast(pl.UInt64),
        pl.col("open").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("transactions").cast(pl.UInt64),
    )

    # Filter by start_date if provided (incremental mode)
    if start_date:
        df = df.filter(pl.col("date") > start_date)

    df = df.sort(["date", "ticker"])

    # Join with ticker details to add ticker_type
    if ticker_details is not None:
        df = df.join(
            ticker_details.select(["ticker", "ticker_type"]),
            on="ticker",
            how="left",
        )

    return validate_daily_aggregates(df)


def apply_splits(daily_aggs: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Apply stock split adjustments to daily aggregate data.

    Args:
        daily_aggs: DataFrame with daily OHLCV data.
        splits: DataFrame with split details including execution dates.

    Returns:
        pl.DataFrame: Split-adjusted daily aggregates.
    """
    logger.info("Applying splits to daily aggregates")

    # Ensure ticker columns have the same dtype for joining
    # Convert both to strings to avoid categorical/string mismatch
    if splits["ticker"].dtype == pl.Categorical:
        splits = splits.with_columns(pl.col("ticker").cast(pl.String))
    if daily_aggs["ticker"].dtype == pl.Categorical:
        daily_aggs = daily_aggs.with_columns(pl.col("ticker").cast(pl.String))

    result = daily_aggs.join(splits, on="ticker", how="left")

    result = result.with_columns(
        pl.when(pl.col("date") < pl.col("execution_date"))
        .then(pl.col("split_from") / pl.col("split_to"))
        .otherwise(1.0)
        .alias("adjustment_factor")
    )

    # Include ticker_type in group_by if it exists
    group_cols = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "transactions",
    ]
    if "ticker_type" in daily_aggs.columns:
        group_cols.append("ticker_type")

    result = result.group_by(
        group_cols,
    ).agg(pl.col("adjustment_factor").product().alias("total_adjustment"))

    result = result.with_columns(
        (pl.col("close") * pl.col("total_adjustment")).cast(pl.Float64).alias("close"),
        (pl.col("open") * pl.col("total_adjustment")).cast(pl.Float64).alias("open"),
        (pl.col("high") * pl.col("total_adjustment")).cast(pl.Float64).alias("high"),
        (pl.col("low") * pl.col("total_adjustment")).cast(pl.Float64).alias("low"),
        (pl.col("volume") / pl.col("total_adjustment")).cast(pl.UInt64).alias("volume"),
    ).drop("total_adjustment")

    return validate_daily_aggregates(result)


def write_daily_aggs(
    df: pl.DataFrame, mode: Literal["error", "append", "overwrite", "ignore"] = "overwrite"
) -> None:
    """Write daily aggregates to silver layer Delta table.

    Args:
        df: DataFrame containing daily aggregates to write.
        mode: Write mode - 'overwrite' for full rebuild or 'append' for incremental.
    """
    logger.info(f"Writing daily aggregates ({df.shape[0]:,} rows) in {mode} mode")
    write_delta_table(df, "daily", mode=mode)


def write_time_aggs(df: pl.DataFrame, period: str, table_name: str) -> None:
    """Write time-aggregated data to Delta table.

    Args:
        df: DataFrame with daily data to aggregate.
        period: Time period for aggregation (e.g., '1w', '1mo').
        table_name: Delta table name (e.g., 'weekly', 'monthly').
    """
    higher_timeframe_df = (
        df.sort(["ticker", "date"])
        .group_by_dynamic(
            "date",
            every=period,
            group_by="ticker",
            label="left",
            start_by="monday",
        )
        .agg([
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        ])
        .sort(["date", "ticker"])
    )

    logger.info(f"Writing {period} aggregates ({higher_timeframe_df.shape[0]:,} rows)")
    write_delta_table(higher_timeframe_df, table_name, mode="overwrite")


def write_weekly_aggs(df: pl.DataFrame) -> None:
    """Write weekly aggregates to silver layer Delta table.

    Args:
        df: DataFrame containing daily data to aggregate weekly.
    """
    write_time_aggs(df, "1w", "weekly")


def write_monthly_aggs(df: pl.DataFrame) -> None:
    """Write monthly aggregates to silver layer Delta table.

    Args:
        df: DataFrame containing daily data to aggregate monthly.
    """
    write_time_aggs(df, "1mo", "monthly")


def _process_reference_data() -> tuple[pl.DataFrame, list[str], pl.DataFrame]:
    """Load and process ticker and split reference data.

    Returns:
        Tuple of (ticker_details, valid_tickers, split_details).
    """
    ticker_details = read_ticker_details()
    etf_holdings = read_all_etf_holdings()
    ticker_details = ticker_details.join(etf_holdings, on="ticker", how="left")
    write_ticker_details(ticker_details)

    valid_tickers = ticker_details["ticker"].to_list()

    split_details = read_split_details(valid_tickers)
    write_split_details(split_details)

    return ticker_details, valid_tickers, split_details


def _process_incremental_data(
    valid_tickers: list[str],
    ticker_details: pl.DataFrame,
    split_details: pl.DataFrame,
    last_processed_date: date,
) -> None:
    """Process new data incrementally and update Delta tables.

    Args:
        valid_tickers: List of valid ticker symbols.
        ticker_details: DataFrame with ticker details.
        split_details: DataFrame with split details.
        last_processed_date: Last date already processed in Delta table.
    """
    logger.info(f"Incremental mode: processing data after {last_processed_date}")

    # Read only new data from bronze layer
    unadjusted_daily_aggs = read_daily_aggs(
        valid_tickers, ticker_details, start_date=last_processed_date
    )

    if unadjusted_daily_aggs.height == 0:
        logger.info("No new data to process")
        return

    logger.info(f"Processing {unadjusted_daily_aggs.height:,} new rows")
    adjusted_daily_aggs = apply_splits(unadjusted_daily_aggs, split_details)

    # For volume ratios, we need full history to calculate 20-day rolling average
    required_tickers = adjusted_daily_aggs["ticker"].unique().to_list()
    history_buffer_days = 40
    history_start = last_processed_date - timedelta(days=history_buffer_days)

    logger.info(
        "Reading existing data to calculate volume ratios",
        history_start=history_start,
        tickers=len(required_tickers),
    )

    lazy_existing = (
        scan_delta_table("daily")
        .filter(pl.col("ticker").is_in(required_tickers))
        .filter(pl.col("date") >= pl.lit(history_start))
        .filter(pl.col("date") <= pl.lit(last_processed_date))
    )

    existing_daily_data = lazy_existing.select(
        ["ticker", "date", "open", "high", "low", "close", "volume", "transactions"]
    ).collect()

    insufficient_history = (
        existing_daily_data.group_by("ticker")
        .count()
        .filter(pl.col("count") < 20)
        .get_column("ticker")
        .to_list()
        if existing_daily_data.height > 0
        else []
    )

    if insufficient_history:
        logger.info(
            "Fetching additional history for tickers with sparse data",
            tickers=insufficient_history,
        )
        extra_history = (
            scan_delta_table("daily")
            .filter(pl.col("ticker").is_in(insufficient_history))
            .filter(pl.col("date") < pl.lit(history_start))
            .select(["ticker", "date", "open", "high", "low", "close", "volume", "transactions"])
            .collect()
        )

        if extra_history.height > 0:
            extra_history = (
                extra_history.sort(["ticker", "date"])
                .group_by("ticker")
                .tail(20)
            )
            existing_daily_data = pl.concat([existing_daily_data, extra_history])

    if existing_daily_data.height > 0:
        existing_daily_data = existing_daily_data.with_columns(
            pl.col("volume").cast(pl.UInt64),
            pl.col("transactions").cast(pl.UInt64),
        )

    combined_parts = [adjusted_daily_aggs]
    if existing_daily_data.height > 0:
        combined_parts.insert(0, existing_daily_data.sort(["ticker", "date"]))

    combined_data = pl.concat(combined_parts).sort(["ticker", "date"])

    # Calculate volume ratios on full dataset
    combined_with_volume_ratio = add_volume_ratio(combined_data)

    # Filter to only the new rows we want to append
    new_rows_with_ratio = combined_with_volume_ratio.filter(pl.col("date") > last_processed_date)

    # Append new data to Delta table
    write_daily_aggs(new_rows_with_ratio, mode="append")

    # For weekly/monthly, we need to rebuild from the full dataset
    logger.info("Rebuilding weekly and monthly aggregates from full dataset")
    full_daily_data = scan_delta_table("daily").collect()
    write_weekly_aggs(full_daily_data)
    write_monthly_aggs(full_daily_data)


def _process_full_rebuild(
    valid_tickers: list[str],
    ticker_details: pl.DataFrame,
    split_details: pl.DataFrame,
) -> None:
    """Process all data from scratch and overwrite Delta tables.

    Args:
        valid_tickers: List of valid ticker symbols.
        ticker_details: DataFrame with ticker details.
        split_details: DataFrame with split details.
    """
    logger.info("Full rebuild mode: processing all data")
    unadjusted_daily_aggs = read_daily_aggs(valid_tickers, ticker_details)
    adjusted_daily_aggs = apply_splits(unadjusted_daily_aggs, split_details)

    adjusted_daily_aggs_with_volume_ratio = add_volume_ratio(adjusted_daily_aggs)

    write_daily_aggs(adjusted_daily_aggs_with_volume_ratio, mode="overwrite")
    write_weekly_aggs(adjusted_daily_aggs)
    write_monthly_aggs(adjusted_daily_aggs)


def main(full_rebuild: bool = False) -> None:
    """Execute silver layer data processing pipeline.

    Args:
        full_rebuild: If True, rebuild all data from scratch. If False, process incrementally.
    """
    silver = SilverLayer()
    silver.run(full_rebuild=full_rebuild)


if __name__ == "__main__":  # pragma: no cover
    main()
