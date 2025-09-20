"""Silver medallion layer for TickerLake."""

import logging

import polars as pl
import structlog

from tickerlake.config import s3_storage_options, settings

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def read_ticker_details() -> pl.DataFrame:
    """Read ticker details from bronze layer and filter for stocks and ETFs.

    Returns:
        pl.DataFrame: DataFrame with ticker, name, and ticker_type columns for CS and ETF types.
    """
    logger.info("Reading ticker details")
    return (
        pl.scan_parquet(
            f"s3://{settings.s3_bucket_name}/bronze/tickers/data.parquet",
            storage_options=s3_storage_options,
        )
        .select(["ticker", "name", "type"])
        .filter(pl.col("type").is_in(["CS", "ETF"]))
        .collect()
        .select(["ticker", "name", pl.col("type").alias("ticker_type")])
        .with_columns(
            pl.col("ticker").cast(pl.Categorical),
            pl.col("ticker_type").cast(pl.Categorical)
        )
        .sort("ticker")
    )


def write_ticker_details(df: pl.DataFrame) -> None:
    """Write ticker details to silver layer.

    Args:
        df: DataFrame containing ticker details to write.
    """
    logger.info("Writing ticker details")
    path = f"s3://{settings.s3_bucket_name}/silver/tickers/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
    )


def read_split_details(valid_tickers: list = []) -> pl.DataFrame:
    """Read split details from bronze layer for specified tickers.

    Args:
        valid_tickers: List of ticker symbols to filter for.

    Returns:
        pl.DataFrame: DataFrame with split details for valid tickers.
    """
    logger.info("Reading split details")
    return (
        pl.scan_parquet(
            f"s3://{settings.s3_bucket_name}/bronze/splits/data.parquet",
            storage_options=s3_storage_options,
        )
        .filter(pl.col("ticker").is_in(valid_tickers))
        .select(["ticker", "execution_date", "split_from", "split_to"])
        .collect()
        .with_columns(pl.col("ticker").cast(pl.Categorical))
        .sort("execution_date")
    )


def write_split_details(df: pl.DataFrame) -> None:
    """Write split details to silver layer.

    Args:
        df: DataFrame containing split details to write.
    """
    logger.info("Writing split details")
    path = f"s3://{settings.s3_bucket_name}/silver/splits/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
    )


def read_etf_holdings(etf_ticker: str) -> pl.DataFrame:
    """Read ETF holdings data from bronze layer.

    Args:
        etf_ticker: ETF ticker symbol to read holdings for.

    Returns:
        pl.DataFrame: DataFrame with ETF holdings including ticker and weight.
    """
    path = f"s3://{settings.s3_bucket_name}/bronze/holdings/{etf_ticker.lower()}/data.parquet"
    return (
        pl.read_parquet(path, storage_options=s3_storage_options)
        .sort("ticker")
        .with_columns(
            pl.lit(etf_ticker.lower()).alias("etf"),
            pl.col("ticker").cast(pl.Categorical),
        )
    )


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
            - 'volume_avg': 20-period rolling mean of 'volume' per 'ticker'.
            - 'volume_avg_ratio': Ratio of 'volume' to 'volume_avg'.
    """
    logger.info("Calculating volume average ratio")
    return df.with_columns(
        pl.col("volume")
        .rolling_mean(window_size=20)
        .over("ticker")
        .cast(pl.UInt64)
        .alias("volume_avg")
    ).with_columns((pl.col("volume") / pl.col("volume_avg")).alias("volume_avg_ratio"))


def read_daily_aggs(valid_tickers: list = [], ticker_details: pl.DataFrame = None) -> pl.DataFrame:
    """Read daily aggregates from bronze layer for specified tickers.

    Args:
        valid_tickers: List of ticker symbols to filter for.
        ticker_details: DataFrame with ticker details including ticker_type.

    Returns:
        pl.DataFrame: DataFrame with daily OHLCV data, date column, and ticker_type.
    """
    logger.info("Reading daily aggregates")
    path = f"s3://{settings.s3_bucket_name}/bronze/daily/*/data.parquet"
    df = (
        pl.scan_parquet(path, storage_options=s3_storage_options)
        .filter(pl.col("ticker").is_in(valid_tickers))
        .select(["ticker", "timestamp", "open", "high", "low", "close", "volume"])
        .collect()
        .with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .cast(pl.Date)
            .alias("date"),
            pl.col("ticker").cast(pl.Categorical),
            pl.col("volume").cast(pl.UInt64),
            pl.col("open").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
        )
        .sort(["date", "ticker"])
    )

    # Join with ticker details to add ticker_type
    if ticker_details is not None:
        df = df.join(
            ticker_details.select(["ticker", "ticker_type"]),
            on="ticker",
            how="left"
        )

    return df


def apply_splits(daily_aggs: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    """Apply stock split adjustments to daily aggregate data.

    Args:
        daily_aggs: DataFrame with daily OHLCV data.
        splits: DataFrame with split details including execution dates.

    Returns:
        pl.DataFrame: Split-adjusted daily aggregates.
    """
    logger.info("Applying splits to daily aggregates")
    result = daily_aggs.join(splits, on="ticker", how="left")

    result = result.with_columns(
        pl.when(pl.col("date") < pl.col("execution_date"))
        .then(pl.col("split_from") / pl.col("split_to"))
        .otherwise(1.0)
        .alias("adjustment_factor")
    )

    # Include ticker_type in group_by if it exists
    group_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    if "ticker_type" in daily_aggs.columns:
        group_cols.append("ticker_type")

    result = result.group_by(
        group_cols,
        maintain_order=True,
    ).agg(pl.col("adjustment_factor").product().alias("total_adjustment"))

    result = result.with_columns(
        (pl.col("close") * pl.col("total_adjustment")).cast(pl.Float64).alias("close"),
        (pl.col("open") * pl.col("total_adjustment")).cast(pl.Float64).alias("open"),
        (pl.col("high") * pl.col("total_adjustment")).cast(pl.Float64).alias("high"),
        (pl.col("low") * pl.col("total_adjustment")).cast(pl.Float64).alias("low"),
        (pl.col("volume") / pl.col("total_adjustment")).cast(pl.UInt64).alias("volume"),
    ).drop("total_adjustment")

    return result


def write_daily_aggs(df: pl.DataFrame) -> None:
    """Write daily aggregates to silver layer.

    Args:
        df: DataFrame containing daily aggregates to write.
    """
    logger.info(f"Writing daily aggregates ({df.shape[0]:,} rows)")
    path = f"s3://{settings.s3_bucket_name}/silver/daily/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
        row_group_size=100_000,
    )


def write_time_aggs(df: pl.DataFrame, period: str, output_dir: str) -> None:
    """Write time-aggregated data to specified directory.

    Args:
        df: DataFrame with daily data to aggregate.
        period: Time period for aggregation (e.g., '1w', '1mo').
        output_dir: Output directory name in silver layer.
    """
    higher_timeframe_df = (
        df.group_by_dynamic(
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
    path = f"s3://{settings.s3_bucket_name}/silver/{output_dir}/data.parquet"
    higher_timeframe_df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
        row_group_size=100_000,
    )


def write_weekly_aggs(df: pl.DataFrame) -> None:
    """Write weekly aggregates to silver layer.

    Args:
        df: DataFrame containing daily data to aggregate weekly.
    """
    write_time_aggs(df, "1w", "weekly")


def write_monthly_aggs(df: pl.DataFrame) -> None:
    """Write monthly aggregates to silver layer.

    Args:
        df: DataFrame containing daily data to aggregate monthly.
    """
    write_time_aggs(df, "1mo", "monthly")


def main() -> None:
    """Execute silver layer data processing pipeline."""
    ticker_details = read_ticker_details()
    etf_holdings = read_all_etf_holdings()
    ticker_details = ticker_details.join(etf_holdings, on="ticker", how="left")
    write_ticker_details(ticker_details)

    valid_tickers = ticker_details["ticker"].to_list()

    split_details = read_split_details(valid_tickers)
    write_split_details(split_details)

    unadjusted_daily_aggs = read_daily_aggs(valid_tickers, ticker_details)
    adjusted_daily_aggs = apply_splits(unadjusted_daily_aggs, split_details)

    unadjusted_daily_aggs = None

    adjusted_daily_aggs_with_volume_ratio = add_volume_ratio(adjusted_daily_aggs)

    write_daily_aggs(adjusted_daily_aggs_with_volume_ratio)
    write_weekly_aggs(adjusted_daily_aggs)
    write_monthly_aggs(adjusted_daily_aggs)


if __name__ == "__main__":  # pragma: no cover
    main()
