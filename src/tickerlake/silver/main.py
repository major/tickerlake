"""Silver medallion layer for TickerLake."""

import logging

import polars as pl
import structlog

from tickerlake.config import s3_storage_options, settings

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def read_ticker_details() -> pl.DataFrame:
    logger.info("Reading ticker details")
    return (
        pl.scan_parquet(
            f"s3://{settings.s3_bucket_name}/bronze/tickers/data.parquet",
            storage_options=s3_storage_options,
        )
        .select(["ticker", "name", "type"])
        .filter(pl.col("type").is_in(["CS", "ETF"]))
        .collect()
        .select(["ticker", "name"])
        .with_columns(pl.col("ticker").cast(pl.Categorical))
        .sort("ticker")
    )


def write_ticker_details(df: pl.DataFrame) -> None:
    logger.info("Writing ticker details")
    path = f"s3://{settings.s3_bucket_name}/silver/tickers/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
    )


def read_split_details(valid_tickers: list = []) -> pl.DataFrame:
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
    logger.info("Writing split details")
    path = f"s3://{settings.s3_bucket_name}/silver/splits/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
    )


def read_etf_holdings(etf_ticker: str) -> pl.DataFrame:
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
    all_etfs = pl.concat([read_etf_holdings(x) for x in settings.etfs])

    etf_membership = all_etfs.group_by("ticker").agg(pl.col("etf").alias("etfs"))
    return etf_membership


def read_daily_aggs(valid_tickers: list = []) -> pl.DataFrame:
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

    return df


def apply_splits(daily_aggs: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
    logger.info("Applying splits to daily aggregates")
    result = daily_aggs.join(splits, on="ticker", how="left")

    result = result.with_columns(
        pl.when(pl.col("date") < pl.col("execution_date"))
        .then(pl.col("split_from") / pl.col("split_to"))
        .otherwise(1.0)
        .alias("adjustment_factor")
    )

    result = result.group_by(
        ["ticker", "date", "open", "high", "low", "close", "volume"],
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
    logger.info(f"Writing daily aggregates ({df.shape[0]:,} rows)")
    path = f"s3://{settings.s3_bucket_name}/silver/daily/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
        compression="zstd",
        row_group_size=100_000,
    )


def write_time_aggs(df: pl.DataFrame, period: str, output_dir: str) -> None:
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
    write_time_aggs(df, "1w", "weekly")


def write_monthly_aggs(df: pl.DataFrame) -> None:
    write_time_aggs(df, "1mo", "monthly")


def main() -> None:
    ticker_details = read_ticker_details()
    etf_holdings = read_all_etf_holdings()
    ticker_details = ticker_details.join(etf_holdings, on="ticker", how="left")
    write_ticker_details(ticker_details)

    valid_tickers = ticker_details["ticker"].to_list()

    split_details = read_split_details(valid_tickers)
    write_split_details(split_details)

    unadjusted_daily_aggs = read_daily_aggs(valid_tickers)
    adjusted_daily_aggs = apply_splits(unadjusted_daily_aggs, split_details)

    write_daily_aggs(adjusted_daily_aggs)
    write_weekly_aggs(adjusted_daily_aggs)
    write_monthly_aggs(adjusted_daily_aggs)


if __name__ == "__main__":
    main()
