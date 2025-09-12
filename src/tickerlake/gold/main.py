"""Silver medallion layer for TickerLake."""

import logging

import polars as pl
import structlog

from tickerlake.config import s3_storage_options, settings

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def get_latest_closing_prices() -> pl.DataFrame:
    """Get the latest closing price for each ticker.

    Returns:
        pl.DataFrame: DataFrame with ticker and latest_close columns.
    """
    path = f"s3://{settings.s3_bucket_name}/silver/daily/data.parquet"
    latest_prices = (
        pl.scan_parquet(path, storage_options=s3_storage_options)
        .group_by("ticker")
        .agg(
            pl.col("close").last().alias("latest_close"),
            pl.col("date").max().alias("latest_date"),
        )
        .select(["ticker", "latest_close"])
        .collect()
    )

    return latest_prices


def get_high_volume_closes() -> pl.DataFrame:
    """Get days with unusually high trading volume and join latest closing prices.

    Returns:
        pl.DataFrame: DataFrame with ticker, date, close, volume, volume_avg_ratio,
                     and latest_close columns.
    """
    path = f"s3://{settings.s3_bucket_name}/silver/daily/data.parquet"
    df = (
        pl.scan_parquet(path, storage_options=s3_storage_options)
        .filter(
            pl.col("volume_avg_ratio") >= 3,
            pl.col("volume_avg") >= 200000,
            pl.col("close") >= 5,
        )
        .collect()
        .sort(["date", "ticker"])
    )

    # Get latest closing prices for each ticker
    latest_prices = get_latest_closing_prices()

    # Join the latest prices to the high volume DataFrame
    df_with_latest = df.join(latest_prices, on="ticker", how="left")

    return df_with_latest


def main():
    df = get_high_volume_closes()

    print(
        df.filter(pl.col("date") == pl.col("date").max())
        .with_columns(pl.col("volume_avg_ratio").round(2).alias("volume_avg_ratio"))
        .select([
            "ticker",
            "volume",
            "volume_avg",
            "volume_avg_ratio",
        ])
    )


if __name__ == "__main__":  # pragma: no cover
    main()
