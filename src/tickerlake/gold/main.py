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
    logger.info("Fetching latest closing prices for each ticker")
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
    logger.info("Extracting high volume closes from daily data")
    path = f"s3://{settings.s3_bucket_name}/silver/daily/data.parquet"

    df = (
        pl.scan_parquet(path, storage_options=s3_storage_options)
        .filter(
            pl.col("volume_avg_ratio") >= 3,
            (
                (
                    (pl.col("ticker_type") == "CS")
                    & (pl.col("volume_avg") >= 200000)
                    & (pl.col("close") >= 5)
                )
                | ((pl.col("ticker_type") == "ETF") & (pl.col("volume_avg") >= 50000))
            ),
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
    logger.info("Starting high volume closes extraction")
    df = get_high_volume_closes()

    hvcs = df.with_columns(
        pl.col("volume_avg_ratio").round(2).alias("volume_avg_ratio")
    ).select([
        "date",
        "ticker",
        "ticker_type",
        "volume_avg_ratio",
        "volume",
        "volume_avg",
    ])
    logger.info(f"Extracted {hvcs.height} high volume close records")

    logger.info("Writing high volume closes to SQLite database")
    hvcs.filter(pl.col("ticker_type") == "CS").drop("ticker_type").write_database(
        table_name="high_volume_closes_stocks",
        connection="sqlite:///hvcs.db",
        if_table_exists="replace",
    )
    hvcs.filter(pl.col("ticker_type") == "ETF").drop("ticker_type").write_database(
        table_name="high_volume_closes_etfs",
        connection="sqlite:///hvcs.db",
        if_table_exists="replace",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
