"""Gold medallion layer for TickerLake."""

import logging

import polars as pl
import structlog

from tickerlake.delta_utils import scan_delta_table

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def get_latest_closing_prices() -> pl.DataFrame:
    """Get the latest closing price for each ticker from Delta table.

    Returns:
        pl.DataFrame: DataFrame with ticker and latest_close columns.
    """
    logger.info("Fetching latest closing prices for each ticker")
    latest_prices = (
        scan_delta_table("daily")
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
    """Get days with unusually high trading volume from Delta table and join latest closing prices.

    Returns:
        pl.DataFrame: DataFrame with ticker, date, close, volume, volume_avg_ratio,
                     and latest_close columns.
    """
    logger.info("Extracting high volume closes from daily data")

    df = (
        scan_delta_table("daily")
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
        .sort(["date", "ticker"], descending=[True, False])
    )

    # Get latest closing prices for each ticker
    latest_prices = get_latest_closing_prices()

    # Join the latest prices to the high volume DataFrame
    df_with_latest = df.join(latest_prices, on="ticker", how="left")

    # Add columns to indicate if latest close is within or near the high volume channel
    df_with_latest = df_with_latest.with_columns(
        (
            (pl.col("latest_close") >= pl.col("low"))
            & (pl.col("latest_close") <= pl.col("high"))
        ).alias("in_hvc_channel"),
        (
            (
                # Above high but within 5% of high
                (pl.col("latest_close") > pl.col("high"))
                & (pl.col("latest_close") <= pl.col("high") * 1.05)
            )
            | (
                # Below low but within 5% of low
                (pl.col("latest_close") < pl.col("low"))
                & (pl.col("latest_close") >= pl.col("low") * 0.95)
            )
        ).alias("near_hvc_channel"),
    )

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
        "in_hvc_channel",
        "near_hvc_channel",
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
