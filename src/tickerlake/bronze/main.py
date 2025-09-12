"""Bronze medallion layer for TickerLake."""

import logging
from datetime import datetime
from pathlib import PurePath

import polars as pl
import pytz
import s3fs
import structlog
from polygon import RESTClient

from tickerlake.config import s3_storage_options, settings
from tickerlake.utils import get_trading_days, is_market_open

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def download_daily_aggregates(date_str: str) -> pl.DataFrame:
    """Download daily stock market aggregates from Polygon.io API.

    Args:
        date_str: Trading day in YYYY-MM-DD format.

    Returns:
        DataFrame containing daily aggregates sorted by ticker.

    """
    client = RESTClient(settings.polygon_api_key.get_secret_value())
    grouped = client.get_grouped_daily_aggs(
        date_str,
        adjusted=False,
        include_otc=False,
    )
    return pl.DataFrame(grouped).sort("ticker")


def store_daily_aggregates(df: pl.DataFrame, date_str: str) -> None:
    """Store daily aggregates DataFrame to S3 as Parquet file.

    Args:
        df: DataFrame containing daily aggregates data.
        date_str: Trading day in YYYY-MM-DD format for file path.

    """
    path = f"s3://{settings.s3_bucket_name}/bronze/daily/{date_str}/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
    )


def get_valid_trading_days():
    """Get list of valid trading days from configured start date to today.

    Returns:
        List of trading days in YYYY-MM-DD format.

    """
    ny_tz = pytz.timezone("America/New_York")
    today_ny = datetime.now(ny_tz).date()

    return get_trading_days(
        start_date=settings.data_start_date,
        end_date=today_ny.strftime("%Y-%m-%d"),
    )


def list_bronze_daily_folders():
    """List existing daily data folders in S3 bronze layer.

    Returns:
        List of folder names (dates) that exist in S3.

    """
    fs = s3fs.S3FileSystem(
        endpoint_url=settings.s3_endpoint_url,
        key=settings.aws_access_key_id.get_secret_value(),
        secret=settings.aws_secret_access_key.get_secret_value(),
    )
    prefix = f"{settings.s3_bucket_name}/bronze/daily/"
    folders = fs.ls(prefix)
    return [PurePath(folder).name for folder in folders if fs.isdir(folder)]


def get_missing_trading_days():
    """
    Identifies trading days for which daily data folders are missing.

    Returns:
        list: A sorted list of dates (as strings in 'YYYY-MM-DD' format) representing
              valid trading days that do not have corresponding bronze daily folders.
              If the market is currently open, today's date is excluded from the list
              of valid trading days.
    """
    valid_days = set(get_valid_trading_days())

    # If market is currently open, exclude today from valid days
    if is_market_open():
        ny_tz = pytz.timezone("America/New_York")
        today_str = datetime.now(ny_tz).strftime("%Y-%m-%d")
        valid_days.discard(today_str)

    existing_days = set(list_bronze_daily_folders())
    return sorted(valid_days - existing_days)


def main():
    """Download and store missing trading day data.

    Downloads daily aggregates for any missing trading days and stores
    them in the bronze layer of the data lake.
    """
    missing_days = get_missing_trading_days()
    logger.info(f"Found {len(missing_days)} missing days.")
    for day in missing_days:
        daily_aggs = download_daily_aggregates(date_str=day)
        store_daily_aggregates(daily_aggs, date_str=day)
        logger.info(f"Stored data for {day}.")


if __name__ == "__main__":
    main()
