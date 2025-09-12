"""Bronze medallion layer for TickerLake."""

import logging
from datetime import date
from pathlib import PurePath

import polars as pl
import s3fs
import structlog
from polygon import RESTClient

from tickerlake.config import s3_storage_options, settings
from tickerlake.utils import get_trading_days

logging.basicConfig(level=logging.INFO)
logger = structlog.get_logger()


def download_daily_aggregates(date_str: str) -> pl.DataFrame:
    client = RESTClient(settings.polygon_api_key.get_secret_value())
    grouped = client.get_grouped_daily_aggs(
        date_str,
        adjusted=False,
        include_otc=False,
    )
    return pl.DataFrame(grouped).sort("ticker")


def store_daily_aggregates(df: pl.DataFrame, date_str: str) -> None:
    path = f"s3://{settings.s3_bucket_name}/bronze/daily/{date_str}/data.parquet"
    df.write_parquet(
        file=path,
        storage_options=s3_storage_options,
    )


def get_valid_trading_days():
    return get_trading_days(
        start_date=settings.data_start_date,
        end_date=date.today().strftime("%Y-%m-%d"),
    )


def list_bronze_daily_folders():
    fs = s3fs.S3FileSystem(
        endpoint_url=settings.s3_endpoint_url,
        key=settings.aws_access_key_id.get_secret_value(),
        secret=settings.aws_secret_access_key.get_secret_value(),
    )
    prefix = f"{settings.s3_bucket_name}/bronze/daily/"
    folders = fs.ls(prefix)
    return [PurePath(folder).name for folder in folders if fs.isdir(folder)]


def get_missing_trading_days():
    valid_days = set(get_valid_trading_days())
    existing_days = set(list_bronze_daily_folders())
    return sorted(valid_days - existing_days)


def main():
    missing_days = get_missing_trading_days()
    logger.info(f"Found {len(missing_days)} missing days.")
    for day in missing_days:
        daily_aggs = download_daily_aggregates(date_str=day)
        store_daily_aggregates(daily_aggs, date_str=day)
        logger.info(f"Stored data for {day}.")


if __name__ == "__main__":
    main()
