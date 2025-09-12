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


def build_polygon_client() -> RESTClient:
    """Build and return a Polygon.io REST client."""
    return RESTClient(settings.polygon_api_key.get_secret_value())


def get_missing_stock_aggs() -> None:
    """
    Identifies missing trading days, downloads daily stock aggregates for each missing day,
    and stores the retrieved data.

    This function performs the following steps:
    1. Retrieves a list of trading days for which aggregate data is missing.
    2. Logs the number of missing days found.
    3. For each missing day:
        a. Downloads the daily aggregate stock data.
        b. Stores the downloaded data.
        c. Logs the completion of data storage for that day.

    Returns:
        None
    """
    missing_days = get_missing_trading_days()
    logger.info(f"Found {len(missing_days)} missing days.")

    for day in missing_days:
        daily_aggs = download_daily_aggregates(date_str=day)
        store_daily_aggregates(daily_aggs, date_str=day)
        logger.info(f"Stored data for {day}.")


def download_daily_aggregates(date_str: str) -> pl.DataFrame:
    """Download daily stock market aggregates from Polygon.io API.

    Args:
        date_str: Trading day in YYYY-MM-DD format.

    Returns:
        DataFrame containing daily aggregates sorted by ticker.

    """
    client = build_polygon_client()
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
    df.write_parquet(file=path, storage_options=s3_storage_options)


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


def get_ticker_details() -> pl.DataFrame:
    """
    Fetches active stock tickers from the Polygon API and returns their details as a Polars DataFrame.

    Returns:
        pl.DataFrame: A DataFrame containing details of up to 1000 active stock tickers.

    Raises:
        Any exceptions raised by the Polygon API client during ticker retrieval.

    Note:
        The function logs the number of tickers fetched.
    """
    client = build_polygon_client()

    tickers = [
        t
        for t in client.list_tickers(
            market="stocks",
            active=True,
            order="asc",
            sort="ticker",
            limit=1000,
        )
    ]

    logger.info(f"Fetched {len(tickers)} active tickers")
    return pl.DataFrame(tickers)


def write_ticker_details(df: pl.DataFrame) -> None:
    """
    Writes the provided Polars DataFrame containing ticker details to a Parquet file in an S3 bucket.

    Args:
        df (pl.DataFrame): The DataFrame containing ticker details to be written.

    Returns:
        None

    Side Effects:
        Saves the DataFrame as a Parquet file to the specified S3 path using the configured storage options.
    """
    path = f"s3://{settings.s3_bucket_name}/bronze/tickers/data.parquet"
    df.write_parquet(file=path, storage_options=s3_storage_options)


def get_split_details() -> pl.DataFrame:
    """
    Fetches recent stock split details from the Polygon API and returns them as a Polars DataFrame.

    Returns:
        pl.DataFrame: A DataFrame containing the fetched split details, with at least an 'id' column of type string.

    Logs:
        The number of splits fetched.
    """
    client = build_polygon_client()

    splits = [
        s
        for s in client.list_splits(
            execution_date_gte=settings.data_start_date,
            order="asc",
            sort="execution_date",
            limit=1000,
        )
    ]

    logger.info(f"Fetched {len(splits)} recent splits")
    return pl.DataFrame(splits, schema={"id": pl.String})


def write_split_details(df: pl.DataFrame) -> None:
    """
    Writes the provided Polars DataFrame containing split details to a Parquet file in an S3 bucket.

    Args:
        df (pl.DataFrame): The DataFrame containing split details to be written.

    Returns:
        None

    Side Effects:
        Writes the DataFrame to the specified S3 location using the provided storage options.
    """
    path = f"s3://{settings.s3_bucket_name}/bronze/splits/data.parquet"
    df.write_parquet(file=path, storage_options=s3_storage_options)


def write_ssga_holdings(source_url: str, etf_ticker: str) -> None:
    """
    Reads ETF holdings data from an Excel file, filters for USD-denominated tickers,
    and writes the sorted list of tickers to a Parquet file in an S3 bucket.

    Args:
        source_url (str): The URL or path to the Excel file containing holdings data.
        etf_ticker (str): The ticker symbol of the ETF whose holdings are being processed.

    Returns:
        None
    """
    logger.info(f"Writing {etf_ticker} holdings")
    df = (
        pl.read_excel(
            source_url,
            sheet_name="holdings",
            read_options={"header_row": 4},
        )
        .rename({"Ticker": "ticker"})
        .filter((pl.col("Local Currency") == "USD") & (pl.col("ticker") != "-"))
        .select("ticker")
        .sort("ticker")
    )
    path = f"s3://{settings.s3_bucket_name}/bronze/holdings/{etf_ticker.lower()}/data.parquet"
    df.write_parquet(file=path, storage_options=s3_storage_options)


def write_qqq_holdings() -> None:
    """
    Reads QQQ holdings data from a CSV file, processes it to extract and sort ticker symbols,
    and writes the result as a Parquet file to an S3 bucket.

    The function performs the following steps:
    1. Reads the source CSV file specified in `settings.qqq_holdings_source`.
    2. Renames the "Holding Ticker" column to "ticker".
    3. Selects only the "ticker" column and sorts it.
    4. Writes the processed DataFrame to the specified S3 path in Parquet format using `s3_storage_options`.

    Returns:
        None
    """
    logger.info("Writing QQQ holdings")
    df = (
        pl.read_csv(settings.qqq_holdings_source)
        .rename({"Holding Ticker": "ticker"})
        .select(["ticker"])
        .sort("ticker")
    )
    path = f"s3://{settings.s3_bucket_name}/bronze/holdings/qqq/data.parquet"
    df.write_parquet(file=path, storage_options=s3_storage_options)


def write_iwm_holdings() -> None:
    """
    Reads IWM holdings data from a CSV file, filters for USD market currency and valid tickers,
    sorts the tickers, and prepares the DataFrame for further processing.

    The CSV source and column names are specified in the settings.
    """
    logger.info("Writing IWM holdings")
    df = (
        pl.read_csv(
            settings.iwm_holdings_source,
            skip_rows=9,
            columns=["Ticker", "Market Currency"],
        )
        .rename({"Ticker": "ticker"})
        .filter((pl.col("Market Currency") == "USD") & (pl.col("ticker") != "-"))
        .select(["ticker"])
        .sort("ticker")
    )
    path = f"s3://{settings.s3_bucket_name}/bronze/holdings/iwm/data.parquet"
    df.write_parquet(file=path, storage_options=s3_storage_options)


def write_spy_holdings() -> None:
    """
    Writes the holdings data for the SPY ETF to the target destination.

    This function retrieves SPY holdings from the source specified in the settings
    and delegates the writing process to the `write_ssga_holdings` function.

    Returns:
        None
    """
    write_ssga_holdings(settings.spy_holdings_source, "SPY")


def write_mdy_holdings() -> None:
    """
    Writes the holdings data for the MDY fund to the target destination.

    This function retrieves the MDY holdings from the source specified in the settings
    and writes them using the `write_ssga_holdings` function.

    Returns:
        None
    """
    write_ssga_holdings(settings.mdy_holdings_source, "MDY")


def write_spsm_holdings() -> None:
    """
    Writes the holdings data for the SPSM fund by invoking the write_ssga_holdings function
    with the SPSM holdings source and the fund identifier "SPSM".

    Returns:
        None
    """
    write_ssga_holdings(settings.spsm_holdings_source, "SPSM")


def main():
    """
    Main entry point for processing and writing financial data.

    This function performs the following tasks:
    1. Retrieves and processes missing stock aggregate data.
    2. Writes ticker details and split details to storage.
    3. Writes holdings data for various ETFs including SPY, MDY, SPSM, QQQ, and IWM.
    """

    # Polygon data
    get_missing_stock_aggs()
    write_ticker_details(get_ticker_details())
    write_split_details(get_split_details())

    # ETF data
    write_spy_holdings()
    write_mdy_holdings()
    write_spsm_holdings()
    write_qqq_holdings()
    write_iwm_holdings()


if __name__ == "__main__":
    main()
