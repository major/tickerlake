"""Bronze layer flatfile processing for stocks data from Polygon.io."""

from datetime import date
from pathlib import Path

import polars as pl
from tqdm import tqdm

from tickerlake.bronze.schemas import (
    STOCKS_SCHEMA,
    STOCKS_SCHEMA_MODIFIED,
)
from tickerlake.bronze.splits import load_splits
from tickerlake.bronze.tickers import load_tickers
from tickerlake.clients import (
    POLYGON_STORAGE_OPTIONS,
    setup_polygon_flatfiles_client,
)
from tickerlake.config import settings
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

# Flip to verbose mode for Polars if needed
pl.Config.set_verbose(False)


def stocks_flatfiles_valid_years() -> list[int]:
    return valid_flatfiles_years(
        first_year_available=settings.polygon_flatfiles_stocks_first_year
    )


def valid_flatfiles_years(first_year_available: int) -> list[int]:
    valid_years = range(first_year_available, date.today().year + 1)
    return sorted(valid_years, reverse=True)


def list_available_stocks_flatfiles() -> list[str]:
    """List available stock flatfiles in the Polygon S3 bucket."""
    return list_available_flatfiles(
        flatfiles_path=settings.polygon_flatfiles_stocks,
        valid_years=stocks_flatfiles_valid_years(),
    )


def list_available_flatfiles(flatfiles_path: str, valid_years: list[int]) -> list[str]:
    """List available stock flatfiles in the Polygon S3 bucket."""
    polygon_s3 = setup_polygon_flatfiles_client()
    logger.info(f"Listing available flatfiles in polygon's s3 for {flatfiles_path}...")

    all_files = [
        polygon_s3.glob(f"{flatfiles_path}/{year}/*/*.csv.gz") for year in valid_years
    ]
    path_list = sorted(
        [f"s3://{path}" for sublist in all_files for path in sublist], reverse=True
    )
    logger.info(f"Found {len(path_list)} flatfiles available.")
    return path_list


def get_missing_dates(
    already_stored_dates: list[str], stored_files: list[str]
) -> list[str]:
    """Return files from stored_files that do not contain any date from already_stored_dates in their filename."""
    return [
        f
        for f in stored_files
        if not any(d in Path(f).name for d in already_stored_dates)
    ]


def previously_stored_dates(destination: str, schema: dict) -> list:
    """Retrieve previously stored dates from the destination Parquet files."""
    logger.info("Retrieving previously stored dates...")
    lf = (
        pl.scan_parquet(
            f"{destination}/date=*/*.parquet",
            schema=schema,
        )
        .select(pl.col("date").dt.strftime("%Y-%m-%d").alias("date"))
        .unique()
        .sort("date")
    )
    return lf.collect().to_series().to_list()


def load_polygon_flatfiles(
    files_to_process: list[str], destination_path: str, schema: dict
) -> None:
    # Extract filename from S3 path for display
    def get_filename(s3_path: str) -> str:
        return Path(s3_path).name

    # Process files with progress bar showing detailed status
    with tqdm(
        files_to_process,
        desc="Processing flatfiles",
        unit="file",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    ) as pbar:
        for source_path in pbar:
            # Update progress bar to show current file being processed
            filename = get_filename(source_path)
            pbar.set_postfix_str(filename, refresh=True)

            lf = (
                pl.scan_csv(
                    source_path,
                    storage_options=POLYGON_STORAGE_OPTIONS,
                    schema_overrides=schema,
                )
                .with_columns(
                    # Polygon uses nanosecond timestamps but all I need is the date (no time).
                    pl.col("window_start")
                    .cast(pl.Datetime("ns"))
                    .cast(pl.Date)
                    .alias("date")
                )
                .drop("window_start")
            )

            try:
                lf.collect().write_parquet(
                    destination_path,
                    partition_by=["date"],
                )
            except OSError:
                logger.info(f"✔️ Reached the last file: {filename}")
                break


def main() -> None:  # pragma: no cover
    """Main function to load stocks flatfiles from Polygon.io into local storage."""
    # Splits
    load_splits()

    # Tickers
    load_tickers()

    # Stocks - ensure directory exists before processing
    stocks_path = Path(f"{settings.bronze_storage_path}/stocks")
    stocks_path.mkdir(parents=True, exist_ok=True)

    stocks_dates_already_stored = previously_stored_dates(
        settings.bronze_storage_path + "/stocks", STOCKS_SCHEMA_MODIFIED
    )
    stocks_files_to_process = get_missing_dates(
        already_stored_dates=stocks_dates_already_stored,
        stored_files=list_available_stocks_flatfiles(),
    )
    load_polygon_flatfiles(
        files_to_process=stocks_files_to_process,
        destination_path=settings.bronze_storage_path + "/stocks",
        schema=STOCKS_SCHEMA,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
