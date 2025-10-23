"""Bronze layer data ingestion from Polygon.io grouped daily aggregates API."""

from datetime import date, datetime
from pathlib import Path

import polars as pl
from tqdm import tqdm

from tickerlake.schemas import (
    STOCKS_RAW_SCHEMA_MODIFIED,
)
from tickerlake.bronze.splits import load_splits
from tickerlake.bronze.tickers import load_tickers
from tickerlake.clients import setup_polygon_api_client
from tickerlake.config import settings
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.utils import get_trading_days, is_data_available_for_today

setup_logging()
logger = get_logger(__name__)

# Flip to verbose mode for Polars if needed
pl.Config.set_verbose(False)


def get_required_trading_days() -> list[str]:
    """Get all required trading days from data_start_year to today.

    Returns:
        List of trading days in YYYY-MM-DD format.
    """
    start_date = date(settings.data_start_year, 1, 1)

    # Include today if market has been closed for 30+ minutes
    end_date = date.today() if is_data_available_for_today() else date.today()

    return get_trading_days(start_date, end_date)


def previously_stored_dates(destination: str, schema: dict) -> list[str]:
    """Retrieve previously stored dates from the destination Parquet files.

    Args:
        destination: Path to the parquet files.
        schema: Schema to use for reading.

    Returns:
        List of dates already stored in YYYY-MM-DD format.
    """
    logger.info("Retrieving previously stored dates...")

    try:
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
    except Exception:
        # No existing data
        logger.info("No existing data found, will fetch all required dates.")
        return []


def get_missing_trading_days(
    required_dates: list[str], stored_dates: list[str]
) -> list[str]:
    """Return dates that are required but not yet stored.

    Args:
        required_dates: All dates that should be present.
        stored_dates: Dates that are already stored.

    Returns:
        List of missing dates in YYYY-MM-DD format.
    """
    stored_set = set(stored_dates)
    missing = [d for d in required_dates if d not in stored_set]
    return sorted(missing)


def load_grouped_daily_aggs(
    dates_to_fetch: list[str], destination_path: str
) -> None:
    """Fetch grouped daily aggregates from Polygon API and save to parquet.

    Fetches data from newest to oldest, stopping when a 403 error is encountered
    (indicating the historical data limit for the API subscription).

    Args:
        dates_to_fetch: List of dates to fetch in YYYY-MM-DD format.
        destination_path: Path to write parquet files.
    """
    if not dates_to_fetch:
        logger.info("✅ No missing dates to fetch.")
        return

    client = setup_polygon_api_client()

    # 🔄 Reverse the list to fetch newest dates first
    reversed_dates = sorted(dates_to_fetch, reverse=True)

    # Process dates with progress bar
    with tqdm(
        reversed_dates,
        desc="Fetching market data",
        unit="day",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    ) as pbar:
        for fetch_date in pbar:
            pbar.set_postfix_str(fetch_date, refresh=True)

            try:
                # Fetch grouped daily aggregates (unadjusted, no OTC)
                response = client.get_grouped_daily_aggs(
                    fetch_date,
                    adjusted=False,
                    include_otc=False,
                )

                # Convert response to list of dicts
                results = [
                    {
                        "ticker": r.ticker,
                        "volume": r.volume,
                        "open": r.open,
                        "close": r.close,
                        "high": r.high,
                        "low": r.low,
                        "window_start": r.timestamp,  # Unix timestamp in milliseconds
                        "transactions": r.transactions,
                    }
                    for r in response
                ]

                if not results:
                    logger.warning(f"⚠️  No data returned for {fetch_date}")
                    continue

                # Create DataFrame and add date column
                df = (
                    pl.DataFrame(results)
                    .with_columns([
                        # Convert timestamp to date
                        pl.col("window_start")
                        .cast(pl.Datetime("ms"))
                        .cast(pl.Date)
                        .alias("date"),
                        # Convert ticker to categorical for consistency
                        pl.col("ticker").cast(pl.Categorical),
                    ])
                    .drop("window_start")
                )

                # Write to parquet with date partitioning
                df.write_parquet(
                    destination_path,
                    partition_by=["date"],
                )

            except Exception as e:
                # 🛑 Check if we hit a 403 (API subscription limit reached)
                if "403" in str(e) or "Forbidden" in str(e):
                    logger.info(
                        f"🚫 Reached API subscription limit at {fetch_date}. "
                        f"This is the furthest back we can access with your plan. "
                        f"Stopping here."
                    )
                    break

                logger.error(f"❌ Failed to fetch data for {fetch_date}: {e}")
                continue


def validate_bronze_data() -> None:
    """Validate bronze data for abnormal record counts per day.

    Checks each day's record count against statistical norms and absolute thresholds
    to detect potential data quality issues.
    """
    stocks_path = f"{settings.bronze_storage_path}/stocks"

    try:
        # Read all bronze data and count records per day
        df = (
            pl.scan_parquet(
                f"{stocks_path}/date=*/*.parquet",
                schema=STOCKS_RAW_SCHEMA_MODIFIED,
            )
            .select("date")
            .group_by("date")
            .agg(pl.len().alias("record_count"))
            .sort("date")
            .collect()
        )

        if len(df) == 0:
            logger.warning("⚠️  No data found for validation")
            return

        # Calculate statistical measures
        mean_count = df["record_count"].mean()
        std_count = df["record_count"].std()
        min_count = df["record_count"].min()
        max_count = df["record_count"].max()

        logger.info(f"📊 Record count statistics:")
        logger.info(f"   Mean: {mean_count:.0f} ± {std_count:.0f}")
        logger.info(f"   Range: {min_count} to {max_count}")

        # Define thresholds for catching REAL problems
        # Relative thresholds: flag if < 50% or > 200% of mean (extreme outliers only)
        relative_min = mean_count * 0.50
        relative_max = mean_count * 2.00

        # Absolute minimum: markets should have at least 5000 tickers
        absolute_min = 5000

        # Check for anomalies (only serious issues)
        anomalies = df.filter(
            (pl.col("record_count") < relative_min) |
            (pl.col("record_count") > relative_max) |
            (pl.col("record_count") < absolute_min)
        )

        if len(anomalies) > 0:
            logger.warning(f"⚠️  Found {len(anomalies)} day(s) with abnormal record counts:")
            for row in anomalies.iter_rows(named=True):
                date_str = row["date"].strftime("%Y-%m-%d")
                count = row["record_count"]
                pct_of_mean = (count / mean_count) * 100

                reasons = []
                if count < absolute_min:
                    reasons.append(f"below absolute minimum of {absolute_min}")
                if count < relative_min:
                    reasons.append(f"only {pct_of_mean:.0f}% of mean")
                if count > relative_max:
                    reasons.append(f"{pct_of_mean:.0f}% of mean (unusually high)")

                logger.warning(f"   📅 {date_str}: {count} records ({', '.join(reasons)})")
        else:
            logger.info("✅ All days have reasonable record counts")

    except Exception as e:
        logger.warning(f"⚠️  Could not validate bronze data: {e}")


def main() -> None:  # pragma: no cover
    """Main function to load stocks data from Polygon.io API into local storage."""
    # Splits
    load_splits()

    # Tickers
    load_tickers()

    # Stocks - ensure directory exists before processing
    stocks_path = Path(f"{settings.bronze_storage_path}/stocks")
    stocks_path.mkdir(parents=True, exist_ok=True)

    # Determine what dates we need
    required_dates = get_required_trading_days()
    logger.info(f"📅 Required trading days: {len(required_dates)} dates")

    stored_dates = previously_stored_dates(
        f"{settings.bronze_storage_path}/stocks",
        STOCKS_RAW_SCHEMA_MODIFIED,
    )
    logger.info(f"💾 Already stored: {len(stored_dates)} dates")

    missing_dates = get_missing_trading_days(required_dates, stored_dates)
    logger.info(f"📥 Missing dates to fetch: {len(missing_dates)}")

    # Fetch missing data
    load_grouped_daily_aggs(
        dates_to_fetch=missing_dates,
        destination_path=f"{settings.bronze_storage_path}/stocks",
    )

    # 🔍 Validate data quality
    logger.info("🔍 Validating bronze data quality...")
    validate_bronze_data()

    logger.info("✅ Bronze layer data ingestion complete!")


if __name__ == "__main__":  # pragma: no cover
    main()
