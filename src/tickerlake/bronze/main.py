"""Bronze layer data ingestion from Polygon.io grouped daily aggregates API. 🥉"""

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date
from typing import Callable, Iterator

import polars as pl
from tqdm import tqdm

from tickerlake.bronze.splits import get_splits
from tickerlake.bronze.tickers import get_tickers
from tickerlake.bronze.transformers import (
    convert_api_response_to_dicts,
    is_api_limit_error,
    transform_stocks_dataframe,
)
from tickerlake.clients import setup_polygon_api_client
from tickerlake.config import settings
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.storage import (
    get_table_path,
    load_checkpoints,
    read_table,
    save_checkpoints,
    table_exists,
    write_table,
)
from tickerlake.utils import (
    get_anomaly_reasons,
    get_trading_days,
    is_anomalous_count,
)

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

    # Always include today (API handles data availability)
    end_date = date.today()

    return get_trading_days(start_date, end_date)


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


@dataclass
class FetchSummary:
    """Collects results from parallel Bronze API fetches."""

    frames: list[pl.DataFrame]
    limit_reached: bool = False


def _fetch_single_date(
    fetch_date: str,
) -> tuple[str, pl.DataFrame | None, bool, Exception | None]:
    """Fetch and transform grouped daily aggregates for a single date."""
    client = setup_polygon_api_client()
    try:
        response = client.get_grouped_daily_aggs(
            fetch_date,
            adjusted=False,
            include_otc=False,
        )
        results = convert_api_response_to_dicts(response)
        if not results:
            return fetch_date, None, False, None

        df = transform_stocks_dataframe(pl.DataFrame(results))
        return fetch_date, df, False, None

    except Exception as exc:  # Capture API or client errors
        return fetch_date, None, is_api_limit_error(exc), exc


def _submit_next_date(
    date_iter: Iterator[str],
    executor: ThreadPoolExecutor,
    futures: dict[Future, str],
) -> bool:
    """Submit the next date fetch task if available."""
    try:
        next_date = next(date_iter)
    except StopIteration:
        return False

    futures[executor.submit(_fetch_single_date, next_date)] = next_date
    return True


def _cancel_pending_futures(futures: dict[Future, str]) -> None:
    """Cancel any pending futures and clear the mapping."""
    for future in list(futures):
        future.cancel()
        futures.pop(future, None)


def _handle_fetch_outcome(
    summary: FetchSummary,
    result_date: str,
    result_df: pl.DataFrame | None,
    hit_limit: bool,
    result_error: Exception | None,
) -> None:
    """Handle results coming back from worker threads."""
    if hit_limit:
        logger.info(
            f"🚫 Reached API subscription limit at {result_date}. "
            f"This is the furthest back we can access with your plan. "
            f"Stopping here."
        )
        summary.limit_reached = True
        return

    if result_error is not None:
        logger.error(f"❌ Failed to fetch data for {result_date}: {result_error}")
        return

    if result_df is None:
        logger.warning(f"⚠️  No data returned for {result_date}")
        return

    summary.frames.append(result_df)


def _download_grouped_daily_aggs_parallel(
    dates: list[str],
    max_workers: int,
    progress_callback: Callable[[str], None],
) -> FetchSummary:
    """Download grouped daily aggregates in parallel batches."""
    summary = FetchSummary(frames=[])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[Future, str] = {}
        date_iter = iter(dates)

        for _ in range(max_workers):
            if not _submit_next_date(date_iter, executor, futures):
                break

        while futures:
            done, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

            for future in done:
                fetch_date = futures.pop(future)
                try:
                    (
                        result_date,
                        result_df,
                        hit_limit,
                        result_error,
                    ) = future.result()
                except Exception as exc:
                    result_date = fetch_date
                    result_df = None
                    hit_limit = False
                    result_error = exc

                progress_callback(result_date)
                _handle_fetch_outcome(
                    summary,
                    result_date,
                    result_df,
                    hit_limit,
                    result_error,
                )

                if summary.limit_reached:
                    _cancel_pending_futures(futures)
                    break

                _submit_next_date(date_iter, executor, futures)

            if summary.limit_reached:
                break

    return summary


def load_grouped_daily_aggs(dates_to_fetch: list[str]) -> None:
    """Fetch grouped daily aggregates from Polygon API and save to Parquet.

    Fetches data from newest to oldest, stopping when a 403 error is encountered
    (indicating the historical data limit for the API subscription).

    Uses date-based Hive partitioning for efficient storage and queries.

    Args:
        dates_to_fetch: List of dates to fetch in YYYY-MM-DD format.
    """
    if not dates_to_fetch:
        logger.info("✅ No missing dates to fetch.")
        return

    stocks_path = get_table_path("bronze", "stocks", partitioned=True)
    table_already_exists = table_exists(stocks_path)
    max_workers = max(1, settings.bronze_parallel_requests)

    reversed_dates = sorted(dates_to_fetch, reverse=True)

    with tqdm(
        reversed_dates,
        desc="Fetching market data",
        unit="day",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
    ) as pbar:
        def update_progress(completed_date: str) -> None:
            pbar.update(1)
            pbar.set_postfix_str(completed_date, refresh=True)

        fetch_summary = _download_grouped_daily_aggs_parallel(
            reversed_dates,
            max_workers,
            update_progress,
        )

    if not fetch_summary.frames:
        return

    combined_df = pl.concat(fetch_summary.frames)
    if table_already_exists:
        existing_df = read_table(stocks_path)
        combined_df = pl.concat([existing_df, combined_df])

    write_table(stocks_path, combined_df, mode="overwrite", partition_by="date")


def _get_record_counts_by_date() -> list[tuple] | None:
    """Get record counts per day from Parquet dataset. 📊

    Returns:
        List of (date, count) tuples, or None if no data available.
    """
    stocks_path = get_table_path("bronze", "stocks", partitioned=True)

    if not table_exists(stocks_path):
        logger.warning("⚠️  No stocks table found for validation")
        return None

    # Use pure Polars for grouping (reads all partitions)
    stocks_df = read_table(stocks_path)

    if len(stocks_df) == 0:
        logger.warning("⚠️  No data found for validation")
        return None

    stats_df = (
        stocks_df
        .group_by("date")
        .agg(pl.len().alias("record_count"))
        .sort("date")
    )

    return list(zip(stats_df["date"], stats_df["record_count"]))


def _find_anomalies(stats: list[tuple]) -> list[tuple]:
    """Find days with anomalous record counts. 🔍

    Args:
        stats: List of (date, count) tuples.

    Returns:
        List of (date, count) tuples for anomalous days.
    """
    counts = [s[1] for s in stats]
    mean_count = sum(counts) / len(counts)

    return [
        (date_obj, count)
        for date_obj, count in stats
        if is_anomalous_count(count, mean_count)
    ]


def _report_anomalies(anomalies: list[tuple], mean_count: float) -> None:
    """Report anomalous record counts. 📢

    Args:
        anomalies: List of (date, count) tuples for anomalous days.
        mean_count: Mean record count across all days.
    """
    logger.warning(
        f"⚠️  Found {len(anomalies)} day(s) with abnormal record counts:"
    )
    for date_obj, count in anomalies:
        date_str = date_obj.strftime("%Y-%m-%d")
        reasons = get_anomaly_reasons(count, mean_count)
        logger.warning(
            f"   📅 {date_str}: {count} records ({', '.join(reasons)})"
        )


def validate_bronze_data() -> None:
    """Validate bronze data for abnormal record counts per day. ✅

    Checks each day's record count against statistical norms and absolute thresholds
    to detect potential data quality issues.
    """
    try:
        stats = _get_record_counts_by_date()
        if stats is None:
            return

        anomalies = _find_anomalies(stats)
        if anomalies:
            counts = [s[1] for s in stats]
            mean_count = sum(counts) / len(counts)
            _report_anomalies(anomalies, mean_count)

    except Exception as e:
        logger.warning(f"⚠️  Could not validate bronze data: {e}")


def main() -> None:  # pragma: no cover
    """Main function to load stocks data from Polygon.io API into Parquet files."""
    # Load splits and tickers (full refresh for these small tables)
    splits_df = get_splits()
    splits_path = get_table_path("bronze", "splits")
    write_table(splits_path, splits_df, mode="overwrite")
    logger.info(f"✅ Loaded {len(splits_df)} splits")

    tickers_df = get_tickers()
    tickers_path = get_table_path("bronze", "tickers")
    write_table(tickers_path, tickers_df, mode="overwrite")
    logger.info(f"✅ Loaded {len(tickers_df)} tickers")

    # Determine what dates we need (incremental based on existing data)
    required_dates = get_required_trading_days()

    # Get existing dates from partitioned Parquet dataset using pure Polars
    stocks_path = get_table_path("bronze", "stocks", partitioned=True)
    if table_exists(stocks_path):
        stocks_df = read_table(stocks_path)
        stored_dates = sorted([str(d) for d in stocks_df["date"].unique()])
    else:
        stored_dates = []

    missing_dates = get_missing_trading_days(required_dates, stored_dates)

    logger.info(
        f"📊 Trading days: {len(required_dates)} required, {len(stored_dates)} stored, {len(missing_dates)} to fetch"
    )

    # Fetch missing data and write to Parquet
    load_grouped_daily_aggs(dates_to_fetch=missing_dates)

    # Update checkpoint with latest date
    if len(missing_dates) > 0:
        checkpoints = load_checkpoints()
        checkpoints["bronze_stocks_last_date"] = str(max(missing_dates))
        save_checkpoints(checkpoints)

    # Validate data quality
    validate_bronze_data()

    logger.info("✅ Bronze layer complete!")


if __name__ == "__main__":  # pragma: no cover
    main()
