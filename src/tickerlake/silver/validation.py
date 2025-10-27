"""Spot check prices for stocks adjusted for splits. 🔍"""

from datetime import datetime

import polars as pl
from rich.console import Console
from rich.table import Table
from sqlalchemy import func, select

from tickerlake.bronze.models import splits as bronze_splits
from tickerlake.clients import setup_polygon_api_client
from tickerlake.db import get_engine
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.silver.models import daily_aggregates, weekly_aggregates
from tickerlake.utils import get_trading_days

setup_logging()
logger = get_logger(__name__)
console = Console()


def get_last_trading_day() -> str:
    """Get the most recent trading day from the silver Postgres layer."""
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            select(func.max(daily_aggregates.c.date))
        ).scalar()

    if result is None:
        raise ValueError("No trading days found in silver layer")

    return result.strftime("%Y-%m-%d")


def get_high_volume_tickers(min_volume: int = 250_000, min_price: float = 20.0) -> list[str]:
    """Get list of tickers with high volume and minimum price on the last trading day.

    Args:
        min_volume: Minimum volume threshold for ticker selection.
        min_price: Minimum closing price threshold (excludes low-priced stocks with frequent reverse splits).

    Returns:
        List of ticker symbols meeting volume and price criteria.
    """
    last_trading_day = get_last_trading_day()
    logger.info(f"📊 Getting high volume tickers from {last_trading_day}...")

    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            select(daily_aggregates.c.ticker)
            .where(daily_aggregates.c.date == datetime.strptime(last_trading_day, "%Y-%m-%d").date())
            .where(daily_aggregates.c.volume >= min_volume)
            .where(daily_aggregates.c.close >= min_price)
        )
        tickers = [row[0] for row in result]

    logger.info(f"✅ Found {len(tickers)} tickers with volume >= {min_volume:,} and price >= ${min_price:.2f}")
    return tickers


def build_split_list_to_check(num_splits: int = 25) -> pl.DataFrame:
    """Build a list of splits to check for high-volume tickers.

    Args:
        num_splits: Number of splits to randomly sample for validation.

    Returns:
        DataFrame containing split information for validation.

    """
    # First, get high volume tickers
    high_volume_tickers = get_high_volume_tickers()

    if not high_volume_tickers:
        logger.warning("⚠️  No high volume tickers found!")
        return pl.DataFrame()

    logger.info(
        f"🔍 Looking for splits among {len(high_volume_tickers)} high-volume tickers..."
    )

    # Calculate cutoff dates for validation window
    # - Upper bound: exclude splits from the last 5 trading days to avoid Polygon API issues
    #   and ensure we have enough data
    # - Lower bound: only check splits from the past 2 years
    from datetime import timedelta

    # Get the last trading day from silver data
    engine = get_engine()
    with engine.connect() as conn:
        silver_max_date = conn.execute(
            select(func.max(daily_aggregates.c.date))
        ).scalar()

    # Calculate date range: past 2 years, excluding last 5 trading days
    two_years_ago = (datetime.now() - timedelta(days=730)).date()

    # Get last 5 trading days to exclude
    from datetime import date as date_class

    # Handle case where silver_max_date might be None
    if silver_max_date is None:
        raise ValueError("No data found in silver layer")

    end_date_for_trading_days = min(silver_max_date, date_class.today())
    start_date_for_trading_days = (datetime.now() - timedelta(days=10)).date()

    recent_trading_days = get_trading_days(
        start_date_for_trading_days.strftime("%Y-%m-%d"),
        end_date_for_trading_days.strftime("%Y-%m-%d")
    )

    # Get the 5th most recent trading day (exclude last 5)
    cutoff_date_max = (
        datetime.strptime(recent_trading_days[-6], "%Y-%m-%d").date()
        if len(recent_trading_days) >= 6
        else (datetime.now() - timedelta(days=7)).date()
    )

    cutoff_date_min = two_years_ago

    logger.info(
        f"📅 Validating splits between {cutoff_date_min} and {cutoff_date_max} "
        f"(past 2 years, excluding last 5 trading days)..."
    )

    # Then get splits for those tickers within the date range from bronze Postgres
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            select(bronze_splits)
            .where(bronze_splits.c.ticker.in_(high_volume_tickers))
            .where(bronze_splits.c.execution_date >= cutoff_date_min)
            .where(bronze_splits.c.execution_date <= cutoff_date_max)
        )
        df = pl.DataFrame(result.fetchall(), schema=["ticker", "execution_date", "split_from", "split_to"])

    if df.is_empty():
        logger.warning("⚠️  No splits found for high volume tickers in date range!")
        return df

    # Sample the requested number (or all if fewer available)
    # Note: No seed parameter so we get a different random selection each time
    sample_size = min(num_splits, len(df))
    logger.info(f"🎲 Sampling {sample_size} splits from {len(df)} available...")

    return df.sample(n=sample_size, shuffle=True)


def get_trading_days_around_split(split_date: str) -> dict[str, str | None]:
    """Get the trading day before, of, and after a split date.

    Args:
        split_date: The split execution date in YYYY-MM-DD format.

    Returns:
        Dictionary with keys 'before', 'split', and 'after' containing trading dates.

    """
    from datetime import timedelta

    split_dt = datetime.strptime(split_date, "%Y-%m-%d")
    # Get a wider window to ensure we capture trading days
    start = (split_dt - timedelta(days=10)).strftime("%Y-%m-%d")
    end = (split_dt + timedelta(days=10)).strftime("%Y-%m-%d")

    trading_days = get_trading_days(start, end)

    # Find the split date index
    if split_date not in trading_days:
        # If split date isn't a trading day, find the next trading day
        split_idx = next(
            (i for i, d in enumerate(trading_days) if d >= split_date),
            len(trading_days) - 1,
        )
    else:
        split_idx = trading_days.index(split_date)

    return {
        "before": trading_days[split_idx - 1] if split_idx > 0 else None,
        "split": trading_days[split_idx],
        "after": trading_days[split_idx + 1]
        if split_idx < len(trading_days) - 1
        else None,
    }


def get_official_stock_prices_around_split(
    ticker: str, split_date: str
) -> dict[str, float | None]:
    """Retrieve official adjusted stock prices from Polygon around split date.

    Args:
        ticker: Stock ticker symbol.
        split_date: Split execution date in YYYY-MM-DD format.

    Returns:
        Dictionary mapping date keys ('before', 'split', 'after') to closing prices.

    """
    polygon_client = setup_polygon_api_client()

    # Get actual trading days around the split
    trading_dates = get_trading_days_around_split(split_date)

    # Get date range for API call
    dates_list = [d for d in trading_dates.values() if d is not None]
    if not dates_list:
        return {}

    start_date = min(dates_list)
    end_date = max(dates_list)

    # Fetch adjusted prices from Polygon
    aggs = []
    for a in polygon_client.list_aggs(
        ticker, 1, "day", start_date, end_date, adjusted=True
    ):
        aggs.append(a)

    # Build price lookup (use UTC since Polygon timestamps are in UTC)
    from datetime import timezone

    price_lookup = {
        datetime.fromtimestamp(x.timestamp / 1000, tz=timezone.utc).strftime("%Y-%m-%d"): x.close
        for x in aggs
    }

    # Map to before/split/after structure
    return {
        key: price_lookup.get(date) if date else None
        for key, date in trading_dates.items()
    }


def get_silver_stock_prices_around_split(
    ticker: str, split_date: str
) -> dict[str, float | None]:
    """Retrieve silver stock prices around split date from Postgres.

    Args:
        ticker: Stock ticker symbol.
        split_date: Split execution date in YYYY-MM-DD format.

    Returns:
        Dictionary mapping date keys ('before', 'split', 'after') to closing prices.
    """
    # Get actual trading days around the split
    trading_dates = get_trading_days_around_split(split_date)

    # Get date range
    dates_list = [d for d in trading_dates.values() if d is not None]
    if not dates_list:
        return {}

    start_date = datetime.strptime(min(dates_list), "%Y-%m-%d").date()
    end_date = datetime.strptime(max(dates_list), "%Y-%m-%d").date()

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(
            select(daily_aggregates.c.date, daily_aggregates.c.close)
            .where(daily_aggregates.c.ticker == ticker)
            .where(daily_aggregates.c.date >= start_date)
            .where(daily_aggregates.c.date <= end_date)
        )
        rows = result.fetchall()

    # Build price lookup
    price_lookup = {
        row[0].strftime("%Y-%m-%d"): float(row[1])
        for row in rows
    }

    # Map to before/split/after structure
    return {
        key: price_lookup.get(date) if date else None
        for key, date in trading_dates.items()
    }


def _compare_single_period(
    api_price: float | None, silver_price: float | None
) -> dict[str, bool | None | str]:
    """Compare prices for a single period.

    Args:
        api_price: Price from Polygon API (or None if unavailable).
        silver_price: Price from silver layer (or None if unavailable).

    Returns:
        Dictionary with match status, display status, and formatted price display.

    """
    if api_price is None and silver_price is None:
        return {"match": None, "status": "⚠️ Both N/A", "display": "N/A"}

    if api_price is None:
        return {
            "match": False,
            "status": "⚠️ Polygon N/A",
            "display": f"S:{silver_price:.2f}",
        }

    if silver_price is None:
        return {"match": False, "status": "❌ Missing", "display": f"P:{api_price:.2f}"}

    # Both prices exist - check if they match (within small tolerance)
    match = abs(api_price - silver_price) < 0.01
    return {
        "match": match,
        "status": "✅" if match else "❌",
        "display": f"P:{api_price:.2f} S:{silver_price:.2f}",
    }


def _calculate_accuracy(comparisons: dict[str, dict]) -> tuple[float, str]:
    """Calculate overall accuracy from period comparisons.

    Args:
        comparisons: Dictionary of comparison results by period.

    Returns:
        Tuple of (accuracy percentage, match count string).

    """
    valid_comparisons = [
        c["match"] for c in comparisons.values() if c["match"] is not None
    ]

    if not valid_comparisons:
        return 0.0, "0/0"

    matches = sum(1 for m in valid_comparisons if m is True)
    accuracy = (matches / len(valid_comparisons)) * 100
    total_matches = f"{matches}/{len(valid_comparisons)}"

    return accuracy, total_matches


def compare_prices(
    ticker: str,
    split_date: str,
    split_ratio: str,
    api_prices: dict[str, float | None],
    silver_prices: dict[str, float | None],
) -> dict:
    """Compare API prices vs Silver prices and determine accuracy.

    Args:
        ticker: Stock ticker symbol.
        split_date: Split execution date.
        split_ratio: Split ratio (e.g., "2-for-1").
        api_prices: Official prices from Polygon API.
        silver_prices: Prices from silver layer.

    Returns:
        Dictionary with comparison results for each period and overall status.

    """
    comparisons = {
        period: _compare_single_period(api_prices.get(period), silver_prices.get(period))
        for period in ["before", "split", "after"]
    }

    accuracy, total_matches = _calculate_accuracy(comparisons)

    return {
        "ticker": ticker,
        "split_date": split_date,
        "split_ratio": split_ratio,
        "comparisons": comparisons,
        "accuracy": accuracy,
        "total_matches": total_matches,
    }


def _validate_single_split(row: dict) -> dict | None:
    """Validate prices for a single split.

    Args:
        row: Split information row containing ticker, execution_date, split_to, split_from.

    Returns:
        Validation result dictionary, or None if validation failed/skipped.

    Raises:
        Exception: Re-raises exceptions for caller to handle.

    """
    ticker = row["ticker"]
    split_date = row["execution_date"].strftime("%Y-%m-%d")
    split_ratio = f"{row['split_to']:.2f}-for-{row['split_from']:.2f}"

    api_prices = get_official_stock_prices_around_split(ticker, split_date)
    silver_prices = get_silver_stock_prices_around_split(ticker, split_date)

    if not api_prices and not silver_prices:
        logger.debug(f"No price data found for {ticker} on {split_date}")
        return None

    return compare_prices(ticker, split_date, split_ratio, api_prices, silver_prices)


def _format_accuracy_display(accuracy: float) -> str:
    """Format accuracy percentage with color coding.

    Args:
        accuracy: Accuracy percentage (0-100).

    Returns:
        Formatted string with Rich color markup.

    """
    if accuracy == 100:
        return f"[bold green]{accuracy:.0f}%[/bold green]"
    if accuracy >= 66:
        return f"[yellow]{accuracy:.0f}%[/yellow]"
    return f"[red]{accuracy:.0f}%[/red]"


def _display_results_table(results: list[dict], skipped_tickers: list[str]) -> None:
    """Display validation results in a formatted table.

    Args:
        results: List of validation result dictionaries.
        skipped_tickers: List of tickers that were skipped due to plan limitations.

    """
    if not results:
        console.print("[yellow]⚠️  No validation results to display.[/yellow]\n")
        return

    console.print("\n[bold cyan]📊 Split Adjustment Validation Results[/bold cyan]\n")

    validation_table = Table(
        title="🎯 Price Verification: Polygon (P) vs Silver (S)",
        show_header=True,
        header_style="bold magenta",
    )
    validation_table.add_column("🎫 Ticker", style="cyan", justify="center")
    validation_table.add_column("📅 Split Date", style="yellow", justify="center")
    validation_table.add_column("🔢 Split Ratio", style="blue", justify="center")
    validation_table.add_column("📉 Day Before", justify="right")
    validation_table.add_column("📊 Split Day", justify="right")
    validation_table.add_column("📈 Day After", justify="right")
    validation_table.add_column("✅ Match", justify="center")
    validation_table.add_column("🎯 Accuracy", justify="right")

    for result in results:
        comparisons = result["comparisons"]
        validation_table.add_row(
            result["ticker"],
            result["split_date"],
            result["split_ratio"],
            f"{comparisons['before']['status']} {comparisons['before']['display']}",
            f"{comparisons['split']['status']} {comparisons['split']['display']}",
            f"{comparisons['after']['status']} {comparisons['after']['display']}",
            result["total_matches"],
            _format_accuracy_display(result["accuracy"]),
        )

    console.print(validation_table)

    # Calculate overall accuracy
    overall_accuracy = sum(r["accuracy"] for r in results) / len(results)
    console.print(f"\n[bold green]🎯 Overall Accuracy: {overall_accuracy:.1f}%[/bold green]")

    # Show summary of skipped tickers if any
    if skipped_tickers:
        console.print(
            f"[dim]ℹ️  Skipped {len(skipped_tickers)} ticker(s) due to Polygon plan limitations: {', '.join(skipped_tickers)}[/dim]\n"
        )
    else:
        console.print()


def main() -> None:  # pragma: no cover
    """Run split validation checks and display summary results."""
    console.print("\n[bold cyan]🔍 Starting Split Validation[/bold cyan]\n")

    splits_df = build_split_list_to_check(num_splits=25)

    if splits_df.is_empty():
        console.print("[yellow]⚠️  No splits to validate![/yellow]")
        return

    results = []
    skipped_tickers = []

    for row in splits_df.iter_rows(named=True):
        ticker = row["ticker"]
        try:
            result = _validate_single_split(row)
            if result:
                results.append(result)
        except Exception as e:
            error_msg = str(e)
            # Check if this is a Polygon plan limitation error
            if "NOT_AUTHORIZED" in error_msg or "Your plan doesn't include" in error_msg:
                logger.debug(f"Skipping {ticker}: not included in Polygon plan")
                skipped_tickers.append(ticker)
            else:
                # For other errors, log and display them
                logger.error(f"❌ Error processing {ticker}: {e}")
                console.print(f"[red]❌ Error processing {ticker}: {e}[/red]\n")

    _display_results_table(results, skipped_tickers)


if __name__ == "__main__":  # pragma: no cover
    main()
