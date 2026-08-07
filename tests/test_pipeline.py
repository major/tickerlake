"""Tests for tickerlake.pipeline — backfill, update, and info orchestration."""

import datetime
import os
import tempfile
from pathlib import Path
from unittest.mock import DEFAULT, patch

import duckdb
import polars as pl
import pytest
from rich.console import Console

_PIPELINE = "tickerlake.pipeline"


def _make_config(tmp_path: Path):
    """Build a Config pointing at tmp_path with a fake API key."""
    os.environ["MASSIVE_API_KEY"] = "test_key"
    from tickerlake.config import Config

    return Config(
        output_dir=tmp_path,
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 1, 31),
    )


def test_find_ticker_pivots_uses_adjusted_daily_consumer_data(
    tmp_path: Path, sample_bars: pl.DataFrame
) -> None:
    """find_ticker_pivots() reads adjusted daily bars then derives timeframe pivots."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    timeframe_bars = sample_bars.filter(pl.col("ticker") == "AAPL")
    expected = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2)],
            "ticker": ["AAPL"],
            "pivot_type": ["high"],
            "price": [152.0],
            "confirmed_at": [datetime.date(2024, 1, 9)],
        }
    )

    with patch.multiple(
        _PIPELINE,
        read_adjusted_daily_bars_for_ticker=DEFAULT,
        bars_for_timeframe=DEFAULT,
        find_pivots=DEFAULT,
        write_consumer_db=DEFAULT,
        write_raw_db=DEFAULT,
        write_splits=DEFAULT,
    ) as mocks:
        mocks["read_adjusted_daily_bars_for_ticker"].return_value = sample_bars
        mocks["bars_for_timeframe"].return_value = timeframe_bars
        mocks["find_pivots"].return_value = expected
        result = pipeline.find_ticker_pivots(config, "AAPL", "weekly", 5)

        mocks["read_adjusted_daily_bars_for_ticker"].assert_called_once_with(
            config.output_dir / "tickerlake.duckdb", "AAPL"
        )
        mocks["bars_for_timeframe"].assert_called_once_with(sample_bars, "weekly")
        mocks["find_pivots"].assert_called_once_with(timeframe_bars, k=5)
        mocks["write_consumer_db"].assert_not_called()
        mocks["write_raw_db"].assert_not_called()
        mocks["write_splits"].assert_not_called()
        assert result.equals(expected)


def test_pivots_logs_empty_result(tmp_path: Path, caplog) -> None:
    """pivots() logs a warning when no pivots are found."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    empty = pl.DataFrame(
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "pivot_type": pl.Utf8,
            "price": pl.Float32,
            "confirmed_at": pl.Date,
        }
    )

    with patch(f"{_PIPELINE}.find_ticker_pivots", return_value=empty):
        pipeline.pivots(config, "AAPL", "daily", 3)

    assert "No pivots found for AAPL" in caplog.text


def test_pivots_logs_non_empty_result(tmp_path: Path, caplog) -> None:
    """pivots() logs one INFO line per pivot row when pivots are found."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    pivots_df = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 5)],
            "ticker": ["AAPL", "AAPL"],
            "pivot_type": ["high", "low"],
            "price": [152.0, 148.0],
            "confirmed_at": [datetime.date(2024, 1, 9), datetime.date(2024, 1, 12)],
        }
    )

    with (
        patch(f"{_PIPELINE}.find_ticker_pivots", return_value=pivots_df),
        caplog.at_level("INFO"),
    ):
        pipeline.pivots(config, "AAPL", "weekly", 4)

    assert "Pivots for AAPL (weekly, k=4)" in caplog.text
    assert "high" in caplog.text
    assert "low" in caplog.text
    assert "152.00" in caplog.text
    assert "148.00" in caplog.text
    assert "2024-01-09" in caplog.text
    assert "2024-01-12" in caplog.text


def test_find_ticker_pivots_invalid_k_does_not_read_db(tmp_path: Path) -> None:
    """find_ticker_pivots() validates k before reading the consumer DB."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)

    with (
        patch(f"{_PIPELINE}.read_adjusted_daily_bars_for_ticker") as read_bars,
        pytest.raises(ValueError, match="k must be >= 1"),
    ):
        pipeline.find_ticker_pivots(config, "AAPL", "weekly", 0)

    read_bars.assert_not_called()


def test_find_ticker_pivots_invalid_timeframe_does_not_read_db(
    tmp_path: Path,
) -> None:
    """find_ticker_pivots() validates timeframe before reading the consumer DB."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)

    with (
        patch(f"{_PIPELINE}.read_adjusted_daily_bars_for_ticker") as read_bars,
        pytest.raises(ValueError, match="timeframe must be one of"),
    ):
        pipeline.find_ticker_pivots(config, "AAPL", "yearly", 5)

    read_bars.assert_not_called()


@pytest.fixture
def sample_bars():
    """Minimal bars DataFrame for pipeline tests."""
    return pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 2)],
            "ticker": ["AAPL", "MSFT"],
            "open": [150.0, 380.0],
            "high": [152.0, 382.0],
            "low": [149.0, 379.0],
            "close": [151.5, 381.5],
            "volume": [1_000_000.0, 1_200_000.0],
            "vwap": [151.2, 381.2],
            "transactions": [5000, 6000],
        }
    ).cast(
        {
            "date": pl.Date,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float32,
            "vwap": pl.Float32,
            "transactions": pl.UInt32,
        }
    )


@pytest.fixture
def sample_splits():
    """Minimal splits DataFrame for pipeline tests."""
    return pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "split_from": [1.0],
            "split_to": [2.0],
            "adjustment_factor": [2.0],
            "adjustment_type": ["forward"],
        }
    ).cast(
        {
            "execution_date": pl.Date,
            "split_from": pl.Float32,
            "split_to": pl.Float32,
            "adjustment_factor": pl.Float64,
        }
    )


@pytest.fixture
def sample_tickers():
    """Minimal tickers DataFrame for pipeline tests."""
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "name": ["Apple Inc.", "Microsoft Corporation"],
            "type": ["CS", "CS"],
            "primary_exchange": ["XNAS", "XNAS"],
            "cik": ["0000320193", "0000789019"],
            "active": [True, True],
        }
    )


@pytest.fixture
def sample_metrics():
    """Minimal metrics DataFrame for pipeline tests."""
    return pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2), datetime.date(2024, 1, 2)],
            "ticker": ["AAPL", "MSFT"],
            "sma_20": [None, None],
            "sma_50": [None, None],
            "sma_200": [None, None],
            "atr_14": [None, None],
            "atr_pct": [None, None],
            "adr_pct": [None, None],
            "volume_sma_20": [None, None],
        }
    ).cast(
        {
            "date": pl.Date,
            "sma_20": pl.Float32,
            "sma_50": pl.Float32,
            "sma_200": pl.Float32,
            "atr_14": pl.Float32,
            "atr_pct": pl.Float32,
            "adr_pct": pl.Float32,
            "volume_sma_20": pl.Float32,
        }
    )


@pytest.fixture
def pipeline_mocks():
    """Patch all pipeline dependencies via patch.multiple, yielding a name-keyed dict."""  # noqa: E501
    with patch.multiple(
        _PIPELINE,
        get_trading_days=DEFAULT,
        MassiveClient=DEFAULT,
        extract_daily_aggs=DEFAULT,
        extract_splits=DEFAULT,
        extract_tickers=DEFAULT,
        adjust_splits=DEFAULT,
        filter_tickers=DEFAULT,
        aggregate_to_monthly=DEFAULT,
        aggregate_to_weekly=DEFAULT,
        compute_metrics=DEFAULT,
        delete_raw_dates=DEFAULT,
        write_raw_db=DEFAULT,
        append_raw_db=DEFAULT,
        read_raw_db=DEFAULT,
        write_splits=DEFAULT,
        write_consumer_db=DEFAULT,
        compute_weekly_fib_zones=DEFAULT,
        get_db_info=DEFAULT,
        get_existing_dates=DEFAULT,
    ) as mocks:
        yield mocks


def _wire_defaults(mocks, sample_bars, sample_splits, sample_tickers, sample_metrics):
    """Set standard return values on all pipeline mocks."""
    mocks["get_trading_days"].return_value = [datetime.date(2024, 1, 2)]
    mocks["extract_daily_aggs"].return_value = sample_bars
    mocks["extract_splits"].return_value = sample_splits
    mocks["extract_tickers"].return_value = sample_tickers
    mocks["adjust_splits"].return_value = sample_bars
    mocks["filter_tickers"].return_value = sample_bars
    mocks["aggregate_to_monthly"].return_value = sample_bars
    mocks["aggregate_to_weekly"].return_value = sample_bars
    mocks["compute_metrics"].return_value = sample_metrics
    mocks["delete_raw_dates"].return_value = None
    mocks["get_existing_dates"].return_value = set()
    mocks["read_raw_db"].return_value = sample_bars


# ═══════════════════════════════════════════════════════════════════════════════
# Backfill
# ═══════════════════════════════════════════════════════════════════════════════


def test_backfill_calls_extract_in_order(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls extract_daily_aggs, extract_splits, extract_tickers."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    pipeline_mocks["extract_daily_aggs"].assert_called_once()
    pipeline_mocks["extract_splits"].assert_called_once()
    pipeline_mocks["extract_tickers"].assert_called_once()


def test_backfill_calls_transform_in_order(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls adjust_splits, filter_tickers, compute_metrics."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    pipeline_mocks["adjust_splits"].assert_called_once()
    assert pipeline_mocks["adjust_splits"].call_args[0][0] is sample_bars
    assert pipeline_mocks["adjust_splits"].call_args[0][1] is sample_splits

    pipeline_mocks["filter_tickers"].assert_called_once()
    assert pipeline_mocks["filter_tickers"].call_args[0][1] is sample_tickers

    assert pipeline_mocks["compute_metrics"].call_count == 3


def test_backfill_calls_write_raw_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls write_raw_db with the raw.duckdb path."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    config = _make_config(tmp_path)
    backfill(config)

    pipeline_mocks["write_raw_db"].assert_called_once()
    assert (
        pipeline_mocks["write_raw_db"].call_args[0][1]
        == config.output_dir / "raw.duckdb"
    )


def test_backfill_calls_write_consumer_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls write_consumer_db with the tickerlake.duckdb path."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    config = _make_config(tmp_path)
    backfill(config)

    pipeline_mocks["write_consumer_db"].assert_called_once()
    assert (
        pipeline_mocks["write_consumer_db"].call_args[0][3]
        == config.output_dir / "tickerlake.duckdb"
    )


def test_backfill_refreshes_weekly_fib_zones_after_consumer_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill refreshes weekly fib zones after writing the consumer DB."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    events = []
    pipeline_mocks["write_consumer_db"].side_effect = lambda *args, **kwargs: (
        events.append("consumer_db")
    )
    pipeline_mocks["compute_weekly_fib_zones"].side_effect = lambda config: (
        events.append("weekly_fib_zones")
    )
    config = _make_config(tmp_path)

    backfill(config)

    assert events == ["consumer_db", "weekly_fib_zones"]
    pipeline_mocks["compute_weekly_fib_zones"].assert_called_once_with(config)


def test_backfill_no_trading_days(pipeline_mocks, tmp_path):
    """If no trading days in range, logs warning and skips extract."""
    from tickerlake.pipeline import backfill

    pipeline_mocks["get_trading_days"].return_value = []
    backfill(_make_config(tmp_path))

    pipeline_mocks["extract_daily_aggs"].assert_not_called()
    pipeline_mocks["extract_splits"].assert_not_called()
    pipeline_mocks["extract_tickers"].assert_not_called()


def test_backfill_skips_cached_dates(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill refetches the latest cached day and any missing dates in range."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["get_trading_days"].return_value = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
    ]
    pipeline_mocks["get_existing_dates"].return_value = {datetime.date(2024, 1, 2)}
    # Mock extract_daily_aggs to return bars with both dates
    pipeline_mocks["extract_daily_aggs"].return_value = sample_bars

    backfill(_make_config(tmp_path))

    call_args = pipeline_mocks["extract_daily_aggs"].call_args
    assert call_args[0][1] == [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    # delete_raw_dates should be called with the intersection of fetched_dates
    # and existing_dates
    pipeline_mocks["delete_raw_dates"].assert_called_once_with(
        tmp_path / "raw.duckdb", {datetime.date(2024, 1, 2)}
    )
    pipeline_mocks["append_raw_db"].assert_called_once()
    pipeline_mocks["write_raw_db"].assert_not_called()


def test_backfill_refetches_latest_five_cached_days(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill drops and refetches the latest five cached trading days."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 9),
    ]
    pipeline_mocks["get_trading_days"].return_value = trading_days
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    # Mock extract_daily_aggs to return bars with dates in the refresh window
    # (last 5 trading days: 2024-01-03 through 2024-01-09)
    bars_with_refresh_dates = sample_bars.with_columns(
        pl.lit(datetime.date(2024, 1, 3)).alias("date")
    )
    pipeline_mocks["extract_daily_aggs"].return_value = bars_with_refresh_dates

    backfill(_make_config(tmp_path))

    # delete_raw_dates should be called with dates that are in both fetched_dates
    # and existing_dates
    pipeline_mocks["delete_raw_dates"].assert_called_once_with(
        tmp_path / "raw.duckdb", {datetime.date(2024, 1, 3)}
    )
    pipeline_mocks["extract_daily_aggs"].assert_called_once()
    assert pipeline_mocks["extract_daily_aggs"].call_args[0][1] == trading_days[-5:]
    pipeline_mocks["write_raw_db"].assert_not_called()
    pipeline_mocks["append_raw_db"].assert_called_once()
    pipeline_mocks["read_raw_db"].assert_called_once()
    pipeline_mocks["write_consumer_db"].assert_called_once()


def test_backfill_cached_count_only_uses_requested_range(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    caplog,
):
    """Backfill logs cached counts using only trading days in the requested range."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
    ]
    pipeline_mocks["get_trading_days"].return_value = trading_days
    pipeline_mocks["get_existing_dates"].return_value = {
        datetime.date(2023, 12, 29),
        *trading_days,
        datetime.date(2024, 2, 1),
    }

    with caplog.at_level("INFO"):
        backfill(_make_config(tmp_path))

    # Cached count should be 3 (intersection of existing and requested).
    # Fetch count should be 3 (all of them are in the refresh window since there
    # are only 3 total).
    assert (
        "Backfill: 2024-01-01 to 2024-01-31 (3 trading days, 3 cached, 3 to fetch)"
        in caplog.text
    )


def test_backfill_no_cache(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill fetches all dates and calls write_raw_db when no cache exists."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )

    backfill(_make_config(tmp_path))

    pipeline_mocks["extract_daily_aggs"].assert_called_once()
    pipeline_mocks["write_raw_db"].assert_called_once()
    pipeline_mocks["append_raw_db"].assert_not_called()


# ═══════════════════════════════════════════════════════════════════════════════
# Update
# ═══════════════════════════════════════════════════════════════════════════════


def test_update_delegates_to_backfill(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update delegates to _run_backfill when raw.duckdb exists with data."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
    ]
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    pipeline_mocks["get_trading_days"].return_value = trading_days

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    # Should call extract_daily_aggs (via _run_backfill)
    pipeline_mocks["extract_daily_aggs"].assert_called_once()


def test_update_refetches_revision_window(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update re-fetches the trailing _REVISION_WINDOW_DAYS cached dates."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 9),
    ]
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    pipeline_mocks["get_trading_days"].return_value = trading_days

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    # Should call get_trading_days with the start of the revision window
    # (min of last 5 cached dates = 2024-01-03)
    call_args = pipeline_mocks["get_trading_days"].call_args
    assert call_args[0][0] == datetime.date(2024, 1, 3)


def test_update_deletes_and_refetches_revision_window(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update deletes and re-fetches the trailing revision window dates."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
    ]
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    pipeline_mocks["get_trading_days"].return_value = trading_days
    # Mock extract_daily_aggs to return bars with all the trading days
    pipeline_mocks["extract_daily_aggs"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    config = _make_config(tmp_path)
    update(config)

    # Should call delete_raw_dates with the revision window dates that are in
    # the fetched data
    pipeline_mocks["delete_raw_dates"].assert_called_once_with(
        config.output_dir / "raw.duckdb", {datetime.date(2024, 1, 2)}
    )
    # Should call append_raw_db
    pipeline_mocks["append_raw_db"].assert_called_once()
    assert (
        pipeline_mocks["append_raw_db"].call_args[0][1]
        == config.output_dir / "raw.duckdb"
    )


def test_update_empty_raw_db_falls_back_to_backfill(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """If raw.duckdb exists but is empty, update falls back to backfill."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["get_existing_dates"].return_value = set()

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    # Should call write_raw_db (backfill path)
    pipeline_mocks["write_raw_db"].assert_called_once()


def test_update_falls_back_to_backfill(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """If raw.duckdb doesn't exist, update falls back to backfill logic."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    update(_make_config(tmp_path))

    pipeline_mocks["write_raw_db"].assert_called_once()


def test_update_fewer_than_window_refetches_all(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update with fewer than _REVISION_WINDOW_DAYS cached dates refetches all."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    # Only 2 cached dates (less than 5)
    cached_dates = {datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)}
    pipeline_mocks["get_existing_dates"].return_value = cached_dates
    pipeline_mocks["get_trading_days"].return_value = list(cached_dates)
    pipeline_mocks["extract_daily_aggs"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    # Should call get_trading_days with the min of all cached dates
    call_args = pipeline_mocks["get_trading_days"].call_args
    assert call_args[0][0] == datetime.date(2024, 1, 2)


def test_update_api_failure_does_not_delete(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update does NOT delete dates if API fails to return them."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
    ]
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    pipeline_mocks["get_trading_days"].return_value = trading_days

    # Build a bars DataFrame spanning the revision window dates, but deliberately
    # omit 2024-01-05 to simulate API failure for that date
    partial_bars = pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 8),
            ],
            "ticker": ["AAPL", "MSFT", "AAPL", "MSFT", "AAPL", "MSFT", "AAPL", "MSFT"],
            "open": [150.0, 380.0, 151.0, 381.0, 152.0, 382.0, 155.0, 385.0],
            "high": [152.0, 382.0, 153.0, 383.0, 154.0, 384.0, 157.0, 387.0],
            "low": [149.0, 379.0, 150.0, 380.0, 151.0, 381.0, 154.0, 384.0],
            "close": [151.5, 381.5, 152.5, 382.5, 153.5, 383.5, 156.5, 386.5],
            "volume": [
                1_000_000.0,
                1_200_000.0,
                1_100_000.0,
                1_300_000.0,
                1_050_000.0,
                1_250_000.0,
                1_075_000.0,
                1_275_000.0,
            ],
            "vwap": [151.2, 381.2, 152.2, 382.2, 153.2, 383.2, 156.2, 386.2],
            "transactions": [5000, 6000, 5500, 6500, 5250, 6250, 5375, 6375],
        }
    ).cast(
        {
            "date": pl.Date,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float32,
            "vwap": pl.Float32,
            "transactions": pl.UInt32,
        }
    )
    pipeline_mocks["extract_daily_aggs"].return_value = partial_bars

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    # delete_raw_dates should be called with only the dates that are both in the
    # fetched data AND already cached (i.e., all dates except 2024-01-05)
    pipeline_mocks["delete_raw_dates"].assert_called_once_with(
        tmp_path / "raw.duckdb",
        {
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
            datetime.date(2024, 1, 8),
        },
    )


def test_update_calls_extract_splits_with_config_dates(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update calls extract_splits with config dates, not narrowed bars_start."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
    ]
    pipeline_mocks["get_existing_dates"].return_value = set(trading_days)
    pipeline_mocks["get_trading_days"].return_value = trading_days
    pipeline_mocks["extract_daily_aggs"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    config = _make_config(tmp_path)
    update(config)

    # extract_splits should be called with config.start_date and config.end_date
    # (signature: extract_splits(client, start_date, end_date))
    call_args = pipeline_mocks["extract_splits"].call_args
    assert call_args[0][1] == config.start_date
    assert call_args[0][2] == config.end_date


# ═══════════════════════════════════════════════════════════════════════════════
# Info
# ═══════════════════════════════════════════════════════════════════════════════


def test_info_calls_get_db_info(pipeline_mocks, tmp_path):
    """Info calls get_db_info for each existing DB file and logs results."""
    from tickerlake.pipeline import info

    pipeline_mocks["get_db_info"].return_value = {
        "tables": ["raw_daily_bars"],
        "row_counts": {"raw_daily_bars": 100},
        "date_range": {"raw_daily_bars": {"min": "2024-01-02", "max": "2024-01-31"}},
        "file_size_bytes": 4096,
    }

    (tmp_path / "raw.duckdb").touch()
    (tmp_path / "tickerlake.duckdb").touch()
    info(_make_config(tmp_path))

    assert pipeline_mocks["get_db_info"].call_count == 2


def test_info_missing_db(tmp_path):
    """Info logs 'not found' if DuckDB files don't exist."""
    from tickerlake.pipeline import info

    info(_make_config(tmp_path))


# ═══════════════════════════════════════════════════════════════════════════════
# Split adjustment spot check
# ═══════════════════════════════════════════════════════════════════════════════


def test_verify_split_adjustment_passes():
    """Spot check passes when adjusted/raw ratio matches split factor."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    adjusted = raw.with_columns(pl.col("close") * 0.25)
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "adjustment_factor": [0.25],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    _verify_split_adjustment(raw, adjusted, splits)


def test_verify_split_adjustment_fails():
    """Spot check raises ValueError when adjusted prices don't match factor."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    adjusted = raw.clone()
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "adjustment_factor": [0.25],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    with pytest.raises(ValueError, match="spot check failed"):
        _verify_split_adjustment(raw, adjusted, splits)


def test_verify_split_adjustment_empty_splits():
    """Spot check is a no-op when splits DataFrame is empty."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    empty_splits = pl.DataFrame(
        schema={
            "ticker": pl.Utf8,
            "execution_date": pl.Date,
            "adjustment_factor": pl.Float64,
        }
    )

    _verify_split_adjustment(raw, raw, empty_splits)


def test_verify_split_adjustment_skips_small_splits():
    """Spot check skips splits with factor >= 0.5 (less than 2:1)."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "adjustment_factor": [0.75],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    _verify_split_adjustment(raw, raw, splits)


def test_verify_split_adjustment_skips_extreme_splits():
    """Spot check skips splits with factor < 0.02 (OTC noise, same-day offsetting splits)."""  # noqa: E501
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["SFE"],
            "close": [0.73],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    splits = pl.DataFrame(
        {
            "ticker": ["SFE"],
            "execution_date": [datetime.date(2024, 1, 16)],
            "adjustment_factor": [0.01],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    _verify_split_adjustment(raw, raw, splits)


def test_verify_split_adjustment_skips_duplicate_ticker():
    """Spot check skips second split for same ticker (if ticker in seen: continue)."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10), datetime.date(2024, 1, 10)],
            "ticker": ["AAPL", "AAPL"],
            "close": [400.0, 400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    adjusted = raw.with_columns(pl.col("close") * 0.25)
    # Two splits for AAPL, both in the sample range
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "execution_date": [datetime.date(2024, 1, 15), datetime.date(2024, 1, 16)],
            "adjustment_factor": [0.25, 0.30],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    # Should not raise; second AAPL split is skipped due to seen check
    _verify_split_adjustment(raw, adjusted, splits)


def test_verify_split_adjustment_skips_no_pre_split_bars():
    """Spot check skips split when ticker has no bars before execution_date."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 20)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    adjusted = raw.with_columns(pl.col("close") * 0.25)
    # Split execution is before any bars
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "adjustment_factor": [0.25],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    # Should not raise; pre_split is empty so continue
    _verify_split_adjustment(raw, adjusted, splits)


def test_verify_split_adjustment_skips_missing_adjusted_row():
    """Spot check skips when adjusted_bars missing row at check_date."""
    from tickerlake.pipeline import _verify_split_adjustment

    raw = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["AAPL"],
            "close": [400.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    # Adjusted bars missing the AAPL row
    adjusted = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 10)],
            "ticker": ["MSFT"],
            "close": [300.0],
        }
    ).cast({"date": pl.Date, "close": pl.Float32})
    splits = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 1, 15)],
            "adjustment_factor": [0.25],
        }
    ).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    # Should not raise; adj_row is empty so continue
    _verify_split_adjustment(raw, adjusted, splits)


def test_verify_split_adjustment_early_exit_at_sample_size():
    """Spot check exits early after verifying _SPOT_CHECK_SAMPLE_SIZE tickers."""
    from tickerlake.pipeline import _verify_split_adjustment

    # Create 6 tickers with bars and splits (more than _SPOT_CHECK_SAMPLE_SIZE=5)
    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META"]
    raw_rows = [
        {"date": datetime.date(2024, 1, 10), "ticker": ticker, "close": 400.0}
        for ticker in tickers
    ]
    raw = pl.DataFrame(raw_rows).cast({"date": pl.Date, "close": pl.Float32})

    # Adjusted with 0.25 factor
    adjusted = raw.with_columns(pl.col("close") * 0.25)

    # Create splits for all 6 tickers
    split_rows = [
        {
            "ticker": ticker,
            "execution_date": datetime.date(2024, 1, 15),
            "adjustment_factor": 0.25,
        }
        for ticker in tickers
    ]
    splits = pl.DataFrame(split_rows).cast(
        {"execution_date": pl.Date, "adjustment_factor": pl.Float64}
    )

    # Should verify exactly _SPOT_CHECK_SAMPLE_SIZE tickers and exit early
    _verify_split_adjustment(raw, adjusted, splits)


def test_backfill_refetches_cached_date_in_revision_window(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """Backfill re-fetches a cached date that falls inside the revision window."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    # Single trading day, already cached. With only one cached date, the
    # trailing revision window (_REVISION_WINDOW_DAYS) always includes it,
    # so fetch_dates is non-empty even though nothing is "missing".
    trading_day = datetime.date(2024, 1, 2)
    pipeline_mocks["get_trading_days"].return_value = [trading_day]
    pipeline_mocks["get_existing_dates"].return_value = {trading_day}
    pipeline_mocks["extract_daily_aggs"].return_value = pl.DataFrame(
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float32,
            "vwap": pl.Float32,
            "transactions": pl.UInt32,
        }
    )

    backfill(_make_config(tmp_path))

    # The revision window forces a re-fetch of the single cached date.
    pipeline_mocks["extract_daily_aggs"].assert_called_once()


def test_compact_logs_before_and_after_sizes(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    caplog,
):
    """Compact logs file size before and after compaction."""
    import logging

    from tickerlake.pipeline import compact

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    config = _make_config(tmp_path)
    raw_path = config.output_dir / "raw.duckdb"

    # Create a real temp DuckDB file with some data
    from tickerlake.load import write_raw_db

    write_raw_db(sample_bars, raw_path)

    with caplog.at_level(logging.INFO):
        compact(config)

    # Should log compacting message with before size
    assert "Compacting" in caplog.text
    assert "raw.duckdb" in caplog.text
    # Should log done message with after size
    assert "Done:" in caplog.text


def test_compact_missing_raw_db(pipeline_mocks, tmp_path, caplog):
    """Compact logs warning and returns when raw.duckdb doesn't exist."""
    import logging

    from tickerlake.pipeline import compact

    config = _make_config(tmp_path)

    with caplog.at_level(logging.WARNING):
        compact(config)

    assert "No raw.duckdb found" in caplog.text


def test_backfill_calls_aggregate_to_weekly(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls aggregate_to_weekly with the filtered bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    pipeline_mocks["aggregate_to_weekly"].assert_called_once()
    call_args = pipeline_mocks["aggregate_to_weekly"].call_args
    assert call_args is not None
    assert call_args[0][0] is sample_bars


def test_backfill_calls_aggregate_to_monthly(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill calls aggregate_to_monthly with the filtered bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    pipeline_mocks["aggregate_to_monthly"].assert_called_once()
    call_args = pipeline_mocks["aggregate_to_monthly"].call_args
    assert call_args is not None
    assert call_args[0][0] is sample_bars


def test_backfill_computes_weekly_metrics(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill computes metrics for daily, weekly, and monthly bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    assert pipeline_mocks["compute_metrics"].call_count == 3


def test_backfill_passes_weekly_to_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """Backfill passes weekly bars and metrics to write_consumer_db."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
    )
    backfill(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "weekly_bars" in call_kwargs
    assert "weekly_metrics" in call_kwargs
    assert "monthly_bars" in call_kwargs
    assert "monthly_metrics" in call_kwargs


def test_update_calls_aggregate_to_weekly(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update calls aggregate_to_weekly with filtered bars."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    pipeline_mocks["aggregate_to_weekly"].assert_called_once()
    pipeline_mocks["aggregate_to_monthly"].assert_called_once()


def test_update_passes_weekly_to_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """Update passes weekly bars and metrics to write_consumer_db."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
    )
    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "weekly_bars" in call_kwargs
    assert "weekly_metrics" in call_kwargs
    assert "monthly_bars" in call_kwargs
    assert "monthly_metrics" in call_kwargs


def test_update_refreshes_weekly_fib_zones_after_consumer_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update refreshes weekly fib zones after writing the consumer DB."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["get_existing_dates"].return_value = {datetime.date(2024, 1, 2)}
    events = []
    pipeline_mocks["write_consumer_db"].side_effect = lambda *args, **kwargs: (
        events.append("consumer_db")
    )
    pipeline_mocks["compute_weekly_fib_zones"].side_effect = lambda config: (
        events.append("weekly_fib_zones")
    )
    (tmp_path / "raw.duckdb").touch()
    config = _make_config(tmp_path)

    update(config)

    assert events == ["consumer_db", "weekly_fib_zones"]
    pipeline_mocks["compute_weekly_fib_zones"].assert_called_once_with(config)


def test_backfill_persists_splits(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """Backfill calls write_splits with extracted splits and raw.duckdb path."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    config = _make_config(tmp_path)
    backfill(config)

    pipeline_mocks["write_splits"].assert_called_once_with(
        sample_splits, config.output_dir / "raw.duckdb"
    )


def test_update_persists_splits(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
):
    """Update calls write_splits with extracted splits and raw.duckdb path."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    (tmp_path / "raw.duckdb").touch()
    config = _make_config(tmp_path)
    update(config)

    pipeline_mocks["write_splits"].assert_called_once_with(
        sample_splits, config.output_dir / "raw.duckdb"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Weekly fib zones compute + screen
# ═══════════════════════════════════════════════════════════════════════════════


def _weekly_bars_df() -> pl.DataFrame:
    """Weekly bars for AAPL, MSFT, and SPY matching the consumer bars schema."""
    return pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 8),
            ],
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT", "SPY", "SPY"],
            "open": [150.0, 155.0, 380.0, 385.0, 400.0, 405.0],
            "high": [152.0, 157.0, 382.0, 387.0, 402.0, 407.0],
            "low": [149.0, 154.0, 379.0, 384.0, 399.0, 404.0],
            "close": [151.5, 156.5, 381.5, 386.5, 401.5, 406.5],
            "volume": [
                1_000_000.0,
                1_100_000.0,
                1_200_000.0,
                1_300_000.0,
                3_000_000.0,
                3_100_000.0,
            ],
            "vwap": [151.2, 156.2, 381.2, 386.2, 401.2, 406.2],
            "transactions": [5000, 5100, 6000, 6100, 7000, 7100],
        }
    ).cast(
        {
            "date": pl.Date,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float32,
            "vwap": pl.Float32,
            "transactions": pl.UInt32,
        }
    )


def _weekly_metrics_df() -> pl.DataFrame:
    """Weekly metrics: AAPL liquid only on its latest row, MSFT illiquid, SPY liquid."""
    return pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 8),
                datetime.date(2024, 1, 8),
            ],
            "ticker": ["AAPL", "AAPL", "MSFT", "SPY"],
            "sma_20": [None] * 4,
            "sma_50": [None] * 4,
            "sma_200": [None] * 4,
            "atr_14": [None] * 4,
            "atr_pct": [None] * 4,
            "adr_pct": [None] * 4,
            "volume_sma_20": [100_000.0, 2_000_000.0, 500_000.0, 3_000_000.0],
        }
    ).cast(
        {
            "date": pl.Date,
            "sma_20": pl.Float32,
            "sma_50": pl.Float32,
            "sma_200": pl.Float32,
            "atr_14": pl.Float32,
            "atr_pct": pl.Float32,
            "adr_pct": pl.Float32,
            "volume_sma_20": pl.Float32,
        }
    )


def _build_consumer_db(tmp_path: Path, metrics: pl.DataFrame) -> Path:
    """Create a consumer DuckDB with weekly tables plus minimal daily/tickers tables."""
    from tickerlake.load import write_consumer_db

    bars = _weekly_bars_df()
    tickers = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "SPY"],
            "name": ["Apple Inc.", "Microsoft Corp.", "SPDR S&P 500 ETF"],
            "type": ["CS", "CS", "ETF"],
            "primary_exchange": ["XNAS", "XNAS", "XNYS"],
            "cik": ["", "", ""],
            "active": [True, True, True],
        }
    )
    db = tmp_path / "tickerlake.duckdb"
    write_consumer_db(
        bars,
        metrics,
        tickers,
        db,
        weekly_bars=bars,
        weekly_metrics=metrics,
    )
    return db


def _zones_df() -> pl.DataFrame:
    """Weekly fib zones rows in deliberately non-sorted ticker order.

    Includes two in_ibz rows (one live, one void), an in_smz, a below_smz
    (void), and an above_ibz row.
    """
    from tickerlake.fib_zones import WEEKLY_FIB_ZONES_SCHEMA

    def row(
        ticker: str,
        swing_low: float,
        swing_high: float,
        current_price: float,
        zone: str,
        status: str,
        degree: int,
    ) -> dict:
        rng = swing_high - swing_low
        return {
            "ticker": ticker,
            "as_of_date": datetime.date(2024, 1, 8),
            "swing_low": swing_low,
            "swing_high": swing_high,
            "range": rng,
            "swing_low_date": datetime.date(2023, 11, 1),
            "swing_high_date": datetime.date(2024, 1, 5),
            "bars_since_swing_high": 1,
            "ibz_low": round(swing_low + 0.786 * rng, 2),
            "ibz_high": round(swing_low + 0.618 * rng, 2),
            "smz_low": round(swing_low + 0.826 * rng, 2),
            "smz_high": round(swing_low + 0.786 * rng, 2),
            "current_price": current_price,
            "pct_retracement": (swing_high - current_price) / rng * 100,
            "zone": zone,
            "primary_degree": degree,
            "primary_status": status,
            "still_making_new_highs": zone == "above_ibz",
            "zigzag_pct": 0.12,
            "bar_count": 200,
        }

    rows = [
        row("NVDA", 400.0, 600.0, 500.0, "in_ibz", "void", 1),
        row("SPY", 280.0, 400.0, 300.0, "below_smz", "void", 2),
        row("AAPL", 80.0, 120.0, 100.0, "in_ibz", "live", 1),
        row("TSLA", 200.0, 500.0, 480.0, "above_ibz", "live", 3),
        row("MSFT", 150.0, 250.0, 200.0, "in_smz", "deep", 2),
    ]
    return pl.DataFrame(rows).cast(WEEKLY_FIB_ZONES_SCHEMA)


def _write_fib_zones_table(db_path: Path, df: pl.DataFrame) -> None:
    """Create/replace the weekly_fib_zones table from a DataFrame."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        df.write_parquet(tmp)
        con = duckdb.connect(str(db_path))
        try:
            con.execute(
                "CREATE OR REPLACE TABLE weekly_fib_zones AS "
                "SELECT * FROM read_parquet(?)",
                [str(tmp)],
            )
        finally:
            con.close()
    finally:
        tmp.unlink(missing_ok=True)


def test_compute_weekly_fib_zones_writes_rows(tmp_path: Path, caplog) -> None:
    """compute_weekly_fib_zones persists the computed zones to the consumer DB."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    db = _build_consumer_db(tmp_path, _weekly_metrics_df())

    with (
        patch(f"{_PIPELINE}.compute_weekly_fib_zones_all", return_value=_zones_df()),
        caplog.at_level("INFO"),
    ):
        pipeline.compute_weekly_fib_zones(config)

    con = duckdb.connect(str(db), read_only=True)
    try:
        rows = con.execute(
            "SELECT ticker, zone FROM weekly_fib_zones ORDER BY ticker"
        ).fetchall()
    finally:
        con.close()

    assert rows == [
        ("AAPL", "in_ibz"),
        ("MSFT", "in_smz"),
        ("NVDA", "in_ibz"),
        ("SPY", "below_smz"),
        ("TSLA", "above_ibz"),
    ]
    assert "2 eligible tickers" in caplog.text
    assert (
        "n_in_ibz=2, n_in_smz=1, n_below_smz=1, n_above_ibz=1, n_void=2" in caplog.text
    )
    assert "n_written=5" in caplog.text


def test_compute_weekly_fib_zones_filters_by_liquidity(tmp_path: Path) -> None:
    """compute_weekly_fib_zones passes only liquid tickers to the compute function.

    AAPL's stale low-volume row must not override its latest liquid row; MSFT
    is below the threshold and must be excluded.
    """
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _build_consumer_db(tmp_path, _weekly_metrics_df())

    captured: dict = {}

    def fake_compute(weekly_bars, *, eligible_tickers, **kwargs):
        captured["bars"] = weekly_bars
        captured["eligible"] = set(eligible_tickers)
        return pl.DataFrame(schema=_zones_df().schema)

    with (
        patch(f"{_PIPELINE}.compute_weekly_fib_zones_all", side_effect=fake_compute),
        patch(f"{_PIPELINE}.write_weekly_fib_zones") as mock_write,
    ):
        pipeline.compute_weekly_fib_zones(config)

    assert captured["eligible"] == {"AAPL", "SPY"}
    assert captured["bars"].is_empty() is False
    mock_write.assert_called_once()
    assert mock_write.call_args[0][1] == config.output_dir / "tickerlake.duckdb"


def test_compute_weekly_fib_zones_missing_db(tmp_path: Path) -> None:
    """compute_weekly_fib_zones raises ValueError when the consumer DB is absent."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)

    with pytest.raises(ValueError, match="Consumer DB not found"):
        pipeline.compute_weekly_fib_zones(config)


def test_compute_weekly_fib_zones_missing_weekly_tables(tmp_path: Path) -> None:
    """compute_weekly_fib_zones raises ValueError when weekly tables are absent."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    db = tmp_path / "tickerlake.duckdb"
    con = duckdb.connect(str(db))
    try:
        con.execute("CREATE TABLE unrelated (x INT)")
    finally:
        con.close()

    with pytest.raises(ValueError, match="tables not found"):
        pipeline.compute_weekly_fib_zones(config)


def test_compute_weekly_fib_zones_rejects_bad_schema(tmp_path: Path) -> None:
    """compute_weekly_fib_zones rejects a malformed compute result."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _build_consumer_db(tmp_path, _weekly_metrics_df())
    bad = _zones_df().drop("current_price")

    with (
        patch(f"{_PIPELINE}.compute_weekly_fib_zones_all", return_value=bad),
        pytest.raises(ValueError, match="weekly_fib_zones schema mismatch"),
    ):
        pipeline.compute_weekly_fib_zones(config)


def test_screen_fib_zones_all_excludes_void(tmp_path: Path, caplog) -> None:
    """zone="all" shows actionable zones and hides void rows."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _write_fib_zones_table(tmp_path / "tickerlake.duckdb", _zones_df())
    recording = Console(record=True)

    with (
        patch(f"{_PIPELINE}.console", recording),
        caplog.at_level("INFO"),
    ):
        pipeline.screen_fib_zones(config, zone="all")

    text = recording.export_text()
    assert "AAPL" in text
    assert "MSFT" in text
    assert "SPY" not in text  # below_smz but void
    assert "NVDA" not in text  # void
    assert "TSLA" not in text  # above_ibz is not actionable
    assert "50.00%" in text
    assert "Screen: 2 total matches, 2 displayed" in caplog.text


def test_screen_fib_zones_in_ibz_filters_zone(tmp_path: Path) -> None:
    """zone="in_ibz" filters to that single zone regardless of status."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _write_fib_zones_table(tmp_path / "tickerlake.duckdb", _zones_df())
    recording = Console(record=True)

    with patch(f"{_PIPELINE}.console", recording):
        pipeline.screen_fib_zones(config, zone="in_ibz")

    text = recording.export_text()
    assert "AAPL" in text
    assert "NVDA" in text  # void row still shown when a specific zone is requested
    assert "MSFT" not in text
    assert "SPY" not in text


def test_screen_fib_zones_limit_caps_displayed(tmp_path: Path, caplog) -> None:
    """limit=N queries all rows but displays only the first N."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _write_fib_zones_table(tmp_path / "tickerlake.duckdb", _zones_df())
    recording = Console(record=True)

    with (
        patch(f"{_PIPELINE}.console", recording),
        caplog.at_level("INFO"),
    ):
        pipeline.screen_fib_zones(config, zone="all", limit=1)

    text = recording.export_text()
    assert "AAPL" in text
    assert "MSFT" not in text
    assert "Screen: 2 total matches, 1 displayed (zone=all, limit=1," in caplog.text
    assert "min_swing_low=5.0" in caplog.text


def test_screen_fib_zones_min_swing_low_filters(tmp_path: Path, caplog) -> None:
    """min_swing_low excludes rows whose swing low is below the cutoff."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _write_fib_zones_table(tmp_path / "tickerlake.duckdb", _zones_df())
    recording = Console(record=True)

    with (
        patch(f"{_PIPELINE}.console", recording),
        caplog.at_level("INFO"),
    ):
        pipeline.screen_fib_zones(config, zone="all", min_swing_low=90.0)

    text = recording.export_text()
    assert "MSFT" in text  # swing_low 150 >= 90
    assert "AAPL" not in text  # swing_low 80 < 90
    assert "Screen: 1 total matches, 1 displayed" in caplog.text


def test_screen_fib_zones_sorted_by_ticker(tmp_path: Path) -> None:
    """Screen output is sorted by ticker even when the table is unsorted."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    _write_fib_zones_table(tmp_path / "tickerlake.duckdb", _zones_df())
    recording = Console(record=True)

    with patch(f"{_PIPELINE}.console", recording):
        pipeline.screen_fib_zones(config, zone="in_ibz")

    text = recording.export_text()
    assert text.index("AAPL") < text.index("NVDA")


def test_screen_fib_zones_missing_table(tmp_path: Path) -> None:
    """screen_fib_zones raises ValueError when weekly_fib_zones is absent."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)
    db = tmp_path / "tickerlake.duckdb"
    con = duckdb.connect(str(db))
    con.close()

    with pytest.raises(ValueError, match="weekly_fib_zones"):
        pipeline.screen_fib_zones(config, zone="all")


def test_screen_fib_zones_missing_db(tmp_path: Path) -> None:
    """screen_fib_zones raises ValueError when the consumer DB is absent."""
    from tickerlake import pipeline

    config = _make_config(tmp_path)

    with pytest.raises(ValueError, match="Consumer DB not found"):
        pipeline.screen_fib_zones(config, zone="all")


# ---- Phase 2: vs-benchmark momentum tests (B1+B2+B3) ----


def test_etf_race_momentum_windows_validation() -> None:
    """etf_race validates momentum windows early, before any output."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config()
    with pytest.raises(
        ValueError, match="Momentum windows must be strictly increasing"
    ):
        pipeline.etf_race(
            config,
            tickers=["SPY", "QQQ"],
            momentum_short_window=13,
            momentum_medium_window=4,  # Invalid: not increasing
            momentum_long_window=26,
        )


def test_etf_race_prints_only_the_leaderboard(tmp_path: Path) -> None:
    """etf-race does not print a title or metadata preamble above the table."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    recording = Console(record=True)

    with (
        patch.multiple(
            _PIPELINE,
            _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
            read_race_bars=DEFAULT,
            _render_relative_view=DEFAULT,
        ) as mocks,
        patch(f"{_PIPELINE}.console", recording),
    ):
        mocks["read_race_bars"].return_value = pl.DataFrame(
            {
                "date": [datetime.date(2024, 1, 1)] * 3,
                "ticker": ["AAPL", "MSFT", "SPY"],
                "close": [150.0, 300.0, 400.0],
            }
        )
        pipeline.etf_race(config, tickers=["AAPL", "MSFT"])

    text = recording.export_text()
    assert "ETF Horserace" not in text
    assert "weekly bars, 365-day lookback" not in text
    mocks["_render_relative_view"].assert_called_once()


def test_etf_race_benchmark_in_race_tickers_single_read(tmp_path: Path) -> None:
    """When benchmark is in race_tickers, only ONE read_race_bars call (B3.1)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        read_qualifying_etfs=DEFAULT,
        _resolve_race_tickers=lambda consumer_path, **kw: ["SPY", "QQQ"],
        read_race_bars=DEFAULT,
        _render_relative_view=DEFAULT,
    ) as mocks:
        mocks["read_race_bars"].return_value = pl.DataFrame(
            {
                "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 1)],
                "ticker": ["SPY", "QQQ"],
                "close": [400.0, 300.0],
            }
        )

        pipeline.etf_race(
            config,
            tickers=["SPY", "QQQ"],
            benchmark="SPY",  # In race_tickers
        )

        # Should call read_race_bars exactly once
        assert mocks["read_race_bars"].call_count == 1
        call_args = mocks["read_race_bars"].call_args
        # The read_list should contain both SPY and QQQ (deduped)
        assert set(call_args.kwargs["tickers"]) == {"SPY", "QQQ"}


def test_etf_race_benchmark_not_in_race_tickers_single_read(tmp_path: Path) -> None:
    """When benchmark not in race_tickers, still only ONE read_race_bars call (B3.2)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        read_qualifying_etfs=DEFAULT,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_race_bars=DEFAULT,
        _render_relative_view=DEFAULT,
    ) as mocks:
        mocks["read_race_bars"].return_value = pl.DataFrame(
            {
                "date": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                ],
                "ticker": ["AAPL", "MSFT", "SPY"],
                "close": [150.0, 300.0, 400.0],
            }
        )

        pipeline.etf_race(
            config,
            tickers=["AAPL", "MSFT"],
            benchmark="SPY",  # Not in race_tickers
        )

        # Should call read_race_bars exactly once
        assert mocks["read_race_bars"].call_count == 1
        call_args = mocks["read_race_bars"].call_args
        # The read_list should contain AAPL, MSFT, and SPY (deduped)
        assert set(call_args.kwargs["tickers"]) == {"AAPL", "MSFT", "SPY"}


def test_etf_race_benchmark_missing_from_db_raises_error(tmp_path: Path) -> None:
    """When benchmark has no rows in DB, ValueError is raised (B3.3)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        read_qualifying_etfs=DEFAULT,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_race_bars=DEFAULT,
    ) as mocks:
        # Simulate: race tickers have bars, but benchmark doesn't
        mocks["read_race_bars"].return_value = pl.DataFrame(
            {
                "date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 1)],
                "ticker": ["AAPL", "MSFT"],
                "close": [150.0, 300.0],
            }
        )

        with pytest.raises(ValueError, match=r"No.*bars found for benchmark"):
            pipeline.etf_race(
                config,
                tickers=["AAPL", "MSFT"],
                benchmark="QQQ",  # Not in the returned bars
            )


def test_etf_race_lowercase_tickers_normalized_no_duplicates(tmp_path: Path) -> None:
    """Lowercase tickers and benchmark are normalized; no duplicate rows (B3.4)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        read_qualifying_etfs=DEFAULT,
        _resolve_race_tickers=lambda consumer_path, **kw: ["spy", "xlk", "xlf"],
        read_race_bars=DEFAULT,
        _render_relative_view=DEFAULT,
    ) as mocks:
        # Return bars with uppercase tickers (as DB would)
        mocks["read_race_bars"].return_value = pl.DataFrame(
            {
                "date": [
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                    datetime.date(2024, 1, 1),
                ],
                "ticker": ["SPY", "XLK", "XLF"],
                "close": [400.0, 80.0, 30.0],
            }
        )

        pipeline.etf_race(
            config,
            tickers=["spy", "xlk", "xlf"],
            benchmark="spy",  # Lowercase, same as first ticker
        )

        # Verify _render_relative_view was called with correct bars (no duplicates)
        render_call = mocks["_render_relative_view"].call_args
        bars = render_call.args[0]  # bars is the first positional argument
        # bars should have 3 rows (one per unique ticker), not 4 (no SPY duplication)
        assert bars.height == 3
        assert set(bars["ticker"].unique().to_list()) == {"SPY", "XLK", "XLF"}


def test_etf_race_short_history_filter_excludes_tickers(tmp_path: Path) -> None:
    """Tickers with ≤ long_window bars are filtered from vs-benchmark table (M2)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        read_qualifying_etfs=DEFAULT,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "NEWCO"],
        read_race_bars=DEFAULT,
        compute_relative_ratio=DEFAULT,
        compute_relative_race_metrics=DEFAULT,
        classify_relative_trend=DEFAULT,
        classify_horse_form=DEFAULT,
        render_relative_leaderboard=DEFAULT,
        console=DEFAULT,
    ) as mocks:
        # AAPL has 30 bars (> long_window=26), NEWCO has 20 bars (≤ long_window),
        # SPY has 30 bars. Only AAPL should reach the relative-momentum table.
        bars_df = pl.DataFrame(
            {
                "date": (
                    [datetime.date(2024, 1, i) for i in range(1, 31)]
                    + [datetime.date(2024, 1, i) for i in range(1, 21)]
                    + [datetime.date(2024, 1, i) for i in range(1, 31)]
                ),
                "ticker": ["AAPL"] * 30 + ["NEWCO"] * 20 + ["SPY"] * 30,
                "close": (
                    [100.0 + i for i in range(30)]
                    + [50.0 + i for i in range(20)]
                    + [400.0 + i for i in range(30)]
                ),
            }
        )
        mocks["read_race_bars"].return_value = bars_df

        # compute_relative_ratio returns only AAPL and NEWCO (excludes benchmark SPY)
        ratio_df = pl.DataFrame(
            {
                "date": (
                    [datetime.date(2024, 1, i) for i in range(1, 31)]
                    + [datetime.date(2024, 1, i) for i in range(1, 21)]
                ),
                "ticker": ["AAPL"] * 30 + ["NEWCO"] * 20,
                "close": (
                    [100.0 + i for i in range(30)] + [50.0 + i for i in range(20)]
                ),
            }
        )
        mocks["compute_relative_ratio"].return_value = ratio_df
        mocks["compute_relative_race_metrics"].return_value = pl.DataFrame(
            {"ticker": ["AAPL"]}
        )

        pipeline.etf_race(
            config,
            tickers=["AAPL", "NEWCO"],
            benchmark="SPY",
            momentum_long_window=26,
        )

        # Verify race metrics were computed from the filtered ratio bars.
        # The call should have NEWCO filtered out (only AAPL with > 26 bars).
        assert mocks["compute_relative_race_metrics"].call_count == 1
        ratio_arg = mocks["compute_relative_race_metrics"].call_args.args[0]
        # After the filter, only AAPL should remain (30 bars > 26)
        assert "AAPL" in ratio_arg["ticker"].to_list()
        assert "NEWCO" not in ratio_arg["ticker"].to_list()


# ---- ciovacco cloud-score report -------------------------------------------


def _ciovacco_bars_df(tickers: list[str]) -> pl.DataFrame:
    """A one-row-per-ticker daily bars frame for ciovacco orchestration."""
    return pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 1)] * len(tickers),
            "ticker": tickers,
            "open": [150.0] * len(tickers),
            "high": [151.0] * len(tickers),
            "low": [149.0] * len(tickers),
            "close": [150.0] * len(tickers),
            "volume": [1_000_000.0] * len(tickers),
            "vwap": [150.0] * len(tickers),
            "transactions": [5000] * len(tickers),
        }
    )


def _ciovacco_scores_df() -> pl.DataFrame:
    """A minimal CLOUD_SCORE_SCHEMA frame for render orchestration."""
    from tickerlake.cloud_score import CLOUD_SCORE_SCHEMA

    return pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "score_1d_cloud": [1.0, 0.75],
            "score_weekly_cloud": [1.0, 0.75],
            "score_2wk_cloud": [1.0, 0.75],
            "score_3wk_cloud": [1.0, 0.75],
            "score_monthly_cloud": [1.0, 0.75],
            "score_200wk_ma": [1, 1],
            "score_200wk_ma_slope": [1, 1],
            "score_300wk_ma": [1, 1],
            "score_300wk_ma_slope": [1, 1],
            "total": [9.0, 7.75],
        },
        schema=CLOUD_SCORE_SCHEMA,
    )


def test_ciovacco_renders_cloud_scorecard(tmp_path: Path) -> None:
    """ciovacco reads daily bars, scores, and renders via console.print."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    recording = Console(record=True)

    with (
        patch.multiple(
            _PIPELINE,
            _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
            read_daily_bars=DEFAULT,
            read_ticker_names=DEFAULT,
            compute_cloud_scores=DEFAULT,
            render_cloud_scorecard=DEFAULT,
        ) as mocks,
        patch(f"{_PIPELINE}.console", recording),
    ):
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAPL", "MSFT", "SPY"]
        )
        mocks["read_ticker_names"].return_value = {
            "AAPL": "Apple Inc.",
            "MSFT": "Microsoft Corporation",
        }
        mocks["compute_cloud_scores"].return_value = _ciovacco_scores_df()
        pipeline.ciovacco(config, tickers=["AAPL", "MSFT"])

    # One read for the universe + benchmark (deduped), scored vs SPY.
    read_call = mocks["read_daily_bars"].call_args
    assert set(read_call.kwargs["tickers"]) == {"AAPL", "MSFT", "SPY"}
    score_call = mocks["compute_cloud_scores"].call_args
    assert set(score_call.kwargs["tickers"]) == {"AAPL", "MSFT"}
    assert score_call.kwargs["benchmark"] == "SPY"
    # Names are read for the scored universe (not the benchmark) and
    # forwarded to the renderer as the ``names`` kwarg.
    names_call = mocks["read_ticker_names"].call_args
    assert set(names_call.kwargs["tickers"]) == {"AAPL", "MSFT"}
    render_call = mocks["render_cloud_scorecard"].call_args
    assert render_call.args[0].equals(_ciovacco_scores_df())
    assert render_call.kwargs["benchmark"] == "SPY"
    assert render_call.kwargs["max_etfs"] == 50
    assert render_call.kwargs["names"] == {
        "AAPL": "Apple Inc.",
        "MSFT": "Microsoft Corporation",
    }
    assert recording.export_text()  # a table was printed


def test_ciovacco_lowercase_tickers_normalized_no_duplicates(tmp_path: Path) -> None:
    """Lowercase tickers and benchmark are normalized and deduped (C3.4)."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["spy", "xlk", "xlf"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(["SPY", "XLK", "XLF"])
        mocks["read_ticker_names"].return_value = {}
        pipeline.ciovacco(config, tickers=["spy", "xlk", "xlf"], benchmark="spy")

    read_call = mocks["read_daily_bars"].call_args
    assert set(read_call.kwargs["tickers"]) == {"SPY", "XLK", "XLF"}
    assert len(read_call.kwargs["tickers"]) == 3


def test_ciovacco_benchmark_missing_from_db_raises_error(tmp_path: Path) -> None:
    """When the benchmark has no bars, ValueError is raised."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(["AAPL", "MSFT"])

        with pytest.raises(ValueError, match=r"No.*bars found for benchmark"):
            pipeline.ciovacco(config, tickers=["AAPL", "MSFT"], benchmark="QQQ")


def test_ciovacco_no_etf_bars_raises_error(tmp_path: Path) -> None:
    """When none of the scored tickers have bars, ValueError is raised."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(["SPY"])

        with pytest.raises(ValueError, match=r"No daily bars found for tickers"):
            pipeline.ciovacco(config, tickers=["AAPL", "MSFT"])


def test_ciovacco_max_etfs_none_forwarded_to_render(tmp_path: Path) -> None:
    """max_etfs=None means the scorecard is rendered uncapped."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAPL", "MSFT", "SPY"]
        )
        mocks["read_ticker_names"].return_value = {}
        pipeline.ciovacco(config, tickers=["AAPL", "MSFT"], max_etfs=None)

    render_call = mocks["render_cloud_scorecard"].call_args
    assert render_call.kwargs["max_etfs"] is None


def test_ciovacco_no_csv_writes_no_file(tmp_path: Path) -> None:
    """When csv_path is None, no CSV file is created."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    csv_path = tmp_path / "ciovacco.csv"
    assert not csv_path.exists()

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAPL", "MSFT", "SPY"]
        )
        mocks["read_ticker_names"].return_value = {}
        mocks["compute_cloud_scores"].return_value = _ciovacco_scores_df()
        pipeline.ciovacco(config, tickers=["AAPL", "MSFT"])

    assert not csv_path.exists()


def test_ciovacco_csv_writes_full_scorecard(tmp_path: Path) -> None:
    """When csv_path is provided, the full score frame is written to CSV."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    csv_path = tmp_path / "subdir" / "ciovacco.csv"  # nested path tests mkdir

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAPL", "MSFT", "QQQ"]
        )
        mocks["read_ticker_names"].return_value = {}
        mocks["compute_cloud_scores"].return_value = _ciovacco_scores_df()
        pipeline.ciovacco(
            config,
            tickers=["AAPL", "MSFT"],
            csv_path=csv_path,
            benchmark="QQQ",
        )

    assert csv_path.exists()
    written = pl.read_csv(csv_path)
    expected_columns = {
        "ticker",
        "benchmark",
        "score_1d_cloud",
        "score_weekly_cloud",
        "score_2wk_cloud",
        "score_3wk_cloud",
        "score_monthly_cloud",
        "score_200wk_ma",
        "score_200wk_ma_slope",
        "score_300wk_ma",
        "score_300wk_ma_slope",
        "total",
    }
    assert set(written.columns) == expected_columns
    # Both ETFs written, sorted by total desc (AAPL 9.0, MSFT 7.75).
    assert written["ticker"].to_list() == ["AAPL", "MSFT"]
    assert written["benchmark"].to_list() == ["QQQ", "QQQ"]
    assert written["total"].to_list() == [9.0, 7.75]


def test_ciovacco_csv_unaffected_by_max_etfs(tmp_path: Path) -> None:
    """CSV gets the full data even when max_etfs caps the Rich table."""
    from tickerlake import pipeline
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    csv_path = tmp_path / "ciovacco.csv"

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAPL", "MSFT"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAPL", "MSFT", "SPY"]
        )
        mocks["read_ticker_names"].return_value = {}
        mocks["compute_cloud_scores"].return_value = _ciovacco_scores_df()
        pipeline.ciovacco(
            config,
            tickers=["AAPL", "MSFT"],
            max_etfs=1,  # display cap: only the top ETF renders
            csv_path=csv_path,
        )

    written = pl.read_csv(csv_path)
    # CSV has both rows; max_etfs only constrains the Rich table.
    assert written["ticker"].to_list() == ["AAPL", "MSFT"]


def test_ciovacco_csv_ties_break_by_ticker_ascending(tmp_path: Path) -> None:
    """Tied totals in the CSV are broken by ticker ascending."""
    from tickerlake import pipeline
    from tickerlake.cloud_score import CLOUD_SCORE_SCHEMA
    from tickerlake.config import Config

    config = Config(output_dir=tmp_path)
    (tmp_path / "tickerlake.duckdb").touch()
    csv_path = tmp_path / "ciovacco.csv"

    # ZZZ and AAA tie on total=2.0; MMM wins with total=9.0.
    tied_scores = pl.DataFrame(
        {
            "ticker": ["ZZZ", "AAA", "MMM"],
            "score_1d_cloud": [0.5, 0.5, 1.0],
            "score_weekly_cloud": [0.5, 0.5, 1.0],
            "score_2wk_cloud": [0.5, 0.5, 1.0],
            "score_3wk_cloud": [0.5, 0.5, 1.0],
            "score_monthly_cloud": [0.5, 0.5, 1.0],
            "score_200wk_ma": [0, 0, 1],
            "score_200wk_ma_slope": [0, 0, 1],
            "score_300wk_ma": [0, 0, 1],
            "score_300wk_ma_slope": [0, 0, 1],
            "total": [2.0, 2.0, 9.0],
        },
        schema=CLOUD_SCORE_SCHEMA,
    )

    with patch.multiple(
        _PIPELINE,
        _resolve_race_tickers=lambda consumer_path, **kw: ["AAA", "MMM", "ZZZ"],
        read_daily_bars=DEFAULT,
        read_ticker_names=DEFAULT,
        compute_cloud_scores=DEFAULT,
        render_cloud_scorecard=DEFAULT,
    ) as mocks:
        mocks["read_daily_bars"].return_value = _ciovacco_bars_df(
            ["AAA", "MMM", "ZZZ", "SPY"]
        )
        mocks["read_ticker_names"].return_value = {}
        mocks["compute_cloud_scores"].return_value = tied_scores
        pipeline.ciovacco(config, tickers=["AAA", "MMM", "ZZZ"], csv_path=csv_path)

    written = pl.read_csv(csv_path)
    # MMM first (highest total), then AAA, then ZZZ (tied totals broken by ticker).
    assert written["ticker"].to_list() == ["MMM", "AAA", "ZZZ"]
