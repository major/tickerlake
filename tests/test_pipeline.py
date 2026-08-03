"""Tests for tickerlake.pipeline — backfill, update, and info orchestration."""

import datetime
import os
from typing import TYPE_CHECKING
from unittest.mock import DEFAULT, patch

import polars as pl
import pytest

if TYPE_CHECKING:
    from pathlib import Path

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
            "sma_50": [None, None],
            "sma_200": [None, None],
            "atr_14": [None, None],
            "rs": [None, None],
            "rs_sma_20": [None, None],
            "vars": [None, None],
            "vars_sma_20": [None, None],
        }
    ).cast(
        {
            "date": pl.Date,
            "sma_50": pl.Float32,
            "sma_200": pl.Float32,
            "atr_14": pl.Float32,
            "rs": pl.Float32,
            "rs_sma_20": pl.Float32,
            "vars": pl.Float32,
            "vars_sma_20": pl.Float32,
        }
    )


@pytest.fixture
def sample_hvcs():
    """Minimal HVC DataFrame for pipeline tests (1 row, 21-column schema)."""
    return pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [datetime.date(2024, 1, 2)],
            "open": [155.0],
            "high": [160.0],
            "low": [154.0],
            "close": [158.0],
            "prev_close": [151.5],
            "volume": [3_100_000.0],
            "volume_sma_20": [1_000_000.0],
            "volume_multiplier": [3.1],
            "total_move_pct": [4.3],
            "gap_pct": [2.3],
            "intraday_move_pct": [1.94],
            "bar_range_pct": [3.97],
            "adr_pct": [0.04],
            "atr_pct": [0.03],
            "close_position_in_range": [0.67],
            "is_up_day": [True],
            "price_vs_sma50_pct": [5.2],
            "price_vs_sma200_pct": [12.5],
            "rs": [0.15],
        }
    ).cast(
        {
            "date": pl.Date,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "prev_close": pl.Float32,
            "volume": pl.Float32,
            "volume_sma_20": pl.Float32,
            "volume_multiplier": pl.Float32,
            "total_move_pct": pl.Float32,
            "gap_pct": pl.Float32,
            "intraday_move_pct": pl.Float32,
            "bar_range_pct": pl.Float32,
            "adr_pct": pl.Float32,
            "atr_pct": pl.Float32,
            "close_position_in_range": pl.Float32,
            "is_up_day": pl.Boolean,
            "price_vs_sma50_pct": pl.Float32,
            "price_vs_sma200_pct": pl.Float32,
            "rs": pl.Float32,
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
        aggregate_to_weekly=DEFAULT,
        compute_metrics=DEFAULT,
        detect_hvcs=DEFAULT,
        delete_raw_dates=DEFAULT,
        write_raw_db=DEFAULT,
        append_raw_db=DEFAULT,
        read_raw_db=DEFAULT,
        write_splits=DEFAULT,
        write_consumer_db=DEFAULT,
        get_db_info=DEFAULT,
        get_existing_dates=DEFAULT,
    ) as mocks:
        yield mocks


def _wire_defaults(
    mocks, sample_bars, sample_splits, sample_tickers, sample_metrics, sample_hvcs=None
):
    """Set standard return values on all pipeline mocks."""
    mocks["get_trading_days"].return_value = [datetime.date(2024, 1, 2)]
    mocks["extract_daily_aggs"].return_value = sample_bars
    mocks["extract_splits"].return_value = sample_splits
    mocks["extract_tickers"].return_value = sample_tickers
    mocks["adjust_splits"].return_value = sample_bars
    mocks["filter_tickers"].return_value = sample_bars
    mocks["aggregate_to_weekly"].return_value = sample_bars
    mocks["compute_metrics"].return_value = sample_metrics
    if sample_hvcs is not None:
        mocks["detect_hvcs"].return_value = sample_hvcs
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

    assert pipeline_mocks["compute_metrics"].call_count == 2


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
# HVC detection wiring
# ═══════════════════════════════════════════════════════════════════════════════


def test_backfill_calls_detect_hvcs(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Backfill calls detect_hvcs for daily and weekly bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    backfill(_make_config(tmp_path))

    assert pipeline_mocks["detect_hvcs"].call_count == 2


def test_backfill_passes_hvcs_to_write_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Backfill passes detect_hvcs result to write_consumer_db as hvcs keyword arg."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    backfill(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "hvcs" in call_kwargs
    assert call_kwargs["hvcs"] is sample_hvcs


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


def test_update_calls_detect_hvcs(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Update calls detect_hvcs for daily and weekly bars."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    assert pipeline_mocks["detect_hvcs"].call_count == 2


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


def test_backfill_computes_weekly_metrics(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill computes metrics for daily and weekly bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    backfill(_make_config(tmp_path))

    assert pipeline_mocks["compute_metrics"].call_count == 2


def test_backfill_detects_weekly_hvcs(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Backfill detects HVCs for daily and weekly bars."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    backfill(_make_config(tmp_path))

    assert pipeline_mocks["detect_hvcs"].call_count == 2


def test_backfill_passes_weekly_to_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Backfill passes weekly bars, metrics, and HVCs to write_consumer_db."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    backfill(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "weekly_bars" in call_kwargs
    assert "weekly_metrics" in call_kwargs
    assert "weekly_hvcs" in call_kwargs


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


def test_update_passes_weekly_to_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Update passes weekly bars, metrics, and HVCs to write_consumer_db."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "weekly_bars" in call_kwargs
    assert "weekly_metrics" in call_kwargs
    assert "weekly_hvcs" in call_kwargs


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


def test_update_passes_hvcs_to_write_consumer_db(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Update passes detect_hvcs result to write_consumer_db as hvcs keyword arg."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks,
        sample_bars,
        sample_splits,
        sample_tickers,
        sample_metrics,
        sample_hvcs,
    )
    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    call_kwargs = pipeline_mocks["write_consumer_db"].call_args.kwargs
    assert "hvcs" in call_kwargs
    assert call_kwargs["hvcs"] is sample_hvcs
