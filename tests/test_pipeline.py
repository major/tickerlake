"""Tests for tickerlake.pipeline — backfill, update, and info orchestration."""

import datetime
import os
from pathlib import Path
from unittest.mock import DEFAULT, patch

import polars as pl
import pytest

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
    """Patch all pipeline dependencies via patch.multiple, yielding a name-keyed dict."""
    with patch.multiple(
        _PIPELINE,
        get_trading_days=DEFAULT,
        MassiveClient=DEFAULT,
        extract_daily_aggs=DEFAULT,
        extract_splits=DEFAULT,
        extract_tickers=DEFAULT,
        adjust_splits=DEFAULT,
        filter_tickers=DEFAULT,
        compute_metrics=DEFAULT,
        detect_hvcs=DEFAULT,
        write_raw_db=DEFAULT,
        append_raw_db=DEFAULT,
        read_raw_db=DEFAULT,
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
    mocks["compute_metrics"].return_value = sample_metrics
    if sample_hvcs is not None:
        mocks["detect_hvcs"].return_value = sample_hvcs
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

    pipeline_mocks["compute_metrics"].assert_called_once()


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
    """Backfill only fetches dates not already in raw.duckdb."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["get_trading_days"].return_value = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
    ]
    pipeline_mocks["get_existing_dates"].return_value = {datetime.date(2024, 1, 2)}

    backfill(_make_config(tmp_path))

    call_args = pipeline_mocks["extract_daily_aggs"].call_args
    assert call_args[0][1] == [datetime.date(2024, 1, 3)]
    pipeline_mocks["append_raw_db"].assert_called_once()
    pipeline_mocks["write_raw_db"].assert_not_called()


def test_backfill_all_cached(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Backfill skips extraction entirely when all dates are cached."""
    from tickerlake.pipeline import backfill

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["get_trading_days"].return_value = [datetime.date(2024, 1, 2)]
    pipeline_mocks["get_existing_dates"].return_value = {datetime.date(2024, 1, 2)}

    backfill(_make_config(tmp_path))

    pipeline_mocks["extract_daily_aggs"].assert_not_called()
    pipeline_mocks["write_raw_db"].assert_not_called()
    pipeline_mocks["append_raw_db"].assert_not_called()
    pipeline_mocks["read_raw_db"].assert_called_once()
    pipeline_mocks["write_consumer_db"].assert_called_once()


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


def test_update_reads_existing_raw_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update calls read_raw_db to get existing data."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["read_raw_db"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    pipeline_mocks["read_raw_db"].assert_called()


def test_update_fetches_only_new_days(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update only fetches days after max date in existing raw.duckdb."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["read_raw_db"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    call_args = pipeline_mocks["get_trading_days"].call_args
    assert call_args[0][0] == datetime.date(2024, 1, 3)


def test_update_appends_to_raw_db(
    pipeline_mocks, tmp_path, sample_bars, sample_splits, sample_tickers, sample_metrics
):
    """Update calls append_raw_db with new bars."""
    from tickerlake.pipeline import update

    _wire_defaults(
        pipeline_mocks, sample_bars, sample_splits, sample_tickers, sample_metrics
    )
    pipeline_mocks["read_raw_db"].return_value = sample_bars

    (tmp_path / "raw.duckdb").touch()
    config = _make_config(tmp_path)
    update(config)

    pipeline_mocks["append_raw_db"].assert_called_once()
    assert (
        pipeline_mocks["append_raw_db"].call_args[0][1]
        == config.output_dir / "raw.duckdb"
    )


def test_update_no_new_days(pipeline_mocks, tmp_path, sample_bars):
    """If no new trading days, logs warning and skips fetching."""
    from tickerlake.pipeline import update

    pipeline_mocks["read_raw_db"].return_value = sample_bars
    pipeline_mocks["get_trading_days"].return_value = []

    (tmp_path / "raw.duckdb").touch()
    update(_make_config(tmp_path))

    pipeline_mocks["extract_daily_aggs"].assert_not_called()


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
    """Backfill calls detect_hvcs once after compute_metrics."""
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

    pipeline_mocks["detect_hvcs"].assert_called_once()


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


def test_update_calls_detect_hvcs(
    pipeline_mocks,
    tmp_path,
    sample_bars,
    sample_splits,
    sample_tickers,
    sample_metrics,
    sample_hvcs,
):
    """Update calls detect_hvcs once in the update flow."""
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

    pipeline_mocks["detect_hvcs"].assert_called_once()


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
