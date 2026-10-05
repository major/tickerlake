"""Focused unit tests for state.py input-validation helpers (Postgres-gated suite)."""

# The private helpers are the unit under test for this file.
# ruff: noqa: SLF001

import datetime as dt
from typing import cast
from uuid import UUID

import polars as pl
import pytest

from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres import state
from tickerlake.postgres.connection import PostgresWriterError
from tickerlake.postgres.models import FetchRequest

_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")


def _req(**kwargs: object) -> FetchRequest:
    """Build a FetchRequest with sensible defaults; only `source` is required."""
    defaults: dict[str, object] = {"run_id": _RUN_ID, "source": "daily"}
    defaults.update(kwargs)
    return FetchRequest(**defaults)  # type: ignore[arg-type]


# --- _daily_scope_ok --------------------------------------------------------


def test_daily_scope_ok_with_only_date() -> None:
    """A single requested date with no range or tickers is valid."""
    assert state._daily_scope_ok(_req(requested_date=dt.date(2025, 1, 2)))


def test_daily_scope_rejects_missing_date() -> None:
    """A daily request without a date is invalid."""
    assert not state._daily_scope_ok(_req())


def test_daily_scope_rejects_range() -> None:
    """A daily request with a range alongside the date is invalid."""
    assert not state._daily_scope_ok(_req(requested_date=dt.date(2025, 1, 2), requested_start=dt.date(2025, 1, 1)))


def test_daily_scope_rejects_ticker_types() -> None:
    """A daily request carrying ticker types is invalid."""
    assert not state._daily_scope_ok(_req(requested_date=dt.date(2025, 1, 2), ticker_types=("CS",)))


# --- _splits_scope_ok -------------------------------------------------------


def test_splits_scope_ok_with_ordered_range() -> None:
    """An ordered date range with no tickers is valid."""
    assert state._splits_scope_ok(
        _req(source="splits", requested_start=dt.date(2025, 1, 1), requested_end=dt.date(2025, 1, 2))
    )


def test_splits_scope_rejects_missing_end() -> None:
    """A split request needs both range endpoints."""
    assert not state._splits_scope_ok(_req(source="splits", requested_start=dt.date(2025, 1, 1)))


def test_splits_scope_rejects_swapped_range() -> None:
    """A range whose start is after its end is invalid."""
    assert not state._splits_scope_ok(
        _req(source="splits", requested_start=dt.date(2025, 1, 2), requested_end=dt.date(2025, 1, 1))
    )


def test_splits_scope_rejects_populated_date() -> None:
    """A split request must not also carry a single date."""
    assert not state._splits_scope_ok(
        _req(
            source="splits",
            requested_date=dt.date(2025, 1, 2),
            requested_start=dt.date(2025, 1, 1),
            requested_end=dt.date(2025, 1, 2),
        )
    )


def test_splits_scope_rejects_ticker_types() -> None:
    """A split request carrying ticker types is invalid."""
    assert not state._splits_scope_ok(
        _req(
            source="splits",
            requested_start=dt.date(2025, 1, 1),
            requested_end=dt.date(2025, 1, 2),
            ticker_types=("CS",),
        )
    )


# --- _tickers_scope_ok ------------------------------------------------------


def test_tickers_scope_ok_with_unique_nonempty_types() -> None:
    """Unique non-empty ticker types with no dates are valid."""
    assert state._tickers_scope_ok(_req(source="tickers", ticker_types=("CS", "ETF")))


def test_tickers_scope_rejects_non_tuple_types() -> None:
    """A non-tuple ticker_types value is rejected before validation."""
    assert not state._tickers_scope_ok(_req(source="tickers", ticker_types=cast("tuple[str, ...]", ["CS"])))


def test_tickers_scope_rejects_empty_string() -> None:
    """A blank ticker type is rejected by the scope predicate."""
    assert not state._tickers_scope_ok(_req(source="tickers", ticker_types=("",)))


def test_tickers_scope_rejects_duplicates() -> None:
    """Duplicate ticker types are rejected by the scope predicate."""
    assert not state._tickers_scope_ok(_req(source="tickers", ticker_types=("CS", "CS")))


def test_tickers_scope_rejects_populated_date() -> None:
    """Ticker requests must not also carry a date."""
    assert not state._tickers_scope_ok(_req(source="tickers", ticker_types=("CS",), requested_date=dt.date(2025, 1, 2)))


# --- _validate_fetch_inputs -------------------------------------------------


def test_validate_fetch_inputs_rejects_unknown_source() -> None:
    """An unknown source raises the safe source error."""
    with pytest.raises(PostgresWriterError, match="Invalid PostgreSQL fetch source"):
        state._validate_fetch_inputs(
            _req(source="bogus"),
            FetchOutcome(status=FetchStatus.populated, frame=pl.DataFrame()),
        )


def test_validate_fetch_inputs_rejects_bad_scope() -> None:
    """A split request missing its range raises the safe scope error."""
    with pytest.raises(PostgresWriterError, match="Invalid PostgreSQL fetch request scope"):
        state._validate_fetch_inputs(
            _req(source="splits"),
            FetchOutcome(status=FetchStatus.failed, frame=pl.DataFrame(), diagnostic="transport_error"),
        )


def test_validate_fetch_inputs_rejects_bad_diagnostic() -> None:
    """An untrusted diagnostic string raises the safe diagnostic error."""
    with pytest.raises(PostgresWriterError, match="Unsupported PostgreSQL fetch diagnostic"):
        state._validate_fetch_inputs(
            _req(source="daily", requested_date=dt.date(2025, 1, 2)),
            FetchOutcome(status=FetchStatus.failed, frame=pl.DataFrame(), diagnostic="token=secret"),
        )


def test_validate_fetch_inputs_source_runs_before_scope() -> None:
    """An unknown source raises before scope or diagnostic checks run."""
    with pytest.raises(PostgresWriterError, match="Invalid PostgreSQL fetch source"):
        state._validate_fetch_inputs(
            _req(source="bogus"),
            FetchOutcome(status=FetchStatus.failed, frame=pl.DataFrame(), diagnostic="token=secret"),
        )
