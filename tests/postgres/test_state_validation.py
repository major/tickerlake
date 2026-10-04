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


def test_tickers_scope_raises_on_empty_string() -> None:
    """A blank ticker type raises the safe scope error."""
    with pytest.raises(PostgresWriterError, match="Invalid PostgreSQL fetch request scope"):
        state._tickers_scope_ok(_req(source="tickers", ticker_types=("",)))


def test_tickers_scope_raises_on_duplicates() -> None:
    """Duplicate ticker types raise the safe scope error."""
    with pytest.raises(PostgresWriterError, match="Invalid PostgreSQL fetch request scope"):
        state._tickers_scope_ok(_req(source="tickers", ticker_types=("CS", "CS")))


def test_tickers_scope_rejects_populated_date() -> None:
    """Ticker requests must not also carry a date."""
    assert not state._tickers_scope_ok(_req(source="tickers", ticker_types=("CS",), requested_date=dt.date(2025, 1, 2)))


# --- _started_at_ok ---------------------------------------------------------


def test_started_at_ok_with_none() -> None:
    """An absent started_at is valid."""
    assert state._started_at_ok(None)


def test_started_at_ok_with_aware_datetime() -> None:
    """An aware datetime is valid."""
    assert state._started_at_ok(dt.datetime(2025, 1, 2, tzinfo=dt.UTC))


def test_started_at_rejects_naive_datetime() -> None:
    """A naive datetime is rejected."""
    assert not state._started_at_ok(dt.datetime(2025, 1, 2))  # noqa: DTZ001


def test_started_at_rejects_plain_date() -> None:
    """A plain date is not a valid started_at."""
    assert not state._started_at_ok(dt.date(2025, 1, 2))


def test_started_at_rejects_tzinfo_without_utcoffset() -> None:
    """A tzinfo that cannot resolve an offset does not make started_at valid."""

    class _NullOffsetTzinfo(dt.tzinfo):
        """A tzinfo whose offset cannot be resolved."""

        def utcoffset(self, when: dt.datetime | None) -> dt.timedelta | None:
            """Report no UTC offset."""
            return None

        def dst(self, when: dt.datetime | None) -> dt.timedelta | None:
            """Report no daylight-saving adjustment."""
            return None

        def tzname(self, when: dt.datetime | None) -> str | None:
            """Report no timezone name."""
            return None

    assert not state._started_at_ok(dt.datetime(2025, 1, 2, tzinfo=_NullOffsetTzinfo()))


# --- _outcome_date_ok -------------------------------------------------------


def test_outcome_date_ok_with_none() -> None:
    """An outcome without a date is always valid."""
    request = _req(requested_date=dt.date(2025, 1, 2))
    outcome = FetchOutcome(status=FetchStatus.populated, frame=pl.DataFrame())
    assert state._outcome_date_ok(request, outcome)


def test_outcome_date_ok_with_matching_daily_date() -> None:
    """An outcome date matching a daily request date is valid."""
    request = _req(requested_date=dt.date(2025, 1, 2))
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 2),
    )
    assert state._outcome_date_ok(request, outcome)


def test_outcome_date_rejects_mismatch() -> None:
    """An outcome date that differs from the request date is invalid."""
    request = _req(requested_date=dt.date(2025, 1, 2))
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 3),
    )
    assert not state._outcome_date_ok(request, outcome)


def test_outcome_date_rejects_non_daily_source() -> None:
    """An outcome date is only meaningful for daily requests."""
    request = _req(source="tickers", ticker_types=("CS",))
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 2),
    )
    assert not state._outcome_date_ok(request, outcome)


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
