"""Contract tests for extraction outcome value types."""

import datetime
import datetime as dt
from dataclasses import FrozenInstanceError
from uuid import uuid4

import polars as pl
import pytest

from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres.models import FetchRequest


def test_fetch_status_values_and_frozen_outcome():
    """Keep the public outcome values stable and immutable."""
    assert {status.value for status in FetchStatus} == {"failed", "quarantined", "populated", "successful_empty"}
    result = FetchOutcome(FetchStatus.successful_empty, pl.DataFrame(), requested_date=datetime.date(2024, 1, 2))
    assert result.requested_date == datetime.date(2024, 1, 2)
    assert result.diagnostic is None
    with pytest.raises(FrozenInstanceError):
        result.status = FetchStatus.failed


def test_fetch_outcome_is_populated() -> None:
    """Only the populated status reports populated."""
    populated = FetchOutcome(status=FetchStatus.populated, frame=pl.DataFrame())
    assert populated.is_populated()
    assert populated.is_accepted()
    for status in (FetchStatus.failed, FetchStatus.quarantined, FetchStatus.successful_empty):
        other = FetchOutcome(status=status, frame=pl.DataFrame())
        assert not other.is_populated()


def test_fetch_outcome_is_accepted() -> None:
    """Populated and successful_empty count as accepted; the others do not."""
    accepted_statuses = {FetchStatus.populated, FetchStatus.successful_empty}
    for status in FetchStatus:
        outcome = FetchOutcome(status=status, frame=pl.DataFrame())
        assert outcome.is_accepted() is (status in accepted_statuses)


def test_fetch_outcome_matches_daily_request_without_date() -> None:
    """An outcome with no requested_date matches any request."""
    outcome = FetchOutcome(status=FetchStatus.populated, frame=pl.DataFrame())
    request = FetchRequest(run_id=uuid4(), source="daily", requested_date=dt.date(2025, 1, 2))
    assert outcome.matches_daily_request(request)


def test_fetch_outcome_matches_daily_request_with_matching_date() -> None:
    """A daily request with the same requested_date matches."""
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 2),
    )
    request = FetchRequest(run_id=uuid4(), source="daily", requested_date=dt.date(2025, 1, 2))
    assert outcome.matches_daily_request(request)


def test_fetch_outcome_matches_daily_request_rejects_mismatch() -> None:
    """An outcome whose date differs from the daily request is rejected."""
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 3),
    )
    request = FetchRequest(run_id=uuid4(), source="daily", requested_date=dt.date(2025, 1, 2))
    assert not outcome.matches_daily_request(request)


def test_fetch_outcome_matches_daily_request_rejects_non_daily_source() -> None:
    """A populated outcome date is meaningless for non-daily requests."""
    outcome = FetchOutcome(
        status=FetchStatus.populated,
        frame=pl.DataFrame(),
        requested_date=dt.date(2025, 1, 2),
    )
    request = FetchRequest(run_id=uuid4(), source="tickers", ticker_types=("CS",))
    assert not outcome.matches_daily_request(request)
