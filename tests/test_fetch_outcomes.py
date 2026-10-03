"""Contract tests for extraction outcome value types."""

import datetime
from dataclasses import FrozenInstanceError

import polars as pl
import pytest

from tickerlake.outcomes import FetchOutcome, FetchStatus


def test_fetch_status_values_and_frozen_outcome():
    """Keep the public outcome values stable and immutable."""
    assert {status.value for status in FetchStatus} == {"failed", "quarantined", "populated", "successful_empty"}
    result = FetchOutcome(FetchStatus.successful_empty, pl.DataFrame(), requested_date=datetime.date(2024, 1, 2))
    assert result.requested_date == datetime.date(2024, 1, 2)
    assert result.diagnostic is None
    with pytest.raises(FrozenInstanceError):
        result.status = FetchStatus.failed
