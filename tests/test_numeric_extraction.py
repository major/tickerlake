"""Numeric precision and boundary behavior for daily aggregate extraction."""

import datetime
from dataclasses import dataclass
from unittest.mock import MagicMock

import polars as pl
import pytest

from tickerlake.extract import extract_daily_aggs
from tickerlake.outcomes import FetchStatus

DAY = datetime.date(2024, 1, 2)


@dataclass
class Record:
    """SDK-like attribute record used by extraction tests."""

    values: dict

    def __getattr__(self, key):
        """Return a stored field or raise the normal missing-attribute error."""
        try:
            return self.values[key]
        except KeyError as error:
            raise AttributeError(key) from error


def aggregate(**updates):
    """Build a valid daily aggregate with optional field overrides."""
    row = {
        "ticker": "AAPL",
        "timestamp": 1704153600000,
        "open": 185.0,
        "high": 186.0,
        "low": 184.0,
        "close": 185.5,
        "volume": 1000.0,
        "transactions": 25,
    }
    row.update(updates)
    return Record(row)


def extract(record):
    """Extract one record using a small fake client boundary."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [record]
    return extract_daily_aggs(client, [DAY])[0]


def test_daily_schema_uses_float64_volume_and_int64_transactions():
    """Expose volume and transaction counts in the wider canonical types."""
    outcome = extract(aggregate())

    assert outcome.status is FetchStatus.populated
    assert outcome.frame.schema["volume"] == pl.Float64
    assert outcome.frame.schema["transactions"] == pl.Int64


def test_volume_keeps_fractional_precision_above_float32_integer_precision():
    """Keep fractional volume precision beyond Float32's exact integer range."""
    volume = 2**24 + 1.25

    outcome = extract(aggregate(volume=volume))

    assert outcome.status is FetchStatus.populated
    assert outcome.frame["volume"].item() == volume


@pytest.mark.parametrize("count", [2**32, 2**63 - 1])
def test_transactions_accept_values_above_uint32_through_int64_boundary(count):
    """Accept nonnegative transaction counts representable as signed Int64."""
    outcome = extract(aggregate(transactions=count))

    assert outcome.status is FetchStatus.populated
    assert outcome.frame["transactions"].item() == count


@pytest.mark.parametrize("count", [2**63, -1, 1.5, True, None])
def test_transactions_outside_nonnegative_int64_are_quarantined(count):
    """Reject missing, invalid, negative, boolean, and overflowing counts."""
    outcome = extract(aggregate(transactions=count))

    assert outcome.status is FetchStatus.quarantined
    assert outcome.frame.is_empty()


def test_large_finite_volume_is_valid_but_float32_price_overflow_is_not():
    """Allow finite Float64 volume without weakening Float32 price validation."""
    volume = extract(aggregate(volume=1e100))
    price = extract(aggregate(open=1e100, high=2e100, low=0.5e100, close=1.5e100))

    assert volume.status is FetchStatus.populated
    assert volume.frame["volume"].item() == float("1e100")
    assert price.status is FetchStatus.quarantined
    assert price.frame.is_empty()
