"""Behavioral tests for validating fetched market data."""

import datetime
import os
import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import polars as pl
import pytest

from tickerlake.extract import (
    extract_daily_aggs,
    extract_splits,
    extract_tickers,
)
from tickerlake.outcomes import FetchStatus

DAY = datetime.date(2024, 1, 2)
DAILY_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.String,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float32,
    "vwap": pl.Float32,
    "transactions": pl.UInt32,
}
SPLIT_SCHEMA = {
    "ticker": pl.String,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.String,
}
TICKER_SCHEMA = {
    "ticker": pl.String,
    "name": pl.String,
    "type": pl.String,
    "primary_exchange": pl.String,
    "cik": pl.String,
    "active": pl.Boolean,
}


@dataclass
class Record:
    """SDK-like record with attributes rather than mapping access."""

    values: dict

    def __getattr__(self, key):
        """Expose SDK fields as attributes."""
        try:
            return self.values[key]
        except KeyError as error:
            raise AttributeError(key) from error


def agg(**updates):
    """Build a daily aggregate SDK record, allowing field overrides."""
    row = {
        "ticker": "AAPL",
        "timestamp": 1704153600000,
        "open": 185.0,
        "high": 186.0,
        "low": 184.0,
        "close": 185.5,
        "volume": 1000.0,
        "vwap": 185.2,
        "transactions": 25,
    }
    row.update(updates)
    return Record(row)


def split(**updates):
    """Build a split SDK record, allowing field overrides."""
    row = {
        "ticker": "AAPL",
        "execution_date": "2024-08-31",
        "split_from": 1.0,
        "split_to": 4.0,
        "historical_adjustment_factor": 4.0,
        "adjustment_type": "forward",
    }
    row.update(updates)
    return Record(row)


def ticker(**updates):
    """Build a ticker SDK record, allowing field overrides."""
    row = {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "type": "CS",
        "primary_exchange": "XNAS",
        "cik": "0000320193",
        "active": True,
    }
    row.update(updates)
    return Record(row)


def test_daily_rows_keep_sdk_values_and_date():
    """Preserve bar values, UTC date, and output schema."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [agg()]
    result = extract_daily_aggs(client, [DAY])[0]
    assert result.status is FetchStatus.populated
    assert dict(result.frame.schema) == DAILY_SCHEMA
    assert result.frame.to_dicts() == [
        {
            "date": DAY,
            "ticker": "AAPL",
            "open": 185.0,
            "high": 186.0,
            "low": 184.0,
            "close": 185.5,
            "volume": 1000.0,
            "vwap": pytest.approx(185.2, abs=0.0001),
            "transactions": 25,
        }
    ]


@pytest.mark.parametrize(
    "records",
    [[], None, [agg(timestamp=0)], [agg(high=100)], [agg(volume=-1)], [agg(transactions=1.5)], [agg(), agg()]],
)
def test_daily_empty_envelope_vs_bad_or_duplicate_data(records):
    """Distinguish a valid empty response from failed and invalid data."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = records
    result = extract_daily_aggs(client, [DAY])[0]
    if records == []:
        assert result.status is FetchStatus.successful_empty
    elif records is None:
        assert result.status is FetchStatus.failed
    else:
        assert result.status is FetchStatus.quarantined
    assert dict(result.frame.schema) == DAILY_SCHEMA


def test_daily_safe_transport_diagnostic_and_continue_after_error():
    """Hide exception details and continue fetching later requested dates."""
    client = MagicMock()
    client.fetch_daily_aggs.side_effect = [RuntimeError("secret token"), [agg()]]
    results = extract_daily_aggs(client, [DAY, DAY])
    assert [result.status for result in results] == [FetchStatus.failed, FetchStatus.populated]
    assert "secret" not in results[0].diagnostic


def test_mixed_daily_outcomes_keep_requested_date_and_independent_frames():
    """Keep outcomes separate across dates when an empty response is suspicious."""
    second = DAY + datetime.timedelta(days=1)
    previous = pl.DataFrame({"date": [DAY], "ticker": ["AAPL"]}).with_columns(
        pl.col("date").cast(pl.Date), pl.col("ticker").cast(pl.String)
    )
    client = MagicMock()
    client.fetch_daily_aggs.side_effect = [[agg()], [], [agg(timestamp=1704240000000)]]
    results = extract_daily_aggs(client, [DAY, DAY, second], previous=previous)
    assert [(item.requested_date, item.status) for item in results] == [
        (DAY, FetchStatus.populated),
        (DAY, FetchStatus.quarantined),
        (second, FetchStatus.populated),
    ]
    assert [item.frame.height for item in results] == [1, 0, 1]
    assert results[0].frame.select("date", "ticker", "transactions").rows() == [(DAY, "AAPL", 25)]
    assert results[2].frame.select("date", "ticker", "transactions").rows() == [(second, "AAPL", 25)]


def test_daily_reference_shrink_quarantines_without_arbitrary_threshold():
    """Quarantine a removed symbol when prior cached rows exist."""
    prior = pl.DataFrame(
        {
            "date": [DAY, DAY],
            "ticker": ["AAPL", "MSFT"],
            "open": [1.0, 1.0],
            "high": [2.0, 2.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [1.0, 1.0],
            "vwap": [None, None],
            "transactions": [1, 1],
        },
        schema=DAILY_SCHEMA,
    )
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [agg()]
    result = extract_daily_aggs(client, [DAY], previous=prior)[0]
    assert result.status is FetchStatus.quarantined
    assert result.diagnostic == "reference_shrink"
    assert result.frame.is_empty()


def test_split_rows_schema_and_empty_previous_comparison():
    """Preserve split fields while casting to the expected schema."""
    client = MagicMock()
    client.fetch_splits.return_value = [split()]
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is FetchStatus.populated
    assert dict(result.frame.schema) == SPLIT_SCHEMA
    assert result.frame.to_dicts() == [
        {
            "ticker": "AAPL",
            "execution_date": datetime.date(2024, 8, 31),
            "split_from": 1.0,
            "split_to": 4.0,
            "adjustment_factor": 4.0,
            "adjustment_type": "forward",
        }
    ]


@pytest.mark.parametrize("records", [None, [split(split_to=0)], [split(execution_date="bad")], [split(), split()]])
def test_split_envelope_and_record_validation(records):
    """Reject malformed envelopes, fields, dates, and duplicate split events."""
    client = MagicMock()
    client.fetch_splits.return_value = records
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is (FetchStatus.failed if records is None else FetchStatus.quarantined)
    assert dict(result.frame.schema) == SPLIT_SCHEMA


def test_ticker_nullable_metadata_and_duplicate_rejection():
    """Allow nullable metadata and quarantine duplicate symbols."""
    client = MagicMock()
    client.fetch_tickers.return_value = [ticker(name=None, active=None)]
    result = extract_tickers(client, ["CS"])
    assert result.status is FetchStatus.populated
    assert dict(result.frame.schema) == TICKER_SCHEMA
    assert result.frame["name"].to_list() == [None]
    assert result.frame["active"].to_list() == [None]
    client.fetch_tickers.return_value = [ticker(), ticker()]
    assert extract_tickers(client, ["CS"]).status is FetchStatus.quarantined


def test_ticker_empty_and_safe_transport_failure():
    """Distinguish successful empty ticker results from transport errors."""
    client = MagicMock()
    client.fetch_tickers.return_value = []
    assert extract_tickers(client, ["CS"]).status is FetchStatus.successful_empty
    client.fetch_tickers.side_effect = RuntimeError("secret")
    result = extract_tickers(client, ["CS"])
    assert result.status is FetchStatus.failed
    assert result.diagnostic == "transport_error"


def test_ticker_rows_keep_all_fields_and_schema():
    """Preserve the complete ticker reference record and public schema."""
    client = MagicMock()
    client.fetch_tickers.return_value = [ticker()]
    result = extract_tickers(client, ["CS"])
    assert dict(result.frame.schema) == TICKER_SCHEMA
    assert result.frame.to_dicts() == [
        {
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "type": "CS",
            "primary_exchange": "XNAS",
            "cik": "0000320193",
            "active": True,
        }
    ]


def test_nullable_optional_sdk_fields_can_be_absent():
    """Accept absent optional VWAP and ticker name/CIK fields from SDK records."""
    bars = MagicMock()
    bars.fetch_daily_aggs.return_value = [Record({key: value for key, value in agg().values.items() if key != "vwap"})]
    bar_result = extract_daily_aggs(bars, [DAY])[0]
    assert bar_result.status is FetchStatus.populated
    assert bar_result.frame["vwap"].to_list() == [None]
    tickers = MagicMock()
    tickers.fetch_tickers.return_value = [
        Record({key: value for key, value in ticker().values.items() if key not in {"name", "cik"}})
    ]
    ticker_result = extract_tickers(tickers, ["CS"])
    assert ticker_result.status is FetchStatus.populated
    assert ticker_result.frame.select("name", "cik").row(0) == (None, None)


def test_previous_daily_comparison_is_scoped_to_requested_date():
    """Rows cached for another date do not cause the requested date to shrink."""
    prior_day = DAY - datetime.timedelta(days=1)
    previous = pl.DataFrame({"date": [prior_day], "ticker": ["MSFT"]})
    client = MagicMock()
    client.fetch_daily_aggs.return_value = []
    result = extract_daily_aggs(client, [DAY], previous=previous)[0]
    assert result.status is FetchStatus.successful_empty


def test_daily_empty_request_does_not_call_client():
    """Return no outcomes without calling the external boundary for no dates."""
    client = MagicMock()
    assert extract_daily_aggs(client, []) == []
    client.fetch_daily_aggs.assert_not_called()


def test_daily_bar_that_overflows_float32_is_quarantined():
    """A self-consistent bar outside Float32 range must not become populated."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [agg(open=1e100, high=2e100, low=0.5e100, close=1.5e100)]
    result = extract_daily_aggs(client, [DAY])[0]
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_split_prior_change_quarantines_and_empty_bootstrap_is_successful():
    """Reject missing cached in-range splits but allow an empty initial history."""
    previous = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "execution_date": [datetime.date(2024, 8, 31)],
            "split_from": [1.0],
            "split_to": [4.0],
            "adjustment_factor": [4.0],
            "adjustment_type": ["forward"],
        },
        schema=SPLIT_SCHEMA,
    )
    client = MagicMock()
    client.fetch_splits.return_value = []
    rejected = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31), previous=previous)
    assert rejected.status is FetchStatus.quarantined
    assert rejected.frame.is_empty()
    accepted = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert accepted.status is FetchStatus.successful_empty


def test_split_float32_overflow_is_quarantined():
    """Quarantine split ratios that become infinite after Float32 casting."""
    client = MagicMock()
    client.fetch_splits.return_value = [split(split_from=1e100, split_to=2e100)]
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_split_ratio_that_underflows_to_zero_is_quarantined():
    """Quarantine positive source ratios that become zero in canonical Float32."""
    client = MagicMock()
    client.fetch_splits.return_value = [split(split_from=1e-100)]
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_splits_collapsing_to_same_canonical_event_are_quarantined():
    """Reject distinct source events that collide under the persisted schema."""
    client = MagicMock()
    client.fetch_splits.return_value = [
        split(split_to=1.00000001),
        split(split_to=1.00000002),
    ]
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_split_adjustment_type_may_be_null():
    """Preserve a nullable optional split adjustment type."""
    client = MagicMock()
    client.fetch_splits.return_value = [split(adjustment_type=None)]
    result = extract_splits(client, datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
    assert result.status is FetchStatus.populated
    assert result.frame["adjustment_type"].to_list() == [None]


def test_daily_oversized_volume_is_quarantined():
    """Quarantine finite source volume that overflows its canonical Float32 type."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [agg(volume=1e100)]
    result = extract_daily_aggs(client, [DAY])[0]
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_daily_utc_timestamp_is_not_shifted_by_host_timezone(monkeypatch):
    """Decode API timestamps in UTC, regardless of the host timezone."""
    if not hasattr(time, "tzset"):
        pytest.skip("Changing process timezone requires time.tzset")
    old_tz = os.environ.get("TZ")
    try:
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        time.tzset()
        client = MagicMock()
        client.fetch_daily_aggs.return_value = [agg(timestamp=1704171600000)]
        result = extract_daily_aggs(client, [DAY])[0]
        assert result.status is FetchStatus.populated
        assert result.frame["date"].to_list() == [DAY]
    finally:
        if old_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", old_tz)
        time.tzset()


@pytest.mark.parametrize("endpoint", ["daily", "splits", "tickers"])
def test_transport_exception_secret_never_reaches_diagnostics_or_logs(endpoint, caplog):
    """Keep private exception details out of outcome diagnostics and logs."""
    client = MagicMock()
    getattr(
        client, {"daily": "fetch_daily_aggs", "splits": "fetch_splits", "tickers": "fetch_tickers"}[endpoint]
    ).side_effect = RuntimeError("sentinel-secret-123")
    if endpoint == "daily":
        results = extract_daily_aggs(client, [DAY])
        diagnostic = results[0].diagnostic
    elif endpoint == "splits":
        diagnostic = extract_splits(client, DAY, DAY).diagnostic
    else:
        diagnostic = extract_tickers(client, ["CS"]).diagnostic
    assert "sentinel-secret-123" not in (diagnostic or "")
    assert "sentinel-secret-123" not in caplog.text


@pytest.mark.parametrize("updates", [{"transactions": None}, {"transactions": 2**32}, {"open": 1e100}])
def test_daily_unrepresentable_or_missing_required_values_are_quarantined(updates):
    """Do not let invalid counts or lossy numeric casts produce populated data."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [agg(**updates)]
    result = extract_daily_aggs(client, [DAY])[0]
    assert result.status is FetchStatus.quarantined
    assert result.frame.is_empty()


def test_daily_nullable_vwap_and_dictionary_records_are_supported():
    """Accept legitimate nullable VWAP on mapping-based SDK responses."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = [
        {
            "ticker": "AAPL",
            "timestamp": 1704153600000,
            "open": 185.0,
            "high": 186.0,
            "low": 184.0,
            "close": 185.5,
            "volume": 1000.0,
            "vwap": None,
            "transactions": 25,
        }
    ]
    result = extract_daily_aggs(client, [DAY])[0]
    assert result.status is FetchStatus.populated
    assert result.frame["vwap"].to_list() == [None]


def test_ticker_previous_symbol_removal_is_quarantined():
    """Quarantine a nonempty reference refresh that omits a cached symbol."""
    previous = pl.DataFrame({"ticker": ["AAPL", "MSFT"]}, schema={"ticker": pl.String})
    client = MagicMock()
    client.fetch_tickers.return_value = [ticker()]
    result = extract_tickers(client, ["CS"], previous=previous)
    assert result.status is FetchStatus.quarantined
    assert result.diagnostic == "reference_shrink"
    assert result.frame.is_empty()


def test_split_comparison_uses_canonical_schema_and_ignores_out_of_range_prior():
    """Compare canonical values by schema order and only within requested range."""
    current = extract_splits(
        MagicMock(fetch_splits=MagicMock(return_value=[split()])),
        datetime.date(2024, 8, 1),
        datetime.date(2024, 8, 31),
    ).frame
    previous = pl.concat([current, current.with_columns(pl.lit(datetime.date(2023, 1, 1)).alias("execution_date"))])
    reordered = previous.select(list(reversed(previous.columns)))
    client = MagicMock()
    client.fetch_splits.return_value = [split()]
    result = extract_splits(client, datetime.date(2024, 8, 1), datetime.date(2024, 8, 31), previous=reordered)
    assert result.status is FetchStatus.populated


@pytest.mark.parametrize("records", [[agg(timestamp=float("nan"))], [agg(ticker=7)], [{"ticker": "AAPL"}]])
def test_daily_bad_timestamp_ticker_or_missing_required_fields_quarantines(records):
    """Quarantine invalid timestamps, symbol types, and missing required fields."""
    client = MagicMock()
    client.fetch_daily_aggs.return_value = records
    assert extract_daily_aggs(client, [DAY])[0].status is FetchStatus.quarantined
