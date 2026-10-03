"""Observable cache and publication behavior for extraction outcomes."""

import datetime
from typing import TYPE_CHECKING

import duckdb
import pytest

from tickerlake import pipeline
from tickerlake.config import Config

if TYPE_CHECKING:
    from pathlib import Path


class _Bar:
    def __init__(self, date: datetime.date, close: float) -> None:
        self.timestamp = int(datetime.datetime.combine(date, datetime.time(), datetime.UTC).timestamp() * 1000)
        self.ticker = "AAA"
        self.open = close
        self.high = close + 1
        self.low = close - 1
        self.close = close
        self.volume = 1000
        self.vwap = close
        self.transactions = 10


class _Ticker:
    ticker = "AAA"
    name = "Example Co"
    type = "CS"
    primary_exchange = "XNAS"
    cik = "0000000001"
    active = True


class _Split:
    def __init__(self, ticker: str, execution_date: datetime.date) -> None:
        self.ticker = ticker
        self.execution_date = execution_date.isoformat()
        self.split_from = 1
        self.split_to = 4
        self.historical_adjustment_factor = 0.25
        self.adjustment_type = "forward"


class _Client:
    def __init__(self) -> None:
        self.bars: dict[datetime.date, list[_Bar]] = {}
        self.fail_dates: set[datetime.date] = set()
        self.fail_splits = False
        self.fail_tickers = False
        self.tickers = [_Ticker()]
        self.splits: list[_Split] = []

    def fetch_daily_aggs(self, date: datetime.date) -> list[_Bar]:
        if date in self.fail_dates:
            raise RuntimeError
        return self.bars.get(date, [])

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list:
        if self.fail_splits:
            raise RuntimeError
        return [
            split
            for split in self.splits
            if start_date <= datetime.date.fromisoformat(split.execution_date) <= end_date
        ]

    def fetch_tickers(self, types: list[str]) -> list[_Ticker]:
        if self.fail_tickers:
            raise RuntimeError
        return [ticker for ticker in self.tickers if ticker.type in types]


def _rows(path: Path, query: str) -> list[tuple]:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(query).fetchall()


def _database_snapshot(path: Path) -> dict[str, list[tuple]]:
    """Capture every table's observable rows for publication-preservation checks."""
    with duckdb.connect(str(path), read_only=True) as connection:
        tables = [row[0] for row in connection.execute("SHOW TABLES").fetchall()]
        return {
            table: sorted(
                connection.execute(f'SELECT * FROM "{table}"').fetchall(),  # noqa: S608
                key=repr,
            )
            for table in tables
        }


def test_failed_daily_fetch_persists_other_successes_but_keeps_old_consumer(tmp_path: Path, monkeypatch) -> None:
    """Persist populated raw outcomes but leave the last consumer generation intact."""
    first = datetime.date(2024, 1, 2)
    second = datetime.date(2024, 1, 3)
    client = _Client()
    client.bars = {first: [_Bar(first, 10)], second: [_Bar(second, 20)]}
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    config = Config(api_key="test", output_dir=tmp_path, start_date=first, end_date=second)
    pipeline.backfill(config)
    old_consumer = _database_snapshot(tmp_path / "tickerlake.duckdb")

    client.bars[first] = [_Bar(first, 11)]
    client.fail_dates.add(second)
    with pytest.raises(pipeline.ExtractionIncompleteError, match="2024-01-03=failed"):
        pipeline.backfill(config)

    assert _rows(tmp_path / "raw.duckdb", "SELECT date, close FROM raw_daily_bars ORDER BY date") == [
        (first, 11.0),
        (second, 20.0),
    ]
    assert _database_snapshot(tmp_path / "tickerlake.duckdb") == old_consumer


@pytest.mark.parametrize("invalid_result", ["empty", "quarantined"])
def test_empty_or_quarantined_daily_result_blocks_consumer_publication(
    tmp_path: Path, monkeypatch, invalid_result: str
) -> None:
    """Do not publish a consumer database when a requested date is empty or quarantined."""
    first = datetime.date(2024, 1, 2)
    second = datetime.date(2024, 1, 3)
    client = _Client()
    client.bars = {first: [_Bar(first, 10)], second: [_Bar(second, 20)]}
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    config = Config(api_key="test", output_dir=tmp_path, start_date=first, end_date=second)
    pipeline.backfill(config)
    old_consumer = _database_snapshot(tmp_path / "tickerlake.duckdb")

    client.bars[first] = [_Bar(first, 11)]
    if invalid_result == "empty":
        # A newly expected session with no bars is a successful response, but
        # is not enough evidence to publish this consumer generation.
        third = datetime.date(2024, 1, 4)
        config.end_date = third
        client.bars[third] = []
        expected_status = "successful_empty"
    else:
        client.bars[second] = [object()]
        expected_status = "quarantined"

    with pytest.raises(pipeline.ExtractionIncompleteError, match=expected_status):
        pipeline.backfill(config)
    assert _rows(tmp_path / "raw.duckdb", "SELECT date, close FROM raw_daily_bars ORDER BY date")[:2] == [
        (first, 11.0),
        (second, 20.0),
    ]
    assert _database_snapshot(tmp_path / "tickerlake.duckdb") == old_consumer


@pytest.mark.parametrize("reference", ["splits", "tickers"])
def test_reference_failure_preserves_consumer(tmp_path: Path, monkeypatch, reference: str) -> None:
    """Preserve consumer state when either reference source fails."""
    date = datetime.date(2024, 1, 2)
    client = _Client()
    client.bars[date] = [_Bar(date, 10)]
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    config = Config(api_key="test", output_dir=tmp_path, start_date=date, end_date=date)
    pipeline.backfill(config)
    original = _rows(tmp_path / "tickerlake.duckdb", "SELECT date, ticker, close FROM daily_bars")

    setattr(client, f"fail_{reference}", True)
    expected = "[Ss]plit extraction" if reference == "splits" else "[Tt]icker extraction"
    with pytest.raises(pipeline.ExtractionIncompleteError, match=expected):
        pipeline.backfill(config)
    assert _rows(tmp_path / "tickerlake.duckdb", "SELECT date, ticker, close FROM daily_bars") == original


def test_ticker_metadata_shrink_preserves_all_consumer_tables_and_split_cache(tmp_path: Path, monkeypatch) -> None:
    """Treat a ticker-catalog omission as quarantine without replacing any published state."""
    first = datetime.date(2024, 1, 2)
    second = datetime.date(2024, 1, 3)
    client = _Client()
    client.bars = {
        first: [_Bar(first, 10), _Bar(first, 20)],
        second: [_Bar(second, 11), _Bar(second, 21)],
    }
    for date in (first, second):
        for bar, ticker in zip(client.bars[date], ("AAA", "BBB"), strict=True):
            bar.ticker = ticker
    ticker_a = _Ticker()
    ticker_a.ticker = "AAA"
    ticker_b = _Ticker()
    ticker_b.ticker = "BBB"
    client.tickers = [ticker_a, ticker_b]
    client.splits = [_Split("AAA", second)]
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    config = Config(api_key="test", output_dir=tmp_path, start_date=first, end_date=second)
    pipeline.backfill(config)
    previous_consumer = _database_snapshot(tmp_path / "tickerlake.duckdb")
    previous_splits = _rows(tmp_path / "raw.duckdb", "SELECT ticker, execution_date, adjustment_factor FROM splits")

    client.tickers = [ticker_a]
    # A valid split addition must still not be written if ticker validation fails.
    client.splits.append(_Split("BBB", second))
    with pytest.raises(pipeline.ExtractionIncompleteError, match="quarantined"):
        pipeline.backfill(config)

    assert _database_snapshot(tmp_path / "tickerlake.duckdb") == previous_consumer
    assert (
        _rows(tmp_path / "raw.duckdb", "SELECT ticker, execution_date, adjustment_factor FROM splits")
        == previous_splits
    )
    assert _rows(tmp_path / "raw.duckdb", "SELECT date, ticker FROM raw_daily_bars ORDER BY date, ticker") == [
        (first, "AAA"),
        (first, "BBB"),
        (second, "AAA"),
        (second, "BBB"),
    ]


def test_narrow_backfill_keeps_split_coverage_for_retained_history(tmp_path: Path, monkeypatch) -> None:
    """Retain older split events when rebuilding from a narrowed date range."""
    first = datetime.date(2024, 1, 2)
    split_date = datetime.date(2024, 1, 3)
    last = datetime.date(2024, 1, 4)
    client = _Client()
    client.bars = {
        first: [_Bar(first, 400)],
        split_date: [_Bar(split_date, 410)],
        last: [_Bar(last, 420)],
    }
    client.splits = [_Split("AAA", split_date)]
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    initial_config = Config(api_key="test", output_dir=tmp_path, start_date=first, end_date=last)
    pipeline.backfill(initial_config)

    narrow_config = Config(api_key="test", output_dir=tmp_path, start_date=last, end_date=last)
    pipeline.backfill(narrow_config)

    assert _rows(tmp_path / "raw.duckdb", "SELECT ticker, execution_date, adjustment_factor FROM splits") == [
        ("AAA", split_date, 0.25)
    ]
    assert _rows(
        tmp_path / "tickerlake.duckdb",
        "SELECT date, ticker, close FROM daily_bars WHERE date = DATE '2024-01-02'",
    ) == [(first, "AAA", 100.0)]
