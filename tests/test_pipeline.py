"""Tests for tickerlake.pipeline — backfill, update, and info orchestration."""

import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

from tickerlake import pipeline
from tickerlake.config import Config
from tickerlake.extract import DAILY_AGGS_SCHEMA
from tickerlake.load import write_raw_db
from tickerlake.pipeline import (
    _verify_split_adjustment,
    compact,
    info,
)
from tickerlake.transform import adjust_splits

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class _ApiBar:
    timestamp: int
    ticker: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    transactions: int


@dataclass
class _ApiTicker:
    ticker: str
    name: str
    type: str
    primary_exchange: str
    cik: str
    active: bool


@dataclass
class _ApiSplit:
    ticker: str
    execution_date: str
    split_from: float
    split_to: float
    historical_adjustment_factor: float
    adjustment_type: str


class _FakeMassiveClient:
    """API-boundary fake for a real backfill smoke scenario."""

    def __init__(self) -> None:
        self.bars_by_date: dict[datetime.date, list[_ApiBar]] = {}
        self.splits: list[_ApiSplit] = []
        self.tickers: list[_ApiTicker] = []
        self.calls: list[str] = []
        self.requested_dates: list[datetime.date] = []
        self.failed_dates: set[datetime.date] = set()

    def fetch_daily_aggs(self, date: datetime.date) -> list[_ApiBar]:
        self.calls.append("bars")
        self.requested_dates.append(date)
        if date in self.failed_dates:
            raise RuntimeError
        return self.bars_by_date.get(date, [])

    def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list[_ApiSplit]:
        self.calls.append("splits")
        return [
            split
            for split in self.splits
            if start_date <= datetime.date.fromisoformat(split.execution_date) <= end_date
        ]

    def fetch_tickers(self, types: list[str]) -> list[_ApiTicker]:
        self.calls.append("tickers")
        return [ticker for ticker in self.tickers if ticker.type in types]


def _make_config(tmp_path: Path):
    """Build a Config pointing at tmp_path with a fake API key."""
    return Config(
        api_key="test_key",
        output_dir=tmp_path,
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 1, 31),
    )


@pytest.fixture
def fake_massive_client(monkeypatch):
    """Replace the external Massive client with a configurable test fake."""
    client = _FakeMassiveClient()
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: client)
    return client


@pytest.fixture
def api_bar():
    """Build API bars from compact date/ticker/price inputs."""

    def build(date: datetime.date, ticker: str, close: float, volume: int) -> _ApiBar:
        timestamp = int(datetime.datetime.combine(date, datetime.time(), datetime.UTC).timestamp() * 1000)
        return _ApiBar(
            timestamp=timestamp,
            ticker=ticker,
            open=close,
            high=close + 2,
            low=close - 2,
            close=close,
            volume=volume,
            vwap=close,
            transactions=10,
        )

    return build


@pytest.fixture
def cached_backfill(tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar):
    """Run backfill against an initial cached date range."""
    dates = [
        datetime.date(2023, 12, 29),
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 9),
        datetime.date(2024, 1, 12),
    ]

    def set_bars(date: datetime.date, revision: int) -> None:
        fake_massive_client.bars_by_date[date] = [
            api_bar(date, "AAA", 100 + revision, 1000 + revision),
            api_bar(date, "BBB", 200 + revision, 2000 + revision),
        ]

    for date in dates:
        set_bars(date, 0)
    fake_massive_client.tickers = [
        _ApiTicker("AAA", "A", "CS", "XNAS", "0000000001", True),
        _ApiTicker("BBB", "B", "CS", "XNYS", "0000000002", True),
    ]

    def run(start_date: datetime.date, end_date: datetime.date) -> None:
        pipeline.backfill(Config(api_key="test_key", output_dir=tmp_path, start_date=start_date, end_date=end_date))

    def bars() -> list[tuple]:
        connection = duckdb.connect(str(tmp_path / "raw.duckdb"), read_only=True)
        try:
            return connection.execute(
                "SELECT date, ticker, close, volume FROM raw_daily_bars ORDER BY date, ticker"
            ).fetchall()
        finally:
            connection.close()

    return dates, set_bars, run, bars, fake_massive_client


@pytest.fixture
def update_history(tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar):
    """Provide persisted history helpers for update behavior tests."""
    dates = [
        datetime.date(2023, 12, day)
        for day in (1, 4, 5, 6, 7, 8, 11, 12, 13, 14, 15, 18, 19, 20, 21, 22, 26, 27, 28, 29)
    ] + [datetime.date(2024, 1, day) for day in (2, 3, 4, 5, 8, 9)]

    def set_bars(date: datetime.date, revision: int) -> None:
        index = dates.index(date) if date in dates else 26 + (date.day - 10)
        fake_massive_client.bars_by_date[date] = [
            api_bar(date, "AAA", 100 + index + revision, 1000 + revision),
            api_bar(date, "BBB", 200 + index + revision, 2000 + revision),
        ]

    for date in dates:
        set_bars(date, 0)
    fake_massive_client.tickers = [
        _ApiTicker("AAA", "A", "CS", "XNAS", "0000000001", True),
        _ApiTicker("BBB", "B", "CS", "XNYS", "0000000002", True),
    ]

    def run(end_date: datetime.date) -> None:
        pipeline.update(
            Config(
                api_key="test_key",
                output_dir=tmp_path,
                start_date=dates[0],
                end_date=end_date,
            )
        )

    def backfill(end_date: datetime.date) -> None:
        pipeline.backfill(
            Config(
                api_key="test_key",
                output_dir=tmp_path,
                start_date=dates[0],
                end_date=end_date,
            )
        )

    def rows(filename: str, query: str) -> list[tuple]:
        connection = duckdb.connect(str(tmp_path / filename), read_only=True)
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    return dates, set_bars, run, backfill, rows, fake_massive_client


def test_update_refreshes_recent_bars_and_rebuilds_split_adjusted_history(update_history):
    """Refresh recent data and rebuild persisted split-adjusted consumers."""
    dates, set_bars, run, _, rows, client = update_history
    client.bars_by_date[dates[1]] = []
    run(dates[-1])
    client.requested_dates.clear()

    for date in dates[:-5]:
        set_bars(date, 99)
    for date in dates[-5:]:
        set_bars(date, 10)
    new_dates = [datetime.date(2024, 1, 10), datetime.date(2024, 1, 11)]
    for date in new_dates:
        set_bars(date, 20)
    client.splits = [_ApiSplit("AAA", "2024-01-02", 1, 4, 0.25, "forward")]
    run(new_dates[-1])

    expected_raw = []
    for index, date in enumerate(dates):
        if date == dates[1]:
            continue
        revision = 10 if date in dates[-5:] else 0
        expected_raw.extend(
            [
                (date, "AAA", float(100 + index + revision), float(1000 + revision)),
                (date, "BBB", float(200 + index + revision), float(2000 + revision)),
            ]
        )
    for index, date in enumerate(new_dates, start=26):
        expected_raw.extend(
            [(date, "AAA", float(100 + index + 20), 1020.0), (date, "BBB", float(200 + index + 20), 2020.0)]
        )
    assert (
        rows("raw.duckdb", "SELECT date, ticker, close, volume FROM raw_daily_bars ORDER BY date, ticker")
        == expected_raw
    )
    assert rows("raw.duckdb", "SELECT ticker, execution_date, adjustment_factor FROM splits") == [
        ("AAA", datetime.date(2024, 1, 2), 0.25)
    ]

    daily = rows("tickerlake.duckdb", "SELECT date, ticker, close, volume FROM daily_bars ORDER BY date, ticker")
    expected_daily = []
    for date, ticker, raw_close, raw_volume in expected_raw:
        close = raw_close
        volume = raw_volume
        if ticker == "AAA" and date < datetime.date(2024, 1, 2):
            close /= 4
            volume *= 4
        expected_daily.append((date, ticker, close, volume))
    assert daily == expected_daily
    assert len({(date, ticker) for date, ticker, _, _ in daily}) == len(daily)
    assert set(client.requested_dates) == set(dates[-5:]) | set(new_dates)

    assert rows(
        "tickerlake.duckdb",
        "SELECT sma_20, atr_14, volume_sma_20 FROM daily_metrics WHERE ticker = 'AAA' ORDER BY date DESC LIMIT 1",
    ) == [(pytest.approx(70.925), pytest.approx(10.3035717), 2804.5)]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, ticker, open, high, low, close, volume FROM weekly_bars "
        "WHERE date = DATE '2024-01-08' ORDER BY ticker",
    ) == [
        (datetime.date(2024, 1, 8), "AAA", 134.0, 149.0, 132.0, 147.0, 4060.0),
        (datetime.date(2024, 1, 8), "BBB", 234.0, 249.0, 232.0, 247.0, 8060.0),
    ]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, ticker, close, volume FROM monthly_bars WHERE date = DATE '2024-01-11' ORDER BY ticker",
    ) == [
        (datetime.date(2024, 1, 11), "AAA", 147.0, 8090.0),
        (datetime.date(2024, 1, 11), "BBB", 247.0, 16090.0),
    ]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, ticker FROM weekly_metrics WHERE date = DATE '2024-01-08' ORDER BY ticker",
    ) == [(datetime.date(2024, 1, 8), "AAA"), (datetime.date(2024, 1, 8), "BBB")]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, ticker FROM monthly_metrics WHERE date = DATE '2024-01-11' ORDER BY ticker",
    ) == [(datetime.date(2024, 1, 11), "AAA"), (datetime.date(2024, 1, 11), "BBB")]


@pytest.mark.parametrize("empty_raw_table", [False, True])
def test_update_backfills_when_raw_database_is_missing_or_empty(
    tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar, empty_raw_table: bool
):
    """Update uses a full backfill for missing or empty raw databases."""
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    fake_massive_client.bars_by_date = {
        date: [api_bar(date, "AAA", 100 + index, 1000)] for index, date in enumerate(dates)
    }
    fake_massive_client.tickers = [_ApiTicker("AAA", "A", "CS", "XNAS", "0000000001", True)]
    raw_path = tmp_path / "raw.duckdb"
    if empty_raw_table:
        write_raw_db(pl.DataFrame(schema=DAILY_AGGS_SCHEMA), raw_path)

    pipeline.update(Config(api_key="test_key", output_dir=tmp_path, start_date=dates[0], end_date=dates[-1]))

    connection = duckdb.connect(str(raw_path), read_only=True)
    try:
        assert connection.execute("SELECT date, ticker, close FROM raw_daily_bars ORDER BY date").fetchall() == [
            (dates[0], "AAA", 100.0),
            (dates[1], "AAA", 101.0),
        ]
    finally:
        connection.close()
    assert set(fake_massive_client.requested_dates) == set(dates)

    fake_massive_client.requested_dates.clear()
    fake_massive_client.bars_by_date = {
        date: [api_bar(date, "AAA", 110 + index, 1010)] for index, date in enumerate(dates)
    }
    pipeline.update(Config(api_key="test_key", output_dir=tmp_path, start_date=dates[0], end_date=dates[-1]))

    connection = duckdb.connect(str(raw_path), read_only=True)
    try:
        assert connection.execute("SELECT date, ticker, close FROM raw_daily_bars ORDER BY date").fetchall() == [
            (dates[0], "AAA", 110.0),
            (dates[1], "AAA", 111.0),
        ]
    finally:
        connection.close()
    assert set(fake_massive_client.requested_dates) == set(dates)


@pytest.mark.parametrize("command", ["backfill", "update"])
def test_api_commands_require_key_before_files_or_api_calls(
    tmp_path: Path, fake_massive_client: _FakeMassiveClient, monkeypatch, command: str
):
    """Reject API commands without a key before creating files or calling API."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    config = Config(api_key="", output_dir=tmp_path)

    with pytest.raises(ValueError, match="MASSIVE_API_KEY"):
        getattr(pipeline, command)(config)

    assert list(tmp_path.iterdir()) == []
    assert fake_massive_client.calls == []


def test_update_does_not_fill_an_older_gap_until_backfill(update_history):
    """Characterize a current update limitation, not a desired guarantee."""
    dates, set_bars, run, run_backfill, rows, client = update_history
    gap = dates[5]
    client.bars_by_date[gap] = []
    run(dates[-1])

    assert rows("raw.duckdb", "SELECT COUNT(*) FROM raw_daily_bars WHERE date = DATE '2023-12-08'") == [(0,)]
    set_bars(gap, 0)
    client.requested_dates.clear()
    run(datetime.date(2024, 1, 10))
    assert rows("raw.duckdb", "SELECT COUNT(*) FROM raw_daily_bars WHERE date = DATE '2023-12-08'") == [(0,)]
    assert gap not in client.requested_dates

    run_backfill(datetime.date(2024, 1, 10))
    assert rows("raw.duckdb", "SELECT COUNT(*) FROM raw_daily_bars WHERE date = DATE '2023-12-08'") == [(2,)]


def test_backfill_refreshes_five_cached_sessions_and_preserves_other_dates(cached_backfill):
    """Refresh only the trailing five cached sessions and retain older rows."""
    dates, set_bars, run, bars, client = cached_backfill
    start = datetime.date(2024, 1, 2)
    end = datetime.date(2024, 1, 10)
    run(dates[0], dates[-1])
    client.requested_dates.clear()

    for date in dates[2:7]:
        set_bars(date, 10)
    new_date = datetime.date(2024, 1, 10)
    set_bars(new_date, 20)
    run(start, end)

    state = bars()
    expected = []
    for date in (dates[0], dates[1], *dates[2:7], new_date, dates[7]):
        revision = 0 if date in (dates[0], dates[1], dates[7]) else 10
        if date == new_date:
            revision = 20
        expected.extend(
            [(date, "AAA", 100.0 + revision, 1000.0 + revision), (date, "BBB", 200.0 + revision, 2000.0 + revision)]
        )
    assert state == expected
    assert set(client.requested_dates) == set(dates[2:7]) | {new_date}
    assert len({(date, ticker) for date, ticker, _, _ in state}) == len(state)


def test_backfill_keeps_failed_or_empty_refreshes_and_updates_successful_dates(cached_backfill):
    """Keep old rows for failed or empty refreshes and store successful revisions."""
    dates, set_bars, run, bars, client = cached_backfill
    run(dates[0], dates[-1])
    client.requested_dates.clear()

    for date in dates[2:7]:
        set_bars(date, 10)
    client.failed_dates.add(dates[2])
    client.bars_by_date[dates[3]] = []
    new_date = datetime.date(2024, 1, 10)
    set_bars(new_date, 20)
    run(datetime.date(2024, 1, 2), new_date)

    state = bars()
    by_date_ticker = {(date, ticker): (close, volume) for date, ticker, close, volume in state}
    assert by_date_ticker[(dates[2], "AAA")] == (100.0, 1000.0)
    assert by_date_ticker[(dates[3], "BBB")] == (200.0, 2000.0)
    for date in (dates[4], dates[5], dates[6]):
        assert by_date_ticker[(date, "AAA")] == (110.0, 1010.0)
        assert by_date_ticker[(date, "BBB")] == (210.0, 2010.0)
    assert by_date_ticker[(new_date, "AAA")] == (120.0, 1020.0)
    assert by_date_ticker[(new_date, "BBB")] == (220.0, 2020.0)
    assert len({(date, ticker) for date, ticker, _, _ in state}) == len(state)


def test_backfill_does_not_delete_cached_dates_when_all_refreshes_fail(cached_backfill):
    """Do not delete cached bars when every refresh fails."""
    dates, set_bars, run, bars, client = cached_backfill
    run(dates[0], dates[-1])
    original = bars()
    client.requested_dates.clear()

    cached_refresh_dates = dates[2:7]
    client.failed_dates.update(cached_refresh_dates)
    new_date = datetime.date(2024, 1, 10)
    set_bars(new_date, 20)
    run(datetime.date(2024, 1, 2), new_date)

    expected = original.copy()
    expected.extend([(new_date, "AAA", 120.0, 1020.0), (new_date, "BBB", 220.0, 2020.0)])
    expected.sort(key=lambda row: (row[0], row[1]))
    assert bars() == expected
    assert set(client.requested_dates) == set(cached_refresh_dates) | {new_date}


def test_backfill_refreshes_all_cached_dates_when_fewer_than_five_exist(cached_backfill):
    """Refresh all available cached sessions when fewer than five exist."""
    dates, set_bars, run, bars, client = cached_backfill
    three_dates = dates[1:4]
    run(three_dates[0], three_dates[-1])
    client.requested_dates.clear()
    for date in three_dates:
        set_bars(date, 10)

    run(three_dates[0], three_dates[-1])

    assert set(client.requested_dates) == set(three_dates)
    assert bars() == [
        (date, ticker, close, volume)
        for date in three_dates
        for ticker, close, volume in (("AAA", 110.0, 1010.0), ("BBB", 210.0, 2010.0))
    ]


def test_backfill_weekend_has_no_files_or_api_data_calls(
    tmp_path: Path, fake_massive_client: _FakeMassiveClient
) -> None:
    """A weekend-only range does not create databases or call the API."""
    config = Config(
        api_key="test_key",
        output_dir=tmp_path,
        start_date=datetime.date(2024, 1, 6),
        end_date=datetime.date(2024, 1, 7),
    )
    pipeline.backfill(config)

    assert fake_massive_client.calls == []
    assert list(tmp_path.iterdir()) == []


@pytest.fixture
def persisted_backfill(tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar):
    """Persist a month of split and filtered data for consumer assertions."""
    dates = [
        datetime.date(2024, 1, day)
        for day in (2, 3, 4, 5, 8, 9, 10, 11, 12, 16, 17, 18, 19, 22, 23, 24, 25, 26, 29, 30)
    ]
    for index, date in enumerate(dates):
        raw_close = 400 + index
        fake_massive_client.bars_by_date[date] = [
            api_bar(date, "SPLT", raw_close, 1000),
            api_bar(date, "HOLD", 50 + index, 2000),
            api_bar(date, "GONE", 25 + index, 3000),
        ]
    fake_massive_client.splits = [_ApiSplit("SPLT", "2024-01-16", 1, 4, 0.25, "forward")]
    fake_massive_client.tickers = [
        _ApiTicker("SPLT", "Split Co", "CS", "XNAS", "0000000001", True),
        _ApiTicker("HOLD", "Hold Co", "CS", "XNYS", "0000000002", True),
    ]
    pipeline.backfill(Config(api_key="test_key", output_dir=tmp_path, start_date=dates[0], end_date=dates[-1]))

    def rows(filename: str, query: str) -> list[tuple]:
        connection = duckdb.connect(str(tmp_path / filename), read_only=True)
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    return rows, dates


def test_backfill_persists_split_adjusted_and_filtered_state(persisted_backfill):
    """Verify split adjustment, ticker filtering, aggregates, and metrics in DuckDB."""
    rows, dates = persisted_backfill
    last = dates[-1]

    assert rows(
        "raw.duckdb", "SELECT ticker, close, volume FROM raw_daily_bars WHERE date = DATE '2024-01-02' ORDER BY ticker"
    ) == [("GONE", 25.0, 3000.0), ("HOLD", 50.0, 2000.0), ("SPLT", 400.0, 1000.0)]
    assert rows("raw.duckdb", "SELECT ticker, execution_date, adjustment_factor FROM splits") == [
        ("SPLT", datetime.date(2024, 1, 16), 0.25)
    ]
    assert rows(
        "tickerlake.duckdb",
        "SELECT ticker, close, volume FROM daily_bars WHERE date = DATE '2024-01-02' ORDER BY ticker",
    ) == [("HOLD", 50.0, 2000.0), ("SPLT", 100.0, 4000.0)]
    assert rows(
        "tickerlake.duckdb", "SELECT ticker, close FROM daily_bars WHERE date = DATE '2024-01-30' ORDER BY ticker"
    ) == [("HOLD", 69.0), ("SPLT", 419.0)]
    assert rows("tickerlake.duckdb", "SELECT ticker, name FROM tickers ORDER BY ticker") == [
        ("HOLD", "Hold Co"),
        ("SPLT", "Split Co"),
    ]

    weekly = rows(
        "tickerlake.duckdb",
        "SELECT date, open, high, low, close, volume FROM weekly_bars WHERE ticker = 'SPLT' ORDER BY date",
    )
    assert weekly == [
        (datetime.date(2024, 1, 1), 100.0, 101.25, 99.5, 100.75, 16000.0),
        (datetime.date(2024, 1, 8), 101.0, 102.5, 100.5, 102.0, 20000.0),
        (datetime.date(2024, 1, 15), 409.0, 414.0, 407.0, 412.0, 4000.0),
        (datetime.date(2024, 1, 22), 413.0, 419.0, 411.0, 417.0, 5000.0),
        (datetime.date(2024, 1, 29), 418.0, 421.0, 416.0, 419.0, 2000.0),
    ]
    assert rows("tickerlake.duckdb", "SELECT date, ticker FROM weekly_metrics ORDER BY ticker, date") == [
        (datetime.date(2024, 1, 1), "HOLD"),
        (datetime.date(2024, 1, 8), "HOLD"),
        (datetime.date(2024, 1, 15), "HOLD"),
        (datetime.date(2024, 1, 22), "HOLD"),
        (datetime.date(2024, 1, 29), "HOLD"),
        (datetime.date(2024, 1, 1), "SPLT"),
        (datetime.date(2024, 1, 8), "SPLT"),
        (datetime.date(2024, 1, 15), "SPLT"),
        (datetime.date(2024, 1, 22), "SPLT"),
        (datetime.date(2024, 1, 29), "SPLT"),
    ]
    assert rows(
        "tickerlake.duckdb",
        "SELECT COUNT(sma_20), COUNT(sma_50), COUNT(sma_200), COUNT(atr_14), "
        "COUNT(atr_pct), COUNT(adr_pct), COUNT(volume_sma_20) FROM weekly_metrics",
    ) == [(0, 0, 0, 0, 0, 0, 0)]
    assert rows(
        "tickerlake.duckdb", "SELECT date, open, high, low, close, volume FROM monthly_bars WHERE ticker = 'SPLT'"
    ) == [(datetime.date(2024, 1, 30), 100.0, 421.0, 99.5, 419.0, 47000.0)]
    assert rows("tickerlake.duckdb", "SELECT date, ticker FROM monthly_metrics ORDER BY ticker, date") == [
        (datetime.date(2024, 1, 30), "HOLD"),
        (datetime.date(2024, 1, 30), "SPLT"),
    ]
    assert rows(
        "tickerlake.duckdb",
        "SELECT COUNT(sma_20), COUNT(sma_50), COUNT(sma_200), COUNT(atr_14), "
        "COUNT(atr_pct), COUNT(adr_pct), COUNT(volume_sma_20) FROM monthly_metrics",
    ) == [(0, 0, 0, 0, 0, 0, 0)]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, sma_20, atr_14, adr_pct, volume_sma_20 FROM daily_metrics "
        "WHERE ticker = 'HOLD' ORDER BY date DESC LIMIT 1",
    ) == [(last, 59.5, 4.0, pytest.approx(0.0678691410), 2000.0)]
    assert rows(
        "tickerlake.duckdb",
        "SELECT date, sma_20, atr_14, adr_pct, volume_sma_20 FROM daily_metrics "
        "WHERE ticker = 'SPLT' ORDER BY date DESC LIMIT 1",
    ) == [(last, pytest.approx(273.15), pytest.approx(25.142857), pytest.approx(0.0097699473), 2350.0)]


def test_backfill_persists_api_data_through_real_pipeline(
    tmp_path: Path, fake_massive_client: _FakeMassiveClient
) -> None:
    """Persist API-fake responses through extraction, transforms, and DuckDB."""
    date = datetime.date(2024, 1, 2)
    fake_massive_client.bars_by_date[date] = [
        _ApiBar(
            timestamp=int(datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC).timestamp() * 1000),
            ticker="AAPL",
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1000.0,
            vwap=100.5,
            transactions=25,
        )
    ]
    fake_massive_client.tickers = [_ApiTicker("AAPL", "Apple Inc.", "CS", "XNAS", "0000320193", True)]
    pipeline.backfill(Config(api_key="test_key", output_dir=tmp_path, start_date=date, end_date=date))

    for filename, table in (
        ("raw.duckdb", "raw_daily_bars"),
        ("tickerlake.duckdb", "daily_bars"),
    ):
        connection = duckdb.connect(str(tmp_path / filename), read_only=True)
        try:
            query = f"SELECT date, ticker, close FROM {table}"  # noqa: S608
            assert connection.execute(query).fetchall() == [(date, "AAPL", 101.0)]
        finally:
            connection.close()

    connection = duckdb.connect(str(tmp_path / "tickerlake.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT ticker, name FROM tickers").fetchall() == [("AAPL", "Apple Inc.")]
    finally:
        connection.close()


# ═══════════════════════════════════════════════════════════════════════════════
# Backfill
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# Update
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# Info
# ═══════════════════════════════════════════════════════════════════════════════


def test_info_reports_missing_database_files(tmp_path: Path, caplog) -> None:
    """Info logs a not-found message for both missing database files."""
    with caplog.at_level(logging.INFO):
        info(_make_config(tmp_path))

    assert "raw.duckdb" in caplog.text
    assert "tickerlake.duckdb" in caplog.text
    assert "not found" in caplog.text


# ═══════════════════════════════════════════════════════════════════════════════
# Split adjustment spot check
# ═══════════════════════════════════════════════════════════════════════════════


def test_verify_split_adjustment_passes():
    """Spot check passes when adjusted/raw ratio matches split factor."""
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
    """Spot check skips splits with factor < 0.02 (OTC noise, same-day offsetting splits)."""
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
    """A bad sixth candidate is outside the five-ticker spot-check sample."""
    # Create 6 tickers with bars and splits (more than _SPOT_CHECK_SAMPLE_SIZE=5)
    tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META"]
    raw_rows = [{"date": datetime.date(2024, 1, 10), "ticker": ticker, "close": 400.0} for ticker in tickers]
    raw = pl.DataFrame(raw_rows).cast({"date": pl.Date, "close": pl.Float32})

    # Adjusted with 0.25 factor
    adjusted = raw.with_columns(
        pl.when(pl.col("ticker") == "META").then(pl.col("close")).otherwise(pl.col("close") * 0.25).alias("close")
    )

    # Create splits for all 6 tickers
    split_rows = [
        {
            "ticker": ticker,
            "execution_date": datetime.date(2024, 1, 15),
            "adjustment_factor": 0.25,
        }
        for ticker in tickers
    ]
    splits = pl.DataFrame(split_rows).cast({"execution_date": pl.Date, "adjustment_factor": pl.Float64})

    # Should verify exactly _SPOT_CHECK_SAMPLE_SIZE tickers and exit early
    _verify_split_adjustment(raw, adjusted, splits)


def test_info_reports_persisted_database_contents(
    tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar, caplog
) -> None:
    """Info reports counts and date ranges from real persisted databases."""
    first = datetime.date(2024, 1, 2)
    middle = datetime.date(2024, 1, 3)
    last = datetime.date(2024, 1, 4)
    fake_massive_client.bars_by_date = {
        first: [api_bar(first, "AAA", 10.0, 100), api_bar(first, "UNKNOWN", 20.0, 200)],
        middle: [],
        last: [api_bar(last, "AAA", 11.0, 100), api_bar(last, "BBB", 30.0, 300)],
    }
    fake_massive_client.tickers = [
        _ApiTicker("AAA", "Example Co", "CS", "XNAS", "0000000001", True),
        _ApiTicker("BBB", "Second Co", "CS", "XNYS", "0000000002", True),
    ]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=first, end_date=last)
    pipeline.backfill(config)

    caplog.clear()
    with caplog.at_level(logging.INFO):
        pipeline.info(config)

    messages = [record.getMessage() for record in caplog.records]
    raw_section = messages.index(f"raw.duckdb: {tmp_path / 'raw.duckdb'}")
    consumer_section = messages.index(f"tickerlake.duckdb: {tmp_path / 'tickerlake.duckdb'}")
    assert raw_section < consumer_section
    raw_messages = [message.strip() for message in messages[raw_section + 1 : consumer_section]]
    raw_bars_index = raw_messages.index("raw_daily_bars: 4 rows")
    assert raw_messages[raw_bars_index + 1] == f"dates: {first} to {last}"
    consumer_messages = [message.strip() for message in messages[consumer_section + 1 :]]
    daily_bars_index = consumer_messages.index("daily_bars: 3 rows")
    assert consumer_messages[daily_bars_index + 1] == f"dates: {first} to {last}"


def test_compact_preserves_raw_bars(tmp_path: Path, fake_massive_client: _FakeMassiveClient, api_bar) -> None:
    """Compact preserves logical raw bar contents."""
    dates = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]
    fake_massive_client.bars_by_date = {
        date: [api_bar(date, "AAA", 10 + index, 100)] for index, date in enumerate(dates)
    }
    fake_massive_client.tickers = [_ApiTicker("AAA", "Example Co", "CS", "XNAS", "0000000001", True)]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=dates[0], end_date=dates[-1])
    pipeline.backfill(config)
    path = tmp_path / "raw.duckdb"
    connection = duckdb.connect(str(path), read_only=True)
    try:
        before = connection.execute("SELECT * FROM raw_daily_bars ORDER BY ticker, date").fetchall()
    finally:
        connection.close()

    pipeline.compact(config)

    connection = duckdb.connect(str(path), read_only=True)
    try:
        after = connection.execute("SELECT * FROM raw_daily_bars ORDER BY ticker, date").fetchall()
    finally:
        connection.close()
    assert after == before


def test_compact_missing_raw_db(tmp_path: Path, caplog) -> None:
    """Compact logs a warning when raw.duckdb does not exist."""
    with caplog.at_level(logging.WARNING):
        compact(_make_config(tmp_path))

    assert "No raw.duckdb found" in caplog.text


def test_backfill_split_guard_failure_preserves_existing_consumer(
    tmp_path: Path,
    fake_massive_client: _FakeMassiveClient,
    api_bar,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed rebuild leaves consumer data intact after raw refresh, then retries cleanly."""
    date = datetime.date(2024, 1, 2)
    fake_massive_client.bars_by_date[date] = [api_bar(date, "AAA", 400.0, 1000)]
    fake_massive_client.splits = [_ApiSplit("AAA", "2024-01-03", 1, 4, 0.25, "forward")]
    fake_massive_client.tickers = [_ApiTicker("AAA", "Example Co", "CS", "XNAS", "0000000001", True)]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=date, end_date=datetime.date(2024, 1, 3))
    pipeline.backfill(config)

    consumer_path = tmp_path / "tickerlake.duckdb"
    connection = duckdb.connect(str(consumer_path), read_only=True)
    try:
        before = connection.execute("SELECT date, ticker, close FROM daily_bars").fetchall()
    finally:
        connection.close()

    fake_massive_client.bars_by_date[date] = [api_bar(date, "AAA", 410.0, 1100)]
    fake_massive_client.splits = [_ApiSplit("AAA", "2024-01-03", 1, 4, 0.25, "forward")]
    monkeypatch.setattr(pipeline, "adjust_splits", lambda bars, splits: bars)
    with pytest.raises(ValueError, match="spot check failed"):
        pipeline.backfill(config)

    raw_path = tmp_path / "raw.duckdb"
    connection = duckdb.connect(str(raw_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT date, ticker, close, volume FROM raw_daily_bars ORDER BY date, ticker"
        ).fetchall() == [(date, "AAA", 410.0, 1100.0)]
        assert connection.execute("SELECT ticker, execution_date, adjustment_factor FROM splits").fetchall() == [
            ("AAA", datetime.date(2024, 1, 3), 0.25)
        ]
    finally:
        connection.close()

    connection = duckdb.connect(str(consumer_path), read_only=True)
    try:
        assert connection.execute("SELECT date, ticker, close FROM daily_bars").fetchall() == before
    finally:
        connection.close()

    monkeypatch.setattr(pipeline, "adjust_splits", adjust_splits)
    pipeline.backfill(config)

    connection = duckdb.connect(str(raw_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT date, ticker, close, volume FROM raw_daily_bars ORDER BY date, ticker"
        ).fetchall() == [(date, "AAA", 410.0, 1100.0)]
        assert connection.execute("SELECT ticker, execution_date, adjustment_factor FROM splits").fetchall() == [
            ("AAA", datetime.date(2024, 1, 3), 0.25)
        ]
    finally:
        connection.close()

    connection = duckdb.connect(str(consumer_path), read_only=True)
    try:
        assert connection.execute("SELECT date, ticker, close, volume FROM daily_bars").fetchall() == [
            (date, "AAA", 102.5, 4400.0)
        ]
    finally:
        connection.close()


def test_backfill_rejects_split_ratio_error_measured_relative_to_expected(
    tmp_path: Path,
    fake_massive_client: _FakeMassiveClient,
    api_bar,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject a ratio 0.8% from expected and preserve existing consumer data."""
    date = datetime.date(2024, 1, 2)
    raw_close = 400.0
    adjusted_close = 100.8
    fake_massive_client.bars_by_date[date] = [api_bar(date, "AAA", raw_close, 1000)]
    fake_massive_client.splits = [_ApiSplit("AAA", "2024-01-03", 1, 4, 0.25, "forward")]
    fake_massive_client.tickers = [_ApiTicker("AAA", "Example Co", "CS", "XNAS", "0000000001", True)]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=date, end_date=datetime.date(2024, 1, 3))
    pipeline.backfill(config)

    consumer_path = tmp_path / "tickerlake.duckdb"
    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        before = connection.execute("SELECT date, ticker, close FROM daily_bars").fetchall()

    original_adjust = adjust_splits

    def slightly_wrong(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
        return original_adjust(bars, splits).with_columns(
            pl.when(pl.col("date") < datetime.date(2024, 1, 3))
            .then(pl.lit(adjusted_close))
            .otherwise(pl.col("close"))
            .alias("close")
        )

    monkeypatch.setattr(pipeline, "adjust_splits", slightly_wrong)
    with pytest.raises(ValueError, match="spot check failed"):
        pipeline.backfill(config)

    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        assert connection.execute("SELECT date, ticker, close FROM daily_bars").fetchall() == before


@pytest.mark.parametrize("skip_first", ["duplicate", "no_pre_split_bars", "missing_adjusted_row"])
def test_backfill_split_verifier_checks_later_bad_ticker(
    tmp_path: Path,
    fake_massive_client: _FakeMassiveClient,
    api_bar,
    monkeypatch: pytest.MonkeyPatch,
    skip_first: str,
) -> None:
    """Continue verification after an unusable candidate and reject a later bad ratio."""
    date = datetime.date(2024, 1, 2)
    execution_date = datetime.date(2024, 1, 3)
    fake_massive_client.bars_by_date[date] = [api_bar(date, ticker, 400.0, 1000) for ticker in ("AAA", "BBB")]
    fake_massive_client.tickers = [
        _ApiTicker(ticker, ticker, "CS", "XNAS", f"000000000{i}", True)
        for i, ticker in enumerate(("AAA", "BBB"), start=1)
    ]
    first_split = _ApiSplit("AAA", "2024-01-03", 1, 4, 0.25, "forward")
    end_date = execution_date
    if skip_first == "duplicate":
        fake_massive_client.splits = [
            _ApiSplit("AAA", "2024-01-03", 1, 4, 0.0625, "forward"),
            _ApiSplit("AAA", "2024-01-04", 1, 4, 0.25, "forward"),
            _ApiSplit("BBB", "2024-01-03", 1, 4, 0.25, "forward"),
        ]
        end_date = datetime.date(2024, 1, 4)
    elif skip_first == "no_pre_split_bars":
        fake_massive_client.splits = [
            _ApiSplit("AAA", "2024-01-02", 1, 4, 0.25, "forward"),
            _ApiSplit("BBB", "2024-01-03", 1, 4, 0.25, "forward"),
        ]
    else:
        fake_massive_client.splits = [
            first_split,
            _ApiSplit("BBB", "2024-01-03", 1, 4, 0.25, "forward"),
        ]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=date, end_date=end_date)
    pipeline.backfill(config)
    consumer_path = tmp_path / "tickerlake.duckdb"
    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        before = connection.execute("SELECT date, ticker, close FROM daily_bars ORDER BY ticker").fetchall()

    original_adjust = adjust_splits

    def corrupt_adjustment(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
        # Fault-inject a bad eligible ticker; the missing-row case also removes
        # the first candidate to exercise continuation after that skip.
        adjusted = original_adjust(bars, splits)
        if skip_first == "missing_adjusted_row":
            adjusted = adjusted.filter(pl.col("ticker") != "AAA")
        return adjusted.with_columns(
            pl.when(pl.col("ticker") == "BBB").then(pl.col("close") * 4).otherwise(pl.col("close")).alias("close")
        )

    monkeypatch.setattr(pipeline, "adjust_splits", corrupt_adjustment)
    with pytest.raises(ValueError, match="spot check failed: BBB"):
        pipeline.backfill(config)

    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        assert connection.execute("SELECT date, ticker, close FROM daily_bars ORDER BY ticker").fetchall() == before


def test_backfill_split_verifier_checks_fifth_eligible_ticker(
    tmp_path: Path,
    fake_massive_client: _FakeMassiveClient,
    api_bar,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject an incorrect adjustment on the fifth eligible ticker."""
    date = datetime.date(2024, 1, 2)
    execution_date = datetime.date(2024, 1, 3)
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
    fake_massive_client.bars_by_date[date] = [api_bar(date, ticker, 400.0, 1000) for ticker in tickers]
    fake_massive_client.tickers = [
        _ApiTicker(ticker, ticker, "CS", "XNAS", f"000000000{i}", True) for i, ticker in enumerate(tickers, start=1)
    ]
    adjustment_factors = [0.1, 0.125, 0.2, 0.25, 1 / 3]
    fake_massive_client.splits = [
        _ApiSplit(ticker, execution_date.isoformat(), 1, round(1 / factor), factor, "forward")
        for ticker, factor in zip(tickers, adjustment_factors, strict=True)
    ]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=date, end_date=execution_date)
    pipeline.backfill(config)
    consumer_path = tmp_path / "tickerlake.duckdb"
    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        before = connection.execute("SELECT date, ticker, close FROM daily_bars ORDER BY ticker").fetchall()

    original_adjust = adjust_splits

    def corrupt_fifth(bars: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
        return original_adjust(bars, splits).with_columns(
            pl.when(pl.col("ticker") == "EEE").then(pl.col("close") * 2).otherwise(pl.col("close")).alias("close")
        )

    monkeypatch.setattr(pipeline, "adjust_splits", corrupt_fifth)
    with pytest.raises(ValueError, match="spot check failed: EEE"):
        pipeline.backfill(config)

    with duckdb.connect(str(consumer_path), read_only=True) as connection:
        assert connection.execute("SELECT date, ticker, close FROM daily_bars ORDER BY ticker").fetchall() == before


# ═══════════════════════════════════════════════════════════════════════════════
