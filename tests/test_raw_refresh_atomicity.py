"""Atomicity guarantees for cached raw-bar refreshes."""

import datetime
from typing import TYPE_CHECKING, Any

import duckdb
import pytest

from tests.test_pipeline import _ApiBar, _ApiTicker, _FakeMassiveClient
from tickerlake import load, pipeline
from tickerlake.config import Config

if TYPE_CHECKING:
    from pathlib import Path
else:
    Path = Any


def _rows(path: Path, query: str) -> list[tuple]:
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute(query).fetchall()


def test_failed_raw_refresh_preserves_raw_and_consumer_then_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed refresh insertion cannot persist deletion of the cached date."""
    first = datetime.date(2024, 1, 2)
    cached = datetime.date(2024, 1, 3)
    last = datetime.date(2024, 1, 4)
    dates = (first, cached, last)
    fake_massive_client = _FakeMassiveClient()
    monkeypatch.setattr(pipeline, "MassiveClient", lambda config: fake_massive_client)

    def api_bar(date: datetime.date, close: float, volume: int) -> _ApiBar:
        timestamp = int(datetime.datetime.combine(date, datetime.time(), datetime.UTC).timestamp() * 1000)
        return _ApiBar(timestamp, "AAA", close, close + 2, close - 2, close, volume, close, 10)

    fake_massive_client.bars_by_date = {
        date: [api_bar(date, 100 + index, 1000 + index)] for index, date in enumerate(dates)
    }
    fake_massive_client.tickers = [_ApiTicker("AAA", "Example", "CS", "XNAS", "0000000001", True)]
    config = Config(api_key="test_key", output_dir=tmp_path, start_date=first, end_date=last)
    pipeline.backfill(config)

    raw_path = tmp_path / "raw.duckdb"
    consumer_path = tmp_path / "tickerlake.duckdb"
    raw_query = "SELECT date, ticker, close, volume FROM raw_daily_bars ORDER BY date, ticker"
    consumer_query = "SELECT date, ticker, close, volume FROM daily_bars ORDER BY date, ticker"
    raw_before = _rows(raw_path, raw_query)
    consumer_before = _rows(consumer_path, consumer_query)

    fake_massive_client.bars_by_date[cached] = [api_bar(cached, 999, 9999)]
    # Fault-inject the raw INSERT SQL builder because the insertion itself fails
    # after the cached-date deletion has been attempted.
    original_read_parquet_sql = load._read_parquet_sql  # noqa: SLF001

    def fail_raw_insert(order_by: str = "") -> str:
        sql = original_read_parquet_sql(order_by)
        if order_by == "ticker, date":
            return "INVALID RAW INSERT SQL"
        return sql

    monkeypatch.setattr(load, "_read_parquet_sql", fail_raw_insert)
    with pytest.raises(duckdb.ParserException):
        pipeline.backfill(config)

    assert _rows(raw_path, raw_query) == raw_before
    assert _rows(consumer_path, consumer_query) == consumer_before

    monkeypatch.setattr(load, "_read_parquet_sql", original_read_parquet_sql)
    pipeline.backfill(config)

    assert _rows(raw_path, raw_query) == [
        (first, "AAA", 100.0, 1000.0),
        (cached, "AAA", 999.0, 9999.0),
        (last, "AAA", 102.0, 1002.0),
    ]
    assert _rows(consumer_path, consumer_query) == [
        (first, "AAA", 100.0, 1000.0),
        (cached, "AAA", 999.0, 9999.0),
        (last, "AAA", 102.0, 1002.0),
    ]
