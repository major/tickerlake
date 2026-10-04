"""PostgreSQL behavioral coverage for cached-window update runs."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import psycopg
import pytest

from tickerlake.calendar import get_closed_sessions
from tickerlake.config import Config
from tickerlake.postgres import backfill as backfill_module
from tickerlake.postgres.connection import writer_connection
from tickerlake.postgres.models import RunSpec
from tickerlake.postgres.rebuild import rebuild_cache
from tickerlake.postgres.state import read_cache_state, start_run

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path


NOW = datetime.datetime(2026, 1, 2, 22, 0, tzinfo=datetime.UTC)
SYMBOLS = ("ACTIVE", "INACTIVE")
SMA_PERIOD = 200


def _row(day: date, symbol: str, close: float = 11.0) -> dict[str, object]:
    timestamp = datetime.datetime(day.year, day.month, day.day, tzinfo=datetime.UTC).timestamp() * 1000
    return {
        "timestamp": float(int(timestamp)),
        "ticker": symbol,
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "volume": 100.0,
        "vwap": close,
        "transactions": 5,
    }


def _rows(day: date, close: float = 11.0) -> list[dict[str, object]]:
    return [_row(day, symbol, close) for symbol in SYMBOLS]


def _config(database_url: str, output_dir: Path, start: date, end: date) -> Config:
    return Config(
        api_key="test-key",
        database_url=database_url,
        output_dir=output_dir,
        start_date=start,
        end_date=end,
        ticker_types=["CS"],
    )


def _request(target: date):
    return backfill_module.BackfillRequest(
        code_version="update-test", schema_version="1", transform_version="1", target=target
    )


def _update_request(target: date | None = None):
    return backfill_module.UpdateRequest(
        code_version="update-test", schema_version="1", transform_version="1", target=target
    )


def _query(database_url: str, query: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    with psycopg.connect(database_url, autocommit=True) as connection:
        return connection.execute(query, params).fetchall()


def _public_snapshot(database_url: str) -> dict[str, list[tuple[object, ...]]]:
    queries = {
        "daily": "SELECT * FROM market.adjusted_daily ORDER BY ticker_id,date",
        "weekly": "SELECT * FROM market.adjusted_weekly ORDER BY ticker_id,date",
        "monthly": "SELECT * FROM market.adjusted_monthly ORDER BY ticker_id,date",
        "latest": "SELECT * FROM market.latest_daily ORDER BY ticker_id",
        "ticker": "SELECT * FROM market.ticker ORDER BY ticker_id",
        "publication": "SELECT * FROM market.publication_state",
    }
    with psycopg.connect(database_url, autocommit=True) as connection:
        return {name: connection.execute(query).fetchall() for name, query in queries.items()}


@dataclass
class FakeMassiveClient:
    """Provider fake for raw daily bars and the reference data used by ingestion."""

    daily: dict[date, list[dict[str, object]]] = field(default_factory=dict)
    failed_dates: set[date] = field(default_factory=set)
    daily_calls: list[date] = field(default_factory=list)

    def fetch_daily_aggs(self, day: date) -> list[dict[str, object]]:
        """Return the configured day's raw bars or raise a transport failure."""
        self.daily_calls.append(day)
        if day in self.failed_dates:
            raise RuntimeError
        return self.daily.get(day, [])

    def fetch_splits(self, start_date: date, end_date: date) -> list[dict[str, object]]:
        """Return no split events for this test provider."""
        return []

    def fetch_tickers(self, types: list[str]) -> list[dict[str, object]]:
        """Return the two test securities for the requested type."""
        return [
            {
                "ticker": symbol,
                "name": symbol.title(),
                "type": types[0],
                "primary_exchange": "X",
                "cik": None,
                "active": symbol == "ACTIVE",
            }
            for symbol in SYMBOLS
        ]


@pytest.fixture
def massive(monkeypatch: pytest.MonkeyPatch) -> FakeMassiveClient:
    """Install a recording provider fake at the external Massive boundary."""
    provider = FakeMassiveClient()
    monkeypatch.setattr(backfill_module, "MassiveClient", lambda _config: provider)
    return provider


def _seed_date(
    database_url: str,
    output_dir: Path,
    massive: FakeMassiveClient,
    day: date,
    close: float = 11.0,
) -> None:
    massive.daily = {day: _rows(day, close)}
    backfill_module.backfill(_config(database_url, output_dir, day, day), _request(day), now=NOW)


def test_update_refreshes_five_cached_sessions_preserves_gaps_and_retained_bounds(
    pg_migrated_database, tmp_path, massive
) -> None:
    """Refresh a five-cached-date window, including gaps and newly closed sessions."""
    dsn = pg_migrated_database.owner_dsn
    sessions = tuple(get_closed_sessions(datetime.date(2024, 1, 2), datetime.date(2024, 1, 19), now=NOW))
    omitted = {sessions[-8], sessions[-3]}
    cached = tuple(day for day in sessions if day not in omitted)
    for day in cached:
        _seed_date(dsn, tmp_path, massive, day)
    retained = _query(dsn, "SELECT retained_start,retained_end FROM ingest.cache_state WHERE singleton=true")[0]

    target = sessions[-1]
    config = _config(dsn, tmp_path, sessions[-10], target)
    # Provider has revised values for cached dates and supplies the uncached
    # dates inside the trailing calendar interval.
    massive.daily = {day: _rows(day, 12.0) for day in sessions}
    massive.daily_calls.clear()
    backfill_module.update(config, _update_request(target), now=NOW)

    expected_window = tuple(day for day in cached if day >= sessions[-6])
    expected_fetch = tuple(day for day in sessions if sessions[-6] <= day <= target)
    expected_raw_dates = set(cached) | set(expected_fetch)
    assert massive.daily_calls == list(expected_fetch)
    assert _query(dsn, "SELECT DISTINCT date FROM ingest.raw_daily ORDER BY date") == [
        (day,) for day in sorted(expected_raw_dates)
    ]
    assert sessions[-8] not in massive.daily_calls
    assert sessions[-3] in massive.daily_calls
    assert all(day in massive.daily_calls for day in expected_window)
    assert _query(dsn, "SELECT retained_start,retained_end FROM ingest.cache_state WHERE singleton=true")[0] == retained
    assert _query(
        dsn,
        "SELECT requested_date,status FROM ingest.fetch_manifest WHERE source='daily' "
        "AND run_id=(SELECT run_id FROM ingest.run ORDER BY started_at DESC LIMIT 1) "
        "AND requested_date = ANY(%s) ORDER BY requested_date",
        (list(expected_fetch),),
    ) == [(day, "populated") for day in expected_fetch]


def test_rejected_refresh_keeps_publication_but_persists_accepted_neighbor_revisions(
    pg_migrated_database, tmp_path, massive
) -> None:
    """A failed trailing refresh does not publish but accepted daily revisions persist."""
    dsn = pg_migrated_database.owner_dsn
    sessions = tuple(get_closed_sessions(datetime.date(2024, 1, 2), datetime.date(2024, 1, 17), now=NOW))
    massive.daily = {day: _rows(day) for day in sessions}
    backfill_module.backfill(_config(dsn, tmp_path, sessions[0], sessions[-1]), _request(sessions[-1]), now=NOW)
    snapshot_before = _public_snapshot(dsn)
    revision_before = _query(dsn, "SELECT input_revision FROM ingest.cache_state WHERE singleton=true")[0][0]
    failed_day = sessions[-3]
    accepted_days = (sessions[-5], sessions[-4], sessions[-2], sessions[-1])
    raw_failed_before = _query(dsn, "SELECT * FROM ingest.raw_daily WHERE date=%s ORDER BY ticker_id", (failed_day,))
    massive.daily = {day: _rows(day, 13.0) for day in sessions[-5:]}
    massive.failed_dates = {failed_day}

    with pytest.raises(backfill_module.BackfillIncompleteError):
        backfill_module.update(_config(dsn, tmp_path, sessions[0], sessions[-1]), _update_request(), now=NOW)

    failed_run = _query(
        dsn, "SELECT run_id,failure_code FROM ingest.run WHERE state='failed' ORDER BY started_at DESC LIMIT 1"
    )[0]
    assert failed_run[1] == "incomplete_fetch"
    assert _query(
        dsn,
        "SELECT status,diagnostic_code FROM ingest.fetch_manifest WHERE run_id=%s AND source='daily' "
        "AND requested_date=%s",
        (failed_run[0], failed_day),
    ) == [("failed", "transport_error")]
    assert _query(
        dsn,
        "SELECT requested_date,status FROM ingest.fetch_manifest WHERE run_id=%s AND source='daily' "
        "ORDER BY requested_date",
        (failed_run[0],),
    ) == [(day, "failed" if day == failed_day else "populated") for day in sessions[-5:]]
    assert _public_snapshot(dsn) == snapshot_before
    assert (
        _query(dsn, "SELECT * FROM ingest.raw_daily WHERE date=%s ORDER BY ticker_id", (failed_day,))
        == raw_failed_before
    )
    assert _query(
        dsn,
        "SELECT DISTINCT close FROM ingest.raw_daily WHERE date = ANY(%s) ORDER BY close",
        (list(accepted_days),),
    ) == [(13.0,)]
    assert _query(dsn, "SELECT input_revision FROM ingest.cache_state WHERE singleton=true")[0][0] == (
        revision_before + len(accepted_days)
    )


def test_update_correction_matches_full_rebuild_with_independent_sma(pg_migrated_database, tmp_path, massive) -> None:
    """Refresh a trailing correction, then prove the published products match a fresh rebuild."""
    dsn = pg_migrated_database.owner_dsn
    sessions = tuple(get_closed_sessions(datetime.date(2023, 1, 3), datetime.date(2023, 10, 31), now=NOW))
    assert len(sessions) > SMA_PERIOD
    close_by_day = {day: 20.0 + index / 20 for index, day in enumerate(sessions)}
    massive.daily = {day: _rows(day, close) for day, close in close_by_day.items()}
    backfill_module.backfill(
        _config(dsn, tmp_path, sessions[0], sessions[-1]), _request(sessions[-1]), now=NOW, batch_size=37
    )

    corrected = sessions[-3]
    newly_cached = sessions[-1] + datetime.timedelta(days=1)
    # Pick the next actual session, not necessarily the next calendar date.
    newly_cached = next(
        day for day in get_closed_sessions(newly_cached, newly_cached + datetime.timedelta(days=10), now=NOW)
    )
    close_by_day[corrected] = 42.0
    close_by_day[newly_cached] = 31.0
    massive.daily = {day: _rows(day, close) for day, close in close_by_day.items()}
    config = _config(dsn, tmp_path, sessions[0], newly_cached)
    backfill_module.update(config, _update_request(newly_cached), now=NOW)

    active_id = _query(dsn, "SELECT ticker_id FROM market.ticker WHERE symbol='ACTIVE'")[0][0]
    assert _query(
        dsn,
        "SELECT close FROM ingest.raw_daily WHERE ticker_id=%s AND date=%s",
        (active_id, corrected),
    ) == [(42.0,)]
    assert _query(
        dsn,
        "SELECT close FROM ingest.raw_daily WHERE ticker_id=%s AND date=%s",
        (active_id, newly_cached),
    ) == [(31.0,)]
    rows = _query(
        dsn,
        "SELECT date,sma_200 FROM market.adjusted_daily WHERE ticker_id=%s ORDER BY date",
        (active_id,),
    )
    expected_closes = [close_by_day[day] for day in (*sessions, newly_cached)]
    expected_sma = [None] * (SMA_PERIOD - 1) + [
        sum(expected_closes[index - SMA_PERIOD + 1 : index + 1]) / SMA_PERIOD
        for index in range(SMA_PERIOD - 1, len(expected_closes))
    ]
    assert [row[0] for row in rows] == [*sessions, newly_cached]
    for (_, actual), expected in zip(rows, expected_sma, strict=True):
        assert actual == pytest.approx(expected) if expected is not None else actual is None

    product_queries = {
        "daily": "SELECT * FROM market.adjusted_daily ORDER BY ticker_id,date",
        "weekly": "SELECT * FROM market.adjusted_weekly ORDER BY ticker_id,date",
        "monthly": "SELECT * FROM market.adjusted_monthly ORDER BY ticker_id,date",
        "latest": "SELECT * FROM market.latest_daily ORDER BY ticker_id",
        "ticker": "SELECT * FROM market.ticker ORDER BY ticker_id",
        "publication": "SELECT published_session FROM market.publication_state",
    }
    published = {name: _query(dsn, query) for name, query in product_queries.items()}
    state_before = _query(
        dsn, "SELECT input_revision,retained_start,retained_end FROM ingest.cache_state WHERE singleton=true"
    )[0]
    published_session = _query(dsn, "SELECT published_session FROM market.publication_state")[0][0]
    with writer_connection(dsn) as connection:
        revision = read_cache_state(connection).input_revision
        run_id = start_run(
            connection,
            RunSpec(
                target=published_session,
                requested_start=sessions[0],
                requested_end=newly_cached,
                code_version="equivalence",
                schema_version="1",
                transform_version="1",
            ),
        )
        rebuild_cache(connection, run_id, ticker_types=("CS",), batch_size=7)
        assert read_cache_state(connection).input_revision == revision

    assert {name: _query(dsn, query) for name, query in product_queries.items()} == published
    assert (
        _query(dsn, "SELECT input_revision,retained_start,retained_end FROM ingest.cache_state WHERE singleton=true")[0]
        == state_before
    )
