"""Fresh Massive backfill, frozen target, and explicit correction ranges (PostgreSQL).

These are sociable integration tests: the real XNYS calendar, extraction,
Polars transforms, PostgreSQL storage, rebuild, and publication code all run.
Only the external Massive client is replaced with a hand-written fake that
returns raw provider-shaped records. The fake lives behind the production
constructor seam ``postgres.backfill.MassiveClient`` and opens its own short
lived observer connection, so no writer-connection code is patched.

The frozen API under test, owned by a separate production lane, is::

    @dataclass(frozen=True, slots=True, kw_only=True)
    class BackfillRequest:
        code_version: str
        schema_version: str
        transform_version: str
        target: datetime.date | None = None
        correction_range: tuple[datetime.date, datetime.date] | None = None

    def backfill(
        config: Config,
        request: BackfillRequest,
        *,
        now: datetime.datetime,
        batch_size: int = 100,
    ) -> PublicationResult: ...

The contract exercised here is observable behavior: which sessions are
requested from the provider, what persists in ``ingest`` and ``market``, and
which public generation survives a blocked run. Tests never assert private
call order inside the production module.
"""

from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import psycopg
import pytest

from tickerlake.config import Config
from tickerlake.postgres import backfill as backfill_module
from tickerlake.postgres.backfill import BackfillError, BackfillRequest
from tickerlake.postgres.connection import WRITER_LOCK_KEY, PostgresWriterError, writer_connection
from tickerlake.postgres.publication import PublicationOutcomeUnknownError

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import date
    from typing import Any
    from uuid import UUID

UTC = datetime.UTC
SESSION_START = datetime.date(2024, 1, 2)
SESSION_TARGET = datetime.date(2024, 1, 5)
SESSIONS = (
    datetime.date(2024, 1, 2),
    datetime.date(2024, 1, 3),
    datetime.date(2024, 1, 4),
    datetime.date(2024, 1, 5),
)
CORRECTION_START = datetime.date(2023, 12, 26)
CORRECTION_END = datetime.date(2024, 1, 2)
CORRECTION_SESSIONS = (
    datetime.date(2023, 12, 26),
    datetime.date(2023, 12, 27),
    datetime.date(2023, 12, 28),
    datetime.date(2023, 12, 29),
    datetime.date(2024, 1, 2),
)
AFTER_CLOSE = datetime.datetime(2026, 1, 2, 22, 0, tzinfo=UTC)
BATCH_SIZE = 2
GATE_TIMEOUT = 30.0
NON_POPULATED = frozenset({"failed", "quarantined", "successful_empty"})


def _utc(*args: int) -> datetime.datetime:
    return datetime.datetime(*args, tzinfo=UTC)


def _millis(day: date) -> float:
    midnight = datetime.datetime(day.year, day.month, day.day, tzinfo=UTC)
    return float(int(midnight.timestamp() * 1000))


def _daily_record(day: date, symbol: str, close: float) -> dict[str, object]:
    return {
        "timestamp": _millis(day),
        "ticker": symbol,
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": close,
        "volume": 100.0,
        "transactions": 5,
    }


def _daily_records(day: date, symbols: tuple[str, ...] = ("ACTIVE", "INACTIVE", "UNKNOWN")) -> list[dict[str, object]]:
    return [_daily_record(day, symbol, 11.0) for symbol in symbols]


def _split_record(day: date, symbol: str = "ACTIVE") -> dict[str, object]:
    return {
        "ticker": symbol,
        "execution_date": day.isoformat(),
        "split_from": 2.0,
        "split_to": 1.0,
        "historical_adjustment_factor": 0.5,
        "adjustment_type": "split",
    }


def _ticker_records(types: tuple[str, ...]) -> list[dict[str, object]]:
    return [
        {"ticker": "ACTIVE", "name": "Active", "type": types[0], "primary_exchange": "X", "cik": None, "active": True},
        {
            "ticker": "INACTIVE",
            "name": "Inactive",
            "type": types[0],
            "primary_exchange": "X",
            "cik": None,
            "active": False,
        },
    ]


def _writer_backend_state(database_url: str) -> str | None:
    """Return the transaction state of the backend holding the shared writer lock."""
    with psycopg.connect(database_url, autocommit=True) as connection:
        row = connection.execute(
            """SELECT a.state FROM pg_locks AS l JOIN pg_stat_activity AS a USING (pid)
               WHERE l.locktype = 'advisory' AND l.granted AND l.classid = 0 AND l.objid = %s""",
            (WRITER_LOCK_KEY,),
        ).fetchone()
    return None if row is None else str(row[0])


@dataclass
class FakeMassiveClient:
    """Hand-written external Massive double returning raw provider-shaped records."""

    daily: dict[date, list[dict[str, object]]] = field(default_factory=dict)
    splits: list[dict[str, object]] = field(default_factory=list)
    tickers: list[dict[str, object]] | None = None
    daily_override: Callable[[date], Any] | None = None
    ticker_override: Callable[[tuple[str, ...]], Any] | None = None
    observe_writer: bool = False
    gate: threading.Event | None = None
    gate_reached: threading.Event | None = None
    config: Config | None = None
    daily_calls: list[date] = field(default_factory=list)
    split_calls: list[tuple[date, date]] = field(default_factory=list)
    ticker_calls: list[tuple[str, ...]] = field(default_factory=list)
    observed_states: list[str | None] = field(default_factory=list)

    def fetch_daily_aggs(self, day: date) -> Any:
        """Return raw provider daily records for one session."""
        self._observe()
        self.daily_calls.append(day)
        self._cross_gate()
        if self.daily_override is not None:
            return self.daily_override(day)
        return self.daily.get(day, [])

    def fetch_splits(self, start_date: date, end_date: date) -> Any:
        """Return raw provider split records that fall inside the requested range."""
        self._observe()
        self.split_calls.append((start_date, end_date))
        return [
            record
            for record in self.splits
            if start_date <= datetime.date.fromisoformat(str(record["execution_date"])) <= end_date
        ]

    def fetch_tickers(self, types: list[str]) -> Any:
        """Return raw provider ticker metadata for the requested types."""
        self._observe()
        self.ticker_calls.append(tuple(types))
        if self.ticker_override is not None:
            return self.ticker_override(tuple(types))
        if self.tickers is not None:
            return self.tickers
        return _ticker_records(tuple(types))

    def _observe(self) -> None:
        """Record the writer backend state while the provider call is in flight."""
        if self.observe_writer:
            self.observed_states.append(_writer_backend_state(self._require_config().database_url))

    def _cross_gate(self) -> None:
        if self.gate is None:
            return
        if self.gate_reached is not None:
            self.gate_reached.set()
        if not self.gate.wait(timeout=GATE_TIMEOUT):
            raise TimeoutError

    def _require_config(self) -> Config:
        if self.config is None:
            raise RuntimeError("fake Massive client was used before backfill supplied a config")  # noqa: TRY003
        return self.config


@pytest.fixture
def massive(monkeypatch: pytest.MonkeyPatch) -> FakeMassiveClient:
    """Install the fake Massive client behind the production constructor seam."""
    fake = FakeMassiveClient()

    def construct(config: Config) -> FakeMassiveClient:
        fake.config = config
        return fake

    monkeypatch.setattr(backfill_module, "MassiveClient", construct)
    return fake


def _config(
    database_url: str,
    *,
    start: date = SESSION_START,
    end: date = SESSION_TARGET,
    api_key: str = "test-key",
) -> Config:
    return Config(
        api_key=api_key,
        database_url=database_url,
        start_date=start,
        end_date=end,
        ticker_types=["CS"],
    )


def _request(
    *,
    target: date | None = SESSION_TARGET,
    correction_range: tuple[date, date] | None = None,
) -> BackfillRequest:
    return BackfillRequest(
        code_version="code-1",
        schema_version="schema-1",
        transform_version="transform-1",
        target=target,
        correction_range=correction_range,
    )


def _seed_feed(massive: FakeMassiveClient, days: tuple[date, ...]) -> None:
    massive.daily = {day: _daily_records(day) for day in days}


def _rows(database_url: str, query: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    with psycopg.connect(database_url, autocommit=True) as connection:
        return connection.execute(query, params).fetchall()


def _public_snapshot(database_url: str) -> tuple[list[tuple[object, ...]], ...]:
    """Capture the published generation only: products, metadata, and public state.

    Private ingest inputs (cache revision/retained bounds and the raw ticker
    reference) are legitimately mutable and are deliberately excluded so a test
    can assert the public generation survived a blocked or failed run.
    """
    queries = (
        "SELECT * FROM market.adjusted_daily ORDER BY ticker_id, date",
        "SELECT * FROM market.adjusted_weekly ORDER BY ticker_id, date",
        "SELECT * FROM market.adjusted_monthly ORDER BY ticker_id, date",
        "SELECT * FROM market.latest_daily ORDER BY ticker_id",
        "SELECT * FROM market.publication_state ORDER BY run_id",
        "SELECT * FROM market.ticker ORDER BY ticker_id",
    )
    with psycopg.connect(database_url, autocommit=True) as connection:
        return tuple(connection.execute(query).fetchall() for query in queries)


def _cache_revision(database_url: str) -> int:
    """Return the current private ingest cache input revision."""
    return int(_rows(database_url, "SELECT input_revision FROM ingest.cache_state WHERE singleton = true")[0][0])


def _raw_close(database_url: str, ticker_id: object, day: date) -> float:
    """Return the persisted raw daily close for one ticker and session."""
    return float(
        _rows(
            database_url,
            "SELECT close FROM ingest.raw_daily WHERE ticker_id = %s AND date = %s",
            (ticker_id, day),
        )[0][0]
    )


def _mutation_snapshot(database_url: str) -> tuple[list[tuple[object, ...]], ...]:
    queries = (
        "SELECT * FROM ingest.run",
        "SELECT * FROM ingest.fetch_manifest",
        "SELECT * FROM ingest.raw_daily",
        "SELECT * FROM market.ticker",
        "SELECT * FROM market.publication_state",
        "SELECT * FROM ingest.cache_state",
    )
    with psycopg.connect(database_url, autocommit=True) as connection:
        return tuple(connection.execute(query).fetchall() for query in queries)


def _bootstrap(database_url: str, massive: FakeMassiveClient) -> UUID:
    _seed_feed(massive, SESSIONS)
    result = backfill_module.backfill(_config(database_url), _request(), now=AFTER_CLOSE)
    assert result.published_session == SESSION_TARGET
    return result.run_id


def _failed_run(database_url: str) -> tuple[object, ...]:
    rows = _rows(
        database_url,
        "SELECT run_id, state, failure_code FROM ingest.run WHERE state = 'failed' ORDER BY started_at DESC LIMIT 1",
    )
    assert rows, "expected a failed run to be recorded"
    return rows[0]


# ---------------------------------------------------------------------------
# Fresh bootstrap
# ---------------------------------------------------------------------------


def test_fresh_bootstrap_fetches_every_closed_session_and_publishes_target(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A fresh bootstrap fetches each closed session once and publishes the frozen target."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)

    result = backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE, batch_size=BATCH_SIZE)

    assert result.published_session == SESSION_TARGET
    assert massive.daily_calls == list(SESSIONS)
    assert massive.split_calls
    assert massive.ticker_calls == [("CS",)]

    raw_count = _rows(dsn, "SELECT count(*) FROM ingest.raw_daily")[0][0]
    assert raw_count == len(SESSIONS) * 3
    symbols = _rows(
        dsn,
        "SELECT DISTINCT t.symbol FROM ingest.raw_daily r JOIN market.ticker t USING (ticker_id) ORDER BY t.symbol",
    )
    assert [row[0] for row in symbols] == ["ACTIVE", "INACTIVE", "UNKNOWN"]

    accepted = _rows(
        dsn,
        """SELECT s.date, s.row_count, m.status, m.requested_date, m.row_count
           FROM ingest.raw_session s JOIN ingest.fetch_manifest m USING (manifest_id)
           ORDER BY s.date""",
    )
    assert [row[0] for row in accepted] == list(SESSIONS)
    assert all(row[1] > 0 and row[2] == "populated" and row[3] == row[0] and row[4] == row[1] for row in accepted)

    manifest_sources = dict(_rows(dsn, "SELECT source, count(*) FROM ingest.fetch_manifest GROUP BY source"))
    assert manifest_sources["daily"] == len(SESSIONS)
    assert manifest_sources["tickers"] == 1
    assert manifest_sources["splits"] == 1

    published = _rows(dsn, "SELECT published_session FROM market.publication_state")
    assert published == [(SESSION_TARGET,)]

    latest = _rows(
        dsn, "SELECT t.symbol FROM market.latest_daily l JOIN market.ticker t USING (ticker_id) ORDER BY t.symbol"
    )
    assert [row[0] for row in latest] == ["ACTIVE"]

    history = _rows(
        dsn,
        """SELECT t.symbol, count(*) FROM market.adjusted_daily d JOIN market.ticker t USING (ticker_id)
           GROUP BY t.symbol ORDER BY t.symbol""",
    )
    assert dict(history) == {"ACTIVE": 4, "INACTIVE": 4, "UNKNOWN": 4}
    assert _rows(dsn, "SELECT count(*) FROM ingest.split_event") == [(0,)]


def test_fresh_bootstrap_writes_no_local_database_files(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """PostgreSQL bootstrap does not create DuckDB or other local artifacts."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)

    backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert list(tmp_path.iterdir()) == []


def test_repeated_backfill_preserves_symbol_identity(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """Republishing the same universe keeps stable ticker IDs instead of renumbering."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)
    config = _config(dsn)

    backfill_module.backfill(config, _request(), now=AFTER_CLOSE)
    first = dict(_rows(dsn, "SELECT symbol, ticker_id FROM market.ticker ORDER BY ticker_id"))

    backfill_module.backfill(config, _request(), now=AFTER_CLOSE)
    second = dict(_rows(dsn, "SELECT symbol, ticker_id FROM market.ticker ORDER BY ticker_id"))

    assert first == second
    assert set(first) == {"ACTIVE", "INACTIVE", "UNKNOWN"}


def test_run_records_versions_and_requested_bounds(pg_migrated_database, tmp_path, massive: FakeMassiveClient) -> None:
    """The durable run stores code versions and bounds that cover every fetched session."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)

    result = backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    row = _rows(
        dsn,
        """SELECT target_date, requested_start, requested_end, code_version, schema_version,
                  transform_version, state
           FROM ingest.run WHERE run_id = %s""",
        (result.run_id,),
    )[0]
    assert row[0] == SESSION_TARGET
    assert row[3:6] == ("code-1", "schema-1", "transform-1")
    assert row[6] == "published"
    assert row[1] is not None
    assert row[2] is not None
    assert row[1] <= min(massive.daily_calls)
    assert row[2] >= max(massive.daily_calls)


@pytest.mark.parametrize(
    ("now", "expected_target", "expected_calls"),
    [
        (_utc(2024, 1, 5, 20, 59), datetime.date(2024, 1, 4), SESSIONS[:3]),
        (_utc(2024, 1, 5, 21, 0), datetime.date(2024, 1, 5), SESSIONS),
        (_utc(2026, 1, 2, 22, 0), datetime.date(2024, 1, 5), SESSIONS),
    ],
    ids=["before-close", "at-close", "late-replay"],
)
def test_frozen_now_selects_target_and_fetch_scope(  # noqa: PLR0913, PLR0917
    pg_migrated_database,
    tmp_path,
    massive: FakeMassiveClient,
    now: datetime.datetime,
    expected_target: date,
    expected_calls: tuple[date, ...],
) -> None:
    """The frozen instant alone decides the target session and the closed fetch scope."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)

    result = backfill_module.backfill(_config(dsn), _request(), now=now)

    assert result.published_session == expected_target
    assert massive.daily_calls == list(expected_calls)


# ---------------------------------------------------------------------------
# Explicit correction ranges
# ---------------------------------------------------------------------------


def test_correction_refetches_explicit_range_including_cached_dates(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A correction range fetches its every closed date, including cached ones, with no implicit window."""
    dsn = pg_migrated_database.owner_dsn
    _bootstrap(dsn, massive)
    massive.daily_calls.clear()

    replacement = [
        _daily_record(datetime.date(2024, 1, 2), "ACTIVE", 11.5),
        _daily_record(datetime.date(2024, 1, 2), "INACTIVE", 11.0),
        _daily_record(datetime.date(2024, 1, 2), "UNKNOWN", 11.0),
    ]
    massive.daily[datetime.date(2024, 1, 2)] = replacement
    for day in CORRECTION_SESSIONS[:-1]:
        massive.daily[day] = _daily_records(day)

    backfill_module.backfill(
        _config(dsn),
        _request(target=SESSION_TARGET, correction_range=(CORRECTION_START, CORRECTION_END)),
        now=AFTER_CLOSE,
    )

    assert massive.daily_calls == list(CORRECTION_SESSIONS)
    for implicit in SESSIONS[1:]:
        assert implicit not in massive.daily_calls

    raw_dates = {row[0] for row in _rows(dsn, "SELECT date FROM ingest.raw_daily GROUP BY date")}
    assert raw_dates == set(CORRECTION_SESSIONS) | set(SESSIONS)
    active_id = _rows(dsn, "SELECT ticker_id FROM market.ticker WHERE symbol = 'ACTIVE'")[0][0]
    updated = _rows(
        dsn, "SELECT close FROM ingest.raw_daily WHERE ticker_id = %s AND date = %s", (active_id, CORRECTION_END)
    )
    assert updated[0][0] == pytest.approx(11.5)
    published_correction = _rows(
        dsn, "SELECT close FROM market.adjusted_daily WHERE ticker_id = %s AND date = %s", (active_id, CORRECTION_END)
    )
    assert published_correction[0][0] == pytest.approx(11.5)
    assert _rows(dsn, "SELECT published_session FROM market.publication_state") == [(SESSION_TARGET,)]


def test_correction_target_must_have_existing_acceptance(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A target outside the corrected range cannot publish without prior accepted raw evidence."""
    dsn = pg_migrated_database.owner_dsn
    feed = {day: _daily_records(day) for day in CORRECTION_SESSIONS}
    massive.daily = feed

    with pytest.raises(PostgresWriterError):
        backfill_module.backfill(
            _config(dsn),
            _request(target=SESSION_TARGET, correction_range=(CORRECTION_START, CORRECTION_END)),
            now=AFTER_CLOSE,
        )

    assert massive.daily_calls == list(CORRECTION_SESSIONS)
    assert _rows(dsn, "SELECT count(*) FROM market.publication_state") == [(0,)]
    assert _rows(dsn, "SELECT count(*) FROM market.adjusted_daily") == [(0,)]
    assert _rows(dsn, "SELECT count(*) FROM ingest.raw_session WHERE date = %s", (SESSION_TARGET,)) == [(0,)]


# ---------------------------------------------------------------------------
# Blocking and preservation
# ---------------------------------------------------------------------------


def _bad_daily(kind: str) -> Callable[[date], Any]:
    def override(day: date) -> Any:
        if day != SESSION_TARGET:
            return _daily_records(day)
        if kind == "transport":
            raise RuntimeError("transport failed")  # noqa: TRY003
        if kind == "invalid":
            return "not-a-list"
        if kind == "shrink":
            return _daily_records(day, symbols=("ACTIVE",))
        return []

    return override


@pytest.mark.parametrize("kind", ["transport", "invalid", "shrink", "empty"])
def test_unacceptable_target_preserves_public_generation_and_cached_raw(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient, kind: str
) -> None:
    """A failed, quarantined, or empty target never replaces the published generation or cached target raw."""
    dsn = pg_migrated_database.owner_dsn
    run_id = _bootstrap(dsn, massive)
    before = _public_snapshot(dsn)
    target_rows = _rows(dsn, "SELECT * FROM ingest.raw_daily WHERE date = %s ORDER BY ticker_id", (SESSION_TARGET,))
    massive.daily_override = _bad_daily(kind)
    massive.daily_calls.clear()

    with pytest.raises(PostgresWriterError):
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert _public_snapshot(dsn) == before
    assert (
        _rows(dsn, "SELECT * FROM ingest.raw_daily WHERE date = %s ORDER BY ticker_id", (SESSION_TARGET,))
        == target_rows
    )
    assert _rows(dsn, "SELECT published_session FROM market.publication_state") == [(SESSION_TARGET,)]
    assert _rows(dsn, "SELECT run_id FROM market.publication_state") == [(run_id,)]

    failed_run = _failed_run(dsn)[0]
    assert _failed_run(dsn)[2] == "incomplete_fetch"
    statuses = _rows(
        dsn,
        "SELECT status FROM ingest.fetch_manifest WHERE run_id = %s AND source = 'daily'",
        (failed_run,),
    )
    assert len(statuses) == len(SESSIONS)
    target_status = _rows(
        dsn,
        "SELECT status FROM ingest.fetch_manifest WHERE run_id = %s AND source = 'daily' AND requested_date = %s",
        (failed_run, SESSION_TARGET),
    )
    assert target_status
    assert target_status[0][0] in NON_POPULATED


def test_middle_session_failure_persists_accepted_raw_but_never_publishes(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A failed middle session keeps accepted changed raw on both sides while the public generation stays frozen.

    Accepted sessions before and after the failing middle session must keep
    their changed raw and advance the private cache revision. The rejected
    session keeps its prior raw. Every requested daily session still gets a
    manifest, but nothing is published.
    """
    dsn = pg_migrated_database.owner_dsn
    _bootstrap(dsn, massive)
    before_public = _public_snapshot(dsn)
    before_revision = _cache_revision(dsn)
    failing = SESSIONS[1]
    changed_days = (SESSIONS[0], SESSIONS[2])
    active_id = _rows(dsn, "SELECT ticker_id FROM market.ticker WHERE symbol = 'ACTIVE'")[0][0]
    baseline_raw = {
        day: _rows(dsn, "SELECT * FROM ingest.raw_daily WHERE date = %s ORDER BY ticker_id", (day,)) for day in SESSIONS
    }

    def override(day: date) -> Any:
        if day == failing:
            raise RuntimeError("transport failed")  # noqa: TRY003
        if day in changed_days:
            return [_daily_record(day, symbol, 11.5) for symbol in ("ACTIVE", "INACTIVE", "UNKNOWN")]
        return _daily_records(day)

    massive.daily_override = override
    massive.daily_calls.clear()

    with pytest.raises(PostgresWriterError):
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert massive.daily_calls == list(SESSIONS)
    failed_run = _failed_run(dsn)[0]
    assert _failed_run(dsn)[2] == "incomplete_fetch"
    manifest_dates = [
        row[0]
        for row in _rows(
            dsn,
            "SELECT requested_date FROM ingest.fetch_manifest "
            "WHERE run_id = %s AND source = 'daily' ORDER BY requested_date",
            (failed_run,),
        )
    ]
    assert manifest_dates == list(SESSIONS)

    assert (
        _rows(dsn, "SELECT * FROM ingest.raw_daily WHERE date = %s ORDER BY ticker_id", (failing,))
        == baseline_raw[failing]
    )
    for day in changed_days:
        assert _raw_close(dsn, active_id, day) == pytest.approx(11.5)
    assert (
        _rows(dsn, "SELECT * FROM ingest.raw_daily WHERE date = %s ORDER BY ticker_id", (SESSIONS[3],))
        == baseline_raw[SESSIONS[3]]
    )

    assert _cache_revision(dsn) == before_revision + len(changed_days)
    assert _public_snapshot(dsn) == before_public


def test_initial_empty_split_history_is_valid_and_published(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A provider with no split events is a valid successful-empty result, not a failure."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)
    massive.splits = []

    result = backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert result.published_session == SESSION_TARGET
    assert _rows(dsn, "SELECT count(*) FROM ingest.split_event") == [(0,)]
    split_manifest = _rows(dsn, "SELECT status FROM ingest.fetch_manifest WHERE source = 'splits'")
    assert split_manifest == [("successful_empty",)]


def test_split_removal_blocks_publication(pg_migrated_database, tmp_path, massive: FakeMassiveClient) -> None:
    """An empty refresh that would delete existing split events is quarantined and blocks publishing."""
    dsn = pg_migrated_database.owner_dsn
    massive.splits = [_split_record(datetime.date(2024, 1, 3))]
    _bootstrap(dsn, massive)
    before = _public_snapshot(dsn)
    assert _rows(dsn, "SELECT count(*) FROM ingest.split_event") == [(1,)]

    massive.splits = []
    with pytest.raises(PostgresWriterError):
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert _public_snapshot(dsn) == before
    assert _rows(dsn, "SELECT count(*) FROM ingest.split_event") == [(1,)]


def test_ticker_metadata_shrink_blocks_and_preserves_references(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A shrinking metadata refresh is quarantined and never deletes existing references."""
    dsn = pg_migrated_database.owner_dsn
    _bootstrap(dsn, massive)
    before = _public_snapshot(dsn)
    assert _rows(dsn, "SELECT count(*) FROM ingest.ticker_reference") == [(2,)]

    massive.ticker_override = lambda types: _ticker_records(types)[:1]

    with pytest.raises(PostgresWriterError):
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert _public_snapshot(dsn) == before
    assert _rows(dsn, "SELECT count(*) FROM ingest.ticker_reference") == [(2,)]


# ---------------------------------------------------------------------------
# Split coverage union
# ---------------------------------------------------------------------------


def test_split_coverage_uses_exact_contiguous_calendar_year_windows(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """Split coverage is refreshed as exact contiguous calendar-year windows that lose no known event.

    A real prior bootstrap and a real wide-config correction seed the retained
    raw bounds (outside the final config/correction) and the existing split
    event bounds (including a future latest-known event). The final run then
    refreshes the split union and every window must be a single, inclusive,
    non-overlapping calendar year whose union covers every known event.
    """
    dsn = pg_migrated_database.owner_dsn
    prior_event = datetime.date(2024, 1, 3)
    outside_event = datetime.date(2023, 3, 1)
    future_event = datetime.date(2026, 6, 1)
    known_events = {prior_event, outside_event, future_event}

    massive.splits = [_split_record(prior_event)]
    _bootstrap(dsn, massive)
    assert _rows(dsn, "SELECT execution_date FROM ingest.split_event") == [(prior_event,)]

    # A real wide-config correction stores an event outside the retained raw
    # bounds and a future latest-known event without widening retained daily data.
    correction_days = {day: _daily_records(day) for day in (outside_event, datetime.date(2023, 3, 2))}
    massive.daily = correction_days
    massive.splits = [_split_record(day) for day in sorted(known_events)]
    backfill_module.backfill(
        _config(dsn, start=datetime.date(2023, 1, 3), end=datetime.date(2026, 12, 31)),
        _request(target=SESSION_TARGET, correction_range=(outside_event, datetime.date(2023, 3, 2))),
        now=AFTER_CLOSE,
    )

    retained = _rows(dsn, "SELECT retained_start, retained_end FROM ingest.cache_state WHERE singleton = true")[0]
    existing_end = _rows(dsn, "SELECT max(execution_date) FROM ingest.split_event")[0][0]
    assert existing_end == future_event
    assert existing_end > retained[1], "the latest known event must sit outside retained raw bounds"

    # Final run: narrow config and correction, retained raw and event bounds outside both.
    massive.daily = dict(correction_days)
    massive.splits = [_split_record(day) for day in sorted(known_events)]
    massive.split_calls.clear()
    narrow = _config(dsn, start=outside_event, end=datetime.date(2023, 3, 2))
    backfill_module.backfill(
        narrow,
        _request(target=SESSION_TARGET, correction_range=(outside_event, datetime.date(2023, 3, 2))),
        now=AFTER_CLOSE,
    )

    assert retained[1] > narrow.end_date
    windows = massive.split_calls
    union_start = min(start for start, _ in windows)
    union_end = max(end for _, end in windows)
    assert union_start == outside_event
    assert union_end == future_event
    expected_window_count = future_event.year - outside_event.year + 1  # 2023, 2024, 2025, 2026
    assert len(windows) == expected_window_count
    for index, (start, end) in enumerate(windows):
        assert start <= end
        assert start.year == end.year, "each window must stay within a single calendar year"
        expected_start = union_start if index == 0 else windows[index - 1][1] + datetime.timedelta(days=1)
        assert start == expected_start, "windows must be contiguous and non-overlapping"
        assert end == min(datetime.date(end.year, 12, 31), union_end)
    for event in known_events:
        assert any(start <= event <= end for start, end in windows), f"window union lost event {event}"
    stored_events = {row[0] for row in _rows(dsn, "SELECT execution_date FROM ingest.split_event")}
    assert stored_events == known_events


# ---------------------------------------------------------------------------
# Preconditions and lock discipline
# ---------------------------------------------------------------------------


def test_missing_api_key_is_rejected_before_mutation(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A backfill without credentials fails before any database row is written."""
    dsn = pg_migrated_database.owner_dsn
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(ValueError, match="MASSIVE_API_KEY"):
        backfill_module.backfill(
            _config(dsn, api_key=""),
            _request(),
            now=AFTER_CLOSE,
        )

    assert _mutation_snapshot(dsn) == baseline


def test_naive_now_is_rejected_before_mutation(pg_migrated_database, tmp_path, massive: FakeMassiveClient) -> None:
    """A naive instant is rejected before the run or any raw row exists."""
    dsn = pg_migrated_database.owner_dsn
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(BackfillError, match="timezone-aware"):
        backfill_module.backfill(_config(dsn), _request(), now=datetime.datetime(2024, 1, 5, 22, 0))  # noqa: DTZ001

    assert _mutation_snapshot(dsn) == baseline


def test_non_date_target_is_rejected_before_mutation(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A target that is not a plain date is rejected before any mutation."""
    dsn = pg_migrated_database.owner_dsn
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(BackfillError, match="target must be a date"):
        backfill_module.backfill(
            _config(dsn),
            _request(target=datetime.datetime(2024, 1, 5, tzinfo=UTC)),
            now=AFTER_CLOSE,
        )

    assert _mutation_snapshot(dsn) == baseline


def test_empty_range_is_rejected_before_mutation(pg_migrated_database, tmp_path, massive: FakeMassiveClient) -> None:
    """A configured range whose end precedes its target is rejected without writing."""
    dsn = pg_migrated_database.owner_dsn
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(BackfillError, match="ordered dates"):
        backfill_module.backfill(
            _config(dsn, start=SESSION_TARGET, end=SESSION_START),
            _request(target=SESSION_START),
            now=AFTER_CLOSE,
        )

    assert _mutation_snapshot(dsn) == baseline


def test_batch_size_above_max_is_rejected_before_client_and_writes(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A batch size above the provider/rebuild maximum is rejected before any client or write."""
    dsn = pg_migrated_database.owner_dsn
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(BackfillError, match="batch size"):
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE, batch_size=1001)

    assert _mutation_snapshot(dsn) == baseline
    assert massive.config is None
    assert massive.daily_calls == []
    assert massive.split_calls == []
    assert massive.ticker_calls == []


def test_too_many_ticker_types_is_rejected_before_client_and_writes(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """More ticker types than the provider scope allows is rejected before any client or write."""
    dsn = pg_migrated_database.owner_dsn
    baseline = _mutation_snapshot(dsn)
    too_many = [f"T{index:02d}" for index in range(21)]
    config = Config(
        api_key="test-key",
        database_url=dsn,
        start_date=SESSION_START,
        end_date=SESSION_TARGET,
        ticker_types=too_many,
    )

    with pytest.raises(BackfillError, match="ticker types"):
        backfill_module.backfill(config, _request(), now=AFTER_CLOSE)

    assert _mutation_snapshot(dsn) == baseline
    assert massive.config is None
    assert massive.daily_calls == []
    assert massive.split_calls == []
    assert massive.ticker_calls == []


def test_empty_weekend_correction_is_rejected_instead_of_falling_back_to_full(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A correction range with no closed sessions is rejected, never widened to the configured history."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)
    baseline = _mutation_snapshot(dsn)

    with pytest.raises(BackfillError, match="no closed sessions"):
        backfill_module.backfill(
            _config(dsn),
            _request(target=SESSION_TARGET, correction_range=(datetime.date(2024, 1, 6), datetime.date(2024, 1, 7))),
            now=AFTER_CLOSE,
        )

    assert _mutation_snapshot(dsn) == baseline
    assert massive.config is None
    assert massive.daily_calls == []
    assert massive.split_calls == []
    assert massive.ticker_calls == []


def test_writer_connection_is_idle_while_the_provider_is_fetched(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """The shared writer lock is held and its backend stays idle during network fetches."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)
    massive.observe_writer = True

    backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    total_provider_calls = len(massive.daily_calls) + len(massive.split_calls) + len(massive.ticker_calls)
    assert total_provider_calls > len(SESSIONS), "the writer must also stay idle for ticker and split fetches"
    assert massive.observed_states == ["idle"] * total_provider_calls


def test_unknown_commit_propagates_without_marking_the_run_failed(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unresolved COMMIT propagates unchanged and is never recorded as a failed run."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)

    # Explicit fault boundary: replace the publication entry point with one that
    # reports an unresolved COMMIT, matching the foundation recovery pattern.
    def lose_commit(connection, run_id, *, ticker_types, batch_size):
        raise PublicationOutcomeUnknownError(run_id)

    monkeypatch.setattr(backfill_module, "rebuild_cache", lose_commit)

    with pytest.raises(PublicationOutcomeUnknownError) as caught:
        backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)

    assert caught.value.run_id is not None
    assert _rows(dsn, "SELECT state, failure_code FROM ingest.run") == [("running", None)]
    assert _rows(dsn, "SELECT count(*) FROM market.publication_state") == [(0,)]
    assert _rows(dsn, "SELECT count(*) FROM market.adjusted_daily") == [(0,)]


def test_second_writer_cannot_acquire_lock_during_fetch(
    pg_migrated_database, tmp_path, massive: FakeMassiveClient
) -> None:
    """A competing writer is locked out for the whole provider fetch, not just database writes."""
    dsn = pg_migrated_database.owner_dsn
    _seed_feed(massive, SESSIONS)
    massive.gate_reached = threading.Event()
    massive.gate = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            backfill_module.backfill(_config(dsn), _request(), now=AFTER_CLOSE)
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert massive.gate_reached.wait(timeout=GATE_TIMEOUT)
        with pytest.raises(PostgresWriterError, match="Another PostgreSQL writer is active"), writer_connection(dsn):
            pass
    finally:
        massive.gate.set()
        thread.join(timeout=GATE_TIMEOUT)

    assert not thread.is_alive()
    assert errors == []
    assert _rows(dsn, "SELECT count(*) FROM market.publication_state") == [(1,)]
