"""Atomic PostgreSQL persistence tests for validated daily outcomes."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl
import pytest

from tickerlake.extract import DAILY_AGGS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres import raw
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.raw import store_daily_outcome
from tickerlake.postgres.state import read_cache_state, start_run

if TYPE_CHECKING:
    import psycopg

DAY = date(2025, 1, 2)
NEXT_DAY = date(2025, 1, 3)
TWO_ROWS = 2
REVISION_THREE = 3
REVISED_CLOSE = 12.0


def _frame(day: date, rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=DAILY_AGGS_SCHEMA)


def _row(ticker: str, *, close: float = 10.0, volume: float = 123.75) -> dict[str, object]:
    return {
        "date": DAY,
        "ticker": ticker,
        "open": 9.5,
        "high": max(10.5, close),
        "low": 9.0,
        "close": close,
        "volume": volume,
    }


def _running_request(connection: psycopg.Connection, day: date) -> FetchRequest:
    run_id = start_run(
        connection,
        RunSpec(
            target=day,
            requested_start=None,
            requested_end=None,
            code_version="test",
            schema_version="1",
            transform_version="test",
        ),
    )
    return FetchRequest(run_id=run_id, source="daily", requested_date=day)


def _outcome(day: date, *rows: dict[str, object], status: FetchStatus = FetchStatus.populated) -> FetchOutcome:
    return FetchOutcome(status, _frame(day, list(rows)), day)


def _daily_rows(connection: psycopg.Connection, day: date) -> list[tuple[object, ...]]:
    return connection.execute(
        """SELECT d.date, t.symbol, d.open, d.high, d.low, d.close, d.volume
           FROM ingest.raw_daily d JOIN market.ticker t USING (ticker_id)
           WHERE d.date = %s ORDER BY t.symbol""",
        (day,),
    ).fetchall()


def test_populates_empty_date_and_adds_new_symbols_to_existing_date(pg_owner_dsn: str) -> None:
    """Store first-day data, then append a ticker on that same date."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        assert store_daily_outcome(connection, request, _outcome(DAY, _row("AAA"))) == 1
        first_id = connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol = 'AAA'").fetchone()[0]
        assert len(_daily_rows(connection, DAY)) == 1
        assert read_cache_state(connection).retained_start == DAY

        request = _running_request(connection, DAY)
        assert store_daily_outcome(connection, request, _outcome(DAY, _row("AAA"), _row("BBB"))) == TWO_ROWS
        assert [row[1] for row in _daily_rows(connection, DAY)] == ["AAA", "BBB"]
        assert connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol = 'AAA'").fetchone()[0] == first_id
        assert connection.execute(
            "SELECT active, screen_eligible FROM market.ticker WHERE symbol = 'BBB'"
        ).fetchone() == (None, False)
        assert (
            connection.execute(
                "SELECT market.ticker.symbol, ingest.ticker_reference.cik "
                "FROM ingest.ticker_reference JOIN market.ticker USING (ticker_id)"
            ).fetchall()
            == []
        )
        assert _daily_rows(connection, DAY)[1][-1:] == (123.75,)


def test_exact_date_replacement_preserves_other_dates_and_advances_revision_once(pg_owner_dsn: str) -> None:
    """Replace only the requested date and leave the neighbor untouched."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        store_daily_outcome(connection, request, _outcome(DAY, _row("AAA"), _row("BBB")))
        neighbor = _row("NEIGHBOR") | {"date": NEXT_DAY}
        request = _running_request(connection, NEXT_DAY)
        store_daily_outcome(connection, request, _outcome(NEXT_DAY, neighbor))
        before = _daily_rows(connection, NEXT_DAY)

        request = _running_request(connection, DAY)
        revision = store_daily_outcome(connection, request, _outcome(DAY, _row("AAA", close=12.0)))
        assert revision == REVISION_THREE
        assert [row[1] for row in _daily_rows(connection, DAY)] == ["AAA"]
        assert _daily_rows(connection, DAY)[0][5] == REVISED_CLOSE
        assert _daily_rows(connection, NEXT_DAY) == before
        state = read_cache_state(connection)
        assert (state.retained_start, state.retained_end, state.input_revision) == (DAY, NEXT_DAY, 3)


def test_identical_populated_and_empty_outcomes_do_not_advance_revision(pg_owner_dsn: str) -> None:
    """Persist manifests for no-op and empty results without changing cache state."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        outcome = _outcome(DAY, _row("AAA"))
        store_daily_outcome(connection, request, outcome)
        request = _running_request(connection, DAY)
        assert store_daily_outcome(connection, request, outcome) == 1
        request = _running_request(connection, NEXT_DAY)
        empty = _outcome(NEXT_DAY, status=FetchStatus.successful_empty)
        assert store_daily_outcome(connection, request, empty) == 1
        state = read_cache_state(connection)
        assert (state.input_revision, state.retained_start, state.retained_end) == (1, DAY, DAY)
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (3,)


@pytest.mark.parametrize(
    "status",
    [FetchStatus.successful_empty, FetchStatus.failed, FetchStatus.quarantined],
)
def test_nonpopulated_outcomes_preserve_existing_date(pg_owner_dsn: str, status: FetchStatus) -> None:
    """Record nonpopulated results without replacing cached daily rows."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        store_daily_outcome(connection, _running_request(connection, DAY), _outcome(DAY, _row("AAA"), _row("BBB")))

        before_rows = _daily_rows(connection, DAY)
        before_tickers = connection.execute("SELECT symbol, ticker_id FROM market.ticker ORDER BY symbol").fetchall()
        before_state = read_cache_state(connection)
        request = _running_request(connection, DAY)
        assert store_daily_outcome(connection, request, _outcome(DAY, status=status)) == before_state.input_revision

        assert _daily_rows(connection, DAY) == before_rows
        assert (
            connection.execute("SELECT symbol, ticker_id FROM market.ticker ORDER BY symbol").fetchall()
            == before_tickers
        )
        after_state = read_cache_state(connection)
        assert (after_state.input_revision, after_state.retained_start, after_state.retained_end) == (
            before_state.input_revision,
            before_state.retained_start,
            before_state.retained_end,
        )
        assert connection.execute("SELECT status FROM ingest.fetch_manifest ORDER BY manifest_id").fetchall() == [
            (FetchStatus.populated.value,),
            (status.value,),
        ]


@pytest.mark.parametrize(
    ("mutate", "status"),
    [
        (lambda frame: frame.with_columns(pl.lit(" ").alias("ticker")), FetchStatus.populated),
        (lambda frame: frame.with_columns(pl.lit(float("inf")).alias("close")), FetchStatus.populated),
        (lambda frame: frame.with_columns(pl.lit(-1.0).alias("volume")), FetchStatus.populated),
        (lambda frame: frame.with_columns(pl.lit(8.0).alias("high")), FetchStatus.populated),
        (lambda frame: frame.with_columns(pl.lit(NEXT_DAY).alias("date")), FetchStatus.populated),
        (lambda frame: pl.DataFrame({"wrong": [1]}), FetchStatus.populated),
        (lambda frame: frame, FetchStatus.failed),
    ],
)
def test_invalid_payload_is_rejected_without_cache_or_manifest_changes(pg_owner_dsn: str, mutate, status) -> None:
    """Reject malformed fetch data without mutating any durable cache rows."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        frame = mutate(_frame(DAY, [_row("AAA")]))
        outcome = FetchOutcome(status, frame, DAY)
        with pytest.raises(PostgresWriterError, match="invalid"):
            store_daily_outcome(connection, request, outcome)
        assert _daily_rows(connection, DAY) == []
        assert connection.execute("SELECT count(*) FROM market.ticker").fetchone() == (0,)
        assert read_cache_state(connection).input_revision == 0
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (0,)


def test_copy_failure_rolls_back_new_ticker_raw_rows_revision_and_manifest(pg_owner_dsn: str, monkeypatch) -> None:
    """Roll back all writes if a failure follows successful PostgreSQL COPY."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        original_copy = raw.copying.copy_frame

        def copy_then_fail(conn, stage_name, frame, columns):
            original_copy(conn, stage_name, frame, columns)
            raise PostgresWriterError

        monkeypatch.setattr(raw.copying, "copy_frame", copy_then_fail)
        with pytest.raises(PostgresWriterError):
            store_daily_outcome(connection, request, _outcome(DAY, _row("AAA")))
        assert _daily_rows(connection, DAY) == []
        assert connection.execute("SELECT count(*) FROM market.ticker").fetchone() == (0,)
        assert read_cache_state(connection).input_revision == 0
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (0,)


def test_database_failure_rolls_back_replacement_revision_and_manifest(pg_owner_dsn: str, monkeypatch) -> None:
    """Roll back row replacement and revision when a later step fails."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _running_request(connection, DAY)
        store_daily_outcome(connection, request, _outcome(DAY, _row("AAA")))
        before = _daily_rows(connection, DAY)
        original_advance = raw.advance_cache_revision

        def advance_then_fail(conn, accepted_date):
            original_advance(conn, accepted_date)
            raise PostgresWriterError

        monkeypatch.setattr(raw, "advance_cache_revision", advance_then_fail)
        request = _running_request(connection, DAY)
        with pytest.raises(PostgresWriterError):
            store_daily_outcome(connection, request, _outcome(DAY, _row("AAA", close=12.0)))
        assert _daily_rows(connection, DAY) == before
        assert read_cache_state(connection).input_revision == 1
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (1,)


@pytest.mark.parametrize(
    ("corruption", "single_row"),
    [("wrong_date", False), ("duplicate_symbol", False), ("unexpected_count", True)],
)
def test_corrupt_copied_stage_preserves_raw_state(
    pg_owner_dsn: str, monkeypatch, corruption: str, single_row: bool
) -> None:
    """Reject corrupted COPY rows before placeholders or durable writes."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        store_daily_outcome(connection, _running_request(connection, DAY), _outcome(DAY, _row("AAA"), _row("BBB")))
        neighbor = _row("NEIGHBOR") | {"date": NEXT_DAY}
        store_daily_outcome(connection, _running_request(connection, NEXT_DAY), _outcome(NEXT_DAY, neighbor))

        before = (
            _daily_rows(connection, DAY),
            _daily_rows(connection, NEXT_DAY),
            connection.execute("SELECT symbol, ticker_id FROM market.ticker ORDER BY symbol").fetchall(),
            connection.execute("SELECT last_value, is_called FROM market.ticker_ticker_id_seq").fetchone(),
            connection.execute(
                "SELECT input_revision, retained_start, retained_end FROM ingest.cache_state WHERE singleton = true"
            ).fetchone(),
            connection.execute(
                "SELECT manifest_id, run_id, source, requested_date, status, row_count "
                "FROM ingest.fetch_manifest ORDER BY manifest_id"
            ).fetchall(),
        )
        request = _running_request(connection, DAY)
        original_copy = raw.copying.copy_frame

        def copy_then_corrupt(conn, stage_name, frame, columns) -> None:
            original_copy(conn, stage_name, frame, columns)
            if corruption == "wrong_date":
                conn.execute("UPDATE pg_temp.raw_stage SET date = %s WHERE symbol = 'NEW'", (NEXT_DAY,))
            elif corruption == "duplicate_symbol":
                conn.execute("UPDATE pg_temp.raw_stage SET symbol = 'AAA' WHERE symbol = 'NEW'")
            else:
                conn.execute("INSERT INTO pg_temp.raw_stage SELECT * FROM pg_temp.raw_stage")

        monkeypatch.setattr(raw.copying, "copy_frame", copy_then_corrupt)
        payload = [_row("AAA", close=12.0)] if single_row else [_row("AAA", close=12.0), _row("NEW")]
        with pytest.raises(PostgresWriterError, match="invalid"):
            store_daily_outcome(connection, request, _outcome(DAY, *payload))

        after = (
            _daily_rows(connection, DAY),
            _daily_rows(connection, NEXT_DAY),
            connection.execute("SELECT symbol, ticker_id FROM market.ticker ORDER BY symbol").fetchall(),
            connection.execute("SELECT last_value, is_called FROM market.ticker_ticker_id_seq").fetchone(),
            connection.execute(
                "SELECT input_revision, retained_start, retained_end FROM ingest.cache_state WHERE singleton = true"
            ).fetchone(),
            connection.execute(
                "SELECT manifest_id, run_id, source, requested_date, status, row_count "
                "FROM ingest.fetch_manifest ORDER BY manifest_id"
            ).fetchall(),
        )
        assert after == before
