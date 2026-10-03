"""PostgreSQL run and cache state integration tests."""

from datetime import date, timedelta

import polars as pl
import psycopg
import pytest

from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.state import (
    advance_cache_revision,
    capture_run_inputs,
    fail_run,
    finish_run,
    read_cache_state,
    record_fetch_outcome,
    start_run,
)


def _spec() -> RunSpec:
    return RunSpec(
        target=date(2025, 1, 3),
        requested_start=date(2025, 1, 2),
        requested_end=date(2025, 1, 3),
        code_version="code",
        schema_version="1",
        transform_version="1",
    )


def _advance_then_abort(connection: psycopg.Connection) -> None:
    with connection.transaction():
        advance_cache_revision(connection, date(2025, 1, 2))
        raise RuntimeError("rollback")


def test_run_revision_capture_and_terminal_states(pg_owner_dsn: str) -> None:
    """Capture revisions and enforce running-to-terminal transitions."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        run_id = start_run(connection, _spec())
        assert connection.execute(
            "SELECT state, input_revision, ended_at FROM ingest.run WHERE run_id = %s", (run_id,)
        ).fetchone() == ("running", 0, None)
        assert advance_cache_revision(connection, date(2025, 1, 2)) == 1
        assert capture_run_inputs(connection, run_id) == 1
        finish_run(connection, run_id)
        row = connection.execute(
            "SELECT state, input_revision, started_at, ended_at, published_at FROM ingest.run WHERE run_id = %s",
            (run_id,),
        ).fetchone()
        assert row[0:2] == ("completed", 1)
        assert row[2] <= row[3]
        assert row[4] is None
        with pytest.raises(PostgresWriterError, match="not running"):
            capture_run_inputs(connection, run_id)


def test_failure_and_nested_transaction_rollback(pg_owner_dsn: str) -> None:
    """Keep nested writes atomic and reject unsafe failure codes."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        run_id = start_run(connection, _spec())
        with pytest.raises(RuntimeError):
            _advance_then_abort(connection)
        assert read_cache_state(connection).input_revision == 0
        fail_run(connection, run_id, "validation_error")
        assert connection.execute(
            "SELECT state, failure_code, ended_at FROM ingest.run WHERE run_id = %s", (run_id,)
        ).fetchone()[:2] == ("failed", "validation_error")
        with pytest.raises(PostgresWriterError, match="failure code"):
            fail_run(connection, run_id, "password=secret")


def test_cache_revision_bounds_and_unlocked_read(pg_owner_dsn: str) -> None:
    """Grow cache date bounds monotonically and permit unlocked reads."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        first_revision = advance_cache_revision(connection, date(2025, 1, 4))
        second_revision = advance_cache_revision(connection, date(2025, 1, 2))
        assert first_revision == 1
        assert second_revision == first_revision + 1
    with psycopg.connect(pg_owner_dsn) as reader:
        assert read_cache_state(reader).input_revision == second_revision
        state = read_cache_state(reader)
        assert (state.retained_start, state.retained_end) == (date(2025, 1, 2), date(2025, 1, 4))


def test_nullable_revision_and_manifest_ids(pg_owner_dsn: str) -> None:
    """Support nullable dates and return distinct persisted manifest IDs."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        run_id = start_run(connection, _spec())
        assert advance_cache_revision(connection) == 1
        assert read_cache_state(connection).retained_start is None
        request = FetchRequest(run_id=run_id, source="daily", requested_date=date(2025, 1, 2))
        outcome = FetchOutcome(status=FetchStatus.populated, frame=pl.DataFrame({"value": [1]}))
        first = record_fetch_outcome(connection, request, outcome)
        second = record_fetch_outcome(connection, request, outcome)
        assert first > 0
        assert second > first
        assert connection.execute(
            "SELECT input_revision FROM ingest.cache_state WHERE singleton = true"
        ).fetchone() == (1,)


def test_fetch_scope_and_diagnostic_validation(pg_owner_dsn: str) -> None:
    """Reject invalid split scopes and untrusted diagnostic strings."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        run_id = start_run(connection, _spec())
        outcome = FetchOutcome(status=FetchStatus.failed, frame=pl.DataFrame(), diagnostic="transport_error")
        with pytest.raises(PostgresWriterError, match="scope"):
            record_fetch_outcome(
                connection,
                FetchRequest(run_id=run_id, source="splits"),
                outcome,
            )
        split = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 1),
            requested_end=date(2025, 1, 2),
        )
        secret = FetchOutcome(status=FetchStatus.failed, frame=pl.DataFrame(), diagnostic="token=secret")
        with pytest.raises(PostgresWriterError, match="diagnostic"):
            record_fetch_outcome(connection, split, secret)
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (0,)


def test_ticker_scopes_persist_and_statement_timestamps(pg_owner_dsn: str) -> None:
    """Persist ticker type scopes and use statement time for finished_at."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        run_id = start_run(connection, _spec())
        first_scope = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        second_scope = FetchRequest(run_id=run_id, source="tickers", ticker_types=("ETF",))
        outcome = FetchOutcome(status=FetchStatus.successful_empty, frame=pl.DataFrame())

        with connection.transaction():
            explicit_started = connection.execute("SELECT statement_timestamp()").fetchone()[0]
            first_id = record_fetch_outcome(
                connection,
                FetchRequest(
                    run_id=run_id,
                    source="tickers",
                    ticker_types=first_scope.ticker_types,
                    started_at=explicit_started,
                ),
                outcome,
            )
            second_id = record_fetch_outcome(connection, second_scope, outcome)

        records = connection.execute(
            "SELECT manifest_id, requested_ticker_types, started_at, finished_at "
            "FROM ingest.fetch_manifest WHERE manifest_id IN (%s, %s) ORDER BY manifest_id",
            (first_id, second_id),
        ).fetchall()
        assert [record[1] for record in records] == [["CS"], ["ETF"]]
        assert records[0][0] != records[1][0]
        assert records[0][3] >= records[0][2]
        assert records[0][2] == explicit_started
        assert records[1][3] >= records[1][2]
        assert records[1][3].tzinfo is not None
        assert records[1][3].utcoffset() == timedelta(0)
        assert connection.execute(
            "SELECT input_revision FROM ingest.cache_state WHERE singleton = true"
        ).fetchone() == (0,)


def test_run_date_bounds_reject_non_dates(pg_owner_dsn: str) -> None:
    """Reject malformed run dates with a safe storage error."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        invalid = RunSpec(
            target=date(2025, 1, 3),
            requested_start="bad",
            requested_end="bad",
            code_version="code",
            schema_version="1",
            transform_version="1",
        )  # type: ignore[arg-type]
        with pytest.raises(PostgresWriterError, match="date range"):
            start_run(connection, invalid)
