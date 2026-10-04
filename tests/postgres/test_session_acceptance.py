"""PostgreSQL evidence that a daily raw session was accepted."""

from __future__ import annotations

import hashlib
from datetime import date
from importlib import resources
from typing import TYPE_CHECKING

import polars as pl
import pytest
from psycopg import sql

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


def _outcome(status: FetchStatus = FetchStatus.populated, day: date = DAY) -> FetchOutcome:
    rows: list[dict[str, object]] = []
    if status == FetchStatus.populated:
        rows.append(
            {
                "date": day,
                "ticker": "AAA",
                "open": 9.0,
                "high": 11.0,
                "low": 8.0,
                "close": 10.0,
                "vwap": None,
                "volume": 100.0,
                "transactions": 3,
            }
        )
    return FetchOutcome(status, pl.DataFrame(rows, schema=DAILY_AGGS_SCHEMA), day)


def _request(connection: psycopg.Connection, day: date = DAY) -> FetchRequest:
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


def test_upgrade_preserves_raw_rows_ids_and_does_not_invent_acceptance(pg_owner_dsn: str) -> None:
    """Applying publication migration over foundation data adds no false evidence."""
    with writer_connection(pg_owner_dsn) as connection:
        foundation = resources.files("tickerlake.migrations").joinpath("0001_foundation.sql").read_bytes()
        with connection.transaction():
            connection.execute("CREATE SCHEMA ingest")
            connection.execute("CREATE SCHEMA market")
            connection.execute(sql.SQL(foundation.decode("utf-8")))
            connection.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (1, %s, %s)",
                (
                    "0001_foundation.sql",
                    hashlib.sha256(foundation).hexdigest(),
                ),
            )
        ticker_id = connection.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('AAA') RETURNING ticker_id"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO ingest.raw_daily (date, ticker_id, open, high, low, close, volume, transactions) "
            "VALUES (%s, %s, 9, 11, 8, 10, 100, 3)",
            (DAY, ticker_id),
        )
        apply_migrations(connection)
        assert connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol = 'AAA'").fetchone() == (ticker_id,)
        assert connection.execute("SELECT count(*) FROM ingest.raw_daily WHERE date = %s", (DAY,)).fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM ingest.raw_session").fetchone() == (0,)


def test_populated_acceptance_links_manifest_and_updates_on_noop(pg_owner_dsn: str) -> None:
    """Every populated refresh records current manifest/revision/count evidence."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        first = _request(connection)
        assert store_daily_outcome(connection, first, _outcome()) == 1
        initial = connection.execute(
            """SELECT s.input_revision, s.row_count, m.manifest_id, m.run_id, m.status
               FROM ingest.raw_session s JOIN ingest.fetch_manifest m USING (manifest_id)
                WHERE s.date = %s""",
            (DAY,),
        ).fetchone()
        assert initial == (1, 1, initial[2], first.run_id, "populated")

        second = _request(connection)
        assert store_daily_outcome(connection, second, _outcome()) == 1
        updated = connection.execute(
            """SELECT s.input_revision, s.row_count, m.run_id, m.status
               FROM ingest.raw_session s JOIN ingest.fetch_manifest m USING (manifest_id)
               WHERE s.date = %s""",
            (DAY,),
        ).fetchone()
        assert updated == (1, 1, second.run_id, "populated")
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (2,)


@pytest.mark.parametrize("status", [FetchStatus.failed, FetchStatus.quarantined, FetchStatus.successful_empty])
def test_nonpopulated_outcomes_preserve_evidence_and_cannot_create_it(pg_owner_dsn: str, status: FetchStatus) -> None:
    """Only validated populated results establish raw-session acceptance."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        store_daily_outcome(connection, _request(connection), _outcome())
        before = connection.execute(
            "SELECT date, input_revision, manifest_id, row_count FROM ingest.raw_session"
        ).fetchall()
        store_daily_outcome(connection, _request(connection), _outcome(status))
        assert (
            connection.execute("SELECT date, input_revision, manifest_id, row_count FROM ingest.raw_session").fetchall()
            == before
        )
        assert read_cache_state(connection).input_revision == 1

    with writer_connection(pg_owner_dsn) as connection:
        next_day = date(2025, 1, 3)
        fresh_request = _request(connection, next_day)
        store_daily_outcome(connection, fresh_request, _outcome(status, next_day))
        assert connection.execute("SELECT date FROM ingest.raw_session WHERE date = %s", (next_day,)).fetchall() == []


def test_manifest_cache_and_acceptance_rollback_together(pg_owner_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """A storage failure after acceptance work commits none of the outcome."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        request = _request(connection)
        original_record = raw.record_fetch_outcome

        def record_then_fail(conn: psycopg.Connection, req: FetchRequest, outcome: FetchOutcome) -> int:
            original_record(conn, req, outcome)
            raise PostgresWriterError

        monkeypatch.setattr(raw, "record_fetch_outcome", record_then_fail)
        with pytest.raises(PostgresWriterError):
            store_daily_outcome(connection, request, _outcome())
        assert connection.execute("SELECT count(*) FROM ingest.raw_daily").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM ingest.raw_session").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (0,)
        assert read_cache_state(connection).input_revision == 0
