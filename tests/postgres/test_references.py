"""Transactional reference storage integration tests."""

from datetime import date

import polars as pl
import psycopg
import pytest

from tickerlake.extract import SPLITS_SCHEMA, TICKERS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres import references
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.references import store_split_outcome, store_ticker_outcome
from tickerlake.postgres.state import read_cache_state, start_run

_REVISION_TWO = 2
_REVISION_THREE = 3


def _run(database: object) -> tuple[psycopg.Connection, object, object]:
    with writer_connection(database.owner_dsn) as conn:
        apply_migrations(conn)
    manager = writer_connection(database.etl_dsn)
    connection = manager.__enter__()
    run_id = start_run(
        connection,
        RunSpec(
            target=date(2025, 1, 10),
            requested_start=date(2025, 1, 1),
            requested_end=date(2025, 1, 10),
            version="test",
        ),
    )
    return connection, run_id, manager


def _ticker_frame(rows: list[tuple[object, ...]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=TICKERS_SCHEMA, orient="row")


def _split_frame(rows: list[tuple[object, ...]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=SPLITS_SCHEMA, orient="row")


def _outcome(status: FetchStatus, frame: pl.DataFrame) -> FetchOutcome:
    """Construct an outcome directly; upstream fetch approval and quarantine are out of scope."""
    return FetchOutcome(status=status, frame=frame)


def test_ticker_scope_noop_and_change_revision(pg_migrated_database: object) -> None:
    """Return the current revision for changed and identical ticker snapshots."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        initial = _ticker_frame([("AAA", "Alpha", "CS", None, "123", True)])
        assert store_ticker_outcome(conn, request, _outcome(FetchStatus.populated, initial)) == 1
        state = read_cache_state(conn)
        assert state.input_revision == 1
        assert state.retained_start is None
        assert state.retained_end is None
        identity = conn.execute("SELECT ticker_id, name, active FROM market.ticker WHERE symbol='AAA'").fetchone()
        assert identity[1:] == (None, None)
        assert store_ticker_outcome(conn, request, _outcome(FetchStatus.populated, initial)) == 1
        assert read_cache_state(conn).input_revision == 1

        revised = _ticker_frame([("AAA", "Alpha II", "CS", "XNAS", "123", True)])
        assert store_ticker_outcome(conn, request, _outcome(FetchStatus.populated, revised)) == _REVISION_TWO
        assert read_cache_state(conn).input_revision == _REVISION_TWO
        assert conn.execute("SELECT name, primary_exchange FROM ingest.ticker_reference").fetchone() == (
            "Alpha II",
            "XNAS",
        )
        assert conn.execute("SELECT name, active FROM market.ticker WHERE symbol='AAA'").fetchone() == (None, None)
    finally:
        manager.__exit__(None, None, None)


def test_ticker_scope_preserves_other_types_and_requires_union_for_type_change(pg_migrated_database: object) -> None:
    """Replace only requested types and reject a private type change outside scope."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        all_types = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS", "ETF"))
        assert (
            store_ticker_outcome(
                conn,
                all_types,
                _outcome(
                    FetchStatus.populated,
                    _ticker_frame([("AAA", "Alpha", "CS", None, None, None), ("BBB", "Beta", "ETF", None, None, None)]),
                ),
            )
            == 1
        )
        cs = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        assert (
            store_ticker_outcome(
                conn,
                cs,
                _outcome(
                    FetchStatus.populated,
                    _ticker_frame([("AAA", "Alpha updated", "CS", None, None, None)]),
                ),
            )
            == _REVISION_TWO
        )
        assert conn.execute(
            "SELECT market.ticker.symbol, ingest.ticker_reference.ticker_type "
            "FROM market.ticker JOIN ingest.ticker_reference USING(ticker_id) "
            "ORDER BY market.ticker.symbol"
        ).fetchall() == [("AAA", "CS"), ("BBB", "ETF")]
        with pytest.raises(PostgresWriterError, match="union scope"):
            store_ticker_outcome(
                conn,
                cs,
                _outcome(
                    FetchStatus.populated,
                    _ticker_frame([("BBB", "Beta", "CS", None, None, None)]),
                ),
            )
        assert conn.execute(
            "SELECT market.ticker.symbol, ingest.ticker_reference.ticker_type "
            "FROM market.ticker JOIN ingest.ticker_reference USING(ticker_id) "
            "ORDER BY market.ticker.symbol"
        ).fetchall() == [("AAA", "CS"), ("BBB", "ETF")]
    finally:
        manager.__exit__(None, None, None)


def test_split_scope_preserves_other_dates_and_uses_full_event_identity(pg_migrated_database: object) -> None:
    """Replace only requested split dates while preserving distinct full event identities."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        window = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 2),
            requested_end=date(2025, 1, 3),
        )
        frame = _split_frame(
            [
                ("AAA", date(2025, 1, 2), 2.0, 1.0, 0.5, None),
                ("AAA", date(2025, 1, 2), 3.0, 1.0, 1 / 3, "forward"),
            ]
        )
        assert store_split_outcome(conn, window, _outcome(FetchStatus.populated, frame)) == 1
        assert read_cache_state(conn).input_revision == 1
        assert store_split_outcome(conn, window, _outcome(FetchStatus.populated, frame)) == 1
        assert read_cache_state(conn).input_revision == 1
        outside = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 4),
            requested_end=date(2025, 1, 4),
        )
        assert (
            store_split_outcome(
                conn,
                outside,
                _outcome(
                    FetchStatus.populated,
                    _split_frame([("BBB", date(2025, 1, 4), 2.0, 1.0, 0.5, None)]),
                ),
            )
            == _REVISION_TWO
        )
        assert conn.execute("SELECT count(*) FROM ingest.split_event").fetchone() == (3,)
        assert read_cache_state(conn).input_revision == _REVISION_TWO
    finally:
        manager.__exit__(None, None, None)


def test_populated_ticker_replacement_removes_omitted_private_member_only(pg_migrated_database: object) -> None:
    """Remove omitted members from a populated type without changing public identities or other types."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        ticker_ids: dict[str, int] = {}
        for symbol, ticker_type in (("AAA", "CS"), ("BBB", "CS"), ("CCC", "ETF")):
            ticker_ids[symbol] = conn.execute(
                "INSERT INTO market.ticker "
                "(symbol, name, ticker_type, primary_exchange, cik, active) "
                "VALUES (%s, %s, %s, 'XNAS', 'public-cik', true) RETURNING ticker_id",
                (symbol, f"Public {symbol}", ticker_type),
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO ingest.raw_daily "
                "(date, ticker_id, open, high, low, close, volume) "
                "VALUES ('2025-01-03', %s, 1, 2, 1, 2, 10)",
                (ticker_ids[symbol],),
            )

        cs_request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        etf_request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("ETF",))
        assert (
            store_ticker_outcome(
                conn,
                cs_request,
                _outcome(
                    FetchStatus.populated,
                    _ticker_frame(
                        [
                            ("AAA", "Private Alpha", "CS", "ARCX", "private-a", False),
                            ("BBB", "Private Beta", "CS", "XNYS", "private-b", True),
                        ]
                    ),
                ),
            )
            == 1
        )
        assert (
            store_ticker_outcome(
                conn,
                etf_request,
                _outcome(FetchStatus.populated, _ticker_frame([("CCC", "Private Gamma", "ETF", None, None, None)])),
            )
            == _REVISION_TWO
        )

        before_public = conn.execute(
            "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
            "FROM market.ticker ORDER BY ticker_id"
        ).fetchall()
        before_bars = conn.execute(
            "SELECT date, ticker_id, open, high, low, close, volume FROM ingest.raw_daily ORDER BY date, ticker_id"
        ).fetchall()
        before_references = conn.execute(
            "SELECT ticker_id, name, ticker_type, primary_exchange, cik, active "
            "FROM ingest.ticker_reference ORDER BY ticker_id"
        ).fetchall()
        before_state = read_cache_state(conn)

        assert (
            store_ticker_outcome(
                conn,
                cs_request,
                _outcome(
                    FetchStatus.populated, _ticker_frame([("AAA", "Private Alpha", "CS", "ARCX", "private-a", False)])
                ),
            )
            == _REVISION_THREE
        )
        assert read_cache_state(conn).input_revision == _REVISION_THREE
        assert read_cache_state(conn).retained_start == before_state.retained_start
        assert read_cache_state(conn).retained_end == before_state.retained_end
        assert (
            conn.execute(
                "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
                "FROM market.ticker ORDER BY ticker_id"
            ).fetchall()
            == before_public
        )
        assert (
            conn.execute(
                "SELECT date, ticker_id, open, high, low, close, volume FROM ingest.raw_daily ORDER BY date, ticker_id"
            ).fetchall()
            == before_bars
        )
        assert conn.execute(
            "SELECT ticker_id, name, ticker_type, primary_exchange, cik, active "
            "FROM ingest.ticker_reference ORDER BY ticker_id"
        ).fetchall() == [before_references[0], before_references[2]]
        assert conn.execute(
            "SELECT market.ticker.symbol, ingest.ticker_reference.ticker_type "
            "FROM market.ticker JOIN ingest.ticker_reference USING (ticker_id) ORDER BY market.ticker.symbol"
        ).fetchall() == [("AAA", "CS"), ("CCC", "ETF")]
        assert (
            conn.execute("SELECT source, status FROM ingest.fetch_manifest ORDER BY manifest_id").fetchall()
            == [("tickers", "populated")] * 3
        )
    finally:
        manager.__exit__(None, None, None)


def test_populated_split_replacement_removes_omitted_window_events_only(pg_migrated_database: object) -> None:
    """Replace a nonempty split window while retaining event IDs and contents outside it."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        ticker_ids = {}
        for symbol in ("AAA", "BBB"):
            ticker_ids[symbol] = conn.execute(
                "INSERT INTO market.ticker "
                "(symbol, name, ticker_type, primary_exchange, cik, active) "
                "VALUES (%s, %s, 'CS', 'XNAS', 'public-cik', true) RETURNING ticker_id",
                (symbol, f"Public {symbol}"),
            ).fetchone()[0]
        conn.execute(
            "INSERT INTO ingest.split_event "
            "(ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, '2025-01-01', 2, 1, 0.5), (%s, '2025-01-05', 3, 1, 0.333333333333)",
            (ticker_ids["AAA"], ticker_ids["BBB"]),
        )
        window = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 2),
            requested_end=date(2025, 1, 4),
        )
        old_window = _split_frame(
            [
                ("AAA", date(2025, 1, 2), 2.0, 1.0, 0.5, None),
                ("BBB", date(2025, 1, 3), 3.0, 1.0, 1 / 3, None),
            ]
        )
        assert store_split_outcome(conn, window, _outcome(FetchStatus.populated, old_window)) == 1
        before_public = conn.execute(
            "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
            "FROM market.ticker ORDER BY ticker_id"
        ).fetchall()
        before_outside = conn.execute(
            "SELECT e.split_id, t.symbol, e.execution_date, e.split_from, e.split_to, "
            "e.adjustment_factor, e.adjustment_type "
            "FROM ingest.split_event e JOIN market.ticker t USING (ticker_id) "
            "WHERE e.execution_date NOT BETWEEN '2025-01-02' AND '2025-01-04' "
            "ORDER BY e.split_id"
        ).fetchall()

        replacement = _split_frame(
            [
                ("AAA", date(2025, 1, 2), 4.0, 1.0, 0.25, None),
                ("AAA", date(2025, 1, 4), 2.0, 1.0, 0.5, "forward"),
            ]
        )
        assert store_split_outcome(conn, window, _outcome(FetchStatus.populated, replacement)) == _REVISION_TWO
        assert read_cache_state(conn).input_revision == _REVISION_TWO
        assert (
            conn.execute(
                "SELECT e.split_id, t.symbol, e.execution_date, e.split_from, e.split_to, "
                "e.adjustment_factor, e.adjustment_type "
                "FROM ingest.split_event e JOIN market.ticker t USING (ticker_id) "
                "WHERE e.execution_date NOT BETWEEN '2025-01-02' AND '2025-01-04' "
                "ORDER BY e.split_id"
            ).fetchall()
            == before_outside
        )
        assert conn.execute(
            "SELECT t.symbol, e.execution_date, e.split_from, e.split_to, e.adjustment_factor, e.adjustment_type "
            "FROM ingest.split_event e JOIN market.ticker t USING (ticker_id) "
            "WHERE e.execution_date BETWEEN '2025-01-02' AND '2025-01-04' "
            "ORDER BY t.symbol, e.execution_date"
        ).fetchall() == [
            ("AAA", date(2025, 1, 2), 4.0, 1.0, 0.25, None),
            ("AAA", date(2025, 1, 4), 2.0, 1.0, 0.5, "forward"),
        ]
        assert (
            conn.execute(
                "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
                "FROM market.ticker ORDER BY ticker_id"
            ).fetchall()
            == before_public
        )
        assert conn.execute("SELECT source, status FROM ingest.fetch_manifest ORDER BY manifest_id").fetchall() == [
            ("splits", "populated"),
            ("splits", "populated"),
        ]
    finally:
        manager.__exit__(None, None, None)


@pytest.mark.parametrize("status", [FetchStatus.failed, FetchStatus.quarantined, FetchStatus.successful_empty])
def test_nonpopulated_outcomes_keep_references_and_revision(pg_migrated_database: object, status: FetchStatus) -> None:
    """Preserve existing public and private references for non-populated outcomes."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        ticker_request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        split_request = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 1),
            requested_end=date(2025, 1, 10),
        )
        assert (
            store_ticker_outcome(
                conn,
                ticker_request,
                _outcome(FetchStatus.populated, _ticker_frame([("AAA", "Alpha", "CS", None, None, True)])),
            )
            == 1
        )
        assert (
            store_split_outcome(
                conn,
                split_request,
                _outcome(
                    FetchStatus.populated,
                    _split_frame([("AAA", date(2025, 1, 3), 2.0, 1.0, 0.5, None)]),
                ),
            )
            == _REVISION_TWO
        )
        conn.execute(
            "UPDATE market.ticker SET name='Public name', ticker_type='CS', primary_exchange='XNAS', "
            "cik='public-cik', active=true WHERE symbol='AAA'"
        )
        before_public = conn.execute(
            "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
            "FROM market.ticker ORDER BY ticker_id"
        ).fetchall()
        before_tickers = conn.execute(
            "SELECT ticker_id, name, ticker_type, primary_exchange, cik, active "
            "FROM ingest.ticker_reference ORDER BY ticker_id"
        ).fetchall()
        before_splits = conn.execute(
            "SELECT split_id, ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type "
            "FROM ingest.split_event ORDER BY split_id"
        ).fetchall()
        before_state = read_cache_state(conn)

        assert (
            store_ticker_outcome(
                conn,
                ticker_request,
                _outcome(status, pl.DataFrame(schema=TICKERS_SCHEMA)),
            )
            == _REVISION_TWO
        )
        assert (
            store_split_outcome(
                conn,
                split_request,
                _outcome(status, pl.DataFrame(schema=SPLITS_SCHEMA)),
            )
            == _REVISION_TWO
        )
        assert read_cache_state(conn).input_revision == _REVISION_TWO
        assert read_cache_state(conn) == before_state
        assert (
            conn.execute(
                "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
                "FROM market.ticker ORDER BY ticker_id"
            ).fetchall()
            == before_public
        )
        assert (
            conn.execute(
                "SELECT ticker_id, name, ticker_type, primary_exchange, cik, active "
                "FROM ingest.ticker_reference ORDER BY ticker_id"
            ).fetchall()
            == before_tickers
        )
        assert (
            conn.execute(
                "SELECT split_id, ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type "
                "FROM ingest.split_event ORDER BY split_id"
            ).fetchall()
            == before_splits
        )
        assert conn.execute("SELECT status FROM ingest.fetch_manifest ORDER BY manifest_id").fetchall() == [
            ("populated",),
            ("populated",),
            (status.value,),
            (status.value,),
        ]
    finally:
        manager.__exit__(None, None, None)


@pytest.mark.parametrize(
    ("source", "corruption"),
    [
        ("tickers", "type"),
        ("tickers", "duplicate"),
        ("tickers", "count"),
        ("splits", "date"),
        ("splits", "duplicate"),
        ("splits", "count"),
    ],
)
def test_corrupt_staged_rows_roll_back_all_reference_effects(
    pg_migrated_database: object,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    corruption: str,
) -> None:
    """Reject post-COPY stage corruption without changing stored state or evidence."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        ticker_request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        split_request = FetchRequest(
            run_id=run_id,
            source="splits",
            requested_start=date(2025, 1, 1),
            requested_end=date(2025, 1, 10),
        )
        store_ticker_outcome(
            conn,
            ticker_request,
            _outcome(FetchStatus.populated, _ticker_frame([("AAA", "Alpha", "CS", None, None, True)])),
        )
        store_split_outcome(
            conn,
            split_request,
            _outcome(
                FetchStatus.populated,
                _split_frame([("AAA", date(2025, 1, 3), 4.0, 1.0, 0.25, None)]),
            ),
        )
        conn.execute(
            "UPDATE market.ticker SET name='Public name', ticker_type='CS', primary_exchange='XNAS', "
            "cik='public-cik', active=true WHERE symbol='AAA'"
        )

        def snapshot() -> tuple[object, ...]:
            return (
                conn.execute(
                    "SELECT ticker_id, symbol, name, ticker_type, primary_exchange, cik, active "
                    "FROM market.ticker ORDER BY ticker_id"
                ).fetchall(),
                conn.execute(
                    "SELECT ticker_id, name, ticker_type, primary_exchange, cik, active "
                    "FROM ingest.ticker_reference ORDER BY ticker_id"
                ).fetchall(),
                conn.execute(
                    "SELECT split_id, ticker_id, execution_date, split_from, split_to, adjustment_factor, "
                    "adjustment_type "
                    "FROM ingest.split_event ORDER BY split_id"
                ).fetchall(),
                read_cache_state(conn),
                conn.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone(),
            )

        before = snapshot()
        sql_by_case = {
            ("tickers", "type"): "UPDATE pg_temp.ticker_stage SET ticker_type='ETF'",
            ("tickers", "duplicate"): "UPDATE pg_temp.ticker_stage SET ticker='AAA' WHERE ticker='BBB'",
            ("tickers", "count"): "DELETE FROM pg_temp.ticker_stage WHERE ticker='BBB'",
            ("splits", "date"): "UPDATE pg_temp.split_stage SET execution_date='2025-01-11'",
            ("splits", "duplicate"): "UPDATE pg_temp.split_stage SET split_from=2, split_to=1, "
            "adjustment_factor=0.5, adjustment_type=NULL WHERE split_from=3",
            ("splits", "count"): "DELETE FROM pg_temp.split_stage WHERE split_from=3",
        }
        actual_copy = references.copy_frame

        def corrupt_after_copy(
            connection: psycopg.Connection,
            stage_name: str,
            frame: pl.DataFrame,
            columns: tuple[str, ...],
        ) -> None:
            actual_copy(connection, stage_name, frame, columns)
            connection.execute(sql_by_case[(source, corruption)])

        monkeypatch.setattr(references, "copy_frame", corrupt_after_copy)
        if source == "tickers":
            rows = [("AAA", "Alpha updated", "CS", None, None, True)]
            if corruption in {"duplicate", "count"}:
                rows.append(("BBB", "Beta", "CS", None, None, True))
            with pytest.raises(PostgresWriterError, match="staged PostgreSQL ticker"):
                store_ticker_outcome(
                    conn,
                    ticker_request,
                    _outcome(FetchStatus.populated, _ticker_frame(rows)),
                )
        else:
            rows = [("AAA", date(2025, 1, 4), 2.0, 1.0, 0.5, None)]
            if corruption in {"duplicate", "count"}:
                rows.append(("AAA", date(2025, 1, 4), 3.0, 1.0, 1 / 3, None))
            with pytest.raises(PostgresWriterError, match="staged PostgreSQL split"):
                store_split_outcome(
                    conn,
                    split_request,
                    _outcome(FetchStatus.populated, _split_frame(rows)),
                )
        assert snapshot() == before
    finally:
        manager.__exit__(None, None, None)


def test_invalid_frame_and_sql_failure_roll_back_everything(
    pg_migrated_database: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reject invalid frames and roll back changes after an injected database failure."""
    conn, run_id, manager = _run(pg_migrated_database)
    try:
        request = FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS",))
        out_of_scope = _ticker_frame([("AAA", "Alpha", "ETF", None, None, None)])
        with pytest.raises(PostgresWriterError):
            store_ticker_outcome(conn, request, _outcome(FetchStatus.populated, out_of_scope))
        with pytest.raises(PostgresWriterError):
            store_ticker_outcome(conn, request, _outcome(FetchStatus.populated, pl.DataFrame({"ticker": ["AAA"]})))
        advance_revision = references.advance_cache_revision

        def fail_after_revision(connection: psycopg.Connection) -> int:
            advance_revision(connection)
            raise psycopg.OperationalError("token=secret")

        monkeypatch.setattr(references, "advance_cache_revision", fail_after_revision)
        with pytest.raises(PostgresWriterError, match="store PostgreSQL reference") as error:
            store_ticker_outcome(
                conn,
                request,
                _outcome(
                    FetchStatus.populated,
                    _ticker_frame([("AAA", "Alpha", "CS", None, None, True)]),
                ),
            )
        assert error.value.__cause__ is None
        assert read_cache_state(conn).input_revision == 0
        assert conn.execute("SELECT count(*) FROM market.ticker").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM ingest.fetch_manifest").fetchone() == (0,)
    finally:
        manager.__exit__(None, None, None)
