"""Read-only ``info`` collection against a migrated PostgreSQL database.

These are sociable integration tests: the real migrations, schemas, and
statistics collector all run. No Massive client is needed because ``info`` only
reads persisted state. The tests seed a small known state, force the statistics
collector to refresh with ``ANALYZE``, then assert the returned ``DatabaseInfo``.
"""

from __future__ import annotations

import datetime
from uuid import uuid4

import psycopg

from tickerlake.postgres.info import CountsInfo, collect_info

_TICKERS = 1
_RUNS = 1
_PUBLICATIONS = 1
_RAW_ROWS = 3
_SPLITS = 2


def _seed(dsn: str) -> object:
    """Insert a small known state and return the published run id."""
    run_id = uuid4()
    with psycopg.connect(dsn, autocommit=True) as connection:
        ticker_id = connection.execute(
            """INSERT INTO market.ticker (symbol, name, ticker_type, primary_exchange, active)
               VALUES ('TEST', 'Test Co', 'CS', 'XNAS', true) RETURNING ticker_id"""
        ).fetchone()[0]
        connection.execute(
            """INSERT INTO ingest.run
               (run_id, target_date, requested_start, requested_end, input_revision,
                code_version, schema_version, transform_version, state,
                started_at, ended_at, published_at)
               VALUES (%s, %s, %s, %s, %s, 'code', 'schema', 'transform', 'published',
                       now(), now(), now())""",
            (
                run_id,
                datetime.date(2024, 1, 5),
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 5),
                1,
            ),
        )
        connection.execute(
            """UPDATE ingest.cache_state
               SET input_revision = 1, retained_start = %s, retained_end = %s
               WHERE singleton = true""",
            (datetime.date(2024, 1, 2), datetime.date(2024, 1, 5)),
        )
        connection.execute(
            """INSERT INTO market.publication_state
               (singleton, published_session, published_at, run_id, ticker_count)
               VALUES (true, %s, now(), %s, %s)""",
            (datetime.date(2024, 1, 5), run_id, 1),
        )
        connection.executemany(
            """INSERT INTO ingest.raw_daily
               (date, ticker_id, open, high, low, close, vwap, volume, transactions)
               VALUES (%s, %s, 10, 12, 9, 11, 10.5, 100, 5)""",
            [
                (datetime.date(2024, 1, 2), ticker_id),
                (datetime.date(2024, 1, 3), ticker_id),
                (datetime.date(2024, 1, 4), ticker_id),
            ],
        )
        connection.executemany(
            """INSERT INTO ingest.split_event
               (ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type)
               VALUES (%s, %s, 2, 1, 0.5, 'split')""",
            [
                (ticker_id, datetime.date(2024, 1, 3)),
                (ticker_id, datetime.date(2024, 1, 4)),
            ],
        )
        connection.execute("ANALYZE")
    return run_id


def test_collect_info_reports_seeded_state(pg_migrated_database) -> None:
    """A seeded database reports registered schemas, tables, publication, cache, and counts."""
    dsn = pg_migrated_database.owner_dsn
    run_id = _seed(dsn)

    result = collect_info(dsn)

    assert result.schemas == ("ingest", "market")
    assert {(table.schema, table.table_name) for table in result.tables} == {
        ("ingest", "raw_daily"),
        ("ingest", "split_event"),
        ("ingest", "ticker_reference"),
        ("ingest", "run"),
        ("ingest", "cache_state"),
        ("market", "adjusted_daily"),
        ("market", "adjusted_weekly"),
        ("market", "adjusted_monthly"),
        ("market", "ticker"),
        ("market", "latest_daily"),
        ("market", "publication_state"),
    }
    counts = {(table.schema, table.table_name): table.row_count for table in result.tables}
    assert counts[("market", "ticker")] == _TICKERS
    assert counts[("ingest", "run")] == _RUNS
    assert counts[("market", "publication_state")] == _PUBLICATIONS
    assert counts[("ingest", "raw_daily")] == _RAW_ROWS
    assert counts[("ingest", "split_event")] == _SPLITS

    assert result.publication is not None
    assert result.publication.run_id == run_id
    assert result.publication.target_session == datetime.date(2024, 1, 5)

    assert result.cache.input_revision == 1
    assert result.cache.retained_start == datetime.date(2024, 1, 2)
    assert result.cache.retained_end == datetime.date(2024, 1, 5)

    assert result.counts == CountsInfo(tickers=_TICKERS, splits=_SPLITS, raw_daily=_RAW_ROWS)


def test_collect_info_on_fresh_database_has_no_publication(pg_migrated_database) -> None:
    """A freshly migrated database has schemas and cache state but no publication."""
    result = collect_info(pg_migrated_database.owner_dsn)

    assert result.schemas == ("ingest", "market")
    assert result.publication is None
    assert result.cache.input_revision == 0
    assert result.cache.retained_start is None
    assert result.cache.retained_end is None
    assert result.counts == CountsInfo(tickers=0, splits=0, raw_daily=0)
