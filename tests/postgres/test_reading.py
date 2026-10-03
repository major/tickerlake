"""PostgreSQL canonical and bounded read behavior."""

import datetime

import polars as pl
import psycopg
import pytest

from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations
from tickerlake.postgres.reading import (
    read_raw_date,
    read_raw_history,
    read_split_history,
    read_ticker_batch,
    read_ticker_reference,
)

_TWO_ROWS = 2


def _migrate(database: object) -> None:
    with writer_connection(database.owner_dsn) as conn:
        apply_migrations(conn)


def _seed(database: object) -> tuple[int, int]:
    _migrate(database)
    with psycopg.connect(database.owner_dsn) as conn:
        first = conn.execute("INSERT INTO market.ticker (symbol) VALUES ('AAA') RETURNING ticker_id").fetchone()[0]
        second = conn.execute("INSERT INTO market.ticker (symbol) VALUES ('BBB') RETURNING ticker_id").fetchone()[0]
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 1, 2, 1, 2, NULL, 10, 3)",
            (datetime.date(2025, 1, 2), first),
        )
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 1, 2, 1, 2, 11, 1.5, 4)",
            (datetime.date(2025, 1, 3), first),
        )
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 5, 7, 5, 6, NULL, 30, 2)",
            (datetime.date(2025, 1, 2), second),
        )
        conn.execute(
            "INSERT INTO ingest.split_event (ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, %s, 2, 1, 0.5)",
            (first, datetime.date(2025, 1, 3)),
        )
        conn.execute(
            "INSERT INTO ingest.split_event (ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, %s, 3, 1, 0.333333333333)",
            (second, datetime.date(2025, 1, 4)),
        )
        conn.execute(
            "INSERT INTO ingest.ticker_reference (ticker_id, name, ticker_type) VALUES (%s, 'Alpha', 'CS')",
            (first,),
        )
        conn.execute(
            "INSERT INTO ingest.ticker_reference (ticker_id, name, ticker_type) VALUES (%s, 'Beta', 'ETF')",
            (second,),
        )
        conn.commit()
    return first, second


def test_reads_return_canonical_data_in_stable_order(pg_migrated_database: object) -> None:
    """Return daily, ticker, split, and reference frames in canonical order and types."""
    first, second = _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        page = read_ticker_batch(conn, limit=1)
        assert page.schema == {"ticker_id": pl.Int32, "symbol": pl.String}
        assert page.get_column("ticker_id").to_list() == [first]
        assert read_ticker_batch(conn, after_id=first).get_column("ticker_id").to_list() == [second]

        daily = read_raw_date(conn, datetime.date(2025, 1, 2))
        assert daily.columns == ["date", "ticker", "open", "high", "low", "close", "volume", "vwap", "transactions"]
        assert daily.schema == {
            "date": pl.Date,
            "ticker": pl.String,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float64,
            "vwap": pl.Float32,
            "transactions": pl.Int64,
        }
        assert daily.rows() == [
            (datetime.date(2025, 1, 2), "AAA", 1.0, 2.0, 1.0, 2.0, 10.0, None, 3),
            (datetime.date(2025, 1, 2), "BBB", 5.0, 7.0, 5.0, 6.0, 30.0, None, 2),
        ]

        history = read_raw_history(conn, [first])
        assert history.schema == daily.schema
        assert history.rows() == [
            (datetime.date(2025, 1, 2), "AAA", 1.0, 2.0, 1.0, 2.0, 10.0, None, 3),
            (datetime.date(2025, 1, 3), "AAA", 1.0, 2.0, 1.0, 2.0, 1.5, 11.0, 4),
        ]

        splits = read_split_history(conn, [first])
        assert splits.schema == {
            "ticker": pl.String,
            "execution_date": pl.Date,
            "split_from": pl.Float32,
            "split_to": pl.Float32,
            "adjustment_factor": pl.Float64,
            "adjustment_type": pl.String,
        }
        assert splits.rows() == [("AAA", datetime.date(2025, 1, 3), 2.0, 1.0, 0.5, None)]

        tickers = read_ticker_reference(conn, ["CS"])
        assert tickers.schema == {
            "ticker": pl.String,
            "name": pl.String,
            "type": pl.String,
            "primary_exchange": pl.String,
            "cik": pl.String,
            "active": pl.Boolean,
        }
        assert tickers.rows() == [("AAA", "Alpha", "CS", None, None, None)]


def test_explicit_empty_scopes_return_typed_empty_frames(pg_migrated_database: object) -> None:
    """Return typed empty results for explicit empty read scopes."""
    _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        assert read_raw_history(conn, []).schema == read_raw_history(conn, [1]).schema
        assert read_raw_history(conn, []).is_empty()
        assert read_split_history(conn, []).is_empty()
        assert read_ticker_reference(conn, []).is_empty()


@pytest.mark.parametrize("ids", [[0], [-1], [True], [1, 1], list(range(1, 1002))])
def test_invalid_id_batches_are_rejected(pg_migrated_database: object, ids: list[int]) -> None:
    """Reject invalid or oversized ticker ID batches."""
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn, pytest.raises(PostgresWriterError):
        read_raw_history(conn, ids)


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.5])
def test_invalid_page_limits_are_rejected(pg_migrated_database: object, limit: int) -> None:
    """Reject page limits outside the bounded page size."""
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn, pytest.raises(PostgresWriterError):
        read_ticker_batch(conn, limit=limit)
