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
    read_split_bounds,
    read_split_history,
    read_split_range,
    read_ticker_batch,
    read_ticker_reference,
)

_TWO_ROWS = 2
_SPLIT_COLUMNS = ["ticker", "execution_date", "split_from", "split_to", "adjustment_factor", "adjustment_type"]
_SPLIT_SCHEMA = {
    "ticker": pl.String,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.String,
}


def _migrate(database: object) -> None:
    with writer_connection(database.owner_dsn) as conn:
        apply_migrations(conn)


def _seed(database: object) -> tuple[int, int]:
    _migrate(database)
    with psycopg.connect(database.owner_dsn) as conn:
        first = conn.execute("INSERT INTO market.ticker (symbol) VALUES ('AAA') RETURNING ticker_id").fetchone()[0]
        second = conn.execute("INSERT INTO market.ticker (symbol) VALUES ('BBB') RETURNING ticker_id").fetchone()[0]
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 1, 2, 1, 2, 10, 3)",
            (datetime.date(2025, 1, 2), first),
        )
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 1, 2, 1, 2, 1.5, 4)",
            (datetime.date(2025, 1, 3), first),
        )
        conn.execute(
            "INSERT INTO ingest.raw_daily VALUES (%s, %s, 5, 7, 5, 6, 30, 2)",
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
        assert daily.columns == ["date", "ticker", "open", "high", "low", "close", "volume", "transactions"]
        assert daily.schema == {
            "date": pl.Date,
            "ticker": pl.String,
            "open": pl.Float32,
            "high": pl.Float32,
            "low": pl.Float32,
            "close": pl.Float32,
            "volume": pl.Float64,
            "transactions": pl.Int64,
        }
        assert daily.rows() == [
            (datetime.date(2025, 1, 2), "AAA", 1.0, 2.0, 1.0, 2.0, 10.0, 3),
            (datetime.date(2025, 1, 2), "BBB", 5.0, 7.0, 5.0, 6.0, 30.0, 2),
        ]

        history = read_raw_history(conn, [first])
        assert history.schema == daily.schema
        assert history.rows() == [
            (datetime.date(2025, 1, 2), "AAA", 1.0, 2.0, 1.0, 2.0, 10.0, 3),
            (datetime.date(2025, 1, 3), "AAA", 1.0, 2.0, 1.0, 2.0, 1.5, 4),
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


def test_split_bounds_report_none_without_stored_splits(pg_migrated_database: object) -> None:
    """Report an empty split storage as two null bounds."""
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        assert read_split_bounds(conn) == (None, None)


def test_split_bounds_report_earliest_and_latest_dates(pg_migrated_database: object) -> None:
    """Report the minimum and maximum stored execution dates."""
    _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        assert read_split_bounds(conn) == (datetime.date(2025, 1, 3), datetime.date(2025, 1, 4))


def test_split_range_treats_endpoint_dates_as_inclusive(pg_migrated_database: object) -> None:
    """Include splits stored exactly on either range endpoint."""
    _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        early = read_split_range(conn, datetime.date(2025, 1, 1), datetime.date(2025, 1, 3))
        assert early.get_column("ticker").to_list() == ["AAA"]
        late = read_split_range(conn, datetime.date(2025, 1, 4), datetime.date(2025, 1, 6))
        assert late.get_column("ticker").to_list() == ["BBB"]
        both = read_split_range(conn, datetime.date(2025, 1, 3), datetime.date(2025, 1, 4))
        assert both.get_column("ticker").to_list() == ["AAA", "BBB"]
        single = read_split_range(conn, datetime.date(2025, 1, 3), datetime.date(2025, 1, 3))
        assert single.get_column("ticker").to_list() == ["AAA"]


def test_split_range_without_stored_splits_is_typed_empty(pg_migrated_database: object) -> None:
    """Return a canonical empty frame for a window with no stored splits."""
    _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        frame = read_split_range(conn, datetime.date(2025, 3, 1), datetime.date(2025, 3, 31))
        assert frame.is_empty()
        assert frame.columns == _SPLIT_COLUMNS
        assert frame.schema == _SPLIT_SCHEMA


def test_split_range_orders_by_symbol_date_and_stored_identity(pg_migrated_database: object) -> None:
    """Order splits by symbol, execution date, then stored split identity."""
    first, second = _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        conn.execute(
            "INSERT INTO ingest.split_event (ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, %s, 3, 1, 0.333333333333)",
            (first, datetime.date(2025, 1, 3)),
        )
        conn.execute(
            "INSERT INTO ingest.split_event (ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, %s, 4, 2, 0.5)",
            (second, datetime.date(2025, 1, 3)),
        )
        conn.commit()
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        frame = read_split_range(conn, datetime.date(2025, 1, 3), datetime.date(2025, 1, 4))
        assert frame.columns == _SPLIT_COLUMNS
        assert frame.schema == _SPLIT_SCHEMA
        assert frame.rows() == [
            ("AAA", datetime.date(2025, 1, 3), 2.0, 1.0, 0.5, None),
            ("AAA", datetime.date(2025, 1, 3), 3.0, 1.0, 0.333333333333, None),
            ("BBB", datetime.date(2025, 1, 3), 4.0, 2.0, 0.5, None),
            ("BBB", datetime.date(2025, 1, 4), 3.0, 1.0, 0.333333333333, None),
        ]


def test_split_range_includes_inactive_and_unreferenced_symbols(pg_migrated_database: object) -> None:
    """Include splits for inactive or reference-less tickers without an activity filter."""
    _seed(pg_migrated_database)
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        inactive = conn.execute(
            "INSERT INTO market.ticker (symbol, active) VALUES ('CCC', false) RETURNING ticker_id"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO ingest.split_event (ticker_id, execution_date, split_from, split_to, adjustment_factor) "
            "VALUES (%s, %s, 4, 1, 0.25)",
            (inactive, datetime.date(2025, 1, 3)),
        )
        conn.commit()
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn:
        frame = read_split_range(conn, datetime.date(2025, 1, 3), datetime.date(2025, 1, 3))
        assert frame.get_column("ticker").to_list() == ["AAA", "CCC"]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (datetime.date(2025, 1, 4), datetime.date(2025, 1, 3)),
        (datetime.datetime(2025, 1, 3, 12, 0, tzinfo=datetime.UTC), datetime.date(2025, 1, 4)),
        (datetime.date(2025, 1, 3), "2025-01-04"),
    ],
)
def test_invalid_split_ranges_are_rejected(
    pg_migrated_database: object, start: datetime.date, end: datetime.date
) -> None:
    """Reject reversed ranges and non-date bounds."""
    with psycopg.connect(pg_migrated_database.etl_dsn) as conn, pytest.raises(PostgresWriterError):
        read_split_range(conn, start, end)
