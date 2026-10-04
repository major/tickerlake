"""Bounded, canonical reads from PostgreSQL ingest relations."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, LiteralString, NoReturn

import polars as pl
import psycopg

from tickerlake.extract import DAILY_AGGS_SCHEMA, SPLITS_SCHEMA, TICKERS_SCHEMA
from tickerlake.postgres.connection import PostgresWriterError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

_MAX_BATCH = 1000
_MAX_REFERENCE_TYPES = 20
_DAILY_COLUMNS = ("date", "ticker", "open", "high", "low", "close", "volume", "vwap", "transactions")
_SPLIT_COLUMNS = ("ticker", "execution_date", "split_from", "split_to", "adjustment_factor", "adjustment_type")
type FrameSchemaValue = type[pl.DataType] | pl.DataType


def _fail(message: str) -> NoReturn:
    raise PostgresWriterError(message) from None


def _empty(schema: Mapping[str, FrameSchemaValue], columns: tuple[str, ...]) -> pl.DataFrame:
    return pl.DataFrame(schema={name: schema[name] for name in columns})


def _query(
    connection: psycopg.Connection, statement: LiteralString, params: tuple[object, ...]
) -> list[tuple[object, ...]]:
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement, params)
            return cursor.fetchall()
    except psycopg.Error:
        _fail("Could not read PostgreSQL market data")


def _frame(
    rows: list[tuple[object, ...]], schema: Mapping[str, FrameSchemaValue], columns: tuple[str, ...]
) -> pl.DataFrame:
    if not rows:
        return _empty(schema, columns)
    return pl.DataFrame(rows, schema=[(name, schema[name]) for name in columns], orient="row")


def _valid_ids(ticker_ids: Sequence[int]) -> list[int]:
    if isinstance(ticker_ids, (str, bytes)) or len(ticker_ids) > _MAX_BATCH:
        _fail("Ticker ID batch must contain at most 1000 IDs")
    ids = list(ticker_ids)
    if any(type(value) is not int or value <= 0 for value in ids) or len(set(ids)) != len(ids):
        _fail("Ticker IDs must be unique positive integers")
    return ids


def read_raw_date(connection: psycopg.Connection, date: datetime.date) -> pl.DataFrame:
    """Read one date of canonical daily aggregates."""
    if not isinstance(date, datetime.date) or isinstance(date, datetime.datetime):
        _fail("Daily date must be a date")
    rows = _query(
        connection,
        """SELECT r.date, t.symbol, r.open, r.high, r.low, r.close, r.volume, r.vwap, r.transactions
           FROM ingest.raw_daily AS r JOIN market.ticker AS t USING (ticker_id)
           WHERE r.date = %s ORDER BY t.symbol""",
        (date,),
    )
    return _frame(rows, DAILY_AGGS_SCHEMA, _DAILY_COLUMNS)


def read_latest_raw_dates(connection: psycopg.Connection, target: datetime.date) -> list[datetime.date]:
    """Read at most the five latest distinct raw dates no later than target."""
    if not isinstance(target, datetime.date) or isinstance(target, datetime.datetime):
        _fail("Daily target must be a date")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT date FROM ingest.raw_daily WHERE date <= %s ORDER BY date DESC LIMIT 5",
                (target,),
            )
            rows = cursor.fetchall()
    except psycopg.Error:
        _fail("Could not read PostgreSQL market data")
    dates = [row[0] for row in rows]
    if any(not isinstance(value, datetime.date) or isinstance(value, datetime.datetime) for value in dates):
        _fail("Stored daily dates must be dates")
    return dates


def read_ticker_batch(connection: psycopg.Connection, after_id: int = 0, limit: int = 1000) -> pl.DataFrame:
    """Read a bounded keyset page of public ticker identities."""
    if type(after_id) is not int or after_id < 0:
        _fail("Ticker cursor must be a nonnegative integer")
    if type(limit) is not int or not 1 <= limit <= _MAX_BATCH:
        _fail("Ticker page limit must be between 1 and 1000")
    schema = {"ticker_id": pl.Int32, "symbol": pl.String}
    rows = _query(
        connection,
        "SELECT ticker_id, symbol FROM market.ticker WHERE ticker_id > %s ORDER BY ticker_id LIMIT %s",
        (after_id, limit),
    )
    return _frame(rows, schema, ("ticker_id", "symbol"))


def read_raw_history(connection: psycopg.Connection, ticker_ids: Sequence[int]) -> pl.DataFrame:
    """Read complete daily history for an explicit bounded ticker ID set."""
    ids = _valid_ids(ticker_ids)
    if not ids:
        return _empty(DAILY_AGGS_SCHEMA, _DAILY_COLUMNS)
    rows = _query(
        connection,
        """SELECT r.date, t.symbol, r.open, r.high, r.low, r.close, r.volume, r.vwap, r.transactions
           FROM ingest.raw_daily AS r JOIN market.ticker AS t USING (ticker_id)
           WHERE r.ticker_id = ANY(%s) ORDER BY t.symbol, r.date""",
        (ids,),
    )
    return _frame(rows, DAILY_AGGS_SCHEMA, _DAILY_COLUMNS)


def read_split_history(connection: psycopg.Connection, ticker_ids: Sequence[int]) -> pl.DataFrame:
    """Read complete split history for an explicit bounded ticker ID set."""
    ids = _valid_ids(ticker_ids)
    if not ids:
        return _empty(SPLITS_SCHEMA, _SPLIT_COLUMNS)
    rows = _query(
        connection,
        """SELECT t.symbol, s.execution_date, s.split_from, s.split_to,
                  s.adjustment_factor, s.adjustment_type
           FROM ingest.split_event AS s JOIN market.ticker AS t USING (ticker_id)
           WHERE s.ticker_id = ANY(%s) ORDER BY t.symbol, s.execution_date, s.split_id""",
        (ids,),
    )
    return _frame(rows, SPLITS_SCHEMA, _SPLIT_COLUMNS)


def read_split_range(
    connection: psycopg.Connection, start_date: datetime.date, end_date: datetime.date
) -> pl.DataFrame:
    """Read splits whose execution date falls within an inclusive date range."""
    for value in (start_date, end_date):
        if not isinstance(value, datetime.date) or isinstance(value, datetime.datetime):
            _fail("Split range bounds must be dates")
    if start_date > end_date:
        _fail("Split range start must not be after end")
    rows = _query(
        connection,
        """SELECT t.symbol, s.execution_date, s.split_from, s.split_to,
                  s.adjustment_factor, s.adjustment_type
           FROM ingest.split_event AS s JOIN market.ticker AS t USING (ticker_id)
           WHERE s.execution_date BETWEEN %s AND %s
           ORDER BY t.symbol, s.execution_date, s.split_id""",
        (start_date, end_date),
    )
    return _frame(rows, SPLITS_SCHEMA, _SPLIT_COLUMNS)


def read_split_bounds(connection: psycopg.Connection) -> tuple[datetime.date | None, datetime.date | None]:
    """Return the earliest and latest stored split execution dates."""
    rows = _query(connection, "SELECT min(execution_date), max(execution_date) FROM ingest.split_event", ())
    earliest = rows[0][0]
    latest = rows[0][1]
    if earliest is not None and (not isinstance(earliest, datetime.date) or isinstance(earliest, datetime.datetime)):
        _fail("Stored split bounds must be dates")
    if latest is not None and (not isinstance(latest, datetime.date) or isinstance(latest, datetime.datetime)):
        _fail("Stored split bounds must be dates")
    return earliest, latest


def read_ticker_reference(connection: psycopg.Connection, ticker_types: Sequence[str]) -> pl.DataFrame:
    """Read references limited to a small explicit set of ticker types."""
    if isinstance(ticker_types, (str, bytes)) or len(ticker_types) > _MAX_REFERENCE_TYPES:
        _fail("Ticker type scope must contain at most 20 types")
    types = list(ticker_types)
    if any(not isinstance(value, str) or not value for value in types) or len(set(types)) != len(types):
        _fail("Ticker type scope must contain unique nonempty strings")
    if not types:
        return _empty(TICKERS_SCHEMA, ("ticker", "name", "type", "primary_exchange", "cik", "active"))
    rows = _query(
        connection,
        """SELECT t.symbol, r.name, r.ticker_type, r.primary_exchange, r.cik, r.active
           FROM ingest.ticker_reference AS r JOIN market.ticker AS t USING (ticker_id)
           WHERE r.ticker_type = ANY(%s) ORDER BY t.symbol, r.ticker_type""",
        (types,),
    )
    return _frame(rows, TICKERS_SCHEMA, ("ticker", "name", "type", "primary_exchange", "cik", "active"))
