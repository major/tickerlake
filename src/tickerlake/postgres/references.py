"""Transactional storage for fetched ticker and split reference data."""

from __future__ import annotations

import datetime
import math
from typing import TYPE_CHECKING, NoReturn

import polars as pl
import psycopg
from psycopg import sql

from tickerlake.extract import SPLITS_SCHEMA, TICKERS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres._validation import is_date, require_unique_nonempty_strings
from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection
from tickerlake.postgres.copying import copy_frame
from tickerlake.postgres.state import advance_cache_revision, read_cache_state, record_fetch_outcome

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tickerlake.postgres.models import FetchRequest

_TICKER_COLUMNS = ("ticker", "name", "type", "primary_exchange", "cik", "active")
_SPLIT_COLUMNS = ("ticker", "execution_date", "split_from", "split_to", "adjustment_factor", "adjustment_type")
_TICKER_STAGE_COLUMNS = ("ticker", "name", "ticker_type", "primary_exchange", "cik", "active")
_SPLIT_STAGE_COLUMNS = _SPLIT_COLUMNS
type FrameSchemaValue = type[pl.DataType] | pl.DataType
_STAGE_CREATE: dict[str, sql.SQL] = {
    "ticker_stage": sql.SQL(
        "CREATE TEMP TABLE ticker_stage (ticker text NOT NULL, name text, ticker_type text NOT NULL, "
        "primary_exchange text, cik text, active boolean) ON COMMIT DROP"
    ),
    "split_stage": sql.SQL(
        "CREATE TEMP TABLE split_stage (ticker text NOT NULL, execution_date date NOT NULL, "
        "split_from real NOT NULL, split_to real NOT NULL, adjustment_factor double precision NOT NULL, "
        "adjustment_type text) ON COMMIT DROP"
    ),
}
_STAGE_DROP: dict[str, sql.SQL | sql.Composed] = {
    name: sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(sql.Identifier(name)) for name in _STAGE_CREATE
}


def _fail(message: str) -> NoReturn:
    raise PostgresWriterError(message) from None


def _canonical_frame(frame: pl.DataFrame, schema: Mapping[str, FrameSchemaValue], columns: tuple[str, ...]) -> None:
    if not isinstance(frame, pl.DataFrame) or tuple(frame.columns) != columns:
        _fail("Invalid canonical PostgreSQL reference frame")
    if frame.schema != {name: schema[name] for name in columns}:
        _fail("Invalid canonical PostgreSQL reference frame")


def _ticker_rows(frame: pl.DataFrame, request: FetchRequest) -> None:
    _canonical_frame(frame, TICKERS_SCHEMA, _TICKER_COLUMNS)
    if frame.is_empty():
        _fail("Populated ticker outcome must not be empty")
    scope = request.ticker_types
    seen: set[str] = set()
    for ticker, name, ticker_type, exchange, cik, active in frame.iter_rows():
        if not isinstance(ticker, str) or not ticker or not ticker.strip() or ticker in seen:
            _fail("Invalid canonical PostgreSQL ticker identity")
        if not isinstance(ticker_type, str) or ticker_type not in scope:
            _fail("Ticker type is outside the requested scope")
        if any(value is not None and not isinstance(value, str) for value in (name, exchange, cik)):
            _fail("Invalid canonical PostgreSQL ticker metadata")
        if active is not None and not isinstance(active, bool):
            _fail("Invalid canonical PostgreSQL ticker metadata")
        seen.add(ticker)


def _split_rows(frame: pl.DataFrame, request: FetchRequest) -> None:
    _canonical_frame(frame, SPLITS_SCHEMA, _SPLIT_COLUMNS)
    if frame.is_empty():
        _fail("Populated split outcome must not be empty")
    seen: set[tuple[object, ...]] = set()
    for ticker, execution_date, split_from, split_to, factor, adjustment_type in frame.iter_rows():
        if not isinstance(ticker, str) or not ticker or not ticker.strip():
            _fail("Invalid canonical PostgreSQL split ticker")
        if not isinstance(execution_date, datetime.date) or isinstance(execution_date, datetime.datetime):
            _fail("Invalid canonical PostgreSQL split date")
        if not request.requested_start <= execution_date <= request.requested_end:
            _fail("Split date is outside the requested scope")
        if not all(
            isinstance(value, (int, float)) and math.isfinite(value) and value > 0
            for value in (split_from, split_to, factor)
        ):
            _fail("Invalid canonical PostgreSQL split ratio")
        if adjustment_type is not None and not isinstance(adjustment_type, str):
            _fail("Invalid canonical PostgreSQL adjustment type")
        identity = (ticker, execution_date, split_from, split_to, factor, adjustment_type)
        if identity in seen:
            _fail("Duplicate canonical PostgreSQL split event")
        seen.add(identity)


def _stage(connection: psycopg.Connection, name: str) -> None:
    if name not in _STAGE_CREATE:
        _fail("Unsupported PostgreSQL reference stage")
    connection.execute(_STAGE_DROP[name])
    connection.execute(_STAGE_CREATE[name])


def _validate_ticker_stage(connection: psycopg.Connection, request: FetchRequest, frame: pl.DataFrame) -> None:
    types = list(request.ticker_types)
    count = connection.execute("SELECT count(*) FROM pg_temp.ticker_stage").fetchone()
    invalid = connection.execute(
        """SELECT 1 FROM pg_temp.ticker_stage
           WHERE ticker IS NULL OR ticker !~ '[^[:space:]]' OR ticker_type IS NULL
              OR NOT (ticker_type = ANY(%s))
           UNION ALL
           SELECT 1 FROM pg_temp.ticker_stage GROUP BY ticker HAVING count(*) > 1
           LIMIT 1""",
        (types,),
    ).fetchone()
    if count is None or count[0] != frame.height or invalid is not None:
        _fail("Invalid staged PostgreSQL ticker references")


def _validate_split_stage(connection: psycopg.Connection, request: FetchRequest, frame: pl.DataFrame) -> None:
    count = connection.execute("SELECT count(*) FROM pg_temp.split_stage").fetchone()
    invalid = connection.execute(
        """SELECT 1 FROM pg_temp.split_stage
           WHERE ticker IS NULL OR ticker !~ '[^[:space:]]'
              OR execution_date NOT BETWEEN %s AND %s
           UNION ALL
           SELECT 1 FROM pg_temp.split_stage
           GROUP BY ticker, execution_date, split_from, split_to, adjustment_factor, adjustment_type
           HAVING count(*) > 1
           LIMIT 1""",
        (request.requested_start, request.requested_end),
    ).fetchone()
    if count is None or count[0] != frame.height or invalid is not None:
        _fail("Invalid staged PostgreSQL split events")


def _validate_scope(request: FetchRequest, source: str) -> None:
    if request.source != source:
        _fail("Invalid PostgreSQL reference outcome")
    if source == "tickers":
        if (
            not isinstance(request.ticker_types, tuple)
            or request.requested_date is not None
            or request.requested_start is not None
            or request.requested_end is not None
        ):
            _fail("Invalid PostgreSQL ticker request scope")
        require_unique_nonempty_strings(
            request.ticker_types,
            field="ticker types",
            message="Invalid PostgreSQL ticker request scope",
        )
        return
    if (
        not is_date(request.requested_start)
        or not is_date(request.requested_end)
        or request.requested_start > request.requested_end
        or request.requested_date is not None
        or request.ticker_types
    ):
        _fail("Invalid PostgreSQL split request scope")


def _store(
    connection: psycopg.Connection,
    request: FetchRequest,
    outcome: FetchOutcome,
    source: str,
    validate: Callable[[pl.DataFrame, FetchRequest], None],
) -> int:
    require_writer_connection(connection)
    _validate_scope(request, source)
    if not isinstance(outcome.status, FetchStatus):
        _fail("Invalid PostgreSQL reference outcome")
    if not isinstance(outcome.frame, pl.DataFrame):
        _fail("Invalid canonical PostgreSQL reference frame")
    schema = TICKERS_SCHEMA if source == "tickers" else SPLITS_SCHEMA
    columns = _TICKER_COLUMNS if source == "tickers" else _SPLIT_COLUMNS
    _canonical_frame(outcome.frame, schema, columns)
    if outcome.is_populated():
        validate(outcome.frame, request)
    elif not outcome.frame.is_empty():
        _fail("Non-populated reference outcome must have an empty frame")

    changed = False
    with connection.transaction():
        if outcome.is_populated():
            changed = (
                _store_tickers(connection, request, outcome.frame)
                if source == "tickers"
                else _store_splits(connection, request, outcome.frame)
            )
        revision = advance_cache_revision(connection) if changed else None
        record_fetch_outcome(connection, request, outcome)
        if revision is None:
            revision = read_cache_state(connection).input_revision
        return revision


def _store_tickers(connection: psycopg.Connection, request: FetchRequest, frame: pl.DataFrame) -> bool:
    _stage(connection, "ticker_stage")
    # The API may return an empty string for cik. Normalize it to NULL so it
    # passes the market.ticker.cik length+charset CHECK at store time (fail-fast).
    stage_frame = frame.rename({"type": "ticker_type"}).with_columns(
        pl.when(pl.col("cik") == "").then(None).otherwise(pl.col("cik")).alias("cik")
    )
    copy_frame(connection, "ticker_stage", stage_frame, _TICKER_STAGE_COLUMNS)
    _validate_ticker_stage(connection, request, frame)
    types = list(request.ticker_types)
    conflict = connection.execute(
        """SELECT 1 FROM pg_temp.ticker_stage s
           JOIN market.ticker t ON t.symbol = s.ticker
           WHERE NOT (t.ticker_type = ANY(%s)) LIMIT 1""",
        (types,),
    ).fetchone()
    if conflict is not None:
        _fail("Ticker type change requires an explicit union scope")
    # Symbol-only insert: must NOT include metadata here. The change-detection
    # EXCEPT-diff below reads `prior` from market.ticker; if we wrote metadata
    # now, new tickers would already be in `prior`, `desired == prior`, no diff,
    # and the cache revision would never advance on a first-population store.
    connection.execute(
        """INSERT INTO market.ticker (symbol) SELECT ticker FROM pg_temp.ticker_stage
           ON CONFLICT (symbol) DO NOTHING"""
    )
    changed_row = connection.execute(
        """WITH desired AS (
               SELECT t.ticker_id, s.name, s.ticker_type, s.primary_exchange, s.cik, s.active
               FROM pg_temp.ticker_stage s JOIN market.ticker t ON t.symbol = s.ticker
           ), prior AS (
               SELECT ticker_id, name, ticker_type, primary_exchange, cik, active
               FROM market.ticker WHERE ticker_type = ANY(%s)
           )
           SELECT EXISTS (
               (SELECT * FROM desired EXCEPT SELECT * FROM prior)
               UNION ALL
               (SELECT * FROM prior EXCEPT SELECT * FROM desired)
           )""",
        (types,),
    ).fetchone()
    if changed_row is None:
        _fail("Could not compare ticker reference snapshots")
    changed = bool(changed_row[0])
    if changed:
        connection.execute(
            """UPDATE market.ticker t
               SET name = s.name, ticker_type = s.ticker_type,
                   primary_exchange = s.primary_exchange, cik = s.cik, active = s.active
               FROM pg_temp.ticker_stage s
               WHERE t.symbol = s.ticker
                 AND ROW(t.name, t.ticker_type, t.primary_exchange, t.cik, t.active)
                     IS DISTINCT FROM ROW(s.name, s.ticker_type, s.primary_exchange, s.cik, s.active)"""
        )
        connection.execute(
            """UPDATE market.ticker
               SET name = NULL, ticker_type = NULL, primary_exchange = NULL, cik = NULL, active = NULL
               WHERE ticker_type = ANY(%s)
                 AND NOT EXISTS (SELECT 1 FROM pg_temp.ticker_stage s WHERE s.ticker = market.ticker.symbol)""",
            (types,),
        )
    return bool(changed)


def _store_splits(connection: psycopg.Connection, request: FetchRequest, frame: pl.DataFrame) -> bool:
    _stage(connection, "split_stage")
    copy_frame(connection, "split_stage", frame, _SPLIT_STAGE_COLUMNS)
    _validate_split_stage(connection, request, frame)
    connection.execute(
        "INSERT INTO market.ticker (symbol) SELECT DISTINCT ticker FROM pg_temp.split_stage "
        "ON CONFLICT (symbol) DO NOTHING"
    )
    start, end = request.requested_start, request.requested_end
    changed_row = connection.execute(
        """WITH desired AS (
               SELECT t.ticker_id, s.execution_date, s.split_from, s.split_to, s.adjustment_factor, s.adjustment_type
               FROM pg_temp.split_stage s JOIN market.ticker t ON t.symbol = s.ticker
           ), prior AS (
               SELECT e.ticker_id, e.execution_date, e.split_from, e.split_to, e.adjustment_factor, e.adjustment_type
               FROM ingest.split_event e WHERE e.execution_date BETWEEN %s AND %s
           )
           SELECT EXISTS (SELECT * FROM desired EXCEPT SELECT * FROM prior)
               OR EXISTS (SELECT * FROM prior EXCEPT SELECT * FROM desired)""",
        (start, end),
    ).fetchone()
    if changed_row is None:
        _fail("Could not compare split reference snapshots")
    changed = bool(changed_row[0])
    if changed:
        connection.execute("DELETE FROM ingest.split_event WHERE execution_date BETWEEN %s AND %s", (start, end))
        connection.execute(
            """INSERT INTO ingest.split_event
               (ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type)
               SELECT t.ticker_id, s.execution_date, s.split_from, s.split_to, s.adjustment_factor, s.adjustment_type
               FROM pg_temp.split_stage s JOIN market.ticker t ON t.symbol = s.ticker"""
        )
    return bool(changed)


def store_ticker_outcome(connection: psycopg.Connection, request: FetchRequest, outcome: FetchOutcome) -> int:
    """Replace requested private ticker types and return the resulting input revision."""
    return _store(connection, request, outcome, "tickers", _ticker_rows)


def store_split_outcome(connection: psycopg.Connection, request: FetchRequest, outcome: FetchOutcome) -> int:
    """Replace split events in the inclusive window and return the resulting input revision."""
    return _store(connection, request, outcome, "splits", _split_rows)
