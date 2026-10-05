"""Atomic persistence of fetched daily bars to PostgreSQL."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import polars as pl
import psycopg

from tickerlake.extract import DAILY_AGGS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres import copying
from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection
from tickerlake.postgres.state import advance_cache_revision, read_cache_state, record_fetch_outcome

if TYPE_CHECKING:
    from datetime import date

    from tickerlake.postgres.models import FetchRequest

_SOURCE_COLUMNS = tuple(DAILY_AGGS_SCHEMA)
_COLUMNS = ("date", "symbol", "open", "high", "low", "close", "volume")
_SAFE = "Daily fetch data is invalid"


def _invalid() -> PostgresWriterError:
    return PostgresWriterError(_SAFE)


def _storage_error() -> PostgresWriterError:
    return PostgresWriterError("Could not store daily fetch outcome")


def _validate_symbols(frame: pl.DataFrame) -> None:
    tickers = frame.get_column("ticker")
    if tickers.null_count() or any(not item.strip() for item in tickers.to_list()):
        raise _invalid()
    if tickers.n_unique() != frame.height:
        raise _invalid()


def _validate_numeric_values(frame: pl.DataFrame) -> None:
    for name in ("open", "high", "low", "close", "volume"):
        if frame.get_column(name).null_count():
            raise _invalid()
    if any(value < 0 for value in frame.get_column("volume").to_list()):
        raise _invalid()
    for name in ("open", "high", "low", "close", "volume"):
        for value in frame.get_column(name).drop_nulls().to_list():
            if not math.isfinite(value):
                raise _invalid()


def _validate_ohlc(frame: pl.DataFrame) -> None:
    if frame.filter(
        (pl.col("high") < pl.max_horizontal("open", "close", "low"))
        | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
    ).height:
        raise _invalid()


def _validate_populated_frame(frame: pl.DataFrame, requested_date: date) -> pl.DataFrame:
    if not frame.height:
        raise _invalid()
    dates = frame.get_column("date")
    if dates.null_count() or dates.unique().to_list() != [requested_date]:
        raise _invalid()
    _validate_symbols(frame)
    _validate_numeric_values(frame)
    _validate_ohlc(frame)
    return frame.rename({"ticker": "symbol"}).select(_COLUMNS)


def _validate(connection: psycopg.Connection, request: FetchRequest, outcome: FetchOutcome) -> pl.DataFrame:
    """Validate the complete payload before opening a mutating transaction."""
    try:
        if (
            request.source != "daily"
            or request.requested_date is None
            or outcome.requested_date != request.requested_date
        ):
            raise _invalid()
        row = connection.execute("SELECT state FROM ingest.run WHERE run_id = %s", (request.run_id,)).fetchone()
        if row is None or row[0] != "running":
            raise _invalid()
        frame = outcome.frame
        if not isinstance(frame, pl.DataFrame) or frame.columns != list(_SOURCE_COLUMNS):
            raise _invalid()
        if frame.schema != DAILY_AGGS_SCHEMA:
            raise _invalid()
        if not isinstance(outcome.status, FetchStatus):
            raise _invalid()
        if not outcome.is_populated():
            if frame.height:
                raise _invalid()
            return frame
        return _validate_populated_frame(frame, request.requested_date)
    except PostgresWriterError:
        raise
    except AttributeError, TypeError, ValueError, OverflowError, psycopg.Error:
        raise _invalid() from None


def _changed(connection: psycopg.Connection, day: date) -> bool:
    row = connection.execute(
        """SELECT EXISTS (
                (SELECT date, ticker_id, open, high, low, close, volume
                 FROM ingest.raw_daily WHERE date = %s
                 EXCEPT
                 SELECT s.date, t.ticker_id, s.open, s.high, s.low, s.close,
                        s.volume
                 FROM pg_temp.raw_stage s JOIN market.ticker t USING (symbol))
                UNION ALL
                (SELECT s.date, t.ticker_id, s.open, s.high, s.low, s.close,
                        s.volume
                 FROM pg_temp.raw_stage s JOIN market.ticker t USING (symbol)
                 EXCEPT
                 SELECT date, ticker_id, open, high, low, close, volume
                 FROM ingest.raw_daily WHERE date = %s)
           )""",
        (day, day),
    ).fetchone()
    return bool(row and row[0])


def store_daily_outcome(
    connection: psycopg.Connection,
    request: FetchRequest,
    outcome: FetchOutcome,
) -> int:
    """Atomically store a daily outcome and return the current input revision."""
    require_writer_connection(connection)
    frame = _validate(connection, request, outcome)
    requested_date = request.requested_date
    if requested_date is None:
        raise _invalid()
    try:
        with connection.transaction():
            manifest_id = None
            if outcome.is_populated():
                connection.execute("DROP TABLE IF EXISTS pg_temp.raw_stage")
                connection.execute(
                    """CREATE TEMP TABLE raw_stage (
                           date date NOT NULL, symbol text NOT NULL,
                           open real NOT NULL, high real NOT NULL, low real NOT NULL,
                           close real NOT NULL, volume double precision NOT NULL
                       ) ON COMMIT DROP"""
                )
                copying.copy_frame(connection, "raw_stage", frame, _COLUMNS)
                staged = connection.execute(
                    """SELECT count(*), count(*) FILTER (WHERE date IS DISTINCT FROM %s),
                              count(DISTINCT symbol)
                       FROM pg_temp.raw_stage""",
                    (requested_date,),
                ).fetchone()
                if staged is None or staged[0] != frame.height or staged[1] != 0 or staged[2] != frame.height:
                    raise _invalid()
                connection.execute(
                    """INSERT INTO market.ticker (symbol)
                       SELECT DISTINCT symbol FROM pg_temp.raw_stage
                       ON CONFLICT (symbol) DO NOTHING"""
                )
                changed = _changed(connection, requested_date)
                if changed:
                    connection.execute("DELETE FROM ingest.raw_daily WHERE date = %s", (requested_date,))
                    connection.execute(
                        """INSERT INTO ingest.raw_daily
                               (date, ticker_id, open, high, low, close, volume)
                           SELECT s.date, t.ticker_id, s.open, s.high, s.low, s.close, s.volume
                           FROM pg_temp.raw_stage s JOIN market.ticker t USING (symbol)"""
                    )
            else:
                changed = False
            revision = (
                advance_cache_revision(connection, requested_date)
                if changed
                else read_cache_state(connection).input_revision
            )
            manifest_id = record_fetch_outcome(connection, request, outcome)
            if outcome.is_populated():
                connection.execute(
                    """INSERT INTO ingest.raw_session (date, manifest_id)
                       VALUES (%s, %s)
                       ON CONFLICT (date) DO UPDATE SET manifest_id = EXCLUDED.manifest_id""",
                    (requested_date, manifest_id),
                )
            return revision
    except PostgresWriterError:
        raise
    except psycopg.Error:
        raise _storage_error() from None
