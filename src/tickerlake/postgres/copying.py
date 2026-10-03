"""Typed row transfer to caller-created PostgreSQL temporary stages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection

if TYPE_CHECKING:
    import polars as pl

_STAGE_NAMES = frozenset(
    {
        "raw_stage",
        "ticker_stage",
        "split_stage",
        "publication_daily_stage",
        "publication_weekly_stage",
        "publication_monthly_stage",
        "publication_ticker_stage",
    }
)
_UNSUPPORTED_STAGE = "Unsupported PostgreSQL staging table"
_INVALID_COLUMNS = "PostgreSQL staging columns must be unique and nonempty"
_MISSING_COLUMNS = "PostgreSQL staging columns are missing from the frame"
_COPY_FAILED = "Could not copy rows to PostgreSQL staging table"


def copy_frame(
    connection: psycopg.Connection,
    stage_name: str,
    frame: pl.DataFrame,
    columns: tuple[str, ...],
) -> None:
    """Copy Polars rows into a permitted pg_temp staging table in column order."""
    require_writer_connection(connection)
    if stage_name not in _STAGE_NAMES:
        raise PostgresWriterError(_UNSUPPORTED_STAGE)
    if not columns or len(columns) != len(set(columns)) or any(not name for name in columns):
        raise PostgresWriterError(_INVALID_COLUMNS)
    missing = set(columns).difference(frame.columns)
    if missing:
        raise PostgresWriterError(_MISSING_COLUMNS)

    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier("pg_temp", stage_name),
        sql.SQL(", ").join(sql.Identifier(name) for name in columns),
    )
    try:
        with connection.cursor() as cursor, cursor.copy(statement) as copy:
            for row in frame.select(columns).iter_rows():
                copy.write_row(row)
    except psycopg.Error:
        raise PostgresWriterError(_COPY_FAILED) from None
