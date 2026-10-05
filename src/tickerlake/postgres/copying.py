"""Typed row transfer to caller-created PostgreSQL temporary stages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from tickerlake.postgres._schema import STAGE_NAMES
from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection

if TYPE_CHECKING:
    import polars as pl

# Publication stages come from the shared schema module; the remaining stages
# are local to raw/reference ingestion. The full set is the copy allowlist.
_STAGE_NAMES = STAGE_NAMES | {
    "raw_stage",
    "ticker_stage",
    "split_stage",
    "publication_ticker_stage",
}
_UNSUPPORTED_STAGE = "Unsupported PostgreSQL staging table"
_INVALID_COLUMNS = "PostgreSQL staging columns must be unique and nonempty"
_MISSING_COLUMNS = "PostgreSQL staging columns are missing from the frame"


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
    with connection.cursor() as cursor, cursor.copy(statement) as copy:
        for row in frame.select(columns).iter_rows():
            copy.write_row(row)
