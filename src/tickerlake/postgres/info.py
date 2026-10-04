"""Read-only inspection of the PostgreSQL ingest and market schemas.

The ``info`` command is a diagnostic view: it reads schema names, estimated
table row counts, the latest publication, and the current cache revision. It
never opens the exclusive writer connection and never mutates state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.conninfo import conninfo_to_dict

from tickerlake.postgres.connection import PostgresWriterError
from tickerlake.postgres.state import read_cache_state

if TYPE_CHECKING:
    import datetime
    from uuid import UUID

    from tickerlake.postgres.models import CacheState

_INVALID_DATABASE_URL: Final = "PostgreSQL database URL must be a nonblank string"
_CONNECTION_FAILED: Final = "Could not connect to PostgreSQL"
_READ_FAILED: Final = "Could not read PostgreSQL database info"

# User-owned tables reported by ``info``, in display order. Row counts come from
# ``pg_stat_user_tables.n_live_tup``, which is an estimate maintained by the
# statistics collector: fast and safe, but not an exact ``count(*)``.
_KNOWN_TABLES: Final = (
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
)


@dataclass(frozen=True, slots=True, kw_only=True)
class TableInfo:
    """Estimated live row count for one user table."""

    schema: str
    table_name: str
    row_count: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationInfo:
    """The currently published generation, if one exists."""

    target_session: datetime.date
    published_at: datetime.datetime
    run_id: UUID


@dataclass(frozen=True, slots=True, kw_only=True)
class CountsInfo:
    """Selected row counts from the ingest and market schemas."""

    tickers: int
    splits: int
    raw_daily: int


@dataclass(frozen=True, slots=True, kw_only=True)
class DatabaseInfo:
    """The standard five-part ``info`` payload."""

    schemas: tuple[str, ...]
    tables: tuple[TableInfo, ...]
    publication: PublicationInfo | None
    cache: CacheState
    counts: CountsInfo


def collect_info(database_url: str) -> DatabaseInfo:
    """Open a read-only connection and return the standard info payload."""
    if not isinstance(database_url, str) or not database_url.strip():
        raise PostgresWriterError(_INVALID_DATABASE_URL)
    try:
        conninfo_to_dict(database_url)
    except psycopg.ProgrammingError, TypeError, ValueError:
        raise PostgresWriterError(_INVALID_DATABASE_URL) from None

    try:
        connection = psycopg.connect(database_url)
    except psycopg.Error:
        raise PostgresWriterError(_CONNECTION_FAILED) from None

    try:
        connection.read_only = True
        with connection.transaction():
            schemas = _read_schemas(connection)
            table_counts = _read_table_counts(connection)
            publication = _read_publication(connection)
            cache = read_cache_state(connection)
    except psycopg.Error:
        raise PostgresWriterError(_READ_FAILED) from None
    finally:
        connection.close()

    tables = tuple(
        TableInfo(schema=schema, table_name=table_name, row_count=table_counts.get((schema, table_name), 0))
        for schema, table_name in _KNOWN_TABLES
    )
    counts = CountsInfo(
        tickers=table_counts.get(("market", "ticker"), 0),
        splits=table_counts.get(("ingest", "split_event"), 0),
        raw_daily=table_counts.get(("ingest", "raw_daily"), 0),
    )
    return DatabaseInfo(schemas=schemas, tables=tables, publication=publication, cache=cache, counts=counts)


def _read_schemas(connection: psycopg.Connection) -> tuple[str, ...]:
    """Return the user-owned schema names in display order."""
    rows = connection.execute(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name IN ('ingest', 'market') ORDER BY schema_name"
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _read_table_counts(connection: psycopg.Connection) -> dict[tuple[str, str], int]:
    """Return estimated live row counts keyed by (schema, table)."""
    rows = connection.execute(
        "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname IN ('ingest', 'market')"
    ).fetchall()
    return {(str(schema), str(table)): int(count) for schema, table, count in rows}


def _read_publication(connection: psycopg.Connection) -> PublicationInfo | None:
    """Return the published generation, or ``None`` when nothing was published."""
    row = connection.execute(
        "SELECT run_id, published_session, published_at FROM market.publication_state WHERE singleton = true"
    ).fetchone()
    if row is None:
        return None
    return PublicationInfo(target_session=row[1], published_at=row[2], run_id=row[0])
