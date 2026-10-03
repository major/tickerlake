"""Exclusive PostgreSQL writer connection management."""

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

import psycopg
from psycopg.conninfo import conninfo_to_dict

if TYPE_CHECKING:
    from collections.abc import Iterator

WRITER_LOCK_KEY = 74839201
_ACTIVE_WRITERS: set[int] = set()

_INVALID_DATABASE_URL = "PostgreSQL database URL must be a nonblank string"
_INVALID_DATABASE_URL_FORMAT = "PostgreSQL database URL is invalid"
_CONNECTION_FAILED = "Could not connect to PostgreSQL"
_LOCK_ACQUIRE_FAILED = "Could not acquire PostgreSQL writer lock"
_ANOTHER_WRITER_ACTIVE = "Another PostgreSQL writer is active"
_WRITER_OPERATION_FAILED = "PostgreSQL writer operation failed"
_LIVE_AUTOCOMMIT_REQUIRED = "A live autocommit writer connection is required"
_LOCK_VERIFY_FAILED = "Could not verify PostgreSQL writer lock"
_LOCK_NOT_HELD = "PostgreSQL writer lock is not held"
_EXCLUSIVE_LOCK_REQUIRED = "PostgreSQL exclusive writer lock is required"


class PostgresWriterError(RuntimeError):
    """A safe error raised by the PostgreSQL writer layer."""


@contextmanager
def writer_connection(database_url: str) -> Iterator[psycopg.Connection]:
    """Yield one autocommit connection holding the process-wide writer lock."""
    if not isinstance(database_url, str) or not database_url.strip():
        raise PostgresWriterError(_INVALID_DATABASE_URL)
    try:
        conninfo_to_dict(database_url)
    except psycopg.ProgrammingError, TypeError, ValueError:
        raise PostgresWriterError(_INVALID_DATABASE_URL_FORMAT) from None

    try:
        connection = psycopg.connect(database_url, autocommit=True)
    except psycopg.Error:
        raise PostgresWriterError(_CONNECTION_FAILED) from None

    locked = False
    try:
        try:
            row = connection.execute("SELECT pg_try_advisory_lock(%s)", (WRITER_LOCK_KEY,)).fetchone()
        except psycopg.Error:
            raise PostgresWriterError(_LOCK_ACQUIRE_FAILED) from None
        locked = bool(row and row[0])
        if not locked:
            raise PostgresWriterError(_ANOTHER_WRITER_ACTIVE)
        _ACTIVE_WRITERS.add(id(connection))
        try:
            yield connection
        except psycopg.Error:
            raise PostgresWriterError(_WRITER_OPERATION_FAILED) from None
    finally:
        _ACTIVE_WRITERS.discard(id(connection))
        if locked and not connection.closed:
            with suppress(psycopg.Error):
                connection.execute("SELECT pg_advisory_unlock(%s)", (WRITER_LOCK_KEY,))
        with suppress(psycopg.Error):
            connection.close()


def require_writer_connection(connection: psycopg.Connection) -> None:
    """Ensure a live connection currently owns the exclusive writer lock."""
    if connection.closed or not connection.autocommit:
        raise PostgresWriterError(_LIVE_AUTOCOMMIT_REQUIRED)
    if id(connection) not in _ACTIVE_WRITERS:
        raise PostgresWriterError(_LOCK_NOT_HELD)
    try:
        row = connection.execute(
            """SELECT EXISTS (
                   SELECT 1 FROM pg_locks
                   WHERE locktype = 'advisory' AND granted
                     AND pid = pg_backend_pid() AND objid = %s AND classid = 0
                     AND objsubid = 1 AND mode = 'ExclusiveLock'
                )""",
            (WRITER_LOCK_KEY,),
        ).fetchone()
    except psycopg.Error:
        raise PostgresWriterError(_LOCK_VERIFY_FAILED) from None
    if not row or not row[0]:
        raise PostgresWriterError(_EXCLUSIVE_LOCK_REQUIRED)
