"""Exclusive PostgreSQL writer connection management."""

from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus

if TYPE_CHECKING:
    from collections.abc import Iterator

WRITER_LOCK_KEY = 74839201
_ACTIVE_WRITERS: set[int] = set()

_INVALID_DATABASE_URL = "PostgreSQL database URL must be a nonblank string"
_INVALID_DATABASE_URL_FORMAT = "PostgreSQL database URL is invalid"
_ANOTHER_WRITER_ACTIVE = "Another PostgreSQL writer is active"
_LIVE_AUTOCOMMIT_REQUIRED = "A live autocommit writer connection is required"
_LOCK_NOT_HELD = "PostgreSQL writer lock is not held"


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

    connection = psycopg.connect(database_url, autocommit=True)

    locked = False
    try:
        row = connection.execute("SELECT pg_try_advisory_lock(%s)", (WRITER_LOCK_KEY,)).fetchone()
        locked = bool(row and row[0])
        if not locked:
            raise PostgresWriterError(_ANOTHER_WRITER_ACTIVE)
        _ACTIVE_WRITERS.add(id(connection))
        yield connection
    finally:
        _ACTIVE_WRITERS.discard(id(connection))
        if locked and not connection.closed:
            with suppress(psycopg.Error):
                connection.execute("SELECT pg_advisory_unlock(%s)", (WRITER_LOCK_KEY,))
        with suppress(psycopg.Error):
            connection.close()


def require_writer_connection(connection: psycopg.Connection) -> None:
    """Ensure a live connection currently owns the writer lock.

    The advisory lock cannot be lost while the session lives, so we trust the
    process-local ``_ACTIVE_WRITERS`` membership plus the live+autocommit
    checks; the previous pg_locks round-trip per call has been dropped.
    """
    if connection.closed or not connection.autocommit:
        raise PostgresWriterError(_LIVE_AUTOCOMMIT_REQUIRED)
    if id(connection) not in _ACTIVE_WRITERS:
        raise PostgresWriterError(_LOCK_NOT_HELD)


def is_writer_connection_idle(connection: psycopg.Connection) -> bool:
    """Return True iff the writer connection is open and not inside a transaction.

    Callers use this to decide whether a follow-up statement (e.g. fail_run)
    would be safe, or whether an aborted transaction state would mask the
    original exception. Mirrors the live+idle gate that ``require_writer_connection``
    enforces on the live-lock axis.
    """
    return not connection.closed and connection.info.transaction_status == TransactionStatus.IDLE
