"""PostgreSQL writer connection behavior."""

import psycopg
import pytest

from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection, writer_connection
from tickerlake.postgres.state import advance_cache_revision, read_cache_state


def test_writer_connection_holds_exclusive_lock_and_releases_it(pg_owner_dsn: str) -> None:
    """Only one writer context can hold the session lock at a time."""
    with writer_connection(pg_owner_dsn) as connection:
        assert connection.autocommit
        require_writer_connection(connection)
        with pytest.raises(PostgresWriterError, match="Another PostgreSQL writer"), writer_connection(pg_owner_dsn):
            pass

    with writer_connection(pg_owner_dsn) as connection:
        require_writer_connection(connection)


def test_lock_survives_transaction_rollback(pg_owner_dsn: str) -> None:
    """Keep the session lock after an explicit transaction rolls back."""
    with writer_connection(pg_owner_dsn) as connection:
        connection.execute("CREATE TEMP TABLE rollback_check (value integer)")
        with pytest.raises(psycopg.Error), connection.transaction():
            connection.execute("INSERT INTO missing_table VALUES (1)")
        require_writer_connection(connection)


def test_writer_connection_rejects_blank_dsn() -> None:
    """Reject blank DSNs without putting their value into the error."""
    with pytest.raises(PostgresWriterError) as error, writer_connection("  "):
        pass
    assert "DATABASE_URL" not in str(error.value)


def test_unlocked_connection_is_rejected(pg_owner_dsn: str) -> None:
    """Reject a connection that did not come from the writer context."""
    with (
        psycopg.connect(pg_owner_dsn, autocommit=True) as connection,
        pytest.raises(PostgresWriterError, match="not held"),
    ):
        require_writer_connection(connection)


def test_shared_locks_do_not_authorize_writer_or_state_mutation(pg_migrated_database) -> None:
    """Reject two shared locks for writer verification and state mutation."""
    with (
        psycopg.connect(pg_migrated_database.owner_dsn, autocommit=True) as first,
        psycopg.connect(pg_migrated_database.owner_dsn, autocommit=True) as second,
    ):
        for connection in (first, second):
            connection.execute("SELECT pg_advisory_lock_shared(%s)", (74839201,))

        with pytest.raises(PostgresWriterError):
            require_writer_connection(first)
        with pytest.raises(PostgresWriterError):
            require_writer_connection(second)
        with pytest.raises(PostgresWriterError):
            advance_cache_revision(first)

    with psycopg.connect(pg_migrated_database.owner_dsn) as reader:
        assert read_cache_state(reader).input_revision == 0


def test_transaction_level_lock_does_not_authorize_writer(pg_owner_dsn: str) -> None:
    """Reject an unmanaged transaction-scoped advisory lock."""
    with psycopg.connect(pg_owner_dsn, autocommit=False) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(%s)", (74839201,))
        with pytest.raises(PostgresWriterError, match="autocommit"):
            require_writer_connection(connection)


def test_marked_connection_must_still_hold_exclusive_lock(pg_owner_dsn: str) -> None:
    """Reject a marked connection after replacing its exclusive lock."""
    with writer_connection(pg_owner_dsn) as connection:
        connection.execute("SELECT pg_advisory_unlock(%s)", (74839201,))
        connection.execute("SELECT pg_advisory_lock_shared(%s)", (74839201,))
        with pytest.raises(PostgresWriterError, match="exclusive"):
            require_writer_connection(connection)


def test_writer_operation_error_is_sanitized(pg_owner_dsn: str) -> None:
    """Hide SQL details and bound values from writer errors."""
    private_value = "private_sql_value"
    with pytest.raises(PostgresWriterError) as error, writer_connection(pg_owner_dsn) as connection:
        connection.execute("SELECT %s FROM missing_table", (private_value,))
    assert private_value not in str(error.value)
    assert "missing_table" not in str(error.value)
    assert isinstance(error.value.__context__, psycopg.Error)
