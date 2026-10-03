"""PostgreSQL migration integration tests."""

from __future__ import annotations

import hashlib
from importlib import resources

import psycopg
import pytest

from tickerlake.postgres import migrations
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations


def test_apply_migrations_requires_live_locked_autocommit_connection(pg_owner_dsn: str) -> None:
    """Reject connections that do not own the writer lock."""
    with (
        psycopg.connect(pg_owner_dsn, autocommit=True) as connection,
        pytest.raises(PostgresWriterError, match="writer lock"),
    ):
        apply_migrations(connection)


def test_migrations_apply_repeat_and_ledger_checksums(pg_owner_dsn: str) -> None:
    """Apply packaged migrations once and preserve their checksums on repeat."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        before = connection.execute(
            "SELECT version, filename, checksum FROM ingest.schema_migration ORDER BY version"
        ).fetchall()
        apply_migrations(connection)
        after = connection.execute(
            "SELECT version, filename, checksum FROM ingest.schema_migration ORDER BY version"
        ).fetchall()
        assert after == before
        assert before == [
            (
                1,
                "0001_foundation.sql",
                hashlib.sha256(
                    resources.files("tickerlake.migrations").joinpath("0001_foundation.sql").read_bytes()
                ).hexdigest(),
            )
        ]


def test_schema_contract_and_access_grants(pg_owner_dsn: str, pg_etl_dsn: str, pg_reader_dsn: str) -> None:
    """Check the foundation columns and least-privilege role access."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'ingest' AND table_name = 'raw_daily' ORDER BY ordinal_position"
        ).fetchall() == [
            ("date", "date", "NO"),
            ("ticker_id", "integer", "NO"),
            ("open", "real", "NO"),
            ("high", "real", "NO"),
            ("low", "real", "NO"),
            ("close", "real", "NO"),
            ("vwap", "real", "YES"),
            ("volume", "double precision", "NO"),
            ("transactions", "bigint", "NO"),
        ]
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'ingest' AND table_name = 'fetch_manifest' "
            "AND column_name = 'requested_ticker_types'"
        ).fetchone() == ("requested_ticker_types", "ARRAY", "YES")
    with psycopg.connect(pg_etl_dsn, autocommit=True) as etl:
        assert etl.execute("SELECT count(*) FROM ingest.cache_state").fetchone() == (1,)
        etl.execute("CREATE TEMP TABLE migration_temp_check (value integer)")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
                ("0" * 64,),
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute("UPDATE ingest.schema_migration SET filename = 'bad.sql' WHERE version = 1")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute("DELETE FROM ingest.schema_migration WHERE version = 1")
    with psycopg.connect(pg_reader_dsn, autocommit=True) as reader:
        assert reader.execute("SELECT count(*) FROM market.ticker").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.raw_daily")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("INSERT INTO market.ticker (symbol) VALUES ('reader-write')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.schema_migration")


def test_migration_revokes_default_acl_grants(pg_owner_dsn: str, pg_etl_dsn: str, pg_reader_dsn: str) -> None:
    """Remove effective default grants before assigning the foundation ACLs."""
    with writer_connection(pg_owner_dsn) as connection:
        connection.execute("CREATE SCHEMA ingest")
        connection.execute("CREATE SCHEMA market")
        for schema in ("ingest", "market"):
            connection.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO tickerlake_etl, tickerlake_reader"
            )
        apply_migrations(connection)
    with psycopg.connect(pg_etl_dsn, autocommit=True) as etl:
        for statement in (
            "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
            "UPDATE ingest.schema_migration SET filename = 'bad.sql' WHERE version = 1",
            "DELETE FROM ingest.schema_migration WHERE version = 1",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                etl.execute(statement, ("0" * 64,) if statement.startswith("INSERT") else None)
    with psycopg.connect(pg_reader_dsn, autocommit=True) as reader:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.raw_daily")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("INSERT INTO market.ticker (symbol) VALUES ('reader-write')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
                ("0" * 64,),
            )


@pytest.mark.parametrize("files", [[], [("0001_empty.sql", b" \n\t")]])
def test_empty_migration_set_fails_before_schema_bootstrap(
    pg_owner_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    files: list[tuple[str, bytes]],
) -> None:
    """Reject empty packaged migration resources before bootstrapping schemas."""

    class Resource:
        def __init__(self, name: str, data: bytes) -> None:
            self.name = name
            self._data = data

        def read_bytes(self) -> bytes:
            return self._data

    class Directory:
        def iterdir(self) -> list[Resource]:
            return [Resource(name, data) for name, data in files]

    monkeypatch.setattr(migrations.resources, "files", lambda _: Directory())
    with writer_connection(pg_owner_dsn) as connection:
        with pytest.raises(PostgresWriterError, match="migration"):
            apply_migrations(connection)
        assert connection.execute("SELECT to_regnamespace('ingest')").fetchone() == (None,)
        assert connection.execute("SELECT to_regnamespace('market')").fetchone() == (None,)


def test_failing_migration_rolls_back_ddl_and_ledger_row(pg_owner_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Roll back a relation created by a migration that later fails."""

    class Resource:
        name = "0001_atomic_failure.sql"

        def read_bytes(self) -> bytes:
            return b"CREATE TABLE ingest.partial_migration (id integer); SELECT 1 / 0;"

    class Directory:
        def iterdir(self) -> tuple[Resource, ...]:
            return (Resource(),)

    monkeypatch.setattr(migrations.resources, "files", lambda _: Directory())
    with writer_connection(pg_owner_dsn) as connection:
        with pytest.raises(PostgresWriterError, match="migrations"):
            apply_migrations(connection)

        assert connection.execute("SELECT to_regclass('ingest.partial_migration')").fetchone() == (None,)
        ledger_exists = connection.execute("SELECT to_regclass('ingest.schema_migration')").fetchone()[0]
        if ledger_exists is not None:
            assert connection.execute("SELECT count(*) FROM ingest.schema_migration WHERE version = 1").fetchone() == (
                0,
            )


def test_corrupt_applied_checksum_and_unknown_version_are_rejected(pg_owner_dsn: str) -> None:
    """Reject checksum changes and unknown applied migration versions."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        connection.execute("UPDATE ingest.schema_migration SET checksum = repeat('0', 64) WHERE version = 1")
        with pytest.raises(PostgresWriterError, match="checksum"):
            apply_migrations(connection)
        connection.execute("UPDATE ingest.schema_migration SET checksum = %s WHERE version = 1", ("0" * 64,))
        connection.execute(
            "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (2, '0002_unknown.sql', %s)",
            ("1" * 64,),
        )
        with pytest.raises(PostgresWriterError, match="unknown"):
            apply_migrations(connection)


def test_invalid_raw_values_and_split_nullsafe_natural_key_are_rejected(pg_owner_dsn: str) -> None:
    """Enforce raw-data constraints and the split null-safe natural key."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        ticker_id = connection.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('TST') RETURNING ticker_id"
        ).fetchone()[0]
        raw_insert = """INSERT INTO ingest.raw_daily
            (date, ticker_id, open, high, low, close, vwap, volume, transactions)
            VALUES ('2025-01-02', %s, 1, 1, 2, 1, NULL, 0, 0)"""
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(raw_insert, (ticker_id,))
        split_insert = """INSERT INTO ingest.split_event
            (ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type)
            VALUES (%s, '2025-01-02', 1, 2, 2, NULL)"""
        connection.execute(split_insert, (ticker_id,))
        with pytest.raises(psycopg.errors.UniqueViolation):
            connection.execute(split_insert, (ticker_id,))
