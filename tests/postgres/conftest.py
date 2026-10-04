"""Owned PostgreSQL test databases and least-privilege role connections."""

from __future__ import annotations

import os
import re
import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg
import pytest
import pytest_postgresql.factories
from psycopg import sql

from tickerlake.postgres.connection import writer_connection
from tickerlake.postgres.migrations import apply_migrations

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pytest_postgresql.executors import PostgreSQLExecutor

_DATABASE_NAME = re.compile(r"tickerlake_test_[a-f0-9]{16}\Z")
_BOOTSTRAP_DATABASE = "postgres"

# pytest-postgresql starts and owns the PostgreSQL cluster for this session. Selecting
# the always-present maintenance database as the bootstrap target keeps the superuser
# and admin connections valid for the whole session.
postgresql_proc = pytest_postgresql.factories.postgresql_proc(
    dbname=_BOOTSTRAP_DATABASE,
    password=secrets.token_urlsafe(32),
)


@dataclass(frozen=True)
class PostgresTestDatabase:
    """Connections scoped to one fresh test-owned database."""

    name: str
    owner_dsn: str
    etl_dsn: str
    reader_dsn: str
    admin_dsn: str


@dataclass(frozen=True)
class PostgresTestHarness:
    """Cluster endpoint and generated role credentials for one pytest session."""

    host: str
    port: int
    owner_password: str
    etl_password: str
    reader_password: str
    admin_password: str


def pytest_configure() -> None:
    """Prevent libpq from inheriting connection details from the host."""
    for key in tuple(os.environ):
        if key in {"DATABASE_URL", "PGSERVICE", "PGSERVICEFILE"} or key.startswith("PG"):
            os.environ.pop(key, None)


@pytest.fixture(scope="session", autouse=True)
def require_owned_postgres_harness(request: pytest.FixtureRequest) -> PostgresTestHarness:
    """Start isolated PostgreSQL only when explicitly requested by the caller."""
    if os.environ.get("TICKERLAKE_TEST_POSTGRES") != "1":
        pytest.skip("set TICKERLAKE_TEST_POSTGRES=1 to run PostgreSQL tests")

    # Resolve the process fixture lazily so the gate above skips before PostgreSQL starts.
    proc: PostgreSQLExecutor = request.getfixturevalue("postgresql_proc")

    credentials = {
        "owner": secrets.token_urlsafe(32),
        "etl": secrets.token_urlsafe(32),
        "reader": secrets.token_urlsafe(32),
        "admin": secrets.token_urlsafe(32),
    }
    host = proc.host
    port = proc.port
    superuser_dsn = _conninfo(host, port, proc.user, proc.password or "", proc.dbname)

    with psycopg.connect(superuser_dsn, autocommit=True) as conn:
        conn.execute("CREATE ROLE tickerlake_etl NOLOGIN")
        conn.execute("CREATE ROLE tickerlake_reader NOLOGIN")
        conn.execute(
            sql.SQL("CREATE ROLE tickerlake_owner LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE").format(
                sql.Literal(credentials["owner"])
            )
        )
        conn.execute(
            sql.SQL(
                "CREATE ROLE tickerlake_etl_login LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "IN ROLE tickerlake_etl"
            ).format(sql.Literal(credentials["etl"]))
        )
        conn.execute(
            sql.SQL(
                "CREATE ROLE tickerlake_reader_login LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "IN ROLE tickerlake_reader"
            ).format(sql.Literal(credentials["reader"]))
        )
        conn.execute(
            sql.SQL("CREATE ROLE tickerlake_test_admin LOGIN PASSWORD {} NOSUPERUSER CREATEDB NOCREATEROLE").format(
                sql.Literal(credentials["admin"])
            )
        )
        conn.execute("GRANT tickerlake_owner TO tickerlake_test_admin")
        conn.execute("GRANT pg_signal_backend TO tickerlake_test_admin")

    return PostgresTestHarness(
        host,
        port,
        credentials["owner"],
        credentials["etl"],
        credentials["reader"],
        credentials["admin"],
    )


@pytest.fixture
def pg_database(require_owned_postgres_harness: PostgresTestHarness) -> Iterator[PostgresTestDatabase]:
    """Create a new database that belongs only to the current test."""
    name = f"tickerlake_test_{secrets.token_hex(8)}"
    if not _DATABASE_NAME.fullmatch(name):
        raise RuntimeError

    host = require_owned_postgres_harness.host
    port = require_owned_postgres_harness.port
    admin_password = require_owned_postgres_harness.admin_password

    def dsn(role: str, password: str) -> str:
        return _conninfo(host, port, role, password, name)

    admin_dsn = dsn("tickerlake_test_admin", admin_password)
    owner_dsn = dsn("tickerlake_owner", require_owned_postgres_harness.owner_password)
    etl_dsn = dsn("tickerlake_etl_login", require_owned_postgres_harness.etl_password)
    reader_dsn = dsn("tickerlake_reader_login", require_owned_postgres_harness.reader_password)
    maintenance_dsn = _conninfo(host, port, "tickerlake_test_admin", admin_password, _BOOTSTRAP_DATABASE)

    with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} OWNER tickerlake_owner").format(sql.Identifier(name)))
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL(
                    "GRANT CONNECT, TEMPORARY ON DATABASE {} TO tickerlake_owner, tickerlake_etl, tickerlake_reader"
                ).format(sql.Identifier(name))
            )
        yield PostgresTestDatabase(name, owner_dsn, etl_dsn, reader_dsn, admin_dsn)
    finally:
        with psycopg.connect(maintenance_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _conninfo(host: str, port: int, user: str, password: str, database: str) -> str:
    """Build libpq conninfo with explicit target and options, independent of PG* variables."""
    return psycopg.conninfo.make_conninfo(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=5,
        options="-c search_path=public",
    )


@pytest.fixture
def pg_owner_dsn(pg_database: PostgresTestDatabase) -> str:
    """Return the connection URL for this test database's owner."""
    return pg_database.owner_dsn


@pytest.fixture
def pg_etl_dsn(pg_database: PostgresTestDatabase) -> str:
    """Return the connection URL for this test database's ETL role."""
    return pg_database.etl_dsn


@pytest.fixture
def pg_reader_dsn(pg_database: PostgresTestDatabase) -> str:
    """Return the connection URL for this test database's reader."""
    return pg_database.reader_dsn


@pytest.fixture
def pg_admin_dsn(pg_database: PostgresTestDatabase) -> str:
    """Return the connection URL for this test database's scoped admin."""
    return pg_database.admin_dsn


@pytest.fixture
def pg_migrated_database(pg_database: PostgresTestDatabase) -> PostgresTestDatabase:
    """Apply the production migrations to the test database."""
    with writer_connection(pg_database.owner_dsn) as conn:
        apply_migrations(conn)
    return pg_database
