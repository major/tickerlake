"""Owned PostgreSQL test databases and least-privilege role connections."""

from __future__ import annotations

import os
import re
import secrets
from dataclasses import dataclass
from urllib.parse import quote

import psycopg
import pytest
from psycopg import sql

from tickerlake.postgres.connection import writer_connection
from tickerlake.postgres.migrations import apply_migrations

_DATABASE_NAME = re.compile(r"tickerlake_test_[a-f0-9]{16}\Z")


@dataclass(frozen=True)
class PostgresTestDatabase:
    """Connections scoped to one fresh harness-owned database."""

    name: str
    owner_dsn: str
    etl_dsn: str
    reader_dsn: str
    admin_dsn: str


def pytest_configure() -> None:
    """Prevent libpq from inheriting connection details from the host."""
    for key in tuple(os.environ):
        if key == "DATABASE_URL" or (
            key.startswith("PG")
            and key
            not in {
                "PG_OWNER_PASSWORD",
                "PG_ETL_PASSWORD",
                "PG_READER_PASSWORD",
                "PG_ADMIN_PASSWORD",
            }
        ):
            os.environ.pop(key, None)


@pytest.fixture(scope="session", autouse=True)
def require_owned_postgres_harness() -> None:
    """Skip integration tests unless launched by the isolated harness."""
    if os.environ.get("TICKERLAKE_TEST_POSTGRES") != "1":
        pytest.skip("run PostgreSQL tests with make test-postgres")


@pytest.fixture
def pg_database(require_owned_postgres_harness: None) -> PostgresTestDatabase:
    """Create a new database that belongs only to the current test."""
    name = f"tickerlake_test_{secrets.token_hex(8)}"
    if not _DATABASE_NAME.fullmatch(name):
        raise RuntimeError

    def dsn(role: str, password: str) -> str:
        return f"postgresql://{role}:{quote(password, safe='')}@postgres:5432/{name}"

    admin_dsn = dsn("tickerlake_test_admin", os.environ["PG_ADMIN_PASSWORD"])
    owner_dsn = dsn("tickerlake_owner", os.environ["PG_OWNER_PASSWORD"])
    etl_dsn = dsn("tickerlake_etl_login", os.environ["PG_ETL_PASSWORD"])
    reader_dsn = dsn("tickerlake_reader_login", os.environ["PG_READER_PASSWORD"])
    with psycopg.connect(admin_dsn.replace(f"/{name}", "/tickerlake_harness"), autocommit=True) as conn:
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
        with psycopg.connect(admin_dsn.replace(f"/{name}", "/tickerlake_harness"), autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


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
    """Return the connection URL for this test database's reader role."""
    return pg_database.reader_dsn


@pytest.fixture
def pg_admin_dsn(pg_database: PostgresTestDatabase) -> str:
    """Return the connection URL for this test database's scoped admin."""
    return pg_database.admin_dsn


@pytest.fixture
def pg_migrated_database(pg_database: PostgresTestDatabase) -> PostgresTestDatabase:
    """Apply the production migrations to this test database."""
    with writer_connection(pg_database.owner_dsn) as conn:
        apply_migrations(conn)
    return pg_database
