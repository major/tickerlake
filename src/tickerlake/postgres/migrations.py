"""Apply packaged, checksum-verified PostgreSQL schema migrations."""

from __future__ import annotations

import hashlib
import re
from importlib import resources
from typing import LiteralString, NoReturn, cast

import psycopg
from psycopg import sql as pg_sql

from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection

_MIGRATION_NAME = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")
_ERRORS = {
    "utf8": "PostgreSQL migration is not valid UTF-8",
    "empty": "PostgreSQL migration is empty",
    "missing": "No PostgreSQL migrations were found",
    "gap": "PostgreSQL migrations have a version gap",
    "history": "Applied PostgreSQL migration history is invalid",
    "unknown": "Database has an unknown PostgreSQL migration version",
    "checksum": "Applied PostgreSQL migration checksum does not match",
    "order": "PostgreSQL migration order is invalid",
}


def _fail(code: str) -> NoReturn:
    raise PostgresWriterError(_ERRORS[code]) from None


def _migration_files() -> list[tuple[int, str, bytes]]:
    directory = resources.files("tickerlake.migrations")
    found: list[tuple[int, str, bytes]] = []
    for resource in directory.iterdir():
        match = _MIGRATION_NAME.fullmatch(resource.name)
        if match:
            content = resource.read_bytes()
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                _fail("utf8")
            if not text.strip():
                _fail("empty")
            found.append((int(match.group(1)), resource.name, content))
    found.sort()
    versions = [version for version, _, _ in found]
    if not found:
        _fail("missing")
    if versions != list(range(1, len(versions) + 1)):
        _fail("gap")
    return found


def apply_migrations(connection: psycopg.Connection) -> None:
    """Apply all packaged migrations atomically, requiring the writer lock."""
    require_writer_connection(connection)
    migrations = _migration_files()

    with connection.transaction():
        connection.execute("CREATE SCHEMA IF NOT EXISTS ingest")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS ingest.schema_migration (
                       version integer PRIMARY KEY,
                       filename text NOT NULL UNIQUE,
                       checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
                       applied_at timestamptz NOT NULL DEFAULT now()
                   )"""
        )
        applied_rows = connection.execute(
            "SELECT version, filename, checksum FROM ingest.schema_migration ORDER BY version"
        ).fetchall()
        applied = {int(row[0]): (row[1], row[2]) for row in applied_rows}
        if sorted(applied) != list(range(1, len(applied) + 1)):
            _fail("history")
        known = {version: (name, hashlib.sha256(sql).hexdigest()) for version, name, sql in migrations}
        if any(version not in known for version in applied):
            _fail("unknown")
        for version, prior in applied.items():
            if prior != known[version]:
                _fail("checksum")

        next_version = len(applied) + 1
        for version, filename, sql in migrations[next_version - 1 :]:
            if version != next_version:
                _fail("order")
            # The migration text comes only from UTF-8-validated packaged resources, never user input.
            trusted_migration: LiteralString = cast("LiteralString", sql.decode("utf-8"))
            connection.execute(pg_sql.SQL(trusted_migration))
            connection.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (%s, %s, %s)",
                (version, filename, hashlib.sha256(sql).hexdigest()),
            )
            next_version += 1
