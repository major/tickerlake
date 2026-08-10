"""PostgreSQL connection helpers using ADBC."""

from __future__ import annotations

import os

import adbc_driver_postgresql.dbapi as pg

_DSN: str | None = None


def get_dsn() -> str:
    """Return the PostgreSQL DSN from TICKERLAKE_DB_URI."""
    global _DSN  # noqa: PLW0603
    if _DSN is None:
        _DSN = os.environ.get("TICKERLAKE_DB_URI", "")
    if not _DSN:
        msg = "TICKERLAKE_DB_URI environment variable is required"
        raise ValueError(msg)
    return _DSN


def connect() -> pg.Connection:
    """Return a new ADBC PostgreSQL connection.

    The caller is responsible for closing the connection.
    """
    return pg.connect(get_dsn())
