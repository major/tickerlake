"""Typed Polars-to-PostgreSQL staging writes."""

import datetime

import polars as pl
import psycopg
import pytest

from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.copying import copy_frame


def test_copy_frame_preserves_column_order_types_nulls_and_values(pg_owner_dsn: str) -> None:
    """Preserve Polars value types, ordering, nulls, and precision in COPY."""
    frame = pl.DataFrame(
        {
            "execution_date": [datetime.date(2024, 2, 29), datetime.date(2024, 3, 1)],
            "count": [9_223_372_036_854_775_000, None],
            "ratio": [1.25, 2.5],
            "price": [1.1, None],
        },
        schema={
            "execution_date": pl.Date,
            "count": pl.Int64,
            "ratio": pl.Float64,
            "price": pl.Float32,
        },
    )
    with writer_connection(pg_owner_dsn) as connection:
        connection.execute(
            "CREATE TEMP TABLE raw_stage (execution_date date, count bigint, ratio double precision, price real)"
        )
        copy_frame(connection, "raw_stage", frame, ("execution_date", "count", "ratio", "price"))
        rows = connection.execute(
            "SELECT execution_date, count, ratio, price FROM pg_temp.raw_stage ORDER BY execution_date"
        ).fetchall()

    assert rows[0] == (datetime.date(2024, 2, 29), 9_223_372_036_854_775_000, 1.25, pytest.approx(1.1))
    assert rows[1] == (datetime.date(2024, 3, 1), None, 2.5, None)


def test_copy_frame_rejects_unapproved_table(pg_owner_dsn: str) -> None:
    """Keep COPY restricted to the approved temporary staging tables."""
    with writer_connection(pg_owner_dsn) as connection, pytest.raises(PostgresWriterError, match="Unsupported"):
        copy_frame(connection, "public.raw_stage", pl.DataFrame({"value": [1]}), ("value",))


def test_copy_frame_requires_a_writer_session_lock(pg_owner_dsn: str) -> None:
    """Require the same exclusive writer context for every staging COPY."""
    frame = pl.DataFrame({"value": [1]})
    with psycopg.connect(pg_owner_dsn, autocommit=True) as connection:
        connection.execute("SELECT pg_advisory_lock_shared(%s)", (74839201,))
        with pytest.raises(PostgresWriterError):
            copy_frame(connection, "raw_stage", frame, ("value",))
