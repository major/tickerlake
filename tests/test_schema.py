"""Tests for PostgreSQL schema initialization."""

from unittest.mock import Mock

from tickerlake.schema import _DDL, init_schema


def test_init_schema_executes_all_ddl_and_closes_cursor() -> None:
    """Schema initialization is applied through one cursor."""
    cursor = Mock()
    conn = Mock()
    conn.cursor.return_value = cursor

    init_schema(conn)

    assert cursor.execute.call_count == len(_DDL)
    cursor.close.assert_called_once_with()
