"""Shared request and scope validation helpers for the postgres backend."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, TypeGuard, cast

from tickerlake.postgres.connection import PostgresWriterError

if TYPE_CHECKING:
    from collections.abc import Sequence


def is_date(value: object) -> TypeGuard[date]:
    """Return True only for a plain date (never a datetime subclass)."""
    return isinstance(value, date) and not isinstance(value, datetime)


def require_unique_nonempty_strings(
    values: Sequence[object],
    *,
    field: str,
    max_len: int | None = None,
    message: str | None = None,
    error: type[PostgresWriterError] | None = None,
) -> list[str]:
    """Validate a non-empty list of unique non-empty strings.

    Args:
        values: Candidate sequence; strings and bytes are rejected outright.
        field: Caller-facing field name used in the default safe message.
        max_len: Optional inclusive upper bound on the number of items.
        message: Exact fixed caller-facing message to raise instead of the default.
        error: Exception class to raise; defaults to PostgresWriterError.

    Returns:
        The validated values as a list for downstream use.
    """
    error_class = error or PostgresWriterError
    text = message if message is not None else f"PostgreSQL {field} must be unique nonempty strings"
    if isinstance(values, (str, bytes)) or not values:
        raise error_class(text)
    items = list(values)
    if (
        (max_len is not None and len(items) > max_len)
        or any(not isinstance(value, str) or not value.strip() for value in items)
        or len(set(items)) != len(items)
    ):
        raise error_class(text)
    return cast("list[str]", items)
