"""Pure validation helper unit tests (no database required)."""

import datetime as dt

import pytest

from tickerlake.postgres._validation import is_date, require_unique_nonempty_strings
from tickerlake.postgres.connection import PostgresWriterError

# --- is_date ----------------------------------------------------------------


def test_is_date_accepts_plain_date() -> None:
    """A plain date is a date."""
    assert is_date(dt.date(2025, 1, 2))


def test_is_date_rejects_datetime() -> None:
    """A datetime is not a plain date even though it subclasses date."""
    assert not is_date(dt.datetime(2025, 1, 2, tzinfo=dt.UTC))


def test_is_date_rejects_none_and_non_dates() -> None:
    """None and non-date objects are rejected."""
    assert not is_date(None)
    assert not is_date("2025-01-02")
    assert not is_date(20250102)


# --- require_unique_nonempty_strings ----------------------------------------


def test_require_unique_nonempty_strings_accepts_unique_nonempty() -> None:
    """Unique non-empty strings are returned unchanged as a list."""
    assert require_unique_nonempty_strings(["a", "b"], field="letters") == ["a", "b"]


def test_require_unique_nonempty_strings_rejects_duplicates() -> None:
    """Repeated values are rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings(["a", "a"], field="letters")


def test_require_unique_nonempty_strings_rejects_empty_string() -> None:
    """Blank strings are rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings(["a", ""], field="letters")


def test_require_unique_nonempty_strings_rejects_string_input() -> None:
    """A bare string is not treated as a sequence of characters."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings("abc", field="letters")


def test_require_unique_nonempty_strings_rejects_empty_sequence() -> None:
    """An empty sequence is rejected."""
    with pytest.raises(PostgresWriterError, match="letters must be unique nonempty strings"):
        require_unique_nonempty_strings([], field="letters")


def test_require_unique_nonempty_strings_respects_custom_message() -> None:
    """A custom message replaces the default safe message."""
    with pytest.raises(PostgresWriterError, match="custom message"):
        require_unique_nonempty_strings([], field="x", message="custom message")
