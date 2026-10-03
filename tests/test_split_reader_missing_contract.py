"""Contracts for reading split data from DuckDB."""

from typing import TYPE_CHECKING

import duckdb
import pytest

from tickerlake.load import read_splits

if TYPE_CHECKING:
    from pathlib import Path


def test_read_splits_missing_database_fails_without_creating_file(
    tmp_path: Path,
) -> None:
    """A missing splits database is not created as a side effect of reading."""
    db_path = tmp_path / "missing.duckdb"

    with pytest.raises((duckdb.Error, OSError)):
        read_splits(db_path)

    assert not db_path.exists()
