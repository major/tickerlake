"""Public reader guarantees for databases that do not exist."""

from typing import TYPE_CHECKING

import duckdb
import pytest

from tickerlake.load import get_db_info, read_raw_db

if TYPE_CHECKING:
    from pathlib import Path


def test_raw_reader_does_not_create_a_missing_database(tmp_path: Path) -> None:
    """A failed raw read must leave an absent database absent."""
    db_path = tmp_path / "missing-raw.duckdb"

    with pytest.raises((duckdb.Error, OSError)):
        read_raw_db(db_path)

    assert not db_path.exists()


def test_database_info_does_not_create_a_missing_database(tmp_path: Path) -> None:
    """Inspecting a missing database must not create an empty database file."""
    db_path = tmp_path / "missing-info.duckdb"

    with pytest.raises((duckdb.Error, OSError)):
        get_db_info(db_path)

    assert not db_path.exists()
