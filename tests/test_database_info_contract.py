"""Behavioral contracts for database metadata reporting."""

import datetime
from typing import TYPE_CHECKING

import duckdb

from tickerlake.load import get_db_info

if TYPE_CHECKING:
    from pathlib import Path


def test_get_db_info_reports_date_extremes_for_unsorted_rows(tmp_path: Path) -> None:
    """Date ranges use minimum and maximum dates, not insertion order."""
    db_path = tmp_path / "dates.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute('CREATE TABLE "market#data" ("date" DATE, value INTEGER)')
        con.execute(
            'INSERT INTO "market#data" VALUES '
            "('2024-03-20', 1), ('2024-01-05', 2), ('2024-02-10', 3), ('2024-03-20', 4)"
        )

    info = get_db_info(db_path)

    assert info["tables"] == ["market#data"]
    assert info["row_counts"] == {"market#data": 4}
    assert info["date_range"] == {"market#data": {"min": datetime.date(2024, 1, 5), "max": datetime.date(2024, 3, 20)}}
    assert info["file_size_bytes"] > 0


def test_get_db_info_reports_empty_date_table_and_non_date_table(tmp_path: Path) -> None:
    """Empty date tables and tables without dates have stable metadata."""
    db_path = tmp_path / "empty-and-undated.duckdb"
    with duckdb.connect(str(db_path)) as con:
        con.execute('CREATE TABLE "empty#dates" ("date" DATE, value INTEGER)')
        con.execute('CREATE TABLE "label""#table" (id INTEGER, name VARCHAR)')
        con.execute("INSERT INTO \"label\"\"#table\" VALUES (1, 'sample'), (2, 'other')")

    info = get_db_info(db_path)

    assert info["tables"] == ["empty#dates", 'label"#table']
    assert info["row_counts"] == {"empty#dates": 0, 'label"#table': 2}
    assert info["date_range"] == {"empty#dates": {"min": None, "max": None}}
    assert info["file_size_bytes"] > 0
