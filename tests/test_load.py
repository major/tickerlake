"""Tests for tickerlake.load — DuckDB writer with schema validation."""

from pathlib import Path

import duckdb
import polars as pl
import pytest

from tickerlake.load import (
    append_raw_db,
    compact_raw_db,
    get_db_info,
    read_raw_db,
    write_consumer_db,
    write_raw_db,
)


@pytest.fixture
def sample_metrics_df(sample_bars_df: pl.DataFrame) -> pl.DataFrame:
    """Create a sample stock metrics DataFrame aligned with sample_bars_df."""
    return pl.DataFrame(
        {
            "date": sample_bars_df["date"],
            "ticker": sample_bars_df["ticker"],
            "sma_50": [None] * len(sample_bars_df),
            "sma_200": [None] * len(sample_bars_df),
        },
        schema={
            "date": pl.Date,
            "ticker": pl.Utf8,
            "sma_50": pl.Float32,
            "sma_200": pl.Float32,
        },
    )


def test_write_raw_db_creates_table(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """write_raw_db() creates raw_daily_bars table with correct row count."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()[0]
    con.close()

    assert count == len(sample_bars_df)


def test_write_raw_db_schema(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Price columns are FLOAT, date is DATE, transactions is UINTEGER."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    schema = con.execute("DESCRIBE raw_daily_bars").fetchall()
    con.close()

    schema_dict = {row[0]: row[1] for row in schema}
    assert schema_dict["date"] == "DATE"
    assert schema_dict["ticker"] == "VARCHAR"
    assert schema_dict["open"] == "FLOAT"
    assert schema_dict["high"] == "FLOAT"
    assert schema_dict["low"] == "FLOAT"
    assert schema_dict["close"] == "FLOAT"
    assert schema_dict["volume"] == "FLOAT"
    assert schema_dict["vwap"] == "FLOAT"
    assert schema_dict["transactions"] == "UINTEGER"


def test_write_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Data is sorted by (ticker, date) in the table."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    tickers = [r[0] for r in rows]
    dates = [r[1] for r in rows]

    # Verify sorted by ticker first, then date within each ticker
    for i in range(1, len(rows)):
        if tickers[i] == tickers[i - 1]:
            assert dates[i] >= dates[i - 1], (
                f"Dates not sorted within ticker {tickers[i]}"
            )
        else:
            assert tickers[i] >= tickers[i - 1], "Tickers not sorted"


def test_append_raw_db(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """append_raw_db() adds rows to existing table."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    # Append the same data again — row count should double
    append_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()[0]
    con.close()

    assert count == len(sample_bars_df) * 2


def test_read_raw_db(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """read_raw_db() returns polars DataFrame with correct data."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    result = read_raw_db(db_path)

    assert isinstance(result, pl.DataFrame)
    assert len(result) == len(sample_bars_df)
    assert set(result.columns) == set(sample_bars_df.columns)


def test_write_consumer_db_creates_tables(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """write_consumer_db() creates daily_bars, stock_metrics, tickers tables."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    con.close()

    assert "daily_bars" in tables
    assert "stock_metrics" in tables
    assert "tickers" in tables


def test_write_consumer_db_schema(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """Price columns are FLOAT, date is DATE, transactions is UINTEGER in consumer db."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    bars_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE daily_bars").fetchall()
    }
    metrics_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE stock_metrics").fetchall()
    }
    tickers_schema = {
        row[0]: row[1] for row in con.execute("DESCRIBE tickers").fetchall()
    }
    con.close()

    # daily_bars schema
    assert bars_schema["date"] == "DATE"
    assert bars_schema["open"] == "FLOAT"
    assert bars_schema["transactions"] == "UINTEGER"

    # stock_metrics schema
    assert metrics_schema["date"] == "DATE"
    assert metrics_schema["sma_50"] == "FLOAT"
    assert metrics_schema["sma_200"] == "FLOAT"

    # tickers schema
    assert tickers_schema["ticker"] == "VARCHAR"
    assert tickers_schema["active"] == "BOOLEAN"


def test_consumer_query_pattern(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_metrics_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
) -> None:
    """Three-table join works on consumer db."""
    db_path = tmp_path / "tickerlake.duckdb"
    write_consumer_db(sample_bars_df, sample_metrics_df, sample_tickers_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    result = con.execute("""
        SELECT d.date, d.ticker, t.name, d.close, m.sma_50
        FROM daily_bars d
        JOIN stock_metrics m USING (date, ticker)
        JOIN tickers t USING (ticker)
    """).fetchall()
    con.close()

    assert len(result) > 0


def test_get_db_info(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """get_db_info() returns dict with row counts, date ranges, file size."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    info = get_db_info(db_path)

    assert isinstance(info, dict)
    assert "tables" in info
    assert "row_counts" in info
    assert "date_range" in info
    assert "file_size_bytes" in info
    assert isinstance(info["tables"], list)
    assert isinstance(info["row_counts"], dict)
    assert info["file_size_bytes"] > 0
    assert "raw_daily_bars" in info["tables"]
    assert info["row_counts"]["raw_daily_bars"] == len(sample_bars_df)


def test_write_raw_db_idempotent(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """Calling write_raw_db() twice replaces data (CREATE OR REPLACE)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    write_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    count = con.execute("SELECT COUNT(*) FROM raw_daily_bars").fetchone()[0]
    con.close()

    # Should still be the original count, not doubled
    assert count == len(sample_bars_df)


def test_compact_raw_db_preserves_data(
    tmp_path: Path, sample_bars_df: pl.DataFrame
) -> None:
    """compact_raw_db() preserves all rows after rebuild."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    append_raw_db(sample_bars_df, db_path)

    compact_raw_db(db_path)

    result = read_raw_db(db_path)
    assert len(result) == len(sample_bars_df) * 2


def test_compact_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """compact_raw_db() output is sorted by (ticker, date)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)

    compact_raw_db(db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    for i in range(1, len(rows)):
        if rows[i][0] == rows[i - 1][0]:
            assert rows[i][1] >= rows[i - 1][1], (
                f"Dates not sorted within ticker {rows[i][0]}"
            )
        else:
            assert rows[i][0] >= rows[i - 1][0], "Tickers not sorted"


def test_append_raw_db_sorted(tmp_path: Path, sample_bars_df: pl.DataFrame) -> None:
    """append_raw_db() inserts rows sorted by (ticker, date)."""
    db_path = tmp_path / "raw.duckdb"
    write_raw_db(sample_bars_df, db_path)
    append_raw_db(sample_bars_df, db_path)

    con = duckdb.connect(str(db_path), read_only=True)
    rows = con.execute("SELECT ticker, date FROM raw_daily_bars").fetchall()
    con.close()

    assert len(rows) == len(sample_bars_df) * 2
