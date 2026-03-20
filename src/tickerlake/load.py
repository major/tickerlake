"""Write polars DataFrames to DuckDB files with correct schema types."""

import datetime
import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl


@contextmanager
def _tmp_parquet(df: pl.DataFrame):
    """Write df to a temp parquet file, yield its path, then delete it."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        df.write_parquet(tmp)
        yield tmp
    finally:
        tmp.unlink(missing_ok=True)


def _read_parquet_sql(tmp: Path, order_by: str = "") -> str:
    """Build a SELECT from read_parquet() with optional ORDER BY clause."""
    sql = f"SELECT * FROM read_parquet('{tmp}')"
    if order_by:
        sql += f" ORDER BY {order_by}"
    return sql


def write_raw_db(bars: pl.DataFrame, path: Path) -> None:
    """Write bars DataFrame to raw_daily_bars table, replacing any existing data."""
    with _tmp_parquet(bars) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            f"CREATE OR REPLACE TABLE raw_daily_bars AS {_read_parquet_sql(tmp, 'ticker, date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def append_raw_db(new_bars: pl.DataFrame, path: Path) -> None:
    """Append new_bars rows to existing raw_daily_bars table."""
    with _tmp_parquet(new_bars) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            f"INSERT INTO raw_daily_bars {_read_parquet_sql(tmp, 'ticker, date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def read_raw_db(path: Path) -> pl.DataFrame:
    """Read raw_daily_bars table from DuckDB file, sorted by (ticker, date)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.execute(
            f"COPY (SELECT * FROM raw_daily_bars ORDER BY ticker, date) TO '{tmp}' (FORMAT PARQUET)"
        )
        con.close()
        return pl.read_parquet(tmp)
    finally:
        tmp.unlink(missing_ok=True)


def get_existing_dates(path: Path) -> set[datetime.date]:
    """Return the set of dates already stored in raw_daily_bars without loading all rows."""
    if not path.exists():
        return set()
    con = None
    try:
        con = duckdb.connect(str(path), read_only=True)
        rows = con.execute("SELECT DISTINCT date FROM raw_daily_bars").fetchall()
        con.close()
        return {row[0] for row in rows}
    except duckdb.CatalogException:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        return set()


def write_consumer_db(
    bars: pl.DataFrame,
    metrics: pl.DataFrame,
    tickers: pl.DataFrame,
    path: Path,
    *,
    hvcs: pl.DataFrame | None = None,
    weekly_bars: pl.DataFrame | None = None,
    weekly_metrics: pl.DataFrame | None = None,
    weekly_hvcs: pl.DataFrame | None = None,
) -> None:
    """Write bars, metrics, tickers, and optionally HVC and weekly DataFrames to consumer DuckDB file."""
    with (
        _tmp_parquet(bars) as bars_tmp,
        _tmp_parquet(metrics) as metrics_tmp,
        _tmp_parquet(tickers) as tickers_tmp,
    ):
        con = duckdb.connect(str(path))
        con.execute(
            f"CREATE OR REPLACE TABLE daily_bars AS {_read_parquet_sql(bars_tmp, 'ticker, date')}"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE daily_metrics AS {_read_parquet_sql(metrics_tmp, 'ticker, date')}"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE tickers AS {_read_parquet_sql(tickers_tmp, 'ticker')}"
        )
        if hvcs is not None:
            with _tmp_parquet(hvcs) as hvcs_tmp:
                con.execute(
                    f"CREATE OR REPLACE TABLE daily_hvcs AS {_read_parquet_sql(hvcs_tmp, 'ticker, date')}"
                )
        if weekly_bars is not None:
            with _tmp_parquet(weekly_bars) as wb_tmp:
                con.execute(
                    f"CREATE OR REPLACE TABLE weekly_bars AS {_read_parquet_sql(wb_tmp, 'ticker, date')}"
                )
        if weekly_metrics is not None:
            with _tmp_parquet(weekly_metrics) as wm_tmp:
                con.execute(
                    f"CREATE OR REPLACE TABLE weekly_metrics AS {_read_parquet_sql(wm_tmp, 'ticker, date')}"
                )
        if weekly_hvcs is not None:
            with _tmp_parquet(weekly_hvcs) as wh_tmp:
                con.execute(
                    f"CREATE OR REPLACE TABLE weekly_hvcs AS {_read_parquet_sql(wh_tmp, 'ticker, date')}"
                )
        con.execute("CHECKPOINT")
        con.close()


def write_splits(splits: pl.DataFrame, path: Path) -> None:
    """Write splits DataFrame to splits table, replacing any existing data."""
    with _tmp_parquet(splits) as tmp:
        con = duckdb.connect(str(path))
        con.execute(
            f"CREATE OR REPLACE TABLE splits AS {_read_parquet_sql(tmp, 'ticker, execution_date')}"
        )
        con.execute("CHECKPOINT")
        con.close()


def read_splits(path: Path) -> pl.DataFrame:
    """Read splits table from DuckDB file, sorted by (ticker, execution_date)."""
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = Path(f.name)
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.execute(
            f"COPY (SELECT * FROM splits ORDER BY ticker, execution_date) TO '{tmp}' (FORMAT PARQUET)"
        )
        con.close()
        return pl.read_parquet(tmp)
    finally:
        tmp.unlink(missing_ok=True)


def compact_raw_db(path: Path) -> None:
    """Rebuild raw.duckdb by exporting and reimporting to reclaim fragmented space."""
    bars = read_raw_db(path)
    write_raw_db(bars, path)


def _table_date_range(con: duckdb.DuckDBPyConnection, table: str) -> dict | None:
    """Return min/max date for a table if it has a date column, else None."""
    cols = {row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()}
    if "date" not in cols:
        return None
    row = con.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
    return {"min": row[0], "max": row[1]} if row else None


def get_db_info(path: Path) -> dict:
    """Return metadata about a DuckDB file: tables, row counts, date range, file size."""
    con = duckdb.connect(str(path), read_only=True)
    tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    row_counts = {
        t: (con.execute(f"SELECT COUNT(*) FROM {t}").fetchone() or (0,))[0]
        for t in tables
    }
    date_range = {
        t: dr for t in tables if (dr := _table_date_range(con, t)) is not None
    }
    con.close()
    return {
        "tables": tables,
        "row_counts": row_counts,
        "date_range": date_range,
        "file_size_bytes": path.stat().st_size,
    }
