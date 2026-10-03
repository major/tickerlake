"""Consumer database schema rejection contracts."""

from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest

from tickerlake.load import write_consumer_db
from tickerlake.transform import compute_metrics

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "table",
    [
        "daily_bars",
        "daily_metrics",
        "tickers",
        "weekly_bars",
        "weekly_metrics",
        "monthly_bars",
        "monthly_metrics",
    ],
    ids=[
        "daily_bars",
        "daily_metrics",
        "tickers",
        "weekly_bars",
        "weekly_metrics",
        "monthly_bars",
        "monthly_metrics",
    ],
)
def test_write_consumer_db_rejects_extra_columns_without_changing_database(
    tmp_path: Path,
    sample_bars_df: pl.DataFrame,
    sample_tickers_df: pl.DataFrame,
    table: str,
) -> None:
    """An extra input column is rejected before replacing any persisted tables."""
    db_path = tmp_path / "consumer.duckdb"
    sample_metrics_df = compute_metrics(sample_bars_df)
    initial_bars = sample_bars_df.with_columns((pl.col("close") + 10).alias("close"))
    initial_metrics = sample_metrics_df.with_columns((pl.col("sma_20") + 10).alias("sma_20"))
    initial_tickers = sample_tickers_df.with_columns(pl.lit("Initial name").alias("name"))
    write_consumer_db(
        initial_bars,
        initial_metrics,
        initial_tickers,
        db_path,
        weekly_bars=initial_bars,
        weekly_metrics=initial_metrics,
        monthly_bars=initial_bars,
        monthly_metrics=initial_metrics,
    )

    with duckdb.connect(str(db_path)) as con:
        con.execute("CREATE TABLE sentinel AS SELECT 'preserve me' AS value")
        before = {
            name: con.execute("SELECT * FROM query_table(?) ORDER BY ALL", [name]).fetchall()
            for (name,) in con.execute("SHOW TABLES").fetchall()
        }

    extra_column = "unexpected"
    bad_frame = {
        "daily_bars": sample_bars_df,
        "daily_metrics": sample_metrics_df,
        "tickers": sample_tickers_df,
        "weekly_bars": sample_bars_df,
        "weekly_metrics": sample_metrics_df,
        "monthly_bars": sample_bars_df,
        "monthly_metrics": sample_metrics_df,
    }[table].with_columns(pl.lit("extra").alias(extra_column))
    args = {
        "daily_bars": sample_bars_df,
        "daily_metrics": sample_metrics_df,
        "tickers": sample_tickers_df,
    }
    kwargs = {}
    if table in args:
        args[table] = bad_frame
    else:
        kwargs[table] = bad_frame

    with pytest.raises(ValueError, match=rf"{table}.*unexpected columns.*{extra_column}"):
        write_consumer_db(
            args["daily_bars"],
            args["daily_metrics"],
            args["tickers"],
            db_path,
            **kwargs,
        )

    with duckdb.connect(str(db_path), read_only=True) as con:
        after = {
            name: con.execute("SELECT * FROM query_table(?) ORDER BY ALL", [name]).fetchall()
            for (name,) in con.execute("SHOW TABLES").fetchall()
        }
    assert after == before
