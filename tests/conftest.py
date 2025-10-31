"""Shared pytest fixtures for the TickerLake test suite."""

from datetime import date as dt_date
from typing import Callable, Iterable

import polars as pl
import pytest


@pytest.fixture
def make_transformed_df() -> Callable[[str, Iterable[str]], pl.DataFrame]:
    """Factory fixture returning a simple transformed stocks DataFrame."""

    def _make(target_date: str, tickers: Iterable[str] = ("AAPL", "MSFT")) -> pl.DataFrame:
        dt_value = dt_date.fromisoformat(target_date)
        rows = list(tickers)
        row_count = len(rows)

        return (
            pl.DataFrame(
                {
                    "ticker": rows,
                    "volume": [1_000 + idx for idx in range(row_count)],
                    "open": [100.0 + idx for idx in range(row_count)],
                    "close": [110.0 + idx for idx in range(row_count)],
                    "high": [115.0 + idx for idx in range(row_count)],
                    "low": [95.0 + idx for idx in range(row_count)],
                    "transactions": [10 + idx for idx in range(row_count)],
                    "date": [dt_value] * row_count,
                }
            )
            .with_columns(pl.col("ticker").cast(pl.Categorical))
        )

    return _make
