"""Tests for bronze transformers."""

from datetime import datetime

import polars as pl

from tickerlake.bronze.transformers import transform_stocks_dataframe


def test_transform_stocks_dataframe_converts_timestamp_and_ticker() -> None:
    """window_start should become a Date column and ticker categorical."""
    ts = int(datetime(2024, 3, 3).timestamp() * 1000)
    df = pl.DataFrame(
        {
            "ticker": ["AAPL"],
            "volume": [100],
            "open": [10.0],
            "close": [12.0],
            "high": [13.5],
            "low": [9.5],
            "transactions": [20],
            "window_start": [ts],
        }
    )

    transformed = transform_stocks_dataframe(df)

    assert "window_start" not in transformed.columns
    assert transformed["date"].dtype == pl.Date
    assert transformed["ticker"].dtype == pl.Categorical
    assert transformed["date"][0].isoformat() == "2024-03-03"
