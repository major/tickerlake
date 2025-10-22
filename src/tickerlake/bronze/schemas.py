"""Schemas for polars dataframes used in the bronze layer."""

import polars as pl

STOCKS_SCHEMA = {
    "ticker": pl.Categorical,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "window_start": pl.Int64,
    "transactions": pl.UInt32,
}

STOCKS_SCHEMA_MODIFIED = {
    "ticker": pl.Categorical,
    "volume": pl.UInt64,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "date": pl.Date,
    "transactions": pl.UInt32,
}

SPLITS_SCHEMA = {
    "id": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "ticker": pl.Categorical,
}

TICKERS_SCHEMA = {
    "active": pl.Boolean,
    "base_currency_name": pl.Utf8,
    "base_currency_symbol": pl.Utf8,
    "cik": pl.Utf8,
    "composite_figi": pl.Utf8,
    "currency_name": pl.Categorical,
    "currency_symbol": pl.Categorical,
    "delisted_utc": pl.Utf8,
    "last_updated_utc": pl.Utf8,
    "locale": pl.Categorical,
    "market": pl.Categorical,
    "name": pl.Utf8,
    "primary_exchange": pl.Categorical,
    "share_class_figi": pl.Utf8,
    "ticker": pl.Categorical,
    "type": pl.Categorical,
}
