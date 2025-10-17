"""Schemas for polars dataframes used in the bronze unified layer."""

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

OPTIONS_SCHEMA = {
    "ticker": pl.Utf8,
    "volume": pl.UInt32,
    "open": pl.Float32,
    "close": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "window_start": pl.Int64,
    "transactions": pl.UInt32,
}

SPLITS_SCHEMA = {
    "id": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float64,
    "split_to": pl.Float64,
    "ticker": pl.Utf8,
}

TICKERS_SCHEMA = {
    "active": pl.Boolean,
    "base_currency_name": pl.Utf8,
    "base_currency_symbol": pl.Utf8,
    "cik": pl.Utf8,
    "composite_figi": pl.Utf8,
    "currency_name": pl.Utf8,
    "currency_symbol": pl.Utf8,
    "delisted_utc": pl.Utf8,
    "last_updated_utc": pl.Utf8,
    "locale": pl.Utf8,
    "market": pl.Utf8,
    "name": pl.Utf8,
    "primary_exchange": pl.Utf8,
    "share_class_figi": pl.Utf8,
    "ticker": pl.Utf8,
    "type": pl.Utf8,
}
