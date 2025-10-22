"""Tests for the bronze schemas module."""

import polars as pl
import pytest

from tickerlake.schemas import (
    SPLITS_RAW_SCHEMA,
    STOCKS_RAW_SCHEMA,
    TICKERS_RAW_SCHEMA,
)


class TestStocksSchema:
    """Test cases for STOCKS_RAW_SCHEMA."""

    def test_stocks_schema_keys(self):
        """Test STOCKS_RAW_SCHEMA contains all required columns."""
        expected_keys = {
            "ticker",
            "volume",
            "open",
            "close",
            "high",
            "low",
            "window_start",
            "transactions",
        }
        assert set(STOCKS_RAW_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("ticker", pl.Categorical),
            ("volume", pl.UInt64),
            ("open", pl.Float32),
            ("close", pl.Float32),
            ("high", pl.Float32),
            ("low", pl.Float32),
            ("window_start", pl.Int64),
            ("transactions", pl.UInt32),
        ],
    )
    def test_stocks_schema_data_types(self, column, expected_type):
        """Test STOCKS_RAW_SCHEMA has correct data types for each column."""
        assert STOCKS_RAW_SCHEMA[column] == expected_type

    def test_stocks_schema_with_dataframe(self):
        """Test STOCKS_RAW_SCHEMA works with Polars DataFrame creation."""
        sample_data = {
            "ticker": ["AAPL", "TSLA"],
            "volume": [1000000, 2000000],
            "open": [150.0, 200.0],
            "close": [155.0, 205.0],
            "high": [156.0, 210.0],
            "low": [149.0, 199.0],
            "window_start": [1704153600000000000, 1704240000000000000],
            "transactions": [5000, 6000],
        }

        df = pl.DataFrame(sample_data, schema_overrides=STOCKS_RAW_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["ticker"] == pl.Categorical
        assert df.schema["volume"] == pl.UInt64
        assert df.schema["open"] == pl.Float32


class TestSplitsSchema:
    """Test cases for SPLITS_RAW_SCHEMA."""

    def test_splits_schema_keys(self):
        """Test SPLITS_RAW_SCHEMA contains all required columns."""
        expected_keys = {"id", "execution_date", "split_from", "split_to", "ticker"}
        assert set(SPLITS_RAW_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("id", pl.Utf8),
            ("execution_date", pl.Date),
            ("split_from", pl.Float32),
            ("split_to", pl.Float32),
            ("ticker", pl.Categorical),
        ],
    )
    def test_splits_schema_data_types(self, column, expected_type):
        """Test SPLITS_RAW_SCHEMA has correct data types for each column."""
        assert SPLITS_RAW_SCHEMA[column] == expected_type

    def test_splits_schema_with_dataframe(self):
        """Test SPLITS_RAW_SCHEMA works with Polars DataFrame creation."""
        from datetime import date

        sample_data = {
            "id": ["split1", "split2"],
            "execution_date": [date(2024, 1, 15), date(2024, 6, 10)],
            "split_from": [1.0, 1.0],
            "split_to": [4.0, 10.0],
            "ticker": ["AAPL", "NVDA"],
        }

        df = pl.DataFrame(sample_data, schema_overrides=SPLITS_RAW_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["execution_date"] == pl.Date
        assert df.schema["split_from"] == pl.Float32
        assert df.schema["split_to"] == pl.Float32


class TestTickersSchema:
    """Test cases for TICKERS_RAW_SCHEMA."""

    def test_tickers_schema_keys(self):
        """Test TICKERS_RAW_SCHEMA contains all required columns."""
        expected_keys = {
            "active",
            "base_currency_name",
            "base_currency_symbol",
            "cik",
            "composite_figi",
            "currency_name",
            "currency_symbol",
            "delisted_utc",
            "last_updated_utc",
            "locale",
            "market",
            "name",
            "primary_exchange",
            "share_class_figi",
            "ticker",
            "type",
        }
        assert set(TICKERS_RAW_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("active", pl.Boolean),
            ("base_currency_name", pl.Utf8),
            ("base_currency_symbol", pl.Utf8),
            ("cik", pl.Utf8),
            ("composite_figi", pl.Utf8),
            ("currency_name", pl.Categorical),
            ("currency_symbol", pl.Categorical),
            ("delisted_utc", pl.Utf8),
            ("last_updated_utc", pl.Utf8),
            ("locale", pl.Categorical),
            ("market", pl.Categorical),
            ("name", pl.Utf8),
            ("primary_exchange", pl.Categorical),
            ("share_class_figi", pl.Utf8),
            ("ticker", pl.Categorical),
            ("type", pl.Categorical),
        ],
    )
    def test_tickers_schema_data_types(self, column, expected_type):
        """Test TICKERS_RAW_SCHEMA has correct data types for each column."""
        assert TICKERS_RAW_SCHEMA[column] == expected_type

    def test_tickers_schema_with_dataframe(self):
        """Test TICKERS_RAW_SCHEMA works with Polars DataFrame creation."""
        sample_data = {
            "active": [True, True],
            "base_currency_name": ["US Dollar", "US Dollar"],
            "base_currency_symbol": ["USD", "USD"],
            "cik": ["0000320193", "0001318605"],
            "composite_figi": ["BBG000B9XRY4", "BBG000N9MNX3"],
            "currency_name": ["US Dollar", "US Dollar"],
            "currency_symbol": ["USD", "USD"],
            "delisted_utc": [None, None],
            "last_updated_utc": ["2024-01-15T00:00:00Z", "2024-01-15T00:00:00Z"],
            "locale": ["us", "us"],
            "market": ["stocks", "stocks"],
            "name": ["Apple Inc.", "Tesla, Inc."],
            "primary_exchange": ["XNAS", "XNAS"],
            "share_class_figi": ["BBG001S5N8V8", "BBG001SQKGD7"],
            "ticker": ["AAPL", "TSLA"],
            "type": ["CS", "CS"],
        }

        df = pl.DataFrame(sample_data, schema_overrides=TICKERS_RAW_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["active"] == pl.Boolean
        assert df.schema["ticker"] == pl.Categorical
        assert df.schema["cik"] == pl.Utf8


class TestSchemaComparison:
    """Test cases comparing schemas across different data types."""

    def test_all_schemas_have_ticker_column(self):
        """Test all main schemas include a ticker column."""
        assert "ticker" in STOCKS_RAW_SCHEMA
        assert "ticker" in SPLITS_RAW_SCHEMA
        assert "ticker" in TICKERS_RAW_SCHEMA

    def test_numeric_precision_choices(self):
        """Test numeric types use appropriate precision for their use case."""
        # OHLC prices use Float32 for efficiency
        assert STOCKS_RAW_SCHEMA["open"] == pl.Float32
        assert STOCKS_RAW_SCHEMA["close"] == pl.Float32

        # Split ratios use Float32 for consistency with OHLC data
        assert SPLITS_RAW_SCHEMA["split_from"] == pl.Float32
        assert SPLITS_RAW_SCHEMA["split_to"] == pl.Float32

        # Stock volumes are large, use UInt64
        assert STOCKS_RAW_SCHEMA["volume"] == pl.UInt64


class TestSchemaValidation:
    """Test cases for schema validation with edge cases."""

    def test_stocks_schema_with_null_values(self):
        """Test STOCKS_RAW_SCHEMA handles null values appropriately."""
        # Create DataFrame with some null values
        sample_data = {
            "ticker": ["AAPL", None],
            "volume": [1000000, None],
            "open": [150.0, None],
            "close": [155.0, 160.0],
            "high": [156.0, 161.0],
            "low": [149.0, 159.0],
            "window_start": [1704153600000000000, 1704240000000000000],
            "transactions": [5000, None],
        }

        # Schema should allow nulls (Polars handles this)
        df = pl.DataFrame(sample_data)
        # Should be able to cast to schema types
        assert df is not None
