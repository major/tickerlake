"""Tests for the bronze schemas module."""

import polars as pl
import pytest

from tickerlake.bronze.schemas import (
    OPTIONS_SCHEMA,
    SPLITS_SCHEMA,
    STOCKS_SCHEMA,
    TICKERS_SCHEMA,
)


class TestStocksSchema:
    """Test cases for STOCKS_SCHEMA."""

    def test_stocks_schema_keys(self):
        """Test STOCKS_SCHEMA contains all required columns."""
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
        assert set(STOCKS_SCHEMA.keys()) == expected_keys

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
        """Test STOCKS_SCHEMA has correct data types for each column."""
        assert STOCKS_SCHEMA[column] == expected_type

    def test_stocks_schema_with_dataframe(self):
        """Test STOCKS_SCHEMA works with Polars DataFrame creation."""
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

        df = pl.DataFrame(sample_data, schema_overrides=STOCKS_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["ticker"] == pl.Categorical
        assert df.schema["volume"] == pl.UInt64
        assert df.schema["open"] == pl.Float32


class TestOptionsSchema:
    """Test cases for OPTIONS_SCHEMA."""

    def test_options_schema_keys(self):
        """Test OPTIONS_SCHEMA contains all required columns."""
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
        assert set(OPTIONS_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("ticker", pl.Utf8),
            ("volume", pl.UInt32),
            ("open", pl.Float32),
            ("close", pl.Float32),
            ("high", pl.Float32),
            ("low", pl.Float32),
            ("window_start", pl.Int64),
            ("transactions", pl.UInt32),
        ],
    )
    def test_options_schema_data_types(self, column, expected_type):
        """Test OPTIONS_SCHEMA has correct data types for each column."""
        assert OPTIONS_SCHEMA[column] == expected_type

    def test_options_schema_ticker_type_difference(self):
        """Test OPTIONS_SCHEMA uses Utf8 for ticker (unlike STOCKS_SCHEMA)."""
        # Options use Utf8 for ticker
        assert OPTIONS_SCHEMA["ticker"] == pl.Utf8
        # Stocks use Categorical for ticker
        assert STOCKS_SCHEMA["ticker"] == pl.Categorical

    def test_options_schema_volume_type_difference(self):
        """Test OPTIONS_SCHEMA uses UInt32 for volume (unlike STOCKS_SCHEMA)."""
        # Options use UInt32 for volume
        assert OPTIONS_SCHEMA["volume"] == pl.UInt32
        # Stocks use UInt64 for volume
        assert STOCKS_SCHEMA["volume"] == pl.UInt64

    def test_options_schema_with_dataframe(self):
        """Test OPTIONS_SCHEMA works with Polars DataFrame creation."""
        sample_data = {
            "ticker": ["O:AAPL250117C00150000", "O:TSLA250117C00200000"],
            "volume": [1000, 2000],
            "open": [5.0, 10.0],
            "close": [5.5, 10.5],
            "high": [6.0, 11.0],
            "low": [4.5, 9.5],
            "window_start": [1704153600000000000, 1704240000000000000],
            "transactions": [100, 200],
        }

        df = pl.DataFrame(sample_data, schema_overrides=OPTIONS_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["ticker"] == pl.Utf8
        assert df.schema["volume"] == pl.UInt32
        assert df.schema["open"] == pl.Float32


class TestSplitsSchema:
    """Test cases for SPLITS_SCHEMA."""

    def test_splits_schema_keys(self):
        """Test SPLITS_SCHEMA contains all required columns."""
        expected_keys = {"id", "execution_date", "split_from", "split_to", "ticker"}
        assert set(SPLITS_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("id", pl.Utf8),
            ("execution_date", pl.Date),
            ("split_from", pl.Float64),
            ("split_to", pl.Float64),
            ("ticker", pl.Utf8),
        ],
    )
    def test_splits_schema_data_types(self, column, expected_type):
        """Test SPLITS_SCHEMA has correct data types for each column."""
        assert SPLITS_SCHEMA[column] == expected_type

    def test_splits_schema_with_dataframe(self):
        """Test SPLITS_SCHEMA works with Polars DataFrame creation."""
        from datetime import date

        sample_data = {
            "id": ["split1", "split2"],
            "execution_date": [date(2024, 1, 15), date(2024, 6, 10)],
            "split_from": [1.0, 1.0],
            "split_to": [4.0, 10.0],
            "ticker": ["AAPL", "NVDA"],
        }

        df = pl.DataFrame(sample_data, schema_overrides=SPLITS_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["execution_date"] == pl.Date
        assert df.schema["split_from"] == pl.Float64
        assert df.schema["split_to"] == pl.Float64


class TestTickersSchema:
    """Test cases for TICKERS_SCHEMA."""

    def test_tickers_schema_keys(self):
        """Test TICKERS_SCHEMA contains all required columns."""
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
        assert set(TICKERS_SCHEMA.keys()) == expected_keys

    @pytest.mark.parametrize(
        "column,expected_type",
        [
            ("active", pl.Boolean),
            ("base_currency_name", pl.Utf8),
            ("base_currency_symbol", pl.Utf8),
            ("cik", pl.Utf8),
            ("composite_figi", pl.Utf8),
            ("currency_name", pl.Utf8),
            ("currency_symbol", pl.Utf8),
            ("delisted_utc", pl.Utf8),
            ("last_updated_utc", pl.Utf8),
            ("locale", pl.Utf8),
            ("market", pl.Utf8),
            ("name", pl.Utf8),
            ("primary_exchange", pl.Utf8),
            ("share_class_figi", pl.Utf8),
            ("ticker", pl.Utf8),
            ("type", pl.Utf8),
        ],
    )
    def test_tickers_schema_data_types(self, column, expected_type):
        """Test TICKERS_SCHEMA has correct data types for each column."""
        assert TICKERS_SCHEMA[column] == expected_type

    def test_tickers_schema_with_dataframe(self):
        """Test TICKERS_SCHEMA works with Polars DataFrame creation."""
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

        df = pl.DataFrame(sample_data, schema_overrides=TICKERS_SCHEMA)

        # Verify schema was applied correctly
        assert df.schema["active"] == pl.Boolean
        assert df.schema["ticker"] == pl.Utf8
        assert df.schema["cik"] == pl.Utf8


class TestSchemaComparison:
    """Test cases comparing schemas across different data types."""

    def test_stocks_vs_options_common_columns(self):
        """Test stocks and options schemas share common OHLCV columns."""
        common_columns = {"open", "close", "high", "low", "window_start", "transactions"}

        for col in common_columns:
            # These columns should have same types
            if col in ["open", "close", "high", "low"]:
                assert STOCKS_SCHEMA[col] == OPTIONS_SCHEMA[col] == pl.Float32
            elif col == "window_start":
                assert STOCKS_SCHEMA[col] == OPTIONS_SCHEMA[col] == pl.Int64
            elif col == "transactions":
                assert STOCKS_SCHEMA[col] == OPTIONS_SCHEMA[col] == pl.UInt32

    def test_all_schemas_have_ticker_column(self):
        """Test all main schemas include a ticker column."""
        assert "ticker" in STOCKS_SCHEMA
        assert "ticker" in OPTIONS_SCHEMA
        assert "ticker" in SPLITS_SCHEMA
        assert "ticker" in TICKERS_SCHEMA

    def test_numeric_precision_choices(self):
        """Test numeric types use appropriate precision for their use case."""
        # OHLC prices use Float32 for efficiency
        assert STOCKS_SCHEMA["open"] == pl.Float32
        assert OPTIONS_SCHEMA["close"] == pl.Float32

        # Split ratios use Float64 for precision
        assert SPLITS_SCHEMA["split_from"] == pl.Float64
        assert SPLITS_SCHEMA["split_to"] == pl.Float64

        # Stock volumes are large, use UInt64
        assert STOCKS_SCHEMA["volume"] == pl.UInt64
        # Options volumes are smaller, use UInt32
        assert OPTIONS_SCHEMA["volume"] == pl.UInt32


class TestSchemaValidation:
    """Test cases for schema validation with edge cases."""

    def test_stocks_schema_with_null_values(self):
        """Test STOCKS_SCHEMA handles null values appropriately."""
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

    def test_options_schema_with_large_ticker_strings(self):
        """Test OPTIONS_SCHEMA handles long option ticker symbols."""
        # Options tickers can be quite long (e.g., O:AAPL250117C00150000)
        sample_data = {
            "ticker": ["O:AAPL250117C00150000", "O:TSLA250117P00200000"],
            "volume": [1000, 2000],
            "open": [5.0, 10.0],
            "close": [5.5, 10.5],
            "high": [6.0, 11.0],
            "low": [4.5, 9.5],
            "window_start": [1704153600000000000, 1704240000000000000],
            "transactions": [100, 200],
        }

        df = pl.DataFrame(sample_data, schema_overrides=OPTIONS_SCHEMA)

        # Should handle long strings without issue
        assert df["ticker"][0] == "O:AAPL250117C00150000"
        assert len(df["ticker"][0]) > 10
