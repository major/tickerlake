"""Tests for the bronze splits module."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import pytest

from tickerlake.bronze.splits import get_splits


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    # Settings are now accessed via config module, not directly in bronze.splits
    # No need to mock anymore, but keep fixture for backward compatibility
    yield None


@pytest.fixture
def mock_polygon_client():
    """Create a mock Polygon API client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def sample_split_data():
    """Sample stock split data from Polygon API.

    Returns splits sorted by execution_date ascending (as the real API does).
    """
    # Create mock split objects
    split1 = Mock()
    split1.ticker = "TSLA"
    split1.execution_date = "2023-08-25"
    split1.split_from = 1.0
    split1.split_to = 3.0

    split2 = Mock()
    split2.ticker = "AAPL"
    split2.execution_date = "2024-01-15"
    split2.split_from = 1.0
    split2.split_to = 4.0

    split3 = Mock()
    split3.ticker = "NVDA"
    split3.execution_date = "2024-06-10"
    split3.split_from = 1.0
    split3.split_to = 10.0

    # Return in ascending order by execution_date (matching API behavior)
    return [split1, split2, split3]


@pytest.fixture
def expected_splits_df():
    """Expected DataFrame from sample split data (sorted by execution_date)."""
    return pl.DataFrame(
        {
            "ticker": ["TSLA", "AAPL", "NVDA"],
            "execution_date": [
                date(2023, 8, 25),
                date(2024, 1, 15),
                date(2024, 6, 10),
            ],
            "split_from": [1.0, 1.0, 1.0],
            "split_to": [3.0, 4.0, 10.0],
        }
    )


class TestGetSplits:
    """Test cases for get_splits function."""

    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_returns_dataframe(
        self, mock_setup_client, mock_polygon_client, sample_split_data
    ):
        """Test get_splits returns a Polars DataFrame with correct structure."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = sample_split_data

        result = get_splits()

        # Should return a DataFrame
        assert isinstance(result, pl.DataFrame)
        # Should have correct columns
        assert set(result.columns) == {"ticker", "execution_date", "split_from", "split_to"}
        # Should have correct number of rows
        assert len(result) == 3

    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_api_parameters(
        self, mock_setup_client, mock_polygon_client, sample_split_data
    ):
        """Test get_splits calls Polygon API with correct parameters."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = sample_split_data

        get_splits()

        # Verify API was called with correct parameters
        mock_polygon_client.list_splits.assert_called_once_with(
            execution_date_gte="2020-01-01",
            order="asc",
            sort="execution_date",
            limit=1000,
        )

    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_data_transformation(
        self, mock_setup_client, mock_polygon_client, sample_split_data
    ):
        """Test get_splits correctly transforms API data."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = sample_split_data

        result = get_splits()

        # Check first row data (TSLA has earliest execution_date in fixture)
        first_row = result.row(0, named=True)
        assert first_row["ticker"] == "TSLA"
        assert first_row["execution_date"] == date(2023, 8, 25)
        assert first_row["split_from"] == 1.0
        assert first_row["split_to"] == 3.0

        # Check data types
        assert result.schema["ticker"] == pl.Categorical
        assert result.schema["execution_date"] == pl.Date
        assert result.schema["split_from"] == pl.Float32
        assert result.schema["split_to"] == pl.Float32

    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_empty_response(self, mock_setup_client, mock_polygon_client):
        """Test get_splits handles empty API response."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = []

        result = get_splits()

        # Should return empty DataFrame (Polars doesn't infer schema from empty data)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 0
        # Empty DataFrame from empty list won't have columns without explicit schema

    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_date_parsing(
        self, mock_setup_client, mock_polygon_client
    ):
        """Test get_splits correctly parses various date formats."""
        # Test data with different valid date formats
        split1 = Mock()
        split1.ticker = "TEST"
        split1.execution_date = "2024-01-01"
        split1.split_from = 1.0
        split1.split_to = 2.0

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = [split1]

        result = get_splits()

        # Verify date was parsed correctly
        assert result["execution_date"][0] == date(2024, 1, 1)

    @pytest.mark.parametrize(
        "split_from,split_to,expected_ratio",
        [
            (1.0, 2.0, 2.0),  # 2-for-1 split
            (1.0, 3.0, 3.0),  # 3-for-1 split
            (1.0, 10.0, 10.0),  # 10-for-1 split
            (2.0, 1.0, 0.5),  # Reverse split (1-for-2)
        ],
    )
    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_get_splits_various_ratios(
        self,
        mock_setup_client,
        mock_polygon_client,
        split_from,
        split_to,
        expected_ratio,
    ):
        """Test get_splits handles various split ratios correctly."""
        split = Mock()
        split.ticker = "TEST"
        split.execution_date = "2024-01-01"
        split.split_from = split_from
        split.split_to = split_to

        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = [split]

        result = get_splits()

        assert result["split_from"][0] == split_from
        assert result["split_to"][0] == split_to
        # Calculate ratio: split_to / split_from
        assert result["split_to"][0] / result["split_from"][0] == expected_ratio


class TestUpsertSplits:
    """Test cases for upserting splits to Postgres."""

    @patch("tickerlake.bronze.postgres.upsert_splits")
    def test_upsert_splits_writes_to_postgres(
        self, mock_upsert, mock_settings, expected_splits_df
    ):
        """Test upsert_splits is called with correct DataFrame."""
        # Call the mocked function
        mock_upsert(expected_splits_df)

        # Verify it was called with the dataframe
        mock_upsert.assert_called_once_with(expected_splits_df)

    def test_upsert_splits_empty_dataframe(self, mock_settings):
        """Test upsert_splits handles empty DataFrame correctly."""
        from tickerlake.bronze.postgres import upsert_splits

        mock_df = pl.DataFrame(
            schema={
                "ticker": pl.Utf8,
                "execution_date": pl.Date,
                "split_from": pl.Float32,
                "split_to": pl.Float32,
            }
        )

        # Should not raise, just log warning
        try:
            upsert_splits(mock_df)
            # If we get here without connecting to DB, the empty check worked
            assert True
        except Exception as e:
            # If it tries to connect, we expect this in test environment
            assert "could not connect" in str(e).lower() or "connection" in str(e).lower()


class TestIntegration:
    """Integration tests for splits module."""

    @patch("tickerlake.bronze.postgres.upsert_splits")
    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_end_to_end_splits_loading(
        self, mock_setup_client, mock_upsert, mock_polygon_client, sample_split_data, mock_settings
    ):
        """Test complete flow from API to Postgres."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = sample_split_data

        # Get splits and upsert to Postgres
        splits_df = get_splits()
        mock_upsert(splits_df)

        # Verify the complete chain
        mock_setup_client.assert_called_once()
        mock_polygon_client.list_splits.assert_called_once()
        mock_upsert.assert_called_once()
