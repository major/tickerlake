"""Tests for the bronze splits module."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import pytest

from tickerlake.bronze.splits import get_splits, load_splits


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.bronze.splits.settings") as mock:
        mock.bronze_unified_storage_path = "s3://tickerlake/unified/bronze"
        yield mock


@pytest.fixture
def mock_polygon_client():
    """Create a mock Polygon API client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def sample_split_data():
    """Sample stock split data from Polygon API."""
    # Create mock split objects
    split1 = Mock()
    split1.ticker = "AAPL"
    split1.execution_date = "2024-01-15"
    split1.split_from = 1.0
    split1.split_to = 4.0

    split2 = Mock()
    split2.ticker = "TSLA"
    split2.execution_date = "2023-08-25"
    split2.split_from = 1.0
    split2.split_to = 3.0

    split3 = Mock()
    split3.ticker = "NVDA"
    split3.execution_date = "2024-06-10"
    split3.split_from = 1.0
    split3.split_to = 10.0

    return [split1, split2, split3]


@pytest.fixture
def expected_splits_df():
    """Expected DataFrame from sample split data."""
    return pl.DataFrame(
        {
            "ticker": ["AAPL", "TSLA", "NVDA"],
            "execution_date": [
                date(2024, 1, 15),
                date(2023, 8, 25),
                date(2024, 6, 10),
            ],
            "split_from": [1.0, 1.0, 1.0],
            "split_to": [4.0, 3.0, 10.0],
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

        # Check first row data
        first_row = result.row(0, named=True)
        assert first_row["ticker"] == "AAPL"
        assert first_row["execution_date"] == date(2024, 1, 15)
        assert first_row["split_from"] == 1.0
        assert first_row["split_to"] == 4.0

        # Check data types
        assert result.schema["ticker"] == pl.Utf8
        assert result.schema["execution_date"] == pl.Date
        assert result.schema["split_from"] == pl.Float64
        assert result.schema["split_to"] == pl.Float64

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


class TestLoadSplits:
    """Test cases for load_splits function."""

    @patch("tickerlake.bronze.splits.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.splits.get_splits")
    def test_load_splits_writes_parquet(
        self, mock_get_splits, mock_settings, expected_splits_df
    ):
        """Test load_splits writes DataFrame to Parquet file."""
        mock_get_splits.return_value = expected_splits_df

        # Mock the write_parquet method
        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_splits()

            # Verify write_parquet was called with correct parameters
            mock_write.assert_called_once_with(
                "s3://tickerlake/unified/bronze/splits/splits.parquet",
                storage_options={"option": "value"},
            )

    @patch("tickerlake.bronze.splits.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.splits.get_splits")
    def test_load_splits_calls_get_splits(self, mock_get_splits, mock_settings):
        """Test load_splits calls get_splits to retrieve data."""
        mock_df = pl.DataFrame(
            {
                "ticker": ["TEST"],
                "execution_date": [date(2024, 1, 1)],
                "split_from": [1.0],
                "split_to": [2.0],
            }
        )
        mock_get_splits.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet"):
            load_splits()

            # Verify get_splits was called
            mock_get_splits.assert_called_once()

    @patch("tickerlake.bronze.splits.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.splits.get_splits")
    def test_load_splits_empty_dataframe(self, mock_get_splits, mock_settings):
        """Test load_splits handles empty DataFrame correctly."""
        mock_df = pl.DataFrame(
            schema={
                "ticker": pl.Utf8,
                "execution_date": pl.Date,
                "split_from": pl.Float64,
                "split_to": pl.Float64,
            }
        )
        mock_get_splits.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_splits()

            # Should still write even if empty
            mock_write.assert_called_once()

    @patch("tickerlake.bronze.splits.s3_storage_options", {"aws_region": "us-east-1"})
    @patch("tickerlake.bronze.splits.get_splits")
    def test_load_splits_storage_options(self, mock_get_splits, mock_settings):
        """Test load_splits uses correct storage options."""
        mock_df = pl.DataFrame(
            {
                "ticker": ["TEST"],
                "execution_date": [date(2024, 1, 1)],
                "split_from": [1.0],
                "split_to": [2.0],
            }
        )
        mock_get_splits.return_value = mock_df

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_splits()

            # Verify storage options were passed
            call_kwargs = mock_write.call_args.kwargs
            assert "storage_options" in call_kwargs
            assert call_kwargs["storage_options"]["aws_region"] == "us-east-1"


class TestIntegration:
    """Integration tests for splits module."""

    @patch("tickerlake.bronze.splits.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.splits.setup_polygon_api_client")
    def test_end_to_end_splits_loading(
        self, mock_setup_client, mock_polygon_client, sample_split_data, mock_settings
    ):
        """Test complete flow from API to Parquet file."""
        mock_setup_client.return_value = mock_polygon_client
        mock_polygon_client.list_splits.return_value = sample_split_data

        with patch.object(pl.DataFrame, "write_parquet") as mock_write:
            load_splits()

            # Verify the complete chain
            mock_setup_client.assert_called_once()
            mock_polygon_client.list_splits.assert_called_once()
            mock_write.assert_called_once()

            # Verify the path is correct
            written_path = mock_write.call_args.args[0]
            assert written_path.endswith("/splits/splits.parquet")
            assert "bronze" in written_path
