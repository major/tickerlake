"""Tests for the bronze unified layer module."""

from datetime import date
from unittest.mock import MagicMock, Mock, patch

import polars as pl
import pytest

from tickerlake.bronze.unified import (
    get_missing_dates,
    list_available_flatfiles,
    list_available_options_flatfiles,
    list_available_stocks_flatfiles,
    load_polygon_flatfiles,
    options_flatfiles_valid_years,
    previously_stored_dates,
    stocks_flatfiles_valid_years,
    valid_flatfiles_years,
)


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.bronze.unified.settings") as mock:
        mock.polygon_flatfiles_stocks_first_year = 2020
        mock.polygon_flatfiles_options_first_year = 2022
        mock.polygon_flatfiles_stocks = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
        mock.polygon_flatfiles_options = "s3://flatfiles/us_options_opra/day_aggs_v1"
        mock.bronze_unified_storage_path = "s3://tickerlake/unified/bronze"
        yield mock


@pytest.fixture
def mock_polygon_s3():
    """Create a mock S3 filesystem for Polygon flatfiles."""
    mock_s3 = MagicMock()
    return mock_s3


@pytest.fixture
def sample_stock_paths():
    """Sample stock flatfile paths from Polygon S3."""
    return [
        "flatfiles/us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz",
        "flatfiles/us_stocks_sip/day_aggs_v1/2024/01/2024-01-03.csv.gz",
        "flatfiles/us_stocks_sip/day_aggs_v1/2023/12/2023-12-29.csv.gz",
    ]


@pytest.fixture
def sample_options_paths():
    """Sample options flatfile paths from Polygon S3."""
    return [
        "flatfiles/us_options_opra/day_aggs_v1/2024/01/2024-01-02.csv.gz",
        "flatfiles/us_options_opra/day_aggs_v1/2024/01/2024-01-03.csv.gz",
    ]


class TestValidFlatfilesYears:
    """Test cases for valid flatfiles year functions."""

    @pytest.mark.parametrize(
        "first_year,current_year,expected_years",
        [
            # Test with 5 years of history
            (2020, 2025, [2025, 2024, 2023, 2022, 2021, 2020]),
            # Test with 2 years of history
            (2023, 2025, [2025, 2024, 2023]),
            # Test current year only
            (2025, 2025, [2025]),
            # Test including future year (current implementation allows this)
            (2024, 2025, [2025, 2024]),
        ],
    )
    def test_valid_flatfiles_years(self, first_year, current_year, expected_years):
        """Test valid_flatfiles_years with various year ranges."""
        with patch("tickerlake.bronze.unified.date") as mock_date:
            mock_date.today.return_value = date(current_year, 1, 1)
            result = valid_flatfiles_years(first_year)
            assert result == expected_years

    @patch("tickerlake.bronze.unified.settings")
    @patch("tickerlake.bronze.unified.date")
    def test_stocks_flatfiles_valid_years(self, mock_date, mock_settings):
        """Test stocks_flatfiles_valid_years uses correct settings."""
        mock_date.today.return_value = date(2025, 1, 1)
        mock_settings.polygon_flatfiles_stocks_first_year = 2022

        result = stocks_flatfiles_valid_years()

        assert result == [2025, 2024, 2023, 2022]

    @patch("tickerlake.bronze.unified.settings")
    @patch("tickerlake.bronze.unified.date")
    def test_options_flatfiles_valid_years(self, mock_date, mock_settings):
        """Test options_flatfiles_valid_years uses correct settings."""
        mock_date.today.return_value = date(2025, 1, 1)
        mock_settings.polygon_flatfiles_options_first_year = 2023

        result = options_flatfiles_valid_years()

        assert result == [2025, 2024, 2023]


class TestListAvailableFlatfiles:
    """Test cases for listing available flatfiles from Polygon S3."""

    def test_list_available_flatfiles(
        self, mock_polygon_s3, sample_stock_paths, mock_settings
    ):
        """Test list_available_flatfiles returns properly formatted paths."""
        flatfiles_path = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
        valid_years = [2024, 2023]

        # Setup mock S3 to return file paths
        mock_polygon_s3.glob.side_effect = [
            [sample_stock_paths[0], sample_stock_paths[1]],  # 2024 files
            [sample_stock_paths[2]],  # 2023 files
        ]

        with patch(
            "tickerlake.bronze.unified.setup_polygon_flatfiles_client",
            return_value=mock_polygon_s3,
        ):
            result = list_available_flatfiles(flatfiles_path, valid_years)

        # Should have 3 files total
        assert len(result) == 3
        # Results should be sorted in reverse (newest first)
        # All paths should start with s3://
        assert all(path.startswith("s3://") for path in result)
        # Check that all sample paths are in the results
        assert f"s3://{sample_stock_paths[0]}" in result
        assert f"s3://{sample_stock_paths[1]}" in result
        assert f"s3://{sample_stock_paths[2]}" in result
        # Check glob was called for each year
        assert mock_polygon_s3.glob.call_count == 2

    def test_list_available_flatfiles_empty_results(
        self, mock_polygon_s3, mock_settings
    ):
        """Test list_available_flatfiles handles empty results gracefully."""
        flatfiles_path = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
        valid_years = [2024]

        mock_polygon_s3.glob.return_value = []

        with patch(
            "tickerlake.bronze.unified.setup_polygon_flatfiles_client",
            return_value=mock_polygon_s3,
        ):
            result = list_available_flatfiles(flatfiles_path, valid_years)

        assert result == []
        mock_polygon_s3.glob.assert_called_once()

    @patch("tickerlake.bronze.unified.list_available_flatfiles")
    @patch("tickerlake.bronze.unified.stocks_flatfiles_valid_years")
    def test_list_available_stocks_flatfiles(
        self, mock_valid_years, mock_list_flatfiles, mock_settings
    ):
        """Test list_available_stocks_flatfiles uses correct parameters."""
        mock_valid_years.return_value = [2024, 2023]
        mock_list_flatfiles.return_value = ["file1.csv.gz", "file2.csv.gz"]

        result = list_available_stocks_flatfiles()

        mock_list_flatfiles.assert_called_once_with(
            flatfiles_path=mock_settings.polygon_flatfiles_stocks,
            valid_years=[2024, 2023],
        )
        assert result == ["file1.csv.gz", "file2.csv.gz"]

    @patch("tickerlake.bronze.unified.list_available_flatfiles")
    @patch("tickerlake.bronze.unified.options_flatfiles_valid_years")
    def test_list_available_options_flatfiles(
        self, mock_valid_years, mock_list_flatfiles, mock_settings
    ):
        """Test list_available_options_flatfiles uses correct parameters."""
        mock_valid_years.return_value = [2024, 2023]
        mock_list_flatfiles.return_value = ["file1.csv.gz", "file2.csv.gz"]

        result = list_available_options_flatfiles()

        mock_list_flatfiles.assert_called_once_with(
            flatfiles_path=mock_settings.polygon_flatfiles_options,
            valid_years=[2024, 2023],
        )
        assert result == ["file1.csv.gz", "file2.csv.gz"]


class TestGetMissingDates:
    """Test cases for identifying missing dates in flatfiles."""

    @pytest.mark.parametrize(
        "already_stored,all_files,expected_missing",
        [
            # No files stored yet - all files are missing
            (
                [],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                    "s3://path/2024-01-04.csv.gz",
                ],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                    "s3://path/2024-01-04.csv.gz",
                ],
            ),
            # Some files already stored
            (
                ["2024-01-02", "2024-01-03"],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                    "s3://path/2024-01-04.csv.gz",
                ],
                ["s3://path/2024-01-04.csv.gz"],
            ),
            # All files already stored
            (
                ["2024-01-02", "2024-01-03", "2024-01-04"],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                    "s3://path/2024-01-04.csv.gz",
                ],
                [],
            ),
            # Dates stored but no matching files
            (
                ["2024-01-05", "2024-01-06"],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                ],
                [
                    "s3://path/2024-01-02.csv.gz",
                    "s3://path/2024-01-03.csv.gz",
                ],
            ),
            # Empty file list
            (["2024-01-02"], [], []),
        ],
    )
    def test_get_missing_dates_scenarios(
        self, already_stored, all_files, expected_missing
    ):
        """Test get_missing_dates with various scenarios."""
        result = get_missing_dates(already_stored, all_files)
        assert result == expected_missing

    def test_get_missing_dates_complex_paths(self):
        """Test get_missing_dates with complex S3 paths."""
        already_stored = ["2024-01-02"]
        all_files = [
            "s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/01/2024-01-02.csv.gz",
            "s3://flatfiles/us_stocks_sip/day_aggs_v1/2024/01/2024-01-03.csv.gz",
        ]

        result = get_missing_dates(already_stored, all_files)

        # Only the file without 2024-01-02 in its name should be returned
        assert len(result) == 1
        assert "2024-01-03" in result[0]


class TestPreviouslyStoredDates:
    """Test cases for retrieving previously stored dates."""

    @patch("tickerlake.bronze.unified.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.unified.pl.scan_parquet")
    def test_previously_stored_dates_success(self, mock_scan_parquet):
        """Test previously_stored_dates retrieves dates from Parquet files."""
        # Create mock DataFrame with dates
        mock_dates = pl.DataFrame({"date": [date(2024, 1, 2), date(2024, 1, 3)]})

        # Setup mock lazy frame
        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.unique.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_dates.with_columns(
            pl.col("date").dt.strftime("%Y-%m-%d").alias("date")
        )

        mock_scan_parquet.return_value = mock_lf

        result = previously_stored_dates("s3://test/destination")

        assert result == ["2024-01-02", "2024-01-03"]
        mock_scan_parquet.assert_called_once()

    @patch("tickerlake.bronze.unified.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.unified.pl.scan_parquet")
    def test_previously_stored_dates_empty(self, mock_scan_parquet):
        """Test previously_stored_dates handles empty results."""
        mock_dates = pl.DataFrame({"date": []}, schema={"date": pl.Utf8})

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.unique.return_value = mock_lf
        mock_lf.sort.return_value = mock_lf
        mock_lf.collect.return_value = mock_dates

        mock_scan_parquet.return_value = mock_lf

        result = previously_stored_dates("s3://test/destination")

        assert result == []


class TestLoadPolygonFlatfiles:
    """Test cases for loading Polygon flatfiles."""

    @patch("tickerlake.bronze.unified.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.unified.POLYGON_STORAGE_OPTIONS", {"polygon": "opts"})
    @patch("tickerlake.bronze.unified.pl.scan_csv")
    @patch("tickerlake.bronze.unified.tqdm")
    def test_load_polygon_flatfiles_single_file(
        self, mock_tqdm, mock_scan_csv, mock_settings
    ):
        """Test load_polygon_flatfiles processes a single file correctly."""
        # Setup test data
        files = ["s3://polygon/2024/01/2024-01-02.csv.gz"]
        destination = "s3://dest/stocks"
        schema = {"ticker": pl.Utf8, "window_start": pl.Int64}

        # Setup mock lazy frame
        mock_lf = MagicMock()
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.drop.return_value = mock_lf
        mock_collected = MagicMock()
        mock_lf.collect.return_value = mock_collected

        mock_scan_csv.return_value = mock_lf

        # Mock tqdm context manager
        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(files))

        load_polygon_flatfiles(files, destination, schema)

        # Verify CSV was scanned with correct options
        mock_scan_csv.assert_called_once_with(
            files[0],
            storage_options={"polygon": "opts"},
            schema_overrides=schema,
        )

        # Verify DataFrame was written to parquet
        mock_collected.write_parquet.assert_called_once_with(
            destination,
            partition_by=["date"],
            storage_options={"option": "value"},
        )

    @patch("tickerlake.bronze.unified.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.unified.POLYGON_STORAGE_OPTIONS", {"polygon": "opts"})
    @patch("tickerlake.bronze.unified.pl.scan_csv")
    @patch("tickerlake.bronze.unified.tqdm")
    def test_load_polygon_flatfiles_multiple_files(
        self, mock_tqdm, mock_scan_csv, mock_settings
    ):
        """Test load_polygon_flatfiles processes multiple files."""
        files = [
            "s3://polygon/2024/01/2024-01-02.csv.gz",
            "s3://polygon/2024/01/2024-01-03.csv.gz",
        ]
        destination = "s3://dest/stocks"
        schema = {"ticker": pl.Utf8}

        # Setup mocks
        mock_lf = MagicMock()
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.drop.return_value = mock_lf
        mock_collected = MagicMock()
        mock_lf.collect.return_value = mock_collected

        mock_scan_csv.return_value = mock_lf

        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(files))

        load_polygon_flatfiles(files, destination, schema)

        # Should scan both files
        assert mock_scan_csv.call_count == 2
        # Should write both times
        assert mock_collected.write_parquet.call_count == 2

    @patch("tickerlake.bronze.unified.s3_storage_options", {"option": "value"})
    @patch("tickerlake.bronze.unified.POLYGON_STORAGE_OPTIONS", {"polygon": "opts"})
    @patch("tickerlake.bronze.unified.pl.scan_csv")
    @patch("tickerlake.bronze.unified.tqdm")
    def test_load_polygon_flatfiles_handles_oserror(
        self, mock_tqdm, mock_scan_csv, mock_settings
    ):
        """Test load_polygon_flatfiles handles OSError gracefully."""
        files = [
            "s3://polygon/2024/01/2024-01-02.csv.gz",
            "s3://polygon/2024/01/2024-01-03.csv.gz",
        ]
        destination = "s3://dest/stocks"
        schema = {"ticker": pl.Utf8}

        # Setup mocks - first file succeeds, second raises OSError
        mock_lf = MagicMock()
        mock_lf.with_columns.return_value = mock_lf
        mock_lf.drop.return_value = mock_lf
        mock_collected = MagicMock()
        mock_lf.collect.return_value = mock_collected

        # First call succeeds, second raises OSError
        mock_collected.write_parquet.side_effect = [None, OSError("API limit")]

        mock_scan_csv.return_value = mock_lf

        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(files))

        # Should not raise, just log and break
        load_polygon_flatfiles(files, destination, schema)

        # Should have tried both files
        assert mock_scan_csv.call_count == 2
        # But only first write succeeded
        assert mock_collected.write_parquet.call_count == 2

    @patch("tickerlake.bronze.unified.tqdm")
    def test_load_polygon_flatfiles_empty_file_list(self, mock_tqdm):
        """Test load_polygon_flatfiles handles empty file list."""
        files = []
        destination = "s3://dest/stocks"
        schema = {"ticker": pl.Utf8}

        mock_pbar = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_pbar
        mock_pbar.__iter__ = Mock(return_value=iter(files))

        # Should not raise any errors
        load_polygon_flatfiles(files, destination, schema)

        # Progress bar should be created but no files processed
        mock_tqdm.assert_called_once()
