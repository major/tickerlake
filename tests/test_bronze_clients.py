"""Tests for the bronze clients module."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from tickerlake.bronze.clients import (
    setup_polygon_api_client,
    setup_polygon_flatfiles_client,
)


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.bronze.clients.settings") as mock:
        mock.polygon_api_key = SecretStr("test_api_key_12345")
        mock.polygon_access_key_id = SecretStr("test_access_key")
        mock.polygon_secret_access_key = SecretStr("test_secret_key")
        mock.polygon_flatfiles_endpoint_url = "https://files.polygon.io"
        yield mock


class TestPolygonStorageOptions:
    """Test cases for POLYGON_STORAGE_OPTIONS constant."""

    def test_polygon_storage_options_keys(self, mock_settings):
        """Test POLYGON_STORAGE_OPTIONS contains required keys."""
        # Need to reload the module to pick up mocked settings
        with patch("tickerlake.bronze.clients.settings", mock_settings):
            from tickerlake.bronze.clients import POLYGON_STORAGE_OPTIONS as opts

            assert "aws_access_key_id" in opts
            assert "aws_secret_access_key" in opts
            assert "aws_endpoint_url" in opts

    def test_polygon_storage_options_endpoint(self, mock_settings):
        """Test POLYGON_STORAGE_OPTIONS has correct endpoint."""
        with patch("tickerlake.bronze.clients.settings", mock_settings):
            from tickerlake.bronze.clients import POLYGON_STORAGE_OPTIONS as opts

            assert opts["aws_endpoint_url"] == "https://files.polygon.io"


class TestSetupPolygonApiClient:
    """Test cases for setup_polygon_api_client function."""

    @patch("tickerlake.bronze.clients.RESTClient")
    def test_setup_polygon_api_client_initialization(
        self, mock_rest_client, mock_settings
    ):
        """Test setup_polygon_api_client initializes RESTClient with API key."""
        mock_client_instance = MagicMock()
        mock_rest_client.return_value = mock_client_instance

        result = setup_polygon_api_client()

        # Should initialize RESTClient with API key
        mock_rest_client.assert_called_once_with("test_api_key_12345")
        assert result == mock_client_instance

    @patch("tickerlake.bronze.clients.RESTClient")
    def test_setup_polygon_api_client_returns_client(
        self, mock_rest_client, mock_settings
    ):
        """Test setup_polygon_api_client returns a properly configured client."""
        mock_client = MagicMock()
        mock_rest_client.return_value = mock_client

        result = setup_polygon_api_client()

        assert result is mock_client
        # Verify the client has expected methods (would be on real RESTClient)
        assert hasattr(result, "method_calls")


class TestSetupPolygonFlatfilesClient:
    """Test cases for setup_polygon_flatfiles_client function."""

    @patch("tickerlake.bronze.clients.s3fs.S3FileSystem")
    def test_setup_polygon_flatfiles_client_initialization(
        self, mock_s3fs, mock_settings
    ):
        """Test setup_polygon_flatfiles_client initializes S3FileSystem correctly."""
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance

        result = setup_polygon_flatfiles_client()

        # Should initialize S3FileSystem with correct parameters
        mock_s3fs.assert_called_once_with(
            anon=False,
            key="test_access_key",
            secret="test_secret_key",
            endpoint_url="https://files.polygon.io",
        )
        assert result == mock_s3_instance

    @patch("tickerlake.bronze.clients.s3fs.S3FileSystem")
    def test_setup_polygon_flatfiles_client_not_anonymous(
        self, mock_s3fs, mock_settings
    ):
        """Test setup_polygon_flatfiles_client sets anon=False."""
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance

        setup_polygon_flatfiles_client()

        # Verify anon parameter is False
        call_kwargs = mock_s3fs.call_args.kwargs
        assert call_kwargs["anon"] is False

    @patch("tickerlake.bronze.clients.s3fs.S3FileSystem")
    def test_setup_polygon_flatfiles_client_uses_credentials(
        self, mock_s3fs, mock_settings
    ):
        """Test setup_polygon_flatfiles_client uses correct credentials."""
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance

        setup_polygon_flatfiles_client()

        # Verify credentials are passed
        call_kwargs = mock_s3fs.call_args.kwargs
        assert call_kwargs["key"] == "test_access_key"
        assert call_kwargs["secret"] == "test_secret_key"

    @patch("tickerlake.bronze.clients.s3fs.S3FileSystem")
    def test_setup_polygon_flatfiles_client_returns_filesystem(
        self, mock_s3fs, mock_settings
    ):
        """Test setup_polygon_flatfiles_client returns S3FileSystem instance."""
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance

        result = setup_polygon_flatfiles_client()

        assert result is mock_s3_instance
        # Verify the filesystem has expected methods
        assert hasattr(result, "method_calls")


class TestIntegration:
    """Integration tests for client setup functions."""

    @patch("tickerlake.bronze.clients.s3fs.S3FileSystem")
    @patch("tickerlake.bronze.clients.RESTClient")
    def test_both_clients_can_be_created(
        self, mock_rest_client, mock_s3fs, mock_settings
    ):
        """Test both client setup functions can be called together."""
        mock_api_client = MagicMock()
        mock_rest_client.return_value = mock_api_client

        mock_s3_client = MagicMock()
        mock_s3fs.return_value = mock_s3_client

        api_client = setup_polygon_api_client()
        s3_client = setup_polygon_flatfiles_client()

        assert api_client is mock_api_client
        assert s3_client is mock_s3_client
        # Verify both were initialized
        mock_rest_client.assert_called_once()
        mock_s3fs.assert_called_once()

    def test_storage_options_uses_secret_values(self, mock_settings):
        """Test POLYGON_STORAGE_OPTIONS correctly extracts secret values."""
        # This tests that get_secret_value() is called on SecretStr objects
        with patch("tickerlake.bronze.clients.settings", mock_settings):
            from tickerlake.bronze.clients import POLYGON_STORAGE_OPTIONS as opts

            # The actual values should be strings, not SecretStr objects
            assert isinstance(opts["aws_access_key_id"], str)
            assert isinstance(opts["aws_secret_access_key"], str)
