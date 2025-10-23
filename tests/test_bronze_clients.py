"""Tests for the bronze clients module."""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from tickerlake.clients import setup_polygon_api_client


@pytest.fixture
def mock_settings():
    """Create a mock settings object for testing."""
    with patch("tickerlake.clients.settings") as mock:
        mock.polygon_api_key = SecretStr("test_api_key_12345")
        yield mock


class TestSetupPolygonApiClient:
    """Test cases for setup_polygon_api_client function."""

    @patch("tickerlake.clients.RESTClient")
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

    @patch("tickerlake.clients.RESTClient")
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
