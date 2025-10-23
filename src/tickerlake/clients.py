"""Client setup for Polygon.io API."""

from polygon import RESTClient

from tickerlake.config import settings


def setup_polygon_api_client() -> RESTClient:
    """Set up the Polygon API client."""
    return RESTClient(settings.polygon_api_key.get_secret_value())
