import s3fs
from polygon import RESTClient

from tickerlake.config import settings

POLYGON_STORAGE_OPTIONS = {
    "aws_access_key_id": settings.polygon_access_key_id.get_secret_value(),
    "aws_secret_access_key": settings.polygon_secret_access_key.get_secret_value(),
    "aws_endpoint_url": "https://files.polygon.io",
}


def setup_polygon_api_client() -> RESTClient:
    return RESTClient(settings.polygon_api_key.get_secret_value())


def setup_polygon_flatfiles_client() -> s3fs.S3FileSystem:
    """Set up the Polygon S3 client using s3fs."""
    polygon_s3 = s3fs.S3FileSystem(
        anon=False,
        key=settings.polygon_access_key_id.get_secret_value(),
        secret=settings.polygon_secret_access_key.get_secret_value(),
        endpoint_url=settings.polygon_flatfiles_endpoint_url,
    )
    return polygon_s3


def setup_aws_s3_client() -> s3fs.S3FileSystem:
    """Set up the AWS S3 client using s3fs."""
    aws_s3 = s3fs.S3FileSystem(
        anon=False,
        key=settings.aws_access_key_id.get_secret_value(),
        secret=settings.aws_secret_access_key.get_secret_value(),
        client_kwargs={"region_name": settings.aws_region},
    )
    return aws_s3
