from datetime import date, timedelta

from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    polygon_api_key: SecretStr = SecretStr("")

    s3_bucket_name: str = "tickerlake"
    s3_endpoint_url: str = "https://s3.us-west-001.backblazeb2.com"
    aws_access_key_id: SecretStr = SecretStr("")
    aws_secret_access_key: SecretStr = SecretStr("")
    aws_region: str = "us-west-001"

    data_start_date: date = date.today() - timedelta(days=5 * 365)

    class Config:
        env_file = ".env"


settings = Settings()

s3_storage_options = {
    "aws_access_key_id": settings.aws_access_key_id.get_secret_value(),
    "aws_secret_access_key": settings.aws_secret_access_key.get_secret_value(),
    "endpoint_url": settings.s3_endpoint_url,
    "region_name": settings.aws_region,
}
