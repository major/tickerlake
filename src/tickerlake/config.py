"""Configuration settings for TickerLake application."""

from datetime import date, timedelta

from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration settings using Pydantic BaseSettings."""

    polygon_api_key: SecretStr = SecretStr("")

    s3_bucket_name: str = "tickerlake"
    s3_endpoint_url: str = "https://s3.us-west-001.backblazeb2.com"
    aws_access_key_id: SecretStr = SecretStr("")
    aws_secret_access_key: SecretStr = SecretStr("")
    aws_region: str = "us-west-001"

    data_start_date: date = date.today() - timedelta(days=5 * 365)

    # Index holdings data source URLs
    spy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    mdy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx"
    spsm_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx"
    qqq_holdings_source: str = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&action=download&ticker=QQQ"
    iwm_holdings_source: str = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

    class Config:
        """Pydantic configuration class."""

        env_file = ".env"


settings = Settings()

s3_storage_options = {
    "aws_access_key_id": settings.aws_access_key_id.get_secret_value(),
    "aws_secret_access_key": settings.aws_secret_access_key.get_secret_value(),
    "endpoint_url": settings.s3_endpoint_url,
    "region_name": settings.aws_region,
}
