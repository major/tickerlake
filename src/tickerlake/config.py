"""Configuration settings for TickerLake application."""

from datetime import date, timedelta

from pydantic import SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application configuration settings using Pydantic BaseSettings."""

    # Polygon API + flatfiles access
    polygon_api_key: SecretStr = SecretStr("")
    polygon_access_key_id: SecretStr = SecretStr("")
    polygon_secret_access_key: SecretStr = SecretStr("")
    polygon_flatfiles_endpoint_url: str = "https://files.polygon.io"
    polygon_flatfiles_stocks: str = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
    polygon_flatfiles_options: str = "s3://flatfiles/us_options_opra/day_aggs_v1"
    polygon_flatfiles_stocks_first_year: int = date.today().year - 5
    polygon_flatfiles_options_first_year: int = date.today().year - 2

    # Unified s3 storage
    bronze_unified_storage_path: str = "s3://tickerlake/unified/bronze"

    s3_bucket_name: str = "tickerlake"
    aws_access_key_id: SecretStr = SecretStr("")
    aws_secret_access_key: SecretStr = SecretStr("")
    aws_region: str = "us-east-1"

    data_start_date: date = date.today() - timedelta(days=4 * 365)

    # Index holdings data source URLs
    etfs: list = ["SPY", "MDY", "SPSM", "QQQ", "IWM"]
    spy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    mdy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx"
    spsm_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx"
    # Using QQQE for this since Invesco got all protective of their data.
    qqq_holdings_source: str = "https://www.direxion.com/holdings/QQQE.csv"
    iwm_holdings_source: str = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

    class Config:
        """Pydantic configuration class."""

        env_file = ".env"


settings = Settings()

s3_storage_options = {
    "aws_access_key_id": settings.aws_access_key_id.get_secret_value(),
    "aws_secret_access_key": settings.aws_secret_access_key.get_secret_value(),
    "aws_region": settings.aws_region,
}
