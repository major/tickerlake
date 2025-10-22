"""Configuration settings for TickerLake application."""

from datetime import date

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings using Pydantic BaseSettings."""

    # Polygon API + flatfiles access
    polygon_api_key: SecretStr = SecretStr("")
    polygon_access_key_id: SecretStr = SecretStr("")
    polygon_secret_access_key: SecretStr = SecretStr("")
    polygon_flatfiles_endpoint_url: str = "https://files.polygon.io"
    polygon_flatfiles_stocks: str = "s3://flatfiles/us_stocks_sip/day_aggs_v1"
    polygon_flatfiles_stocks_first_year: int = date.today().year - 0

    # Local storage paths
    bronze_storage_path: str = "./data/bronze"
    silver_storage_path: str = "./data/silver"

    # Technical indicators configuration
    hvc_volume_ratio_threshold: float = 3.0

    # Index holdings data source URLs
    etfs: list = ["SPY", "MDY", "SPSM", "QQQ", "IWM"]
    spy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    mdy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx"
    spsm_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx"
    # Using QQQE for this since Invesco got all protective of their data.
    qqq_holdings_source: str = "https://www.direxion.com/holdings/QQQE.csv"
    iwm_holdings_source: str = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
