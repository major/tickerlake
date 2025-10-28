"""Configuration settings for TickerLake application."""

from datetime import date

from pydantic import SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings using Pydantic BaseSettings."""

    # Polygon API access
    polygon_api_key: SecretStr = SecretStr("")
    data_start_year: int = date.today().year - 5

    # Postgres configuration (replaces bronze_storage_path)
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "tickerlake"
    postgres_user: SecretStr = SecretStr("")
    postgres_password: SecretStr = SecretStr("")
    postgres_sslmode: str = "require"  # require/verify-ca/verify-full/disable

    # Local storage paths (for gold layer only)
    data_root_path: str = "./data"

    # Index holdings data source URLs
    etfs: list = ["SPY", "MDY", "SPSM", "QQQ", "IWM"]
    spy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
    mdy_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mdy.xlsx"
    spsm_holdings_source: str = "https://www.ssga.com/us/en/individual/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spsm.xlsx"
    # Using QQQE for this since Invesco got all protective of their data.
    qqq_holdings_source: str = "https://www.direxion.com/holdings/QQQE.csv"
    iwm_holdings_source: str = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"

    # Gold layer - HVC (High Volume Close) Streaks configuration
    hvc_volume_threshold: float = 3.0  # Volume must be >= N × volume_ma_20
    hvc_min_streak_length: int = 2  # Minimum consecutive HVCs to report
    hvc_timeframes: list[str] = ["daily", "weekly"]  # Timeframes to analyze

    @computed_field  # type: ignore[prop-decorator]
    @property
    def postgres_url(self) -> str:
        """Build SQLAlchemy connection URL with TLS."""
        return (
            f"postgresql+psycopg://"
            f"{self.postgres_user.get_secret_value()}:"
            f"{self.postgres_password.get_secret_value()}@"
            f"{self.postgres_host}:{self.postgres_port}/"
            f"{self.postgres_database}"
            f"?sslmode={self.postgres_sslmode}"
        )

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
