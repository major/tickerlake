"""Configuration settings for TickerLake application."""

from datetime import date
from pathlib import Path

from pydantic import SecretStr, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration settings using Pydantic BaseSettings."""

    # Polygon API access
    polygon_api_key: SecretStr = SecretStr("")
    data_start_year: int = date.today().year - 5

    # Local filesystem storage configuration
    data_dir: str = "data"  # Local directory for Parquet files and checkpoints

    # Processing configuration
    checkpoint_file: str = "checkpoints.json"
    bronze_parallel_requests: int = 4  # Number of parallel Bronze API requests

    @computed_field  # type: ignore[prop-decorator]
    @property
    def base_path(self) -> str:
        """Local base path for Parquet files."""
        # Convert to absolute path and create directory if it doesn't exist
        data_path = Path(self.data_dir).resolve()
        data_path.mkdir(parents=True, exist_ok=True)
        return str(data_path)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def checkpoint_path(self) -> str:
        """Local path for checkpoint file."""
        return str(Path(self.base_path) / self.checkpoint_file)

    @field_validator("bronze_parallel_requests")
    @classmethod
    def _ensure_parallel_request_bounds(_cls, value: int) -> int:
        """Ensure parallel requests stays within safe bounds."""
        if value < 1:
            raise ValueError("bronze_parallel_requests must be at least 1")
        return value

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
