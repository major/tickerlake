"""Configuration-related tests."""

import pytest

from tickerlake.config import Settings


def test_settings_rejects_invalid_parallel_requests(tmp_path) -> None:
    """bronze_parallel_requests must be >= 1."""
    with pytest.raises(ValueError):
        Settings(
            polygon_api_key="test-key",
            data_dir=str(tmp_path),
            bronze_parallel_requests=0,
        )


def test_settings_accepts_valid_parallel_requests(tmp_path) -> None:
    """A positive bronze_parallel_requests value should be accepted."""
    settings = Settings(
        polygon_api_key="test-key",
        data_dir=str(tmp_path),
        bronze_parallel_requests=3,
    )

    assert settings.bronze_parallel_requests == 3
