"""Tests for the tickerlake CLI."""

import datetime
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tickerlake import main
from tickerlake.config import Config


class TestHelpAndSubcommands:
    """Test help output and subcommand discovery."""

    def test_help_shows_subcommands(self, monkeypatch):
        """Verify --help output contains all three subcommands."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr("sys.argv", ["tickerlake", "--help"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0

    def test_no_subcommand_shows_error(self, monkeypatch, capsys):
        """Verify running with no subcommand exits with error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr("sys.argv", ["tickerlake"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0


class TestBackfillSubcommand:
    """Test backfill subcommand and its options."""

    def test_backfill_subcommand_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify backfill subcommand invokes pipeline.backfill."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.backfill") as mock_backfill:
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "backfill", "--output-dir", str(tmp_path)]
            )
            main()
            mock_backfill.assert_called_once()
            config = mock_backfill.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path

    def test_backfill_custom_start_date(self, monkeypatch, tmp_path):
        """Verify --start-date option sets config.start_date."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.backfill") as mock_backfill:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "backfill",
                    "--start-date",
                    "2023-01-01",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            config = mock_backfill.call_args[0][0]
            assert config.start_date == datetime.date(2023, 1, 1)

    def test_backfill_custom_end_date(self, monkeypatch, tmp_path):
        """Verify --end-date option sets config.end_date."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.backfill") as mock_backfill:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "backfill",
                    "--end-date",
                    "2024-12-31",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            config = mock_backfill.call_args[0][0]
            assert config.end_date == datetime.date(2024, 12, 31)

    def test_backfill_custom_output_dir(self, monkeypatch, tmp_path):
        """Verify --output-dir option sets config.output_dir."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.backfill") as mock_backfill:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "backfill", "--output-dir", str(tmp_path)],
            )
            main()
            config = mock_backfill.call_args[0][0]
            assert config.output_dir == tmp_path

    def test_backfill_invalid_start_date_format(self, monkeypatch):
        """Verify invalid --start-date format exits with error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "backfill", "--start-date", "not-a-date"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_backfill_invalid_end_date_format(self, monkeypatch):
        """Verify invalid --end-date format exits with error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "backfill", "--end-date", "2024/12/31"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0


class TestUpdateSubcommand:
    """Test update subcommand and its options."""

    def test_update_subcommand_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify update subcommand invokes pipeline.update."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.update") as mock_update:
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "update", "--output-dir", str(tmp_path)]
            )
            main()
            mock_update.assert_called_once()
            config = mock_update.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path

    def test_update_custom_output_dir(self, monkeypatch, tmp_path):
        """Verify --output-dir option sets config.output_dir for update."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.update") as mock_update:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "update", "--output-dir", str(tmp_path)],
            )
            main()
            config = mock_update.call_args[0][0]
            assert config.output_dir == tmp_path


class TestInfoSubcommand:
    """Test info subcommand and its options."""

    def test_info_subcommand_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify info subcommand invokes pipeline.info."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.info") as mock_info:
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "info", "--output-dir", str(tmp_path)]
            )
            main()
            mock_info.assert_called_once()
            config = mock_info.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path

    def test_info_custom_output_dir(self, monkeypatch, tmp_path):
        """Verify --output-dir option sets config.output_dir for info."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.info") as mock_info:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "info", "--output-dir", str(tmp_path)],
            )
            main()
            config = mock_info.call_args[0][0]
            assert config.output_dir == tmp_path


class TestConfigDefaults:
    """Test that Config defaults are applied when CLI options are omitted."""

    def test_backfill_uses_config_defaults(self, monkeypatch):
        """Verify backfill without date options uses Config defaults."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.backfill") as mock_backfill:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "backfill"],
            )
            main()
            config = mock_backfill.call_args[0][0]
            # Config defaults: start_date is 5 years ago, end_date is today
            assert config.start_date is not None
            assert config.end_date is not None
            assert config.start_date < config.end_date

    def test_update_uses_config_defaults(self, monkeypatch):
        """Verify update without options uses Config defaults."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.update") as mock_update:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "update"],
            )
            main()
            config = mock_update.call_args[0][0]
            assert isinstance(config, Config)

    def test_info_uses_config_defaults(self, monkeypatch):
        """Verify info without options uses Config defaults."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.info") as mock_info:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "info"],
            )
            main()
            config = mock_info.call_args[0][0]
            assert isinstance(config, Config)
