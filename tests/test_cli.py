"""Tests for the tickerlake CLI."""

import datetime
from unittest.mock import patch

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


class TestCompactSubcommand:
    """Test compact subcommand and its options."""

    def test_compact_subcommand_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify compact subcommand invokes pipeline.compact."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.compact") as mock_compact:
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "compact", "--output-dir", str(tmp_path)]
            )
            main()
            mock_compact.assert_called_once()
            config = mock_compact.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path


class TestPivotsSubcommand:
    """Test pivots subcommand and its options."""

    def test_pivots_subcommand_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify pivots subcommand invokes pipeline.pivots."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.pivots") as mock_pivots:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "pivots",
                    "aapl",
                    "--timeframe",
                    "monthly",
                    "--k",
                    "3",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            mock_pivots.assert_called_once()
            config, ticker, timeframe, k = mock_pivots.call_args[0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path
            assert ticker == "aapl"
            assert timeframe == "monthly"
            assert k == 3

    def test_pivots_defaults_to_weekly_k4(self, monkeypatch):
        """Verify pivots defaults to weekly timeframe and k=4."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.pivots") as mock_pivots:
            monkeypatch.setattr("sys.argv", ["tickerlake", "pivots", "AAPL"])
            main()
            _, _, timeframe, k = mock_pivots.call_args[0]
            assert timeframe == "weekly"
            assert k == 4

    def test_pivots_without_api_key_dispatches(self, monkeypatch):
        """Verify read-only pivots command does not require MASSIVE_API_KEY."""
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        with patch("tickerlake.pipeline.pivots") as mock_pivots:
            monkeypatch.setattr("sys.argv", ["tickerlake", "pivots", "AAPL"])
            main()

        mock_pivots.assert_called_once()
        config = mock_pivots.call_args[0][0]
        assert config.api_key == ""

    def test_pivots_invalid_timeframe(self, monkeypatch):
        """Verify invalid timeframe exits with error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv", ["tickerlake", "pivots", "AAPL", "--timeframe", "yearly"]
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_pivots_invalid_k_parse_failure(self, monkeypatch):
        """Verify --k must be a positive integer."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr("sys.argv", ["tickerlake", "pivots", "AAPL", "--k", "0"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_pivots_value_error_becomes_cli_error(self, monkeypatch, capsys):
        """Verify pivot lookup ValueError exits cleanly without traceback."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.pivots", side_effect=ValueError("missing db")):
            monkeypatch.setattr("sys.argv", ["tickerlake", "pivots", "AAPL"])
            with pytest.raises(SystemExit) as exc_info:
                main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "missing db" in captured.err
        assert "Traceback" not in captured.err

    def test_compact_custom_output_dir(self, monkeypatch, tmp_path):
        """Verify --output-dir option sets config.output_dir for compact."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.compact") as mock_compact:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "compact", "--output-dir", str(tmp_path)],
            )
            main()
            config = mock_compact.call_args[0][0]
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
            # Config defaults: start_date is 1 year ago, end_date is today
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

    def test_backfill_without_api_key_exits_cleanly(self, monkeypatch, capsys):
        """Verify Massive commands require MASSIVE_API_KEY at dispatch."""
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        monkeypatch.setattr("sys.argv", ["tickerlake", "backfill"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "MASSIVE_API_KEY environment variable is required" in captured.err
        assert "Traceback" not in captured.err

    def test_update_without_api_key_exits_cleanly(self, monkeypatch, capsys):
        """Verify update requires MASSIVE_API_KEY at dispatch."""
        monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
        monkeypatch.setattr("sys.argv", ["tickerlake", "update"])

        with pytest.raises(SystemExit) as exc_info:
            main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "MASSIVE_API_KEY environment variable is required" in captured.err
        assert "Traceback" not in captured.err


class TestFibZonesSubcommand:
    """Test the fib-zones compute/screen subcommands."""

    def test_fib_zones_compute_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify fib-zones compute invokes pipeline.compute_weekly_fib_zones."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.compute_weekly_fib_zones") as mock_compute:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fib-zones",
                    "compute",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            mock_compute.assert_called_once()
            config = mock_compute.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path

    def test_fib_zones_screen_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify fib-zones screen passes zone and output-dir to the pipeline."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fib_zones") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fib-zones",
                    "screen",
                    "--zone",
                    "in_ibz",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            mock_screen.assert_called_once()
            config = mock_screen.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path
            assert mock_screen.call_args.kwargs["zone"] == "in_ibz"

    def test_fib_zones_screen_defaults(self, monkeypatch, tmp_path):
        """Verify fib-zones screen defaults to zone=all and no limit."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fib_zones") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                ["tickerlake", "fib-zones", "screen", "--output-dir", str(tmp_path)],
            )
            main()
            assert mock_screen.call_args.kwargs["zone"] == "all"
            assert mock_screen.call_args.kwargs["limit"] is None

    def test_fib_zones_screen_limit(self, monkeypatch, tmp_path):
        """Verify fib-zones screen --limit caps the number of rows displayed."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fib_zones") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fib-zones",
                    "screen",
                    "--limit",
                    "5",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            assert mock_screen.call_args.kwargs["limit"] == 5

    def test_fib_zones_screen_invalid_zone(self, monkeypatch):
        """Verify an unknown --zone value exits with an argparse error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "fib-zones", "screen", "--zone", "bogus"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fib_zones_screen_invalid_limit(self, monkeypatch):
        """Verify --limit must be a positive integer."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "fib-zones", "screen", "--limit", "0"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fib_zones_screen_invalid_limit_type(self, monkeypatch):
        """Verify --limit rejects a non-integer value."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "fib-zones", "screen", "--limit", "many"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fib_zones_no_subcommand(self, monkeypatch):
        """Verify fib-zones without a subcommand exits with an error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr("sys.argv", ["tickerlake", "fib-zones"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fib_zones_compute_value_error_becomes_cli_error(self, monkeypatch, capsys):
        """Verify a compute ValueError exits cleanly without a traceback."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch(
            "tickerlake.pipeline.compute_weekly_fib_zones",
            side_effect=ValueError("missing db"),
        ):
            monkeypatch.setattr("sys.argv", ["tickerlake", "fib-zones", "compute"])
            with pytest.raises(SystemExit) as exc_info:
                main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "missing db" in captured.err
        assert "Traceback" not in captured.err

    def test_fib_zones_screen_value_error_becomes_cli_error(self, monkeypatch, capsys):
        """Verify a screen ValueError exits cleanly without a traceback."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch(
            "tickerlake.pipeline.screen_fib_zones",
            side_effect=ValueError("missing db"),
        ):
            monkeypatch.setattr("sys.argv", ["tickerlake", "fib-zones", "screen"])
            with pytest.raises(SystemExit) as exc_info:
                main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "missing db" in captured.err
        assert "Traceback" not in captured.err


class TestFairValueBandsSubcommand:
    """Test Fair Value Bands timeframe forwarding and screen options."""

    def test_fair_value_bands_compute_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify compute forwards the selected timeframe."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.compute_fair_value_bands") as mock_compute:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fair-value-bands",
                    "compute",
                    "--timeframe",
                    "daily",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            mock_compute.assert_called_once()
            config = mock_compute.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path
            assert mock_compute.call_args[0][1] == "daily"

    def test_fair_value_bands_compute_defaults_to_monthly(self, monkeypatch):
        """Verify compute defaults to the monthly timeframe."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.compute_fair_value_bands") as mock_compute:
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "fair-value-bands", "compute"]
            )
            main()
            assert mock_compute.call_args[0][1] == "monthly"

    def test_fair_value_bands_screen_calls_pipeline(self, monkeypatch, tmp_path):
        """Verify screen forwards timeframe, zone, min-close, and output-dir."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fair_value_bands") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fair-value-bands",
                    "screen",
                    "--timeframe",
                    "weekly",
                    "--zone",
                    "below_lower",
                    "--min-close",
                    "10",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            mock_screen.assert_called_once()
            config = mock_screen.call_args[0][0]
            assert isinstance(config, Config)
            assert config.output_dir == tmp_path
            assert mock_screen.call_args[0][1] == "weekly"
            assert mock_screen.call_args.kwargs["zone"] == "below_lower"
            assert mock_screen.call_args.kwargs["min_close"] == 10.0

    def test_fair_value_bands_screen_defaults(self, monkeypatch, tmp_path):
        """Verify fair-value-bands screen defaults to zone=all, min_close=5.0."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fair_value_bands") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fair-value-bands",
                    "screen",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            assert mock_screen.call_args[0][1] == "monthly"
            assert mock_screen.call_args.kwargs["zone"] == "all"
            assert mock_screen.call_args.kwargs["limit"] is None
            assert mock_screen.call_args.kwargs["min_close"] == 5.0

    def test_fair_value_bands_screen_limit(self, monkeypatch, tmp_path):
        """Verify fair-value-bands screen --limit caps the number of rows displayed."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch("tickerlake.pipeline.screen_fair_value_bands") as mock_screen:
            monkeypatch.setattr(
                "sys.argv",
                [
                    "tickerlake",
                    "fair-value-bands",
                    "screen",
                    "--limit",
                    "5",
                    "--output-dir",
                    str(tmp_path),
                ],
            )
            main()
            assert mock_screen.call_args.kwargs["limit"] == 5

    def test_fair_value_bands_screen_invalid_zone(self, monkeypatch):
        """Verify an unknown --zone value exits with an argparse error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "fair-value-bands", "screen", "--zone", "bogus"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fair_value_bands_screen_invalid_limit(self, monkeypatch):
        """Verify --limit must be a positive integer."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr(
            "sys.argv",
            ["tickerlake", "fair-value-bands", "screen", "--limit", "0"],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fair_value_bands_no_subcommand(self, monkeypatch):
        """Verify fair-value-bands without a subcommand exits with an error."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        monkeypatch.setattr("sys.argv", ["tickerlake", "fair-value-bands"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code != 0

    def test_fair_value_bands_compute_value_error_becomes_cli_error(
        self, monkeypatch, capsys
    ):
        """Verify a compute ValueError exits cleanly without a traceback."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch(
            "tickerlake.pipeline.compute_fair_value_bands",
            side_effect=ValueError("missing db"),
        ):
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "fair-value-bands", "compute"]
            )
            with pytest.raises(SystemExit) as exc_info:
                main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "missing db" in captured.err
        assert "Traceback" not in captured.err

    def test_fair_value_bands_screen_value_error_becomes_cli_error(
        self, monkeypatch, capsys
    ):
        """Verify a screen ValueError exits cleanly without a traceback."""
        monkeypatch.setenv("MASSIVE_API_KEY", "test_key")
        with patch(
            "tickerlake.pipeline.screen_fair_value_bands",
            side_effect=ValueError("missing db"),
        ):
            monkeypatch.setattr(
                "sys.argv", ["tickerlake", "fair-value-bands", "screen"]
            )
            with pytest.raises(SystemExit) as exc_info:
                main()

        captured = capsys.readouterr()
        assert exc_info.value.code != 0
        assert "missing db" in captured.err
        assert "Traceback" not in captured.err
