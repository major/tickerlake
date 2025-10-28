"""Tests for gold layer main orchestrator. 🥇"""

from unittest.mock import MagicMock, patch



class TestGoldMain:
    """Test gold layer main function. 🎯"""

    @patch("tickerlake.gold.main.run_hvc_streaks_analysis")
    @patch("tickerlake.gold.main.clear_all_tables")
    @patch("tickerlake.gold.main.init_gold_schema")
    def test_main_runs_all_reports(self, mock_init_schema, mock_clear_tables, mock_hvc_analysis):
        """Should initialize schema, clear tables, and run all reports."""
        from tickerlake.gold.main import main

        main(reset_schema_flag=False)

        mock_init_schema.assert_called_once()
        mock_clear_tables.assert_called_once()
        mock_hvc_analysis.assert_called_once()

    @patch("tickerlake.gold.main.run_hvc_streaks_analysis")
    @patch("tickerlake.gold.main.reset_schema")
    @patch("tickerlake.gold.main.clear_all_tables")
    def test_main_with_reset_schema(self, mock_clear_tables, mock_reset_schema, mock_hvc_analysis):
        """Should reset schema when flag is True (no separate clear needed)."""
        from tickerlake.gold.main import main

        main(reset_schema_flag=True)

        mock_reset_schema.assert_called_once()
        # clear_all_tables should NOT be called when resetting schema
        mock_clear_tables.assert_not_called()
        mock_hvc_analysis.assert_called_once()

    @patch("tickerlake.gold.main.run_hvc_streaks_analysis")
    @patch("tickerlake.gold.main.clear_all_tables")
    @patch("tickerlake.gold.main.init_gold_schema")
    def test_main_default_parameters(self, mock_init_schema, mock_clear_tables, mock_hvc_analysis):
        """Should use default parameters when not specified."""
        from tickerlake.gold.main import main

        main()  # No arguments

        mock_init_schema.assert_called_once()
        mock_clear_tables.assert_called_once()
        mock_hvc_analysis.assert_called_once()


class TestGoldCLI:
    """Test gold layer CLI. 🖥️"""

    @patch("tickerlake.gold.main.main")
    @patch("argparse.ArgumentParser.parse_args")
    def test_cli_default_args(self, mock_parse_args, mock_main):
        """Should parse default CLI arguments."""
        from tickerlake.gold.main import cli

        # Mock argparse to return default args
        mock_args = MagicMock()
        mock_args.reset_schema = False
        mock_parse_args.return_value = mock_args

        cli()

        mock_main.assert_called_once_with(reset_schema_flag=False)

    @patch("tickerlake.gold.main.main")
    @patch("argparse.ArgumentParser.parse_args")
    def test_cli_reset_schema_flag(self, mock_parse_args, mock_main):
        """Should pass reset_schema flag to main."""
        from tickerlake.gold.main import cli

        # Mock argparse with reset_schema=True
        mock_args = MagicMock()
        mock_args.reset_schema = True
        mock_parse_args.return_value = mock_args

        cli()

        mock_main.assert_called_once_with(reset_schema_flag=True)


class TestGoldPostgres:
    """Test gold layer postgres functions. 🗄️"""

    @patch("tickerlake.gold.postgres.init_schema")
    def test_init_gold_schema(self, mock_init_schema):
        """Should call init_schema with gold metadata."""
        from tickerlake.gold.postgres import init_gold_schema

        init_gold_schema()

        mock_init_schema.assert_called_once()
        # Verify it's called with the gold metadata
        args = mock_init_schema.call_args
        assert args[0][1] == "gold"  # layer name

    @patch("tickerlake.gold.postgres.drop_schema")
    @patch("tickerlake.gold.postgres.init_gold_schema")
    def test_reset_schema(self, mock_init, mock_drop):
        """Should drop and recreate schema."""
        from tickerlake.gold.postgres import reset_schema

        reset_schema()

        mock_drop.assert_called_once()
        mock_init.assert_called_once()

    @patch("tickerlake.gold.postgres.get_engine")
    def test_clear_all_tables(self, mock_get_engine):
        """Should clear all gold tables."""
        from tickerlake.gold.postgres import clear_all_tables

        # Mock engine and connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        clear_all_tables()

        # Should execute DELETE for each table
        assert mock_conn.execute.call_count >= 2  # At least 2 HVC tables
