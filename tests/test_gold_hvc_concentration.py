"""Tests for gold layer HVC concentration analysis. 📅"""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from tickerlake.gold.hvc_concentration import (
    calculate_hvc_concentration_for_timeframe,
    run_hvc_concentration_analysis,
)


class TestCalculateHVCConcentrationForTimeframe:
    """Test calculate_hvc_concentration_for_timeframe function. 📊"""

    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_daily_concentration_calculation(self, mock_get_engine):
        """Should calculate daily HVC counts per date correctly."""
        # Mock database response: 3 dates with varying HVC counts
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (date(2024, 1, 15), 25),  # 25 HVCs on Jan 15
            (date(2024, 1, 10), 18),  # 18 HVCs on Jan 10
            (date(2024, 1, 5), 12),  # 12 HVCs on Jan 5
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run calculation
        result = calculate_hvc_concentration_for_timeframe(
            timeframe="daily", volume_threshold=3.0
        )

        # Verify results
        assert len(result) == 3
        assert result["date"].to_list() == [
            date(2024, 1, 15),
            date(2024, 1, 10),
            date(2024, 1, 5),
        ]
        assert result["hvc_count"].to_list() == [25, 18, 12]
        assert all(
            isinstance(dt, datetime) for dt in result["calculated_at"].to_list()
        )

    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_weekly_concentration_calculation(self, mock_get_engine):
        """Should calculate weekly HVC counts per date correctly."""
        # Mock database response: 2 weeks with HVC counts
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (date(2024, 1, 14), 15),  # Week ending Jan 14: 15 HVCs
            (date(2024, 1, 7), 8),  # Week ending Jan 7: 8 HVCs
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run calculation
        result = calculate_hvc_concentration_for_timeframe(
            timeframe="weekly", volume_threshold=3.0
        )

        # Verify results
        assert len(result) == 2
        assert result["date"].to_list() == [date(2024, 1, 14), date(2024, 1, 7)]
        assert result["hvc_count"].to_list() == [15, 8]

    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_no_hvcs_found(self, mock_get_engine):
        """Should handle case with no HVCs gracefully."""
        # Mock empty database response
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run calculation
        result = calculate_hvc_concentration_for_timeframe(
            timeframe="daily", volume_threshold=3.0
        )

        # Should return empty DataFrame with correct schema
        assert len(result) == 0
        assert "date" in result.columns
        assert "hvc_count" in result.columns
        assert "calculated_at" in result.columns

    def test_invalid_timeframe(self):
        """Should raise ValueError for invalid timeframe."""
        with pytest.raises(ValueError, match="Invalid timeframe"):
            calculate_hvc_concentration_for_timeframe(
                timeframe="invalid", volume_threshold=3.0
            )

    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_high_concentration_days(self, mock_get_engine):
        """Should correctly identify days with very high HVC concentration."""
        # Mock database response with one extremely high concentration day
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (date(2024, 1, 20), 150),  # Market-wide event with 150 HVCs
            (date(2024, 1, 15), 25),
            (date(2024, 1, 10), 20),
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run calculation
        result = calculate_hvc_concentration_for_timeframe(
            timeframe="daily", volume_threshold=3.0
        )

        # Verify max concentration is detected
        assert result["hvc_count"].max() == 150
        assert result["date"][0] == date(2024, 1, 20)


class TestRunHVCConcentrationAnalysis:
    """Test run_hvc_concentration_analysis integration. 🚀"""

    @patch("tickerlake.gold.hvc_concentration.settings")
    @patch("tickerlake.gold.hvc_concentration.bulk_load")
    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_runs_for_all_configured_timeframes(
        self, mock_get_engine, mock_bulk_load, mock_settings
    ):
        """Should process all configured timeframes."""
        # Configure settings
        mock_settings.hvc_timeframes = ["daily", "weekly"]
        mock_settings.hvc_volume_threshold = 3.0

        # Mock database to return some data for both timeframes
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (date(2024, 1, 15), 20),
            (date(2024, 1, 10), 15),
        ]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run analysis
        run_hvc_concentration_analysis()

        # Should call bulk_load twice (once for daily, once for weekly)
        assert mock_bulk_load.call_count == 2

    @patch("tickerlake.gold.hvc_concentration.settings")
    @patch("tickerlake.gold.hvc_concentration.bulk_load")
    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_handles_empty_results(
        self, mock_get_engine, mock_bulk_load, mock_settings
    ):
        """Should handle case where no HVCs are found."""
        # Configure settings
        mock_settings.hvc_timeframes = ["daily"]
        mock_settings.hvc_volume_threshold = 3.0

        # Mock database to return no data
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run analysis - should not crash
        run_hvc_concentration_analysis()

        # Should not call bulk_load when there's no data
        assert mock_bulk_load.call_count == 0

    @patch("tickerlake.gold.hvc_concentration.settings")
    @patch("tickerlake.gold.hvc_concentration.bulk_load")
    @patch("tickerlake.gold.hvc_concentration.get_engine")
    def test_uses_correct_tables(
        self, mock_get_engine, mock_bulk_load, mock_settings
    ):
        """Should write to correct gold layer tables for each timeframe."""
        # Configure settings
        mock_settings.hvc_timeframes = ["daily", "weekly"]
        mock_settings.hvc_volume_threshold = 3.0

        # Mock database to return some data
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(date(2024, 1, 15), 20)]
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Run analysis
        run_hvc_concentration_analysis()

        # Verify correct tables were used
        call_args_list = mock_bulk_load.call_args_list
        assert len(call_args_list) == 2

        # First call should be for daily
        first_table = call_args_list[0][0][0]
        assert first_table.name == "gold_hvc_concentration_daily"

        # Second call should be for weekly
        second_table = call_args_list[1][0][0]
        assert second_table.name == "gold_hvc_concentration_weekly"
