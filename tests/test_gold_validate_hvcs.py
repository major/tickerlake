"""Tests for gold layer HVC validation. 🧪"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from tickerlake.gold.validate_hvcs import (
    run_hvc_validation,
    validate_hvc_absent,
    validate_hvc_present,
)


class TestValidateHVCPresent:
    """Test validate_hvc_present function. ✅"""

    @patch("tickerlake.gold.validate_hvcs.get_engine")
    def test_hvc_found_valid_ratio(self, mock_get_engine):
        """Should return True when HVC is found with valid ratio."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock result: HVC exists with ratio 4.04
        mock_row = ("DELL", date(2024, 2, 26), 4.04)
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        result = validate_hvc_present("DELL", date(2024, 2, 26), "weekly", 4.0)

        assert result is True

    @patch("tickerlake.gold.validate_hvcs.get_engine")
    def test_hvc_not_found(self, mock_get_engine):
        """Should return False when HVC is not found."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock result: No HVC found
        mock_conn.execute.return_value.fetchone.return_value = None

        result = validate_hvc_present("AAPL", date(2024, 1, 1), "daily", 3.0)

        assert result is False

    @patch("tickerlake.gold.validate_hvcs.get_engine")
    def test_hvc_ratio_too_low(self, mock_get_engine):
        """Should return False when HVC ratio is below minimum."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock result: HVC exists but ratio is too low
        mock_row = ("TEST", date(2024, 1, 1), 2.5)
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        result = validate_hvc_present("TEST", date(2024, 1, 1), "weekly", 3.0)

        assert result is False


class TestValidateHVCAbsent:
    """Test validate_hvc_absent function. ✅"""

    @patch("tickerlake.gold.validate_hvcs.get_engine")
    def test_correctly_absent(self, mock_get_engine):
        """Should return True when HVC is correctly absent."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock result: No HVC found (correct)
        mock_conn.execute.return_value.fetchone.return_value = None

        result = validate_hvc_absent(
            "AMD", date(2025, 10, 6), "weekly", "ratio too low"
        )

        assert result is True

    @patch("tickerlake.gold.validate_hvcs.get_engine")
    def test_incorrectly_present(self, mock_get_engine):
        """Should return False when HVC is incorrectly present."""
        # Mock database connection
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        # Mock result: HVC found (incorrect - should not be there)
        mock_row = ("AMD", date(2025, 10, 6), 2.69)
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        result = validate_hvc_absent(
            "AMD", date(2025, 10, 6), "weekly", "ratio too low"
        )

        assert result is False


class TestRunHVCValidation:
    """Test run_hvc_validation function. 🧪"""

    @patch("tickerlake.gold.validate_hvcs.validate_hvc_present")
    @patch("tickerlake.gold.validate_hvcs.validate_hvc_absent")
    def test_all_checks_pass(self, mock_absent, mock_present):
        """Should complete successfully when all checks pass."""
        # All validation functions return True
        mock_present.return_value = True
        mock_absent.return_value = True

        # Should not raise
        run_hvc_validation()

        # Should have called validation functions
        assert mock_present.call_count == 3  # 2 weekly + 1 daily
        assert mock_absent.call_count == 1  # 1 non-HVC

    @patch("tickerlake.gold.validate_hvcs.validate_hvc_present")
    @patch("tickerlake.gold.validate_hvcs.validate_hvc_absent")
    def test_check_fails(self, mock_absent, mock_present):
        """Should raise RuntimeError when a check fails."""
        # One validation function returns False
        mock_present.side_effect = [True, False, True]  # Second one fails
        mock_absent.return_value = True

        with pytest.raises(RuntimeError, match="HVC validation failed"):
            run_hvc_validation()
