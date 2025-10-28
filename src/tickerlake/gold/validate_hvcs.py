"""HVC validation - ensures known test cases are correctly identified. ✅

This module validates that the HVC identification system is working correctly
by checking known test cases against expected results.
"""

from datetime import date

from sqlalchemy import select

from tickerlake.db import get_engine
from tickerlake.gold.models import hvcs_daily, hvcs_weekly
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


# Known test cases with expected results 🧪
EXPECTED_WEEKLY_HVCS = [
    {"ticker": "DELL", "date": date(2024, 2, 26), "min_ratio": 4.0},
    {"ticker": "CRDO", "date": date(2024, 12, 2), "min_ratio": 3.1},
]

EXPECTED_DAILY_HVCS = [
    {"ticker": "CRDO", "date": date(2025, 6, 3), "min_ratio": 4.6},
]

EXPECTED_NOT_HVCS_WEEKLY = [
    {
        "ticker": "AMD",
        "date": date(2025, 10, 6),
        "reason": "volume ratio 2.69x < 3.0x threshold",
    },
]


def validate_hvc_present(
    ticker: str, check_date: date, timeframe: str, min_ratio: float
) -> bool:
    """Validate that a specific HVC is present in the cache. ✅

    Args:
        ticker: Stock ticker symbol.
        check_date: Date to check.
        timeframe: 'daily' or 'weekly'.
        min_ratio: Minimum expected volume ratio.

    Returns:
        True if HVC is found and meets criteria, False otherwise.
    """
    engine = get_engine()
    table = hvcs_daily if timeframe == "daily" else hvcs_weekly

    with engine.connect() as conn:
        query = select(
            table.c.ticker, table.c.date, table.c.volume_ratio
        ).where((table.c.ticker == ticker) & (table.c.date == check_date))

        result = conn.execute(query)
        row = result.fetchone()

    if row is None:
        logger.error(
            f"❌ {ticker} {check_date} ({timeframe}): NOT FOUND in HVC cache"
        )
        return False

    if row[2] < min_ratio:
        logger.error(
            f"❌ {ticker} {check_date} ({timeframe}): "
            f"Found but ratio {row[2]:.2f}x < expected {min_ratio:.2f}x"
        )
        return False

    logger.info(
        f"✅ {ticker} {check_date} ({timeframe}): "
        f"VALID HVC with ratio {row[2]:.2f}x"
    )
    return True


def validate_hvc_absent(
    ticker: str, check_date: date, timeframe: str, reason: str
) -> bool:
    """Validate that a specific date is NOT identified as an HVC. ✅

    Args:
        ticker: Stock ticker symbol.
        check_date: Date to check.
        timeframe: 'daily' or 'weekly'.
        reason: Expected reason why it's not an HVC.

    Returns:
        True if HVC is correctly absent, False if incorrectly present.
    """
    engine = get_engine()
    table = hvcs_daily if timeframe == "daily" else hvcs_weekly

    with engine.connect() as conn:
        query = select(
            table.c.ticker, table.c.date, table.c.volume_ratio
        ).where((table.c.ticker == ticker) & (table.c.date == check_date))

        result = conn.execute(query)
        row = result.fetchone()

    if row is not None:
        logger.error(
            f"❌ {ticker} {check_date} ({timeframe}): "
            f"INCORRECTLY identified as HVC (ratio {row[2]:.2f}x)"
        )
        return False

    logger.info(
        f"✅ {ticker} {check_date} ({timeframe}): "
        f"Correctly NOT an HVC ({reason})"
    )
    return True


def run_hvc_validation() -> None:
    """Run all HVC validation checks. 🧪

    Validates that known test cases are correctly identified (or not identified)
    as HVCs. This ensures the HVC identification system is working as expected.

    Raises:
        RuntimeError: If any validation check fails.
    """
    logger.info("🧪 Starting HVC Validation...")

    all_passed = True

    # Check expected weekly HVCs
    logger.info("=" * 60)
    logger.info("📊 Validating Expected Weekly HVCs...")
    logger.info("=" * 60)
    for case in EXPECTED_WEEKLY_HVCS:
        passed = validate_hvc_present(
            case["ticker"], case["date"], "weekly", case["min_ratio"]
        )
        all_passed = all_passed and passed

    # Check expected daily HVCs
    logger.info("=" * 60)
    logger.info("📊 Validating Expected Daily HVCs...")
    logger.info("=" * 60)
    for case in EXPECTED_DAILY_HVCS:
        passed = validate_hvc_present(
            case["ticker"], case["date"], "daily", case["min_ratio"]
        )
        all_passed = all_passed and passed

    # Check expected non-HVCs
    logger.info("=" * 60)
    logger.info("📊 Validating Expected Non-HVCs...")
    logger.info("=" * 60)
    for case in EXPECTED_NOT_HVCS_WEEKLY:
        passed = validate_hvc_absent(
            case["ticker"], case["date"], "weekly", case["reason"]
        )
        all_passed = all_passed and passed

    # Summary
    logger.info("=" * 60)
    if all_passed:
        logger.info("✅ All HVC validation checks PASSED! 🎉")
    else:
        logger.error("❌ Some HVC validation checks FAILED!")
        raise RuntimeError(
            "HVC validation failed - check logs for details. "
            "This indicates the HVC identification system may not be working correctly."
        )
    logger.info("=" * 60)
