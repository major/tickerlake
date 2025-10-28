"""Gold layer - Business-level analytics and reports. 🥇"""

import argparse

from tickerlake.gold.hvcs import run_hvc_identification
from tickerlake.gold.hvc_returns import run_hvc_returns_analysis
from tickerlake.gold.hvc_streak_breakers import run_streak_breaker_analysis
from tickerlake.gold.hvc_streaks import run_hvc_streaks_analysis
from tickerlake.gold.postgres import clear_all_tables, init_gold_schema, reset_schema
from tickerlake.gold.validate_hvcs import run_hvc_validation
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def main(reset_schema_flag: bool = False) -> None:
    """Execute all gold layer reports. 🥇

    Runs all configured business analytics in sequence:
    1. HVC (High Volume Close) Identification (caching from silver layer)
    2. HVC Streaks Analysis
    3. HVC Streak Breakers (Momentum Reversals)
    4. HVC Returns (Price revisiting previous HVC zones)
    5. [Future] Momentum Analysis
    6. [Future] Volatility Analysis
    etc.

    Args:
        reset_schema_flag: If True, drop and recreate all gold tables before running.
    """
    logger.info("🥇 Starting Gold Layer Pipeline...")

    # Schema management
    if reset_schema_flag:
        reset_schema()
    else:
        init_gold_schema()
        # Clear existing analytics since we're recalculating fresh snapshots
        clear_all_tables()

    # STEP 1: Identify and cache all HVCs (this MUST run first!)
    logger.info("=" * 60)
    logger.info("🔥 Running HVC Identification (caching from silver layer)...")
    logger.info("=" * 60)
    run_hvc_identification()

    # Run all downstream analyses (they all read from cached HVCs)
    logger.info("=" * 60)
    logger.info("📊 Running HVC Streaks Analysis...")
    logger.info("=" * 60)
    run_hvc_streaks_analysis()

    logger.info("=" * 60)
    logger.info("🔔 Running HVC Streak Breakers Analysis...")
    logger.info("=" * 60)
    run_streak_breaker_analysis()

    logger.info("=" * 60)
    logger.info("🔄 Running HVC Returns Analysis...")
    logger.info("=" * 60)
    run_hvc_returns_analysis()

    # Future reports will be added here:
    # logger.info("=" * 60)
    # logger.info("📈 Running Momentum Analysis...")
    # logger.info("=" * 60)
    # run_momentum_analysis()

    # Validate HVC identification is working correctly
    logger.info("=" * 60)
    logger.info("🧪 Running HVC Validation...")
    logger.info("=" * 60)
    run_hvc_validation()

    logger.info("=" * 60)
    logger.info("✅ Gold Layer Complete! 🎉")
    logger.info("=" * 60)


def cli() -> None:  # pragma: no cover
    """CLI entry point with argument parsing. 🖥️"""
    parser = argparse.ArgumentParser(
        description="🥇 Gold layer: Business-level analytics and reports"
    )
    parser.add_argument(
        "--reset-schema",
        action="store_true",
        help="⚠️  Drop and recreate all gold tables (use when schema changes)",
    )

    args = parser.parse_args()
    main(reset_schema_flag=args.reset_schema)


if __name__ == "__main__":  # pragma: no cover
    cli()
