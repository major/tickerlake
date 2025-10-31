"""Gold layer - Business-level analytics and reports. 🥇"""

import argparse

from tickerlake.gold.vwap_signals import run_vwap_analysis
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def main() -> None:
    """Execute all gold layer reports. 🥇

    Runs all configured business analytics in sequence:
    1. VWAP Signals (Year-to-Date and Quarter-to-Date VWAP analysis)
    2. [Future] Momentum Analysis
    3. [Future] Volatility Analysis
    etc.

    Note: With Parquet files, no schema initialization is needed. Files are
    created automatically on first write. Most gold tables use overwrite mode
    for fresh calculations each run.
    """
    logger.info("🥇 Starting Gold Layer Pipeline...")

    # Calculate VWAP signals
    logger.info("=" * 60)
    logger.info("📊 Running VWAP Signals Analysis...")
    logger.info("=" * 60)
    run_vwap_analysis()

    # Future reports will be added here:
    # logger.info("=" * 60)
    # logger.info("📈 Running Momentum Analysis...")
    # logger.info("=" * 60)
    # run_momentum_analysis()

    logger.info("=" * 60)
    logger.info("✅ Gold Layer Complete! 🎉")
    logger.info("=" * 60)


def cli() -> None:  # pragma: no cover
    """CLI entry point. 🖥️"""
    parser = argparse.ArgumentParser(
        description="🥇 Gold layer: Business-level analytics and reports"
    )

    parser.parse_args()  # Parse args for help/version, but no flags needed currently
    main()


if __name__ == "__main__":  # pragma: no cover
    cli()
