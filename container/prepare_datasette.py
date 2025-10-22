"""Prepare data for Datasette container.

This script exports data from the gold DuckDB database into SQLite format
for serving via Datasette.
"""

import sqlite3
from pathlib import Path

import duckdb

from tickerlake.config import settings
from tickerlake.gold.main import get_duckdb_path
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def create_datasette_db(
    stage_db_path: str = "./container/stage_analysis.db",
    hvc_db_path: str = "./container/high_volume_close.db",
) -> None:
    """Create SQLite databases for Datasette from DuckDB gold layer.

    Creates two separate databases:
    - stage_analysis.db: Contains Weinstein stage tables
    - high_volume_close.db: Contains daily and weekly HVC tables

    Args:
        stage_db_path: Path to output stage analysis database.
        hvc_db_path: Path to output high volume close database.
    """
    logger.info("📊 Preparing Datasette databases...")

    # Connect to DuckDB gold database
    duck_path = get_duckdb_path()
    logger.info(f"Reading from DuckDB: {duck_path}")
    duck_con = duckdb.connect(duck_path, read_only=True)

    # Create stage analysis SQLite database
    stage_file = Path(stage_db_path)
    stage_file.parent.mkdir(parents=True, exist_ok=True)
    if stage_file.exists():
        stage_file.unlink()
        logger.info(f"Removed existing database: {stage_db_path}")

    stage_con = sqlite3.connect(stage_db_path)
    logger.info(f"Created SQLite database: {stage_db_path}")

    # Create HVC SQLite database
    hvc_file = Path(hvc_db_path)
    hvc_file.parent.mkdir(parents=True, exist_ok=True)
    if hvc_file.exists():
        hvc_file.unlink()
        logger.info(f"Removed existing database: {hvc_db_path}")

    hvc_con = sqlite3.connect(hvc_db_path)
    logger.info(f"Created SQLite database: {hvc_db_path}")

    # Export Stage 1 stocks
    logger.info("📋 Exporting Stage 1 stocks...")
    stage_1_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(close, 2) as close,
            volume,
            stage,
            weeks_in_stage,
            ROUND(ma_slope_pct, 2) as ma_slope_pct,
            ROUND(ma_30, 2) as ma_30,
            ROUND(volume_ratio, 2) as volume_ratio
        FROM latest_stage
        WHERE stage = 1
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
          AND close >= 5.0
        ORDER BY weeks_in_stage DESC, ticker
    """).df()
    stage_1_df.to_sql("stage_1_stocks", stage_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(stage_1_df)} Stage 1 stocks")

    # Export Stage 2 stocks
    logger.info("📈 Exporting Stage 2 stocks...")
    stage_2_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(close, 2) as close,
            volume,
            stage,
            weeks_in_stage,
            ROUND(ma_slope_pct, 2) as ma_slope_pct,
            ROUND(ma_30, 2) as ma_30,
            ROUND(volume_ratio, 2) as volume_ratio
        FROM latest_stage
        WHERE stage = 2
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
          AND close >= 5.0
        ORDER BY weeks_in_stage DESC, ticker
    """).df()
    stage_2_df.to_sql("stage_2_stocks", stage_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(stage_2_df)} Stage 2 stocks")

    # Export Stage 3 stocks
    logger.info("📊 Exporting Stage 3 stocks...")
    stage_3_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(close, 2) as close,
            volume,
            stage,
            weeks_in_stage,
            ROUND(ma_slope_pct, 2) as ma_slope_pct,
            ROUND(ma_30, 2) as ma_30,
            ROUND(volume_ratio, 2) as volume_ratio
        FROM latest_stage
        WHERE stage = 3
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
          AND close >= 5.0
        ORDER BY weeks_in_stage DESC, ticker
    """).df()
    stage_3_df.to_sql("stage_3_stocks", stage_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(stage_3_df)} Stage 3 stocks")

    # Export Stage 4 stocks
    logger.info("📉 Exporting Stage 4 stocks...")
    stage_4_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(close, 2) as close,
            volume,
            stage,
            weeks_in_stage,
            ROUND(ma_slope_pct, 2) as ma_slope_pct,
            ROUND(ma_30, 2) as ma_30,
            ROUND(volume_ratio, 2) as volume_ratio
        FROM latest_stage
        WHERE stage = 4
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
        ORDER BY weeks_in_stage DESC, ticker
    """).df()
    stage_4_df.to_sql("stage_4_stocks", stage_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(stage_4_df)} Stage 4 stocks")

    # Export Daily HVCs
    logger.info("🔥 Exporting Daily HVCs...")
    daily_hvcs_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(open, 2) as open,
            ROUND(high, 2) as high,
            ROUND(low, 2) as low,
            ROUND(close, 2) as close,
            volume,
            ROUND(volume_ratio, 2) as volume_ratio
        FROM daily_enriched
        WHERE is_hvc = true
          AND type = 'CS'
        ORDER BY date DESC, volume_ratio DESC
    """).df()
    daily_hvcs_df.to_sql("daily_hvcs", hvc_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(daily_hvcs_df)} Daily HVCs")

    # Export Weekly HVCs
    logger.info("🔥 Exporting Weekly HVCs...")
    weekly_hvcs_df = duck_con.execute("""
        SELECT
            ticker,
            name,
            date,
            ROUND(close, 2) as close,
            volume,
            ROUND(volume_ratio, 2) as volume_ratio,
            stage,
            weeks_in_stage,
            ROUND(price_vs_ma_pct, 2) as price_vs_ma_pct,
            ROUND(ma_slope_pct, 2) as ma_slope_pct,
            ROUND(ma_30, 2) as ma_30
        FROM weekly_enriched
        WHERE is_hvc = true
          AND type = 'CS'
        ORDER BY date DESC, volume_ratio DESC
    """).df()
    weekly_hvcs_df.to_sql("weekly_hvcs", hvc_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Exported {len(weekly_hvcs_df)} Weekly HVCs")

    # Create stage summary table
    logger.info("📊 Creating stage summary table...")
    import pandas as pd
    stage_summary_data = {
        "category": [
            "Stage 1 Stocks",
            "Stage 2 Stocks",
            "Stage 3 Stocks",
            "Stage 4 Stocks",
        ],
        "count": [
            len(stage_1_df),
            len(stage_2_df),
            len(stage_3_df),
            len(stage_4_df),
        ],
        "description": [
            "Stocks in Stage 1 (Basing)",
            "Stocks in Stage 2 (Advancing)",
            "Stocks in Stage 3 (Topping)",
            "Stocks in Stage 4 (Declining)",
        ],
    }
    stage_summary_df = pd.DataFrame(stage_summary_data)
    stage_summary_df.to_sql("summary", stage_con, if_exists="replace", index=False)
    logger.info(f"  ✅ Created stage summary table")

    # Close connections
    duck_con.close()
    stage_con.close()
    hvc_con.close()

    logger.info(f"✅ Datasette databases ready:")
    logger.info(f"  📊 Stage Analysis: {stage_db_path}")
    logger.info(f"     Size: {stage_file.stat().st_size / 1024 / 1024:.2f} MB")
    logger.info(f"  🔥 High Volume Close: {hvc_db_path}")
    logger.info(f"     Size: {hvc_file.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    create_datasette_db()
