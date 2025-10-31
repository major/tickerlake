"""VWAP (Volume Weighted Average Price) signal identification for gold layer. 📊

This module calculates Year-to-Date (YTD) and Quarter-to-Date (QTD) VWAP levels
for all stocks and identifies those trading above both levels.

VWAP Formula:
    VWAP = Sum(Price × Volume) / Sum(Volume) over a period

Signals:
    - YTD VWAP: Calculated from January 1st to current date
    - QTD VWAP: Calculated from quarter start (Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct) to current date
    - above_both: Stocks where current_price > YTD_VWAP AND current_price > QTD_VWAP
"""

import polars as pl

from tickerlake.logging_config import get_logger
from tickerlake.storage.operations import read_table, write_table
from tickerlake.storage.paths import get_table_path
from tickerlake.utils import add_timestamp

logger = get_logger(__name__)


def calculate_vwap_signals() -> pl.DataFrame:
    """Calculate YTD and QTD VWAP signals from silver layer daily aggregates. 📈

    Process:
        1. Read all daily_aggregates from silver layer
        2. Calculate year and quarter for each date
        3. Compute cumulative price*volume and cumulative volume within periods
        4. Calculate YTD_VWAP and QTD_VWAP
        5. Identify stocks above both VWAPs

    Returns:
        DataFrame with VWAP signals for all ticker-date combinations
    """
    logger.info("📊 Calculating VWAP signals from silver layer...")

    # Get Parquet file path for silver daily aggregates
    agg_table = get_table_path("silver", "daily_aggregates")

    # Read silver layer data with pure Polars
    logger.info("📖 Reading daily aggregates from silver layer...")
    df = (
        read_table(agg_table)
        .filter(
            (pl.col("close").is_not_null())
            & (pl.col("volume").is_not_null())
            & (pl.col("volume") > 0)
        )
        .select(["ticker", "date", "close", "volume"])
        .sort(["ticker", "date"])
    )

    if len(df) == 0:
        logger.warning("⚠️  No daily aggregates found in silver layer")
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.Date,
                "close": pl.Float64,
                "ytd_vwap": pl.Float64,
                "qtd_vwap": pl.Float64,
                "above_ytd_vwap": pl.Boolean,
                "above_qtd_vwap": pl.Boolean,
                "above_both": pl.Boolean,
                "calculated_at": pl.Datetime,
            }
        )

    logger.info(
        f"✅ Loaded {len(df):,} daily records for {df['ticker'].n_unique():,} tickers"
    )

    # Calculate year and quarter for grouping
    logger.info("🗓️  Calculating year and quarter columns...")
    df = df.with_columns(
        [
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.quarter().alias("quarter"),
            (pl.col("close") * pl.col("volume")).alias("price_volume"),
        ]
    )

    # Calculate YTD VWAP (cumulative within ticker + year)
    logger.info("📈 Calculating YTD VWAP...")
    df = df.with_columns(
        [
            pl.col("price_volume")
            .cum_sum()
            .over(["ticker", "year"])
            .alias("ytd_cum_pv"),
            pl.col("volume").cum_sum().over(["ticker", "year"]).alias("ytd_cum_volume"),
        ]
    )

    # Calculate QTD VWAP (cumulative within ticker + year + quarter)
    logger.info("📈 Calculating QTD VWAP...")
    df = df.with_columns(
        [
            pl.col("price_volume")
            .cum_sum()
            .over(["ticker", "year", "quarter"])
            .alias("qtd_cum_pv"),
            pl.col("volume")
            .cum_sum()
            .over(["ticker", "year", "quarter"])
            .alias("qtd_cum_volume"),
        ]
    )

    # Calculate VWAP values
    logger.info("🧮 Computing VWAP values and signals...")
    df = df.with_columns(
        [
            (pl.col("ytd_cum_pv") / pl.col("ytd_cum_volume")).alias("ytd_vwap"),
            (pl.col("qtd_cum_pv") / pl.col("qtd_cum_volume")).alias("qtd_vwap"),
        ]
    )

    # Create boolean signals
    df = df.with_columns(
        [
            (pl.col("close") > pl.col("ytd_vwap")).alias("above_ytd_vwap"),
            (pl.col("close") > pl.col("qtd_vwap")).alias("above_qtd_vwap"),
        ]
    )

    df = df.with_columns(
        [
            (pl.col("above_ytd_vwap") & pl.col("above_qtd_vwap")).alias("above_both")
        ]
    )

    # Select final columns
    result = df.select(
        [
            "ticker",
            "date",
            "close",
            "ytd_vwap",
            "qtd_vwap",
            "above_ytd_vwap",
            "above_qtd_vwap",
            "above_both",
        ]
    )

    # Add timestamp
    result = add_timestamp(result)

    # Log summary statistics
    total_signals = len(result)
    above_both_count = result.filter(pl.col("above_both")).height
    above_both_pct = (above_both_count / total_signals * 100) if total_signals > 0 else 0

    logger.info(f"✅ Calculated VWAP signals for {total_signals:,} ticker-date pairs")
    logger.info(
        f"📊 Stocks above both VWAPs: {above_both_count:,} ({above_both_pct:.1f}%)"
    )

    # Show sample of latest signals above both
    latest_date = result["date"].max()
    latest_above_both = result.filter(
        (pl.col("date") == latest_date) & pl.col("above_both")
    ).select(["ticker", "close", "ytd_vwap", "qtd_vwap"])

    if len(latest_above_both) > 0:
        logger.info(
            f"💡 Latest date ({latest_date}): "
            f"{len(latest_above_both):,} stocks above both VWAPs"
        )
    else:
        logger.info(f"ℹ️  Latest date ({latest_date}): No stocks above both VWAPs")

    return result


def run_vwap_analysis() -> None:
    """Main entry point for VWAP analysis. 🎯

    Calculates all VWAP signals from silver layer and writes them to gold layer table.
    """
    logger.info("📊 Starting VWAP Analysis...")

    # Calculate VWAP signals
    results = calculate_vwap_signals()

    if len(results) > 0:
        # Write to gold layer
        table_path = get_table_path("gold", "vwap_signals")
        write_table(table_path, results, mode="overwrite")
        logger.info(f"💾 Wrote {len(results):,} VWAP signals to Parquet")
    else:
        logger.info("ℹ️  No VWAP signals to write")

    logger.info("✅ VWAP Analysis Complete! 🎉")
