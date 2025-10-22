"""DuckDB view definitions for enriched data analysis."""

from pathlib import Path

import duckdb
import polars as pl

from tickerlake.config import settings
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def get_data_path(filename: str) -> str:
    """Get absolute path to a data file.

    Args:
        filename: Name of the file (e.g., 'daily_aggregates.parquet').

    Returns:
        Absolute path to the file.
    """
    if "ticker_metadata" in filename:
        return str(Path(f"{settings.silver_storage_path}/{filename}").resolve())
    return str(Path(f"{settings.silver_storage_path}/stocks/{filename}").resolve())


def create_enriched_view(
    con: duckdb.DuckDBPyConnection, timeframe: str = "daily"
) -> None:
    """Create enriched view joining aggregates, indicators, and ticker metadata.

    Args:
        con: DuckDB connection.
        timeframe: Timeframe for the view ('daily', 'weekly', or 'monthly').
    """
    view_name = f"{timeframe}_enriched"
    aggs_path = get_data_path(f"{timeframe}_aggregates.parquet")
    indicators_path = get_data_path(f"{timeframe}_indicators.parquet")
    metadata_path = get_data_path("ticker_metadata.parquet")

    logger.info(f"Creating view: {view_name}")

    # Base columns for all timeframes
    base_columns = """
            a.ticker,
            a.date,
            a.open,
            a.high,
            a.low,
            a.close,
            a.volume,
            a.transactions,
            i.sma_20,
            i.sma_50,
            i.sma_200,
            i.atr_14,
            i.volume_ma_20,
            i.volume_ratio,
            i.is_hvc,
            t.name,
            t.type,
            t.primary_exchange,
            t.active,
            t.cik"""

    # Add Weinstein stage columns for weekly data only
    if timeframe == "weekly":
        columns = base_columns + """,
            i.ma_30,
            i.price_vs_ma_pct,
            i.ma_slope_pct,
            CAST(i.raw_stage AS UTINYINT) as raw_stage,
            CAST(i.stage AS UTINYINT) as stage,
            i.stage_changed,
            CAST(i.weeks_in_stage AS UTINYINT) as weeks_in_stage"""
    else:
        columns = base_columns

    # Create the enriched view with all data joined
    con.execute(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
{columns}
        FROM read_parquet('{aggs_path}') a
        JOIN read_parquet('{indicators_path}') i
            ON a.ticker = i.ticker AND a.date = i.date
        JOIN read_parquet('{metadata_path}') t
            ON a.ticker = t.ticker
        ORDER BY a.ticker, a.date
    """)

    logger.info(f"✅ Created view: {view_name}")


def create_recent_hvcs_view(con: duckdb.DuckDBPyConnection, days: int = 30) -> None:
    """Create view for recent high volume closes.

    Args:
        con: DuckDB connection.
        days: Number of days to look back (default: 30).
    """
    logger.info(f"Creating view: recent_hvcs (last {days} days)")

    con.execute(f"""
        CREATE OR REPLACE VIEW recent_hvcs AS
        SELECT *
        FROM daily_enriched
        WHERE is_hvc = true
          AND date >= current_date - INTERVAL '{days} days'
        ORDER BY date DESC, volume_ratio DESC
    """)

    logger.info("✅ Created view: recent_hvcs")


def create_liquid_stocks_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view for liquid stocks based on volume thresholds.

    Uses the same thresholds as HVC calculations:
    - CS (Common Stock): 200K+ average daily volume
    - ETF: 50K+ average daily volume
    - Price >= $5
    """
    logger.info("Creating view: liquid_stocks")

    con.execute("""
        CREATE OR REPLACE VIEW liquid_stocks AS
        SELECT *
        FROM daily_enriched
        WHERE (
            (type = 'CS' AND volume_ma_20 >= 200000 AND close >= 5.0)
            OR (type = 'ETF' AND volume_ma_20 >= 50000)
        )
        ORDER BY ticker, date
    """)

    logger.info("✅ Created view: liquid_stocks")


def create_trending_stocks_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view for stocks trending above all moving averages.

    Identifies stocks where:
    - Close > SMA 20
    - Close > SMA 50
    - Close > SMA 200
    """
    logger.info("Creating view: trending_stocks")

    con.execute("""
        CREATE OR REPLACE VIEW trending_stocks AS
        SELECT *
        FROM daily_enriched
        WHERE close > sma_20
          AND close > sma_50
          AND close > sma_200
          AND sma_20 IS NOT NULL
          AND sma_50 IS NOT NULL
          AND sma_200 IS NOT NULL
        ORDER BY volume_ratio DESC, ticker, date
    """)

    logger.info("✅ Created view: trending_stocks")


def create_latest_prices_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view showing the most recent price data for each ticker.

    Useful for screening and getting current state of all tickers.
    """
    logger.info("Creating view: latest_prices")

    con.execute("""
        CREATE OR REPLACE VIEW latest_prices AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM daily_enriched
        )
        SELECT
            ticker, date, open, high, low, close, volume, transactions,
            sma_20, sma_50, sma_200, atr_14, volume_ma_20, volume_ratio, is_hvc,
            name, type, primary_exchange, active, cik
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker
    """)

    logger.info("✅ Created view: latest_prices")


def create_latest_stage_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view showing the most recent Weinstein stage for each ticker.

    Useful for screening stocks by current stage.
    """
    logger.info("Creating view: latest_stage")

    con.execute("""
        CREATE OR REPLACE VIEW latest_stage AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM weekly_enriched
        )
        SELECT
            ticker, name, type, date, close, volume,
            CAST(stage AS UTINYINT) as stage,
            CAST(weeks_in_stage AS UTINYINT) as weeks_in_stage,
            stage_changed,
            price_vs_ma_pct, ma_slope_pct, ma_30,
            sma_20, sma_50, sma_200,
            volume_ma_20, volume_ratio,
            primary_exchange, active
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker
    """)

    logger.info("✅ Created view: latest_stage")


def create_stage_2_stocks_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view for stocks currently in Stage 2 (Advancing).

    Filters for liquid CS stocks in Stage 2 that have been advancing for 2+ weeks.
    """
    logger.info("Creating view: stage_2_stocks")

    con.execute("""
        CREATE OR REPLACE VIEW stage_2_stocks AS
        SELECT *
        FROM latest_stage
        WHERE stage = 2
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
          AND close >= 5.0
        ORDER BY weeks_in_stage DESC, price_vs_ma_pct DESC
    """)

    logger.info("✅ Created view: stage_2_stocks")


def create_stage_4_stocks_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create view for stocks currently in Stage 4 (Declining).

    Filters for liquid CS stocks in Stage 4 that have been declining for 2+ weeks.
    """
    logger.info("Creating view: stage_4_stocks")

    con.execute("""
        CREATE OR REPLACE VIEW stage_4_stocks AS
        SELECT *
        FROM latest_stage
        WHERE stage = 4
          AND type = 'CS'
          AND weeks_in_stage >= 2
          AND volume_ma_20 >= 200000
        ORDER BY weeks_in_stage DESC, price_vs_ma_pct ASC
    """)

    logger.info("✅ Created view: stage_4_stocks")


def create_all_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create all DuckDB views for data analysis.

    Args:
        con: DuckDB connection.
    """
    logger.info("Creating all DuckDB views... 📊")

    # Core enriched views (daily, weekly, monthly)
    for timeframe in ["daily", "weekly", "monthly"]:
        create_enriched_view(con, timeframe)

    # Specialized analysis views
    create_recent_hvcs_view(con, days=30)
    create_liquid_stocks_view(con)
    create_trending_stocks_view(con)
    create_latest_prices_view(con)

    # Weinstein Stage Analysis views
    create_latest_stage_view(con)
    create_stage_2_stocks_view(con)
    create_stage_4_stocks_view(con)

    logger.info("✅ All views created successfully! 🎉")


def list_views(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """List all available views in the database.

    Args:
        con: DuckDB connection.

    Returns:
        DataFrame with view names and types.
    """
    result = con.execute("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_type = 'VIEW'
        ORDER BY table_name
    """).pl()

    return result


def query_view(
    con: duckdb.DuckDBPyConnection, view_name: str, limit: int | None = None
) -> pl.DataFrame:
    """Query a view and return results as a Polars DataFrame.

    Args:
        con: DuckDB connection.
        view_name: Name of the view to query.
        limit: Optional limit on number of rows to return.

    Returns:
        DataFrame with query results.
    """
    query = f"SELECT * FROM {view_name}"
    if limit:
        query += f" LIMIT {limit}"

    result = con.execute(query).pl()
    return result
