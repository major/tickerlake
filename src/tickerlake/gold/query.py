"""Helper functions for querying DuckDB views."""

from datetime import date
from typing import Literal

import duckdb
import polars as pl

from tickerlake.gold.main import get_duckdb_path
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)

TimeframeType = Literal["daily", "weekly", "monthly"]


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection to the gold database.

    Args:
        db_path: Path to database file. If None, uses default gold.duckdb.

    Returns:
        DuckDB connection.
    """
    if db_path is None:
        db_path = get_duckdb_path()

    return duckdb.connect(db_path, read_only=True)


def get_ticker_data(
    ticker: str,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    timeframe: TimeframeType = "daily",
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get enriched data for a specific ticker.

    Args:
        ticker: Ticker symbol to query.
        start_date: Start date for filtering (inclusive). None for no filter.
        end_date: End date for filtering (inclusive). None for no filter.
        timeframe: Timeframe to query ('daily', 'weekly', 'monthly').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with enriched data for the ticker.
    """
    con = get_connection(db_path)
    view_name = f"{timeframe}_enriched"

    query = f"SELECT * FROM {view_name} WHERE ticker = '{ticker}'"

    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"

    query += " ORDER BY date"

    result = con.execute(query).pl()
    con.close()

    return result


def get_recent_hvcs(
    days: int = 30,
    min_ratio: float | None = None,
    ticker_type: Literal["CS", "ETF"] | None = None,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get recent high volume closes.

    Args:
        days: Number of days to look back.
        min_ratio: Minimum volume ratio to filter by (e.g., 3.0 for 3x average).
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with recent HVC records.
    """
    con = get_connection(db_path)

    query = f"SELECT * FROM recent_hvcs WHERE date >= current_date - INTERVAL '{days} days'"

    if min_ratio:
        query += f" AND volume_ratio >= {min_ratio}"
    if ticker_type:
        query += f" AND type = '{ticker_type}'"

    query += " ORDER BY date DESC, volume_ratio DESC"

    result = con.execute(query).pl()
    con.close()

    return result


def get_trending_stocks(
    min_volume: int | None = None,
    ticker_type: Literal["CS", "ETF"] | None = None,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get stocks trending above all moving averages.

    Args:
        min_volume: Minimum average daily volume to filter by.
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with trending stocks.
    """
    con = get_connection(db_path)

    query = "SELECT * FROM trending_stocks WHERE 1=1"

    if min_volume:
        query += f" AND volume_ma_20 >= {min_volume}"
    if ticker_type:
        query += f" AND type = '{ticker_type}'"

    query += " ORDER BY volume_ratio DESC, date DESC"

    result = con.execute(query).pl()
    con.close()

    return result


def get_latest_prices(
    tickers: list[str] | None = None,
    ticker_type: Literal["CS", "ETF"] | None = None,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get latest price data for tickers.

    Args:
        tickers: List of tickers to filter by. None for all tickers.
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with latest prices.
    """
    con = get_connection(db_path)

    query = "SELECT * FROM latest_prices WHERE 1=1"

    if tickers:
        ticker_list = "','".join(tickers)
        query += f" AND ticker IN ('{ticker_list}')"
    if ticker_type:
        query += f" AND type = '{ticker_type}'"

    query += " ORDER BY ticker"

    result = con.execute(query).pl()
    con.close()

    return result


def scan_for_pattern(
    pattern_query: str,
    timeframe: TimeframeType = "daily",
    db_path: str | None = None,
) -> pl.DataFrame:
    """Scan all tickers for a specific pattern using SQL.

    This is useful for backtesting custom patterns across all tickers.

    Args:
        pattern_query: SQL WHERE clause to filter by (without 'WHERE' keyword).
                      Example: "close > sma_20 AND volume_ratio > 2.0"
        timeframe: Timeframe to query ('daily', 'weekly', 'monthly').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with matching records.
    """
    con = get_connection(db_path)
    view_name = f"{timeframe}_enriched"

    query = f"SELECT * FROM {view_name} WHERE {pattern_query} ORDER BY date DESC, ticker"

    result = con.execute(query).pl()
    con.close()

    return result


def execute_custom_query(query: str, db_path: str | None = None) -> pl.DataFrame:
    """Execute a custom SQL query against the gold database.

    Args:
        query: SQL query to execute.
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with query results.
    """
    con = get_connection(db_path)
    result = con.execute(query).pl()
    con.close()

    return result


def get_stocks_by_stage(
    stage: int,
    ticker_type: Literal["CS", "ETF"] = "CS",
    min_weeks: int = 2,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get stocks currently in a specific Weinstein stage.

    Args:
        stage: Weinstein stage (1-4) to filter by.
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        min_weeks: Minimum weeks in stage to filter by (default: 2).
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with stocks in the specified stage.
    """
    con = get_connection(db_path)

    query = f"""
        SELECT *
        FROM latest_stage
        WHERE stage = {stage}
          AND type = '{ticker_type}'
          AND weeks_in_stage >= {min_weeks}
        ORDER BY weeks_in_stage DESC, ticker
    """

    result = con.execute(query).pl()
    con.close()

    return result


def get_stage_2_stocks(
    min_weeks: int = 2,
    ticker_type: Literal["CS", "ETF"] = "CS",
    min_price: float = 5.0,
    min_volume: int = 200000,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get stocks currently in Stage 2 (Advancing).

    Filters for liquid stocks that have been in Stage 2 for a minimum duration.

    Args:
        min_weeks: Minimum weeks in Stage 2 (default: 2).
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        min_price: Minimum stock price (default: $5).
        min_volume: Minimum average daily volume (default: 200K).
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with Stage 2 stocks.
    """
    con = get_connection(db_path)

    query = f"""
        SELECT *
        FROM latest_stage
        WHERE stage = 2
          AND type = '{ticker_type}'
          AND weeks_in_stage >= {min_weeks}
          AND close >= {min_price}
          AND volume_ma_20 >= {min_volume}
        ORDER BY weeks_in_stage DESC, price_vs_ma_pct DESC
    """

    result = con.execute(query).pl()
    con.close()

    return result


def get_stage_transitions(
    weeks: int = 4,
    from_stage: int | None = None,
    to_stage: int | None = None,
    ticker_type: Literal["CS", "ETF"] | None = None,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get recent stage transitions.

    Useful for finding stocks that recently changed stages.

    Args:
        weeks: Number of weeks to look back (default: 4).
        from_stage: Filter transitions from this stage (optional).
        to_stage: Filter transitions to this stage (optional).
        ticker_type: Filter by ticker type ('CS' or 'ETF').
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with recent stage transitions.
    """
    con = get_connection(db_path)

    query = f"""
        WITH current_stage AS (
            SELECT ticker, stage, weeks_in_stage, date
            FROM latest_stage
            WHERE stage_changed = true
              AND weeks_in_stage <= {weeks}
        ),
        previous_stage AS (
            SELECT
                w.ticker,
                w.stage as prev_stage,
                w.date as prev_date,
                ROW_NUMBER() OVER (PARTITION BY w.ticker ORDER BY w.date DESC) as rn
            FROM weekly_enriched w
            WHERE w.stage_changed = true
                AND EXISTS (
                    SELECT 1 FROM current_stage cs2
                    WHERE cs2.ticker = w.ticker AND w.date < cs2.date
                )
        )
        SELECT
            cs.ticker,
            ls.name,
            ls.type,
            ps.prev_stage,
            cs.stage as current_stage,
            cs.date as transition_date,
            cs.weeks_in_stage,
            ls.close,
            ls.price_vs_ma_pct,
            ls.ma_slope_pct
        FROM current_stage cs
        JOIN latest_stage ls ON cs.ticker = ls.ticker
        LEFT JOIN previous_stage ps ON cs.ticker = ps.ticker AND ps.rn = 1
        WHERE 1=1
    """

    if from_stage is not None:
        query += f" AND ps.prev_stage = {from_stage}"
    if to_stage is not None:
        query += f" AND cs.stage = {to_stage}"
    if ticker_type:
        query += f" AND ls.type = '{ticker_type}'"

    query += " ORDER BY transition_date DESC, ticker"

    result = con.execute(query).pl()
    con.close()

    return result


def get_stage_history(
    ticker: str,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    db_path: str | None = None,
) -> pl.DataFrame:
    """Get full Weinstein stage history for a ticker.

    Args:
        ticker: Ticker symbol to query.
        start_date: Start date for filtering (inclusive). None for no filter.
        end_date: End date for filtering (inclusive). None for no filter.
        db_path: Path to database file. If None, uses default.

    Returns:
        DataFrame with weekly stage history.
    """
    con = get_connection(db_path)

    query = f"""
        SELECT
            ticker, date, close, volume,
            stage, weeks_in_stage, stage_changed,
            price_vs_ma_pct, ma_slope_pct, ma_30,
            sma_20, sma_50, sma_200
        FROM weekly_enriched
        WHERE ticker = '{ticker}'
    """

    if start_date:
        query += f" AND date >= '{start_date}'"
    if end_date:
        query += f" AND date <= '{end_date}'"

    query += " ORDER BY date"

    result = con.execute(query).pl()
    con.close()

    return result
