"""PostgreSQL schema definitions for tickerlake data."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import adbc_driver_postgresql.dbapi as pg

_DDL = (
    "CREATE SCHEMA IF NOT EXISTS raw",
    """
    CREATE TABLE IF NOT EXISTS raw.raw_daily_bars (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        vwap REAL,
        transactions INTEGER,
        PRIMARY KEY (ticker, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.splits (
        ticker VARCHAR NOT NULL,
        execution_date DATE NOT NULL,
        split_from REAL,
        split_to REAL,
        adjustment_factor DOUBLE PRECISION,
        adjustment_type VARCHAR,
        PRIMARY KEY (ticker, execution_date)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_raw_daily_bars_date ON raw.raw_daily_bars (date)",
    "CREATE SCHEMA IF NOT EXISTS tickerlake",
    """
    CREATE TABLE IF NOT EXISTS tickerlake.daily_bars (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        vwap REAL,
        transactions INTEGER,
        PRIMARY KEY (ticker, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.weekly_bars (
        LIKE tickerlake.daily_bars INCLUDING ALL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.monthly_bars (
        LIKE tickerlake.daily_bars INCLUDING ALL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.daily_metrics (
        date DATE NOT NULL,
        ticker VARCHAR NOT NULL,
        sma_20 REAL,
        sma_50 REAL,
        sma_200 REAL,
        atr_14 REAL,
        atr_pct REAL,
        adr_pct REAL,
        volume_sma_20 REAL,
        PRIMARY KEY (ticker, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.weekly_metrics (
        LIKE tickerlake.daily_metrics INCLUDING ALL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.monthly_metrics (
        LIKE tickerlake.daily_metrics INCLUDING ALL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.tickers (
        ticker VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR,
        type VARCHAR,
        primary_exchange VARCHAR,
        cik VARCHAR,
        active BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tickerlake.weekly_fib_zones (
        ticker VARCHAR NOT NULL PRIMARY KEY,
        as_of_date DATE,
        swing_low REAL,
        swing_high REAL,
        range REAL,
        swing_low_date DATE,
        swing_high_date DATE,
        bars_since_swing_high INTEGER,
        ibz_low REAL,
        ibz_high REAL,
        smz_low REAL,
        smz_high REAL,
        current_price REAL,
        pct_retracement REAL,
        zone VARCHAR,
        primary_degree INTEGER,
        primary_status VARCHAR,
        still_making_new_highs BOOLEAN,
        zigzag_pct REAL,
        bar_count INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_daily_bars_date ON tickerlake.daily_bars (date)",
    (
        "CREATE INDEX IF NOT EXISTS idx_daily_metrics_date "
        "ON tickerlake.daily_metrics (date)"
    ),
)


def init_schema(conn: pg.Connection) -> None:
    """Create tickerlake's PostgreSQL schemas, tables, and indexes if absent."""
    cursor = conn.cursor()
    try:
        for statement in _DDL:
            cursor.execute(statement)
    finally:
        cursor.close()
