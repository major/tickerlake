"""SQLAlchemy table definitions for gold layer. 🥇"""

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    DOUBLE_PRECISION,
    Integer,
    MetaData,
    String,
    Table,
)

metadata = MetaData()

# 🔥 HVC (High Volume Close) Cache Tables
# These are populated first and reused by all downstream analyses (streaks, breakers, returns)

# Daily HVCs - All periods where volume_ratio >= threshold
hvcs_daily = Table(
    "gold_hvcs_daily",
    metadata,
    Column("ticker", String(10), nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),
    Column("open", DOUBLE_PRECISION, nullable=False),
    Column("high", DOUBLE_PRECISION, nullable=False),
    Column("low", DOUBLE_PRECISION, nullable=False),
    Column("close", DOUBLE_PRECISION, nullable=False),
    Column("volume", BigInteger, nullable=False),  # BigInt for high-volume stocks
    Column("volume_ratio", DOUBLE_PRECISION, nullable=False),  # volume / volume_ma_20
    Column("volume_ma_20", DOUBLE_PRECISION, nullable=False),  # For reference
    Column("calculated_at", DateTime, nullable=False),
)

# Weekly HVCs - All weeks where volume_ratio >= threshold
hvcs_weekly = Table(
    "gold_hvcs_weekly",
    metadata,
    Column("ticker", String(10), nullable=False, index=True),
    Column("date", Date, nullable=False, index=True),  # Week end date
    Column("open", DOUBLE_PRECISION, nullable=False),
    Column("high", DOUBLE_PRECISION, nullable=False),
    Column("low", DOUBLE_PRECISION, nullable=False),
    Column("close", DOUBLE_PRECISION, nullable=False),
    Column("volume", BigInteger, nullable=False),  # BigInt for high-volume stocks
    Column("volume_ratio", DOUBLE_PRECISION, nullable=False),
    Column("volume_ma_20", DOUBLE_PRECISION, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 📊 Daily HVC (High Volume Close) Streaks
# Tracks most recent streak of high-volume closes going in same direction
hvc_streaks_daily = Table(
    "gold_hvc_streaks_daily",
    metadata,
    Column("ticker", String(10), primary_key=True),
    Column("streak_length", Integer, nullable=False),
    Column("direction", String(10), nullable=False),  # 'up' or 'down'
    Column("streak_start_date", Date, nullable=False),
    Column("streak_end_date", Date, nullable=False),  # Most recent HVC date
    Column("latest_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_price_change_pct", DOUBLE_PRECISION, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 📈 Weekly HVC (High Volume Close) Streaks
# More rare and important than daily - tracks weekly volume surges
hvc_streaks_weekly = Table(
    "gold_hvc_streaks_weekly",
    metadata,
    Column("ticker", String(10), primary_key=True),
    Column("streak_length", Integer, nullable=False),
    Column("direction", String(10), nullable=False),  # 'up' or 'down'
    Column("streak_start_date", Date, nullable=False),
    Column("streak_end_date", Date, nullable=False),  # Most recent HVC date
    Column("latest_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_price_change_pct", DOUBLE_PRECISION, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 🔔 Daily HVC Streak Breakers
# Detects when a streak of 3+ HVCs is broken by opposite direction HVC
hvc_streak_breakers_daily = Table(
    "gold_hvc_streak_breakers_daily",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(10), nullable=False, index=True),
    Column("streak_direction", String(10), nullable=False),  # Direction that was broken
    Column("streak_length", Integer, nullable=False),  # How many HVCs in the streak
    Column("streak_start_date", Date, nullable=False),
    Column("streak_end_date", Date, nullable=False),  # Last date before break
    Column("breaker_date", Date, nullable=False),  # Date of the HVC that broke it
    Column("streak_start_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_end_close", DOUBLE_PRECISION, nullable=False),
    Column("breaker_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_price_change_pct", DOUBLE_PRECISION, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 🔔 Weekly HVC Streak Breakers
# Weekly version - more significant momentum shifts
hvc_streak_breakers_weekly = Table(
    "gold_hvc_streak_breakers_weekly",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(10), nullable=False, index=True),
    Column("streak_direction", String(10), nullable=False),
    Column("streak_length", Integer, nullable=False),
    Column("streak_start_date", Date, nullable=False),
    Column("streak_end_date", Date, nullable=False),
    Column("breaker_date", Date, nullable=False),
    Column("streak_start_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_end_close", DOUBLE_PRECISION, nullable=False),
    Column("breaker_close", DOUBLE_PRECISION, nullable=False),
    Column("streak_price_change_pct", DOUBLE_PRECISION, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 🔄 Daily HVC Returns
# Detects when price returns to (falls within) a previous HVC's range
# Useful for identifying revisits to high-volume zones
hvc_returns_daily = Table(
    "gold_hvc_returns_daily",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(10), nullable=False, index=True),
    Column("return_date", Date, nullable=False, index=True),  # Date when price returned
    Column("current_close", DOUBLE_PRECISION, nullable=False),
    Column("hvc_date", Date, nullable=False),  # Date of the HVC being returned to
    Column("hvc_low", DOUBLE_PRECISION, nullable=False),
    Column("hvc_high", DOUBLE_PRECISION, nullable=False),
    Column("hvc_close", DOUBLE_PRECISION, nullable=False),
    Column("hvc_volume_ratio", DOUBLE_PRECISION, nullable=False),
    Column("days_since_hvc", Integer, nullable=False),  # Must be >= 5
    Column("calculated_at", DateTime, nullable=False),
)

# 🔄 Weekly HVC Returns
# Weekly version - more significant when price revisits a prior high-volume week
hvc_returns_weekly = Table(
    "gold_hvc_returns_weekly",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(10), nullable=False, index=True),
    Column("return_date", Date, nullable=False, index=True),  # Week end date when price returned
    Column("current_close", DOUBLE_PRECISION, nullable=False),
    Column("hvc_date", Date, nullable=False),  # Week end date of the HVC being returned to
    Column("hvc_low", DOUBLE_PRECISION, nullable=False),
    Column("hvc_high", DOUBLE_PRECISION, nullable=False),
    Column("hvc_close", DOUBLE_PRECISION, nullable=False),
    Column("hvc_volume_ratio", DOUBLE_PRECISION, nullable=False),
    Column("weeks_since_hvc", Integer, nullable=False),  # Must be >= 4
    Column("calculated_at", DateTime, nullable=False),
)

# 📅 HVC Concentration by Date
# Aggregates count of HVCs per date to identify market-wide high-volume days
hvc_concentration_daily = Table(
    "gold_hvc_concentration_daily",
    metadata,
    Column("date", Date, primary_key=True),
    Column("hvc_count", Integer, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# 📊 HVC Concentration by Week
# Aggregates count of HVCs per week to identify market-wide high-volume weeks
hvc_concentration_weekly = Table(
    "gold_hvc_concentration_weekly",
    metadata,
    Column("date", Date, primary_key=True),  # Week end date
    Column("hvc_count", Integer, nullable=False),
    Column("calculated_at", DateTime, nullable=False),
)

# Future gold layer tables will be added here:
# - Momentum scores
# - Volatility analysis
# - Correlation matrices
# - Sector rotation signals
# etc.
