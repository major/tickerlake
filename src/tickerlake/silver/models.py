"""SQLAlchemy table definitions for silver layer."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DOUBLE_PRECISION,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Table,
)

metadata = MetaData()

# 📊 Dimension table for ticker metadata
ticker_metadata = Table(
    "silver_ticker_metadata",
    metadata,
    Column("ticker", String(10), primary_key=True),
    Column("name", String),
    Column("ticker_type", String(10)),  # CS or ETF
    Column("primary_exchange", String(10)),
    Column("active", Boolean),
    Column("cik", String(20)),
    Index("idx_silver_ticker_metadata_type", "ticker_type"),
)

# 📈 Daily aggregates (split-adjusted OHLCV)
daily_aggregates = Table(
    "silver_daily_aggregates",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", DOUBLE_PRECISION, nullable=False),
    Column("high", DOUBLE_PRECISION, nullable=False),
    Column("low", DOUBLE_PRECISION, nullable=False),
    Column("close", DOUBLE_PRECISION, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("transactions", BigInteger, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_daily_aggs_date", "date"),
)

# 🔍 Daily technical indicators
daily_indicators = Table(
    "silver_daily_indicators",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("sma_20", DOUBLE_PRECISION),
    Column("sma_50", DOUBLE_PRECISION),
    Column("sma_200", DOUBLE_PRECISION),
    Column("atr_14", DOUBLE_PRECISION),
    Column("volume_ma_20", BigInteger),
    Column("volume_ratio", DOUBLE_PRECISION),
    Column("is_hvc", Boolean, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_daily_ind_date", "date"),
    Index("idx_silver_daily_ind_hvc", "is_hvc"),
)

# 📊 Weekly aggregates
weekly_aggregates = Table(
    "silver_weekly_aggregates",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", DOUBLE_PRECISION, nullable=False),
    Column("high", DOUBLE_PRECISION, nullable=False),
    Column("low", DOUBLE_PRECISION, nullable=False),
    Column("close", DOUBLE_PRECISION, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("transactions", BigInteger, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_weekly_aggs_date", "date"),
)

# 🎯 Weekly technical indicators (includes Weinstein Stage Analysis)
weekly_indicators = Table(
    "silver_weekly_indicators",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("sma_20", DOUBLE_PRECISION),
    Column("sma_50", DOUBLE_PRECISION),
    Column("sma_200", DOUBLE_PRECISION),
    Column("atr_14", DOUBLE_PRECISION),
    Column("volume_ma_20", BigInteger),
    Column("volume_ratio", DOUBLE_PRECISION),
    Column("is_hvc", Boolean, nullable=False),
    # Weinstein Stage Analysis columns
    Column("ma_30", DOUBLE_PRECISION),
    Column("price_vs_ma_pct", DOUBLE_PRECISION),
    Column("ma_slope_pct", DOUBLE_PRECISION),
    Column("raw_stage", SmallInteger),  # 1-4
    Column("stage", SmallInteger, nullable=False),  # 1-4
    Column("stage_changed", Boolean, nullable=False),
    Column("weeks_in_stage", Integer, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_weekly_ind_date", "date"),
    Index("idx_silver_weekly_ind_stage", "stage"),
    Index("idx_silver_weekly_ind_hvc", "is_hvc"),
)

# 📅 Monthly aggregates
monthly_aggregates = Table(
    "silver_monthly_aggregates",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", DOUBLE_PRECISION, nullable=False),
    Column("high", DOUBLE_PRECISION, nullable=False),
    Column("low", DOUBLE_PRECISION, nullable=False),
    Column("close", DOUBLE_PRECISION, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("transactions", BigInteger, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_monthly_aggs_date", "date"),
)

# 📊 Monthly technical indicators
monthly_indicators = Table(
    "silver_monthly_indicators",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("sma_20", DOUBLE_PRECISION),
    Column("sma_50", DOUBLE_PRECISION),
    Column("sma_200", DOUBLE_PRECISION),
    Column("atr_14", DOUBLE_PRECISION),
    Column("volume_ma_20", BigInteger),
    Column("volume_ratio", DOUBLE_PRECISION),
    Column("is_hvc", Boolean, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_silver_monthly_ind_date", "date"),
    Index("idx_silver_monthly_ind_hvc", "is_hvc"),
)
