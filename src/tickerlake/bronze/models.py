"""SQLAlchemy table definitions for bronze layer."""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    REAL,
    String,
    Table,
    TIMESTAMP,
)

metadata = MetaData()

stocks = Table(
    "stocks",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("date", Date, nullable=False),
    Column("open", REAL, nullable=False),
    Column("high", REAL, nullable=False),
    Column("low", REAL, nullable=False),
    Column("close", REAL, nullable=False),
    Column("volume", BigInteger, nullable=False),
    Column("transactions", Integer, nullable=False),
    PrimaryKeyConstraint("ticker", "date"),
    Index("idx_stocks_date", "date"),
)

tickers = Table(
    "tickers",
    metadata,
    Column("ticker", String(10), primary_key=True),
    Column("name", String),
    Column("ticker_type", String(10)),
    Column("active", Boolean),
    Column("locale", String(5)),
    Column("market", String(20)),
    Column("primary_exchange", String(10)),
    Column("currency_name", String(10)),
    Column("currency_symbol", String(5)),
    Column("cik", String(20)),
    Column("composite_figi", String(20)),
    Column("share_class_figi", String(20)),
    Column("base_currency_name", String(10)),
    Column("base_currency_symbol", String(5)),
    Column("delisted_utc", TIMESTAMP),
    Column("last_updated_utc", TIMESTAMP),
)

splits = Table(
    "splits",
    metadata,
    Column("ticker", String(10), nullable=False),
    Column("execution_date", Date, nullable=False),
    Column("split_from", REAL, nullable=False),
    Column("split_to", REAL, nullable=False),
    PrimaryKeyConstraint("ticker", "execution_date"),
)
