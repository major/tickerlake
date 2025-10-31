"""Bronze layer schema documentation (Parquet files).

The bronze layer stores raw data from Polygon.io API:
- stocks: Raw OHLCV data (primary key: ticker, date) - DATE PARTITIONED 📁
- tickers: Ticker metadata (primary key: ticker)
- splits: Stock split events (primary key: ticker, execution_date)

Parquet files are stored locally at:
- data/bronze/stocks/ (Hive-partitioned directory with date=YYYY-MM-DD/ subdirs)
- data/bronze/tickers.parquet
- data/bronze/splits.parquet

Schema validation is handled by Polars schemas in src/tickerlake/schemas.py
"""

# Table schemas for reference (actual schemas in schemas.py)

STOCKS_SCHEMA_DOC = """
stocks table (Partitioned Parquet Dataset):
- ticker: String (max 10 chars)
- date: Date
- open: Float32
- high: Float32
- low: Float32
- close: Float32
- volume: Int64
- transactions: Int32

Storage:
- Hive-style partitioned dataset with zstd compression
- Partitioned by date column (creates date=YYYY-MM-DD/ subdirectories)
- Each partition contains one day's worth of data across all tickers
- Benefits: Efficient date-range queries, parallel processing, easy backfills
- Typical size: 2-4GB compressed (5 years of data)
- Fast queries via Polars lazy API
"""

TICKERS_SCHEMA_DOC = """
tickers table (Parquet):
- ticker: String (max 10 chars, primary key)
- name: String
- type: String (CS, ETF, PFD, WARRANT, etc.)
- active: Boolean
- locale: String
- market: String
- primary_exchange: String
- currency_name: String
- currency_symbol: String
- cik: String
- composite_figi: String
- share_class_figi: String
- base_currency_name: String
- base_currency_symbol: String
- delisted_utc: Datetime
- last_updated_utc: Datetime

Storage:
- Single Parquet file with zstd compression
- Typical size: 5-10MB (~10k tickers)
- Full refresh on each run
"""

SPLITS_SCHEMA_DOC = """
splits table (Parquet):
- ticker: String (max 10 chars)
- execution_date: Date
- split_from: Float32
- split_to: Float32

Storage:
- Single Parquet file with zstd compression
- Typical size: 1-2MB (~500 splits)
- Full refresh on each run
"""
