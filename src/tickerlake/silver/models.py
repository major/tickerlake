"""Silver layer schema documentation (Parquet files).

The silver layer stores split-adjusted OHLCV data with technical indicators:
- ticker_metadata: Filtered ticker dimension (CS, ETF only)
- {daily,weekly,monthly}_aggregates: Split-adjusted OHLCV
- {daily,weekly,monthly}_indicators: Technical indicators (SMA, ATR, volume metrics)

Parquet files are stored locally at:
- data/silver/ticker_metadata.parquet
- data/silver/daily_aggregates.parquet
- data/silver/daily_indicators.parquet
- data/silver/weekly_aggregates.parquet
- data/silver/weekly_indicators.parquet
- data/silver/monthly_aggregates.parquet
- data/silver/monthly_indicators.parquet

Schema validation is handled by Polars schemas in src/tickerlake/schemas.py
"""

# Table schemas for reference (actual schemas in schemas.py)

TICKER_METADATA_SCHEMA_DOC = """
ticker_metadata table:
- ticker: String (primary key)
- name: String
- type: String (CS or ETF only - filtered)
- primary_exchange: String
- active: Boolean
- cik: String

Optimized for:
- Single Parquet file by: ticker
- Typical size: 5-10MB (~10k tickers)
- Single file (small table, full rewrite mode)
"""

AGGREGATES_SCHEMA_DOC = """
{daily,weekly,monthly}_aggregates tables:
- ticker: String
- date: Date
- open: Float64 (split-adjusted)
- high: Float64 (split-adjusted)
- low: Float64 (split-adjusted)
- close: Float64 (split-adjusted)
- volume: Int64
- transactions: Int64

Optimized for:
- Single Parquet file by: ticker, date
- Typical size: 2-4GB per timeframe
- Compacted to: ~20 files @ 128MB each
- Append-only (incremental processing)
"""

INDICATORS_SCHEMA_DOC = """
{daily,weekly,monthly}_indicators tables:
- ticker: String
- date: Date
- sma_20: Float64 (20-period simple moving average)
- sma_50: Float64 (50-period SMA)
- sma_200: Float64 (200-period SMA)
- atr_14: Float64 (14-period average true range)
- volume_ma_20: Int64 (20-period volume moving average)
- volume_ratio: Float64 (volume / volume_ma_20, for volume surge detection)

Optimized for:
- Single Parquet file by: ticker, date
- Typical size: 1-2GB per timeframe
- Compacted to: ~10 files @ 128MB each
- Append-only (incremental processing)
- volume_ratio used for identifying unusual volume activity
"""

PROCESSING_NOTES = """
Processing Strategy:

**Smart Incremental Mode** (default):
- Check for new splits since last run
- If splits found: Full rewrite (prices need retroactive adjustment)
- If no splits: Append new data only (fast!)

**Batch Processing** (memory efficiency):
Phase 1 - Aggregation:
  - Process 250 tickers at a time (configurable)
  - Load bronze → apply splits → aggregate to daily/weekly/monthly
  - Write to Parquet immediately
  - Free memory before next batch

Phase 2 - Indicator Calculation:
  - Process 500 tickers at a time (configurable)
  - Read aggregates back → calculate indicators
  - Write to Parquet immediately
  - Free memory before next batch

**Data Quality**:
- Only common stocks (CS) and ETFs included
- Preferred shares, warrants, ADRs, ETNs excluded
- Split adjustments applied retroactively
"""
