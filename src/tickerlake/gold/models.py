"""Gold layer schema documentation (Parquet files).

The gold layer stores business-level analytics for VWAP (Volume Weighted Average Price) signals:
- vwap_signals: YTD and QTD VWAP levels with price comparison signals

Parquet files are stored locally at:
- data/gold/vwap_signals.parquet

Schema validation is handled by Polars schemas in src/tickerlake/schemas.py
"""

VWAP_SIGNALS_SCHEMA_DOC = """
vwap_signals table:
- ticker: String
- date: Date
- close: Float64 (closing price for the period)
- ytd_vwap: Float64 (Year-to-Date VWAP from Jan 1 to current date)
- qtd_vwap: Float64 (Quarter-to-Date VWAP from quarter start to current date)
- above_ytd_vwap: Boolean (True if close > ytd_vwap)
- above_qtd_vwap: Boolean (True if close > qtd_vwap)
- above_both: Boolean (True if close > ytd_vwap AND close > qtd_vwap)
- calculated_at: Datetime (when signal was calculated)

VWAP Formula:
    VWAP = Sum(Price × Volume) / Sum(Volume) over a period

Use Cases:
    - Identify stocks in strong uptrends (above both YTD and QTD VWAP)
    - Filter for stocks where price > ytd_vwap AND price > qtd_vwap
    - Track when stocks cross above/below VWAP levels
    - Use as support/resistance levels for trading decisions

Storage:
- Single Parquet file with zstd compression
- Typical size: 500MB-1GB compressed (all ticker-date combinations)
- Overwrite mode (recalculated each run)
- Fast queries via Polars lazy API
- Filter by above_both = True to find strongest stocks
"""

PROCESSING_NOTES = """
Processing Strategy:

**VWAP Calculation** (vwap_signals.py):
1. Read all daily_aggregates from silver layer (close, volume, date, ticker)
2. Calculate year and quarter columns for grouping
3. Compute cumulative price*volume and cumulative volume using Polars window functions
4. Calculate YTD_VWAP = cum_pv_ytd / cum_volume_ytd
5. Calculate QTD_VWAP = cum_pv_qtd / cum_volume_qtd
6. Generate boolean signals (above_ytd_vwap, above_qtd_vwap, above_both)
7. Write to vwap_signals table

**Performance**:
- Gold layer runtime: 2-5 minutes depending on data volume
- Pure Polars operations for maximum performance
- Polars window functions provide efficient cumulative calculations
- Overwrite mode ensures fresh calculations each run
- Lazy evaluation and streaming for memory efficiency
"""
