# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development
- `uv sync` - Install all dependencies
- `uv run bronze` - Run the bronze layer (data ingestion)
- `uv run silver` - Run the silver layer (incremental processing by default)
- `uv run gold` - Run the gold layer (analytics and exports)

### Silver Layer Modes
- **Incremental mode** (default): Processes only new data since last run
  - Checks max date in Delta table
  - Only reads new bronze data
  - Appends to existing Delta tables
  - Rebuilds weekly/monthly aggregates from full dataset

- **Full rebuild mode**: Reprocesses all data from scratch
  - Use when schema changes or data corrections needed
  - Add `full_rebuild=True` parameter to `main()` function

### GitHub Actions
- Workflow runs on push/PR to main branch
- Requires GitHub secrets: `POLYGON_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

## Architecture

### Data Pipeline Structure
TickerLake follows a medallion architecture for financial data processing:

- **Bronze Layer** (`src/tickerlake/bronze/`): Raw data ingestion from Polygon.io API
  - Downloads daily stock aggregates for NYSE trading days
  - Stores data as Parquet files in S3-compatible storage
  - Implements incremental loading by checking existing vs. required trading days

- **Silver Layer** (`src/tickerlake/silver/`): Cleaned and enriched data using Delta Lake
  - Uses Delta tables for ACID transactions and efficient incremental updates
  - Applies split adjustments to historical price data
  - Calculates volume ratios and technical indicators
  - Incremental mode: Only processes new data since last run
  - Full rebuild mode: Reprocesses all data from bronze layer
  - Automatically generates weekly and monthly aggregates

- **Gold Layer** (`src/tickerlake/gold/`): Business-level aggregates and analytics
  - Identifies high-volume trading days for pattern analysis
  - Reads from silver Delta tables for optimal performance
  - Exports to SQLite for easy querying and visualization

### Configuration Management
- Uses Pydantic Settings with `.env` file support (`src/tickerlake/config.py`)
- All sensitive credentials handled as `SecretStr` types
- S3 storage options pre-configured for various backends (Backblaze B2, MinIO, etc.)
- Default data window: 5 years from current date

### Key Components

**Trading Calendar Integration** (`src/tickerlake/utils.py`):
- `get_trading_days()`: Returns NYSE trading days for date range
- `is_market_open()`: Real-time market status using pandas_market_calendars
- Handles timezone conversion automatically (NYSE uses US/Eastern)

**S3 Storage**:
- Bronze path structure: `s3://bucket/bronze/daily/{YYYY-MM-DD}/data.parquet`
- Silver path structure: `s3://bucket/silver/{table_name}/_delta_log/...`
- Uses Polars for efficient Parquet/Delta I/O with cloud storage

**Delta Lake Integration** (`src/tickerlake/delta_utils.py`):
- `write_delta_table()`: Write DataFrames to Delta tables with overwrite/append modes
- `read_delta_table()`: Read Delta tables into Polars DataFrames
- `scan_delta_table()`: Lazy scanning for query optimization
- `merge_to_delta_table()`: Upsert operations for reference data
- `get_max_date_from_delta()`: Track incremental processing state
- `optimize_delta_table()`: Compact small files for better performance
- `vacuum_delta_table()`: Clean up old file versions

### Data Sources
- **Polygon.io**: Primary source for US stock market data
- **pandas_market_calendars**: NYSE trading calendar and hours
- **Storage**: S3-compatible object storage (Backblaze B2 by default)

### Environment Variables
```
POLYGON_API_KEY=your_api_key
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
```

The bronze layer automatically identifies and downloads missing trading days on each run, making it safe for scheduled execution.

## Code Standards

### Documentation
- All functions and classes must have PEP 257 compliant docstrings
- One-line docstrings should be in imperative mood (e.g., "Get trading days" not "Gets trading days")
- Multi-line docstrings should include Args and Returns sections where applicable
- Module-level docstrings are required for all Python modules

### Type Hints
- All functions must have complete type annotations for parameters and return values
- Use appropriate types from `typing` module when needed (e.g., `List`, `Dict`, `Optional`)
- Class attributes should have type hints where possible
