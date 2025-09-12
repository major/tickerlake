# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development
- `uv sync` - Install all dependencies
- `uv run tickerlake` - Run the main application
- `uv run python src/tickerlake/bronze/main.py` - Run the bronze layer data pipeline directly

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
- Path structure: `s3://bucket/bronze/daily/{YYYY-MM-DD}/data.parquet`
- Supports custom endpoints via `s3_endpoint_url` setting
- Uses Polars for efficient Parquet I/O with cloud storage

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
