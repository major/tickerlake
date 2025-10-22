# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development
- `uv sync` - Install all dependencies
- `uv run bronze` - Run the bronze layer (data ingestion)
- `uv run silver` - Run the silver layer (incremental processing by default)
- `uv run gold` - Run the gold layer (analytics and exports)
- `uv run publish` - Run the publish layer (generate and publish Hugo blog reports)

### Silver Layer Modes
- **Incremental mode** (default): Processes only new data since last run
  - Checks max date in Delta table
  - Only reads new bronze data
  - Appends to existing Delta tables
  - Rebuilds weekly/monthly aggregates from full dataset

- **Full rebuild mode**: Reprocesses all data from scratch
  - Use when schema changes or data corrections needed
  - Add `full_rebuild=True` parameter to `main()` function

### Gold Layer Usage

**Running the gold layer:**
```bash
uv run gold  # Creates gold.duckdb with all views
```

**Python query examples:**
```python
from tickerlake.gold.query import get_ticker_data, get_recent_hvcs
from datetime import date

# Get ticker data with all indicators
df = get_ticker_data('AAPL', start_date=date(2024, 1, 1))

# Find recent high volume closes
hvcs = get_recent_hvcs(days=30, min_ratio=3.0, ticker_type='CS')

# Custom SQL query
from tickerlake.gold.query import execute_custom_query
df = execute_custom_query("SELECT * FROM daily_enriched WHERE ticker = 'AAPL'")
```

**Direct DuckDB usage:**
```python
import duckdb
con = duckdb.connect('./data/gold/gold.duckdb')
df = con.execute('SELECT * FROM daily_enriched LIMIT 10').pl()
```

### GitHub Actions
- Workflow runs on push/PR to main branch
- Requires GitHub secret: `POLYGON_API_KEY` (for Polygon.io flat file access)

## Architecture

### Data Pipeline Structure
TickerLake follows a medallion architecture for financial data processing:

- **Bronze Layer** (`src/tickerlake/bronze/`): Raw data ingestion from Polygon.io API
  - Downloads daily stock aggregates for NYSE trading days from Polygon S3 flat files
  - Stores data as Parquet files in local `./data/bronze` directory
  - Implements incremental loading by checking existing vs. required trading days

- **Silver Layer** (`src/tickerlake/silver/`): Cleaned and enriched data using Delta Lake
  - Uses Delta tables for ACID transactions and efficient incremental updates
  - Applies split adjustments to historical price data
  - Calculates volume ratios and technical indicators
  - Incremental mode: Only processes new data since last run
  - Full rebuild mode: Reprocesses all data from bronze layer
  - Automatically generates weekly and monthly aggregates

- **Gold Layer** (`src/tickerlake/gold/`): DuckDB views for analytics and querying
  - Creates pure views (query-time joins) for always-fresh data
  - **Ticker Metadata**: Dimension table with CS/ETF ticker info from bronze layer
  - **Enriched Views**: Joins OHLCV aggregates + technical indicators + ticker metadata
    - `daily_enriched`, `weekly_enriched`, `monthly_enriched`
  - **Analysis Views**: Specialized views for common patterns
    - `recent_hvcs`: High volume closes (3x+ avg volume) from last 30 days
    - `liquid_stocks`: CS stocks with 200K+ volume, $5+ price; ETFs with 50K+ volume
    - `trending_stocks`: Stocks with price above all SMAs (20/50/200)
    - `latest_prices`: Most recent data for each ticker
  - Python helper functions for ad-hoc queries and backtesting
  - Persistent DuckDB database at `./data/gold/gold.duckdb`

- **Publish Layer** (`src/tickerlake/publish/`): Hugo blog content generation and publishing
  - Generates markdown reports from gold layer data
  - Publishes to Hugo blog at github.com/major/thetanerd.com
  - **Reports Generated**:
    - `index.md`: Overview page with stage counts and links
    - `stage-1.md` through `stage-4.md`: Stocks in each Weinstein stage
    - `weekly-hvcs.md`: Weekly high volume closes from past 2 years
  - Automated git workflow: clone/update → write content → commit → push
  - Uses GitHub PAT for authenticated pushes

### Configuration Management
- Uses Pydantic Settings with `.env` file support (`src/tickerlake/config.py`)
- All sensitive credentials handled as `SecretStr` types
- Local filesystem storage in `./data/bronze` and `./data/silver` directories
- Polygon.io S3 credentials required for accessing flat files
- Default data window: 5 years from current date

### Key Components

**Trading Calendar Integration** (`src/tickerlake/utils.py`):
- `get_trading_days()`: Returns NYSE trading days for date range
- `is_market_open()`: Real-time market status using pandas_market_calendars
- Handles timezone conversion automatically (NYSE uses US/Eastern)

**Local Storage**:
- Bronze path structure: `./data/bronze/stocks/date={YYYY-MM-DD}/*.parquet`
- Silver path structure: `./data/silver/{table_name}/_delta_log/...`
- Uses Polars for efficient Parquet/Delta I/O with local filesystem
- Polygon S3 access: Fetches flat files from Polygon.io S3 bucket (read-only)

**Delta Lake Integration** (`src/tickerlake/delta_utils.py`):
- `write_delta_table()`: Write DataFrames to Delta tables with overwrite/append modes
- `read_delta_table()`: Read Delta tables into Polars DataFrames
- `scan_delta_table()`: Lazy scanning for query optimization
- `merge_to_delta_table()`: Upsert operations for reference data
- `get_max_date_from_delta()`: Track incremental processing state
- `optimize_delta_table()`: Compact small files for better performance
- `vacuum_delta_table()`: Clean up old file versions

### Data Sources
- **Polygon.io**: Primary source for US stock market data (via S3 flat files)
- **pandas_market_calendars**: NYSE trading calendar and hours
- **Storage**: Local filesystem in `./data` directory

**Important Polygon.io API Limitations**:
- ⚠️ **Current Day Data**: The Polygon API does NOT allow requesting daily aggregate bars for the current day until the market has closed
- When the market is still open or hasn't closed yet, you can only request data up to yesterday's date
- Any validation scripts or API calls that attempt to fetch today's data will fail
- This is why the validation script uses `get_last_trading_day()` to find the most recent complete trading day from the silver data
- **Timezone Note**: Polygon returns timestamps in UTC. Always use `datetime.fromtimestamp(timestamp/1000, tz=timezone.utc)` when converting Polygon timestamps to avoid off-by-one date errors

### Environment Variables
```
POLYGON_API_KEY=your_api_key
POLYGON_ACCESS_KEY_ID=your_polygon_s3_access_key
POLYGON_SECRET_ACCESS_KEY=your_polygon_s3_secret_key
GITHUB_PAT=your_github_personal_access_token
```

Notes:
- Polygon S3 credentials are only used for accessing Polygon.io flat files, not for data storage
- GitHub PAT is used for pushing report updates to the Hugo blog repository

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
