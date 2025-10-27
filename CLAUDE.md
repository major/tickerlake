# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview 🎯

TickerLake is a medallion architecture data lake for stock market data using Bronze → Silver → Gold layers:
- **Bronze 🥉**: Raw data ingestion from Polygon.io API into PostgreSQL
- **Silver 🥈**: Split-adjusted OHLCV data with technical indicators
- **Gold 🥇**: Business-level aggregates and analytics (in development)

## Development Commands 🛠️

### Core Pipeline Commands
```bash
# Run the entire data pipeline (all layers)
uv run bronze    # Ingest raw data from Polygon.io
uv run silver    # Process and adjust for splits, calculate indicators
uv run gold      # Create business-level views (in development)
uv run validate  # Validate silver layer data
uv run clean     # Clean bronze layer data
```

### Testing & Quality
```bash
# Run all quality checks
make all                      # lint + test + typecheck + deadcode + deps

# Individual checks
uv run pytest                 # Run all tests with coverage
uv run pytest tests/test_bronze_main.py  # Run single test file
uv run pytest -k test_name    # Run specific test
uv run ruff check --fix       # Lint and auto-fix
uv run pyright src/*          # Type checking
uv run radon cc src/ -s -a    # Cyclomatic complexity
uv run vulture src/ --min-confidence 80  # Dead code detection
uv run deptry src/            # Dependency analysis
```

### Development Setup
```bash
uv sync                       # Install all dependencies
uv pip list                   # List installed packages
```

## Architecture Deep Dive 🏗️

### Data Flow Pipeline
1. **Bronze Layer** (`src/tickerlake/bronze/`):
   - Fetches grouped daily aggregates from Polygon.io API (newest to oldest)
   - Stops on 403 errors (API subscription limit reached)
   - Stores raw OHLCV data in PostgreSQL `stocks` table
   - Also fetches ticker metadata and split events
   - Validates data quality (checks for abnormal record counts per day)

2. **Silver Layer** (`src/tickerlake/silver/`):
   - Reads bronze data in batches (default: 250 tickers) to manage memory
   - Applies stock split adjustments retroactively to historical prices
   - Filters to tradeable securities only (CS, ETF, PFD, WARRANT, ADRC, ADRP, ETN)
   - Generates three timeframes: daily, weekly, monthly aggregates
   - Calculates technical indicators (SMA 20/50/200, ATR 14, volume metrics)
   - Writes split-adjusted data and indicators to separate PostgreSQL tables

3. **Gold Layer** (`src/tickerlake/gold/`):
   - Currently in development
   - Intended for business-level aggregates and analytics

### Key Database Architecture 🗄️
- **PostgreSQL** is the primary storage (replaced DuckDB files)
- Bronze tables: `stocks`, `tickers`, `splits`
- Silver tables: `silver_ticker_metadata`, `silver_daily_aggregates`, `silver_daily_indicators`, `silver_weekly_aggregates`, `silver_weekly_indicators`, `silver_monthly_aggregates`, `silver_monthly_indicators`
- Uses SQLAlchemy Table objects (not ORM models) for schema definitions
- Bulk loading via `bulk_load(table, df)` in `src/tickerlake/db/operations.py`

### Configuration 📝
- Uses **Pydantic Settings** with `.env` file (`src/tickerlake/config.py`)
- Required environment variables:
  - `POLYGON_API_KEY`: API key for Polygon.io
  - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`: PostgreSQL connection
  - `POSTGRES_USER`, `POSTGRES_PASSWORD`: PostgreSQL credentials
  - `POSTGRES_SSLMODE`: TLS mode (require/verify-ca/verify-full/disable)
  - `DATA_START_YEAR`: How far back to fetch data (default: 5 years)

### Polars-Centric Design 🐻
- **Polars** is the primary DataFrame library (not pandas)
- Schema validation in `src/tickerlake/schemas.py` using Polars dtypes
- Efficient lazy evaluation for split adjustments
- Type hints use `pl.DataFrame`, not `pd.DataFrame`

### Testing Strategy 🧪
- Uses **pytest** with extensive parameterization and fixtures
- Coverage reporting enabled (term + HTML + XML)
- Tests in `tests/` mirror `src/tickerlake/` structure
- Bronze layer tests use fake data (no actual API calls in tests)

### Batch Processing Pattern 🚀
The silver layer uses memory-efficient batch processing:
1. Load N tickers at a time (configurable batch_size)
2. Process each batch: apply splits → aggregate → calculate indicators
3. Write results immediately to PostgreSQL
4. Free memory before next batch
5. Concatenate all batches at the end for final indicator calculation

This allows processing millions of rows on modest hardware.

## Common Patterns 🎨

### Adding a New Technical Indicator
1. Add calculation function to `src/tickerlake/silver/indicators.py`
2. Update `calculate_all_indicators()` to include new indicator
3. Add new column to `INDICATORS_SCHEMA` in `src/tickerlake/schemas.py`
4. Update SQLAlchemy table definitions in `src/tickerlake/silver/models.py`
5. Add tests in `tests/test_silver_main.py`

### Adding a New Data Source
1. Create client setup in `src/tickerlake/clients.py`
2. Add new module in bronze layer (e.g., `src/tickerlake/bronze/new_source.py`)
3. Define SQLAlchemy table in `src/tickerlake/bronze/models.py`
4. Add integration to `src/tickerlake/bronze/main.py`
5. Use `bulk_load()` for writing to PostgreSQL

### Working with Schemas
- Schemas are defined in `src/tickerlake/schemas.py` using Polars dtypes
- Use `validate_daily_aggregates()` and `validate_indicators()` for schema validation
- SQLAlchemy tables in `models.py` files define the database schema
- Keep Polars schemas and SQLAlchemy schemas in sync

## Important Notes ⚠️

- **Do not push to git** - User prefers to commit/push manually
- **Batch size tuning**: Silver layer `batch_size` parameter trades RAM usage vs speed
- **API limits**: Bronze layer gracefully handles 403 errors when subscription limits are reached
- **Split adjustments**: Applied retroactively (prices BEFORE split are adjusted)
- **Categorical columns**: Tickers are stored as categorical dtype for memory efficiency
- **TLS required**: PostgreSQL connections use `sslmode` setting from config
