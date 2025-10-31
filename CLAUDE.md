# CLAUDE.md

TickerLake guidance for Claude Code.

## Project Overview 🎯

Medallion architecture stock data lake: Bronze (raw Polygon.io) → Silver (split-adjusted + indicators) → Gold (VWAP analytics). All data stored locally in Parquet files.

## Commands 🛠️

```bash
# Pipeline (full rebuild daily - fast with local storage!)
uv run bronze      # Fetch from Polygon.io
uv run silver      # Apply splits + calculate indicators
uv run gold        # Calculate VWAP signals
uv run validate    # Validate silver data quality
uv run clean       # Clean bronze data

# Quality
make all           # lint + test + typecheck + deadcode + deps
uv run pytest [-k test_name]
uv run ruff check --fix
uv run pyright src/*
```

## Architecture 🏗️

### Data Flow
**Bronze** (`src/tickerlake/bronze/`): Fetch Polygon.io → Parquet (stocks, tickers, splits). Checkpoint-based incremental, stops on 403 errors.

**Silver** (`src/tickerlake/silver/`): Smart incremental - detects splits → full rewrite or append-only. Two-phase batching:
- Phase 1 (default: 250 tickers): Apply splits → aggregate daily/weekly/monthly
- Phase 2 (default: 500 tickers): Calculate indicators (SMA 20/50/200, ATR 14, volume metrics)
- Filters: CS and ETF only (excludes preferred, warrants, ADRs, ETNs)

**Gold** (`src/tickerlake/gold/`): VWAP analytics (Year-to-Date and Quarter-to-Date). Identifies stocks trading above both VWAP levels. Full rebuild each run (fast with Parquet!). DuckDB for SQL queries.

### Storage 🗄️
- **Parquet on local filesystem** (simple, fast, no transaction log overhead)
- **DuckDB** for efficient queries on Parquet files
- Files: bronze (`stocks.parquet`, `tickers.parquet`, `splits.parquet`), silver (`{daily,weekly,monthly}_{aggregates,indicators}.parquet`, `ticker_metadata.parquet`), gold (`vwap_signals.parquet`)
- Storage API in `src/tickerlake/storage/`: `write_table()`, `read_table()`, `query_parquet()`, `load_checkpoints()`, `save_checkpoints()`
- Location: `./data/` directory (gitignored, created automatically)

### Config 📝
Pydantic Settings with `.env` (`src/tickerlake/config.py`):
- Required: `POLYGON_API_KEY`
- Optional: `DATA_DIR` (default: "data"), `DATA_START_YEAR` (default: 5 years back)

### Key Tech 🔧
- **Polars** (not pandas): Schema validation in `src/tickerlake/schemas.py`, use `pl.DataFrame` types
- **Testing**: pytest with parameterization/fixtures. **Do NOT test logging** - focus on business logic only
- **Write modes**: All Parquet files use overwrite mode (full rebuilds are fast!)
- **Checkpoints**: JSON state in `data/checkpoints.json`
- **Compression**: Parquet files use zstd compression for optimal storage

## Common Patterns 🎨

**Add Technical Indicator**: Update `silver/indicators.py`, `calculate_all_indicators()`, `schemas.py::INDICATORS_SCHEMA`, `silver/models.py`, add tests.

**Add Data Source**: Create client in `clients.py`, new bronze module, document schema in `bronze/models.py`, integrate in `bronze/main.py`.

**Add Gold Analytics**: Create `gold/new_analysis.py`, implement analysis function, update `gold/main.py`, add tests.

**Query Parquet Files**:
```python
from tickerlake.storage import query_parquet, get_table_path
path = get_table_path("silver", "daily_aggregates")
df = query_parquet(f"SELECT * FROM '{path}' WHERE ticker = ? AND date > ?", ["AAPL", "2024-01-01"])
```

## Important Notes ⚠️

- **Do not push to git** - user commits/pushes manually
- **Batch tuning**: `--batch-size` (aggregation, default: 250), `--indicator-batch-size` (indicators, default: 500)
- **Daily rebuilds**: Silver + Gold layers are rebuilt daily (fast with local SSDs!)
- **Split adjustments**: Retroactive (prices before split adjusted)
- **Categorical columns**: Tickers stored as categorical for memory efficiency
