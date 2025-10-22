# TickerLake Datasette Container

Interactive web interface for exploring TickerLake stock analysis data using [Datasette](https://datasette.io/).

## 📊 What's Included

The container serves **two separate databases** with interactive tables:

### Database 1: stage_analysis (0.38 MB)
Weinstein Stage Analysis tables:
- **Stage 1 Stocks** - Basing stocks (building bases before breakouts)
- **Stage 2 Stocks** - Advancing stocks (confirmed uptrends)
- **Stage 3 Stocks** - Topping stocks (distribution phase)
- **Stage 4 Stocks** - Declining stocks (confirmed downtrends)
- **Summary** - Overview of stock counts per stage

**Stage table columns:**
- ticker, name, date, close, volume, stage, weeks_in_stage
- ma_slope_pct, ma_30, volume_ratio

### Database 2: high_volume_close (26.17 MB)
High Volume Close (HVC) tables:
- **Daily HVCs** - 208,583 daily closes with 3x+ volume spikes
- **Weekly HVCs** - 39,004 weekly closes with 3x+ volume spikes

**Daily HVC columns:**
- ticker, name, date, open, high, low, close, volume, volume_ratio

**Weekly HVC columns:**
- ticker, name, date, close, volume, volume_ratio
- stage, weeks_in_stage, ma_slope_pct, ma_30

## 🚀 Quick Start

### Build the container:
```bash
./container/build.sh
```

### Run the container:
```bash
podman run -p 8001:8001 tickerlake-datasette:latest
```

### Access the web interface:
Open your browser to: http://localhost:8001

## 🔧 Manual Build Steps

If you prefer to build manually:

1. **Prepare the data:**
   ```bash
   uv run python container/prepare_datasette.py
   ```

2. **Build the container:**
   ```bash
   podman build -t tickerlake-datasette:latest -f container/Containerfile container/
   ```

3. **Run the container:**
   ```bash
   podman run -p 8001:8001 tickerlake-datasette:latest
   ```

## 📖 Using Datasette

Datasette provides a powerful web interface for exploring data:

- **Browse tables** - Click on any table to see its data
- **Filter & search** - Use the search box and facets to narrow results
- **Sort** - Click column headers to sort
- **SQL queries** - Write custom SQL queries for advanced analysis
- **Export** - Download results as JSON, CSV, or use the API

### Example Queries

**Find Stage 2 stocks that have been advancing for 20+ weeks:**
```sql
SELECT ticker, name, close, weeks_in_stage, price_vs_ma_pct
FROM stage_2_stocks
WHERE weeks_in_stage >= 20
ORDER BY weeks_in_stage DESC
```

**Recent weekly HVCs in Stage 2:**
```sql
SELECT ticker, name, date, volume_ratio, weeks_in_stage
FROM weekly_hvcs
WHERE stage = 2 AND date >= date('now', '-3 months')
ORDER BY date DESC, volume_ratio DESC
```

## 🔄 Updating Data

To refresh the data in the container:

1. Run the data pipeline to update gold layer:
   ```bash
   uv run bronze
   uv run silver
   uv run gold
   ```

2. Rebuild the container:
   ```bash
   ./container/build.sh
   ```

## 🌐 Deployment

The container can be deployed to any container platform:

- **Local**: `podman run -p 8001:8001 tickerlake-datasette:latest`
- **Cloud Run**: Push to GCR and deploy
- **Fly.io**: `flyctl deploy`
- **Kubernetes**: Create deployment with the image

## 📝 Configuration

Edit `metadata.yml` to customize:
- Table titles and descriptions
- Column descriptions
- Facets (filter options)
- Sort orders
- License and source information

Then rebuild the container to apply changes.

## 🎯 Liquidity Filters

Stage tables include only liquid stocks:
- **Common stocks (CS)** only
- Minimum **2 weeks** in current stage
- Average daily volume ≥ **200,000 shares**
- Price ≥ **$5.00** (Stage 1-3 only)

## 🔗 Resources

- [Datasette Documentation](https://docs.datasette.io/)
- [TickerLake GitHub](https://github.com/major/tickerlake)
- [Polygon.io Data Source](https://polygon.io/)
