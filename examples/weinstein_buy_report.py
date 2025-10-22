"""Generate Weinstein Buy Candidates Report.

Creates comprehensive reports of stocks meeting Weinstein buy criteria:
- Buy Candidates: Early Stage 2 stocks (2-6 weeks in stage)
- Watchlist: Stage 1 stocks near breakout (within 5% of 30-week MA)

Outputs in CSV, HTML, and Markdown formats.
"""

import sys
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tickerlake.gold.main import get_duckdb_path


def get_buy_candidates(
    con: duckdb.DuckDBPyConnection,
    min_price: float = 10.0,
    min_volume: int = 200000,
    min_weeks: int = 2,
    max_weeks: int = 6,
) -> pl.DataFrame:
    """Get early Stage 2 buy candidates.

    Args:
        con: DuckDB connection to gold database.
        min_price: Minimum stock price (default: $10).
        min_volume: Minimum 20-day average volume (default: 200K).
        min_weeks: Minimum weeks in Stage 2 (default: 2).
        max_weeks: Maximum weeks in Stage 2 for "early" (default: 6).

    Returns:
        DataFrame with buy candidates and key metrics.
    """
    query = f"""
        SELECT
            ticker,
            name,
            primary_exchange,
            date,
            close,
            stage,
            weeks_in_stage,
            price_vs_ma_pct,
            ma_slope_pct,
            ma_30,
            sma_20,
            sma_50,
            sma_200,
            volume,
            volume_ma_20,
            volume_ratio,
            -- Calculate price position vs shorter-term MAs
            ROUND((close - sma_20) / sma_20 * 100, 2) as price_vs_sma20_pct,
            ROUND((close - sma_50) / sma_50 * 100, 2) as price_vs_sma50_pct,
            ROUND((close - sma_200) / sma_200 * 100, 2) as price_vs_sma200_pct
        FROM latest_stage
        WHERE stage = 2
          AND type = 'CS'
          AND weeks_in_stage >= {min_weeks}
          AND weeks_in_stage <= {max_weeks}
          AND close >= {min_price}
          AND volume_ma_20 >= {min_volume}
        ORDER BY weeks_in_stage ASC, price_vs_ma_pct DESC
    """

    return con.execute(query).pl()


def get_watchlist_candidates(
    con: duckdb.DuckDBPyConnection,
    min_price: float = 10.0,
    min_volume: int = 200000,
    max_pct_below_ma: float = -5.0,
) -> pl.DataFrame:
    """Get Stage 1 watchlist candidates near breakout.

    Args:
        con: DuckDB connection to gold database.
        min_price: Minimum stock price (default: $10).
        min_volume: Minimum 20-day average volume (default: 200K).
        max_pct_below_ma: Maximum % below 30-week MA (default: -5%).

    Returns:
        DataFrame with watchlist candidates.
    """
    query = f"""
        SELECT
            ticker,
            name,
            primary_exchange,
            date,
            close,
            stage,
            weeks_in_stage,
            price_vs_ma_pct,
            ma_slope_pct,
            ma_30,
            sma_20,
            sma_50,
            sma_200,
            volume,
            volume_ma_20,
            volume_ratio,
            -- Calculate price position vs shorter-term MAs
            ROUND((close - sma_20) / sma_20 * 100, 2) as price_vs_sma20_pct,
            ROUND((close - sma_50) / sma_50 * 100, 2) as price_vs_sma50_pct,
            ROUND((close - sma_200) / sma_200 * 100, 2) as price_vs_sma200_pct
        FROM latest_stage
        WHERE stage = 1
          AND type = 'CS'
          AND price_vs_ma_pct >= {max_pct_below_ma}
          AND price_vs_ma_pct < 0
          AND close >= {min_price}
          AND volume_ma_20 >= {min_volume}
        ORDER BY price_vs_ma_pct DESC, ma_slope_pct DESC
    """

    return con.execute(query).pl()


def export_to_csv(
    buy_df: pl.DataFrame, watchlist_df: pl.DataFrame, output_dir: Path
) -> None:
    """Export dataframes to CSV files.

    Args:
        buy_df: Buy candidates dataframe.
        watchlist_df: Watchlist candidates dataframe.
        output_dir: Directory to save CSV files.
    """
    print("\n📄 Exporting to CSV...")

    buy_path = output_dir / "weinstein_buy_report.csv"
    watchlist_path = output_dir / "weinstein_watchlist.csv"

    buy_df.write_csv(buy_path)
    watchlist_df.write_csv(watchlist_path)

    print(f"   ✅ Buy candidates: {buy_path}")
    print(f"   ✅ Watchlist: {watchlist_path}")


def export_to_html(
    buy_df: pl.DataFrame,
    watchlist_df: pl.DataFrame,
    output_dir: Path,
    timestamp: str,
) -> None:
    """Export dataframes to styled HTML file.

    Args:
        buy_df: Buy candidates dataframe.
        watchlist_df: Watchlist candidates dataframe.
        output_dir: Directory to save HTML file.
        timestamp: Report generation timestamp.
    """
    print("\n🌐 Exporting to HTML...")

    html_path = output_dir / "weinstein_buy_report.html"

    # HTML template with styling
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Weinstein Buy Candidates Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            max-width: 1400px;
            margin: 20px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 2.5em;
        }}
        .header p {{
            margin: 5px 0;
            opacity: 0.9;
        }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            color: #333;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .summary-card {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid #667eea;
        }}
        .summary-card .label {{
            font-size: 0.85em;
            color: #666;
            margin-bottom: 5px;
        }}
        .summary-card .value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        thead {{
            background: #667eea;
            color: white;
        }}
        th {{
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 8px;
            border-bottom: 1px solid #e0e0e0;
        }}
        tbody tr:hover {{
            background-color: #f5f7ff;
        }}
        .positive {{
            color: #10b981;
            font-weight: 600;
        }}
        .negative {{
            color: #ef4444;
            font-weight: 600;
        }}
        .stage-2 {{
            background-color: #10b981;
            color: white;
            padding: 3px 8px;
            border-radius: 3px;
            font-weight: 600;
        }}
        .stage-1 {{
            background-color: #3b82f6;
            color: white;
            padding: 3px 8px;
            border-radius: 3px;
            font-weight: 600;
        }}
        .filters {{
            background: #fffbeb;
            border-left: 4px solid #f59e0b;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 5px;
        }}
        .filters h3 {{
            margin-top: 0;
            color: #92400e;
        }}
        .filters ul {{
            margin: 10px 0;
            padding-left: 20px;
        }}
        .no-data {{
            text-align: center;
            padding: 40px;
            color: #666;
            font-style: italic;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Weinstein Buy Candidates Report</h1>
        <p>Generated: {timestamp}</p>
        <p>Based on Stan Weinstein's Stage Analysis Methodology</p>
    </div>

    <div class="section">
        <div class="filters">
            <h3>📋 Filter Criteria</h3>
            <ul>
                <li><strong>Security Type:</strong> Common Stocks (CS)</li>
                <li><strong>Minimum Price:</strong> $10.00</li>
                <li><strong>Minimum Volume:</strong> 200,000 shares (20-day average)</li>
                <li><strong>Buy Candidates:</strong> Stage 2, 2-6 weeks in stage</li>
                <li><strong>Watchlist:</strong> Stage 1, within 5% of 30-week MA</li>
            </ul>
        </div>
    </div>

    <div class="section">
        <h2>🟢 Buy Candidates - Early Stage 2 Stocks</h2>
        <div class="summary">
            <div class="summary-card">
                <div class="label">Total Candidates</div>
                <div class="value">{len(buy_df)}</div>
            </div>
"""

    if len(buy_df) > 0:
        avg_weeks = buy_df["weeks_in_stage"].mean()
        avg_price_vs_ma = buy_df["price_vs_ma_pct"].mean()
        html += f"""
            <div class="summary-card">
                <div class="label">Avg Weeks in Stage 2</div>
                <div class="value">{avg_weeks:.1f}</div>
            </div>
            <div class="summary-card">
                <div class="label">Avg Price vs MA</div>
                <div class="value">{avg_price_vs_ma:.1f}%</div>
            </div>
"""

    html += """
        </div>
"""

    if len(buy_df) > 0:
        html += """
        <div style="overflow-x: auto;">
        <table>
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Name</th>
                    <th>Exchange</th>
                    <th>Price</th>
                    <th>Date</th>
                    <th>Stage</th>
                    <th>Weeks</th>
                    <th>Price vs MA</th>
                    <th>MA Slope</th>
                    <th>vs SMA20</th>
                    <th>vs SMA50</th>
                    <th>vs SMA200</th>
                    <th>Volume (20d avg)</th>
                    <th>Vol Ratio</th>
                </tr>
            </thead>
            <tbody>
"""
        for row in buy_df.iter_rows(named=True):
            pct_class = "positive" if row["price_vs_ma_pct"] and row["price_vs_ma_pct"] > 0 else "negative"
            slope_class = "positive" if row["ma_slope_pct"] and row["ma_slope_pct"] > 0 else "negative"
            sma20_class = "positive" if row["price_vs_sma20_pct"] and row["price_vs_sma20_pct"] > 0 else "negative"
            sma50_class = "positive" if row["price_vs_sma50_pct"] and row["price_vs_sma50_pct"] > 0 else "negative"
            sma200_class = "positive" if row["price_vs_sma200_pct"] and row["price_vs_sma200_pct"] > 0 else "negative"

            # Format values with null handling
            pct_val = f"{row['price_vs_ma_pct']:.1f}%" if row['price_vs_ma_pct'] is not None else "N/A"
            slope_val = f"{row['ma_slope_pct']:.1f}%" if row['ma_slope_pct'] is not None else "N/A"
            sma20_val = f"{row['price_vs_sma20_pct']:.1f}%" if row['price_vs_sma20_pct'] is not None else "N/A"
            sma50_val = f"{row['price_vs_sma50_pct']:.1f}%" if row['price_vs_sma50_pct'] is not None else "N/A"
            sma200_val = f"{row['price_vs_sma200_pct']:.1f}%" if row['price_vs_sma200_pct'] is not None else "N/A"

            html += f"""
                <tr>
                    <td><strong>{row['ticker']}</strong></td>
                    <td>{row['name']}</td>
                    <td>{row['primary_exchange']}</td>
                    <td>${row['close']:.2f}</td>
                    <td>{row['date']}</td>
                    <td><span class="stage-2">Stage 2</span></td>
                    <td>{row['weeks_in_stage']}</td>
                    <td class="{pct_class}">{pct_val}</td>
                    <td class="{slope_class}">{slope_val}</td>
                    <td class="{sma20_class}">{sma20_val}</td>
                    <td class="{sma50_class}">{sma50_val}</td>
                    <td class="{sma200_class}">{sma200_val}</td>
                    <td>{row['volume_ma_20']:,}</td>
                    <td>{row['volume_ratio']:.2f}x</td>
                </tr>
"""
        html += """
            </tbody>
        </table>
        </div>
"""
    else:
        html += '<div class="no-data">No buy candidates found matching criteria.</div>'

    html += """
    </div>

    <div class="section">
        <h2>👀 Watchlist - Stage 1 Near Breakout</h2>
        <div class="summary">
            <div class="summary-card">
                <div class="label">Total Watchlist</div>
                <div class="value">{}</div>
            </div>
""".format(
        len(watchlist_df)
    )

    if len(watchlist_df) > 0:
        avg_price_vs_ma = watchlist_df["price_vs_ma_pct"].mean()
        html += f"""
            <div class="summary-card">
                <div class="label">Avg Distance from MA</div>
                <div class="value">{avg_price_vs_ma:.1f}%</div>
            </div>
"""

    html += """
        </div>
"""

    if len(watchlist_df) > 0:
        html += """
        <div style="overflow-x: auto;">
        <table>
            <thead>
                <tr>
                    <th>Ticker</th>
                    <th>Name</th>
                    <th>Exchange</th>
                    <th>Price</th>
                    <th>Date</th>
                    <th>Stage</th>
                    <th>Weeks</th>
                    <th>Price vs MA</th>
                    <th>MA Slope</th>
                    <th>vs SMA20</th>
                    <th>vs SMA50</th>
                    <th>vs SMA200</th>
                    <th>Volume (20d avg)</th>
                    <th>Vol Ratio</th>
                </tr>
            </thead>
            <tbody>
"""
        for row in watchlist_df.iter_rows(named=True):
            pct_class = "positive" if row["price_vs_ma_pct"] and row["price_vs_ma_pct"] > 0 else "negative"
            slope_class = "positive" if row["ma_slope_pct"] and row["ma_slope_pct"] > 0 else "negative"
            sma20_class = "positive" if row["price_vs_sma20_pct"] and row["price_vs_sma20_pct"] > 0 else "negative"
            sma50_class = "positive" if row["price_vs_sma50_pct"] and row["price_vs_sma50_pct"] > 0 else "negative"
            sma200_class = "positive" if row["price_vs_sma200_pct"] and row["price_vs_sma200_pct"] > 0 else "negative"

            # Format values with null handling
            pct_val = f"{row['price_vs_ma_pct']:.1f}%" if row['price_vs_ma_pct'] is not None else "N/A"
            slope_val = f"{row['ma_slope_pct']:.1f}%" if row['ma_slope_pct'] is not None else "N/A"
            sma20_val = f"{row['price_vs_sma20_pct']:.1f}%" if row['price_vs_sma20_pct'] is not None else "N/A"
            sma50_val = f"{row['price_vs_sma50_pct']:.1f}%" if row['price_vs_sma50_pct'] is not None else "N/A"
            sma200_val = f"{row['price_vs_sma200_pct']:.1f}%" if row['price_vs_sma200_pct'] is not None else "N/A"

            html += f"""
                <tr>
                    <td><strong>{row['ticker']}</strong></td>
                    <td>{row['name']}</td>
                    <td>{row['primary_exchange']}</td>
                    <td>${row['close']:.2f}</td>
                    <td>{row['date']}</td>
                    <td><span class="stage-1">Stage 1</span></td>
                    <td>{row['weeks_in_stage']}</td>
                    <td class="{pct_class}">{pct_val}</td>
                    <td class="{slope_class}">{slope_val}</td>
                    <td class="{sma20_class}">{sma20_val}</td>
                    <td class="{sma50_class}">{sma50_val}</td>
                    <td class="{sma200_class}">{sma200_val}</td>
                    <td>{row['volume_ma_20']:,}</td>
                    <td>{row['volume_ratio']:.2f}x</td>
                </tr>
"""
        html += """
            </tbody>
        </table>
        </div>
"""
    else:
        html += '<div class="no-data">No watchlist candidates found matching criteria.</div>'

    html += """
    </div>

    <div class="section">
        <h3>📖 Understanding the Metrics</h3>
        <ul>
            <li><strong>Stage 2 (Advancing):</strong> Price >2% above 30-week MA with rising slope - primary buy zone</li>
            <li><strong>Stage 1 (Basing):</strong> Price below 30-week MA - potential future breakouts</li>
            <li><strong>Price vs MA:</strong> Distance from 30-week moving average (positive = above MA)</li>
            <li><strong>MA Slope:</strong> Rate of change in 30-week MA over 4 weeks (positive = rising)</li>
            <li><strong>vs SMA20/50/200:</strong> Distance from shorter-term moving averages</li>
            <li><strong>Vol Ratio:</strong> Current volume divided by 20-day average</li>
        </ul>
    </div>
</body>
</html>
"""

    html_path.write_text(html, encoding="utf-8")
    print(f"   ✅ HTML report: {html_path}")


def export_to_markdown(
    buy_df: pl.DataFrame,
    watchlist_df: pl.DataFrame,
    output_dir: Path,
    timestamp: str,
) -> None:
    """Export dataframes to Markdown file.

    Args:
        buy_df: Buy candidates dataframe.
        watchlist_df: Watchlist candidates dataframe.
        output_dir: Directory to save Markdown file.
        timestamp: Report generation timestamp.
    """
    print("\n📝 Exporting to Markdown...")

    md_path = output_dir / "weinstein_buy_report.md"

    md = f"""# 📊 Weinstein Buy Candidates Report

**Generated:** {timestamp}
**Based on:** Stan Weinstein's Stage Analysis Methodology

---

## 📋 Filter Criteria

- **Security Type:** Common Stocks (CS)
- **Minimum Price:** $10.00
- **Minimum Volume:** 200,000 shares (20-day average)
- **Buy Candidates:** Stage 2, 2-6 weeks in stage
- **Watchlist:** Stage 1, within 5% of 30-week MA

---

## 🟢 Buy Candidates - Early Stage 2 Stocks

**Total Candidates:** {len(buy_df)}
"""

    if len(buy_df) > 0:
        avg_weeks = buy_df["weeks_in_stage"].mean()
        avg_price_vs_ma = buy_df["price_vs_ma_pct"].mean()
        md += f"""
**Average Weeks in Stage 2:** {avg_weeks:.1f}
**Average Price vs MA:** {avg_price_vs_ma:.1f}%

| Ticker | Name | Exchange | Price | Date | Stage | Weeks | Price vs MA | MA Slope | vs SMA20 | vs SMA50 | vs SMA200 | Volume (20d avg) | Vol Ratio |
|--------|------|----------|-------|------|-------|-------|-------------|----------|----------|----------|-----------|------------------|-----------|
"""
        for row in buy_df.iter_rows(named=True):
            pct_val = f"{row['price_vs_ma_pct']:.1f}%" if row['price_vs_ma_pct'] is not None else "N/A"
            slope_val = f"{row['ma_slope_pct']:.1f}%" if row['ma_slope_pct'] is not None else "N/A"
            sma20_val = f"{row['price_vs_sma20_pct']:.1f}%" if row['price_vs_sma20_pct'] is not None else "N/A"
            sma50_val = f"{row['price_vs_sma50_pct']:.1f}%" if row['price_vs_sma50_pct'] is not None else "N/A"
            sma200_val = f"{row['price_vs_sma200_pct']:.1f}%" if row['price_vs_sma200_pct'] is not None else "N/A"
            md += f"| **{row['ticker']}** | {row['name']} | {row['primary_exchange']} | ${row['close']:.2f} | {row['date']} | Stage 2 | {row['weeks_in_stage']} | {pct_val} | {slope_val} | {sma20_val} | {sma50_val} | {sma200_val} | {row['volume_ma_20']:,} | {row['volume_ratio']:.2f}x |\n"
    else:
        md += "\n*No buy candidates found matching criteria.*\n"

    md += f"""
---

## 👀 Watchlist - Stage 1 Near Breakout

**Total Watchlist:** {len(watchlist_df)}
"""

    if len(watchlist_df) > 0:
        avg_price_vs_ma = watchlist_df["price_vs_ma_pct"].mean()
        md += f"""
**Average Distance from MA:** {avg_price_vs_ma:.1f}%

| Ticker | Name | Exchange | Price | Date | Stage | Weeks | Price vs MA | MA Slope | vs SMA20 | vs SMA50 | vs SMA200 | Volume (20d avg) | Vol Ratio |
|--------|------|----------|-------|------|-------|-------|-------------|----------|----------|----------|-----------|------------------|-----------|
"""
        for row in watchlist_df.iter_rows(named=True):
            pct_val = f"{row['price_vs_ma_pct']:.1f}%" if row['price_vs_ma_pct'] is not None else "N/A"
            slope_val = f"{row['ma_slope_pct']:.1f}%" if row['ma_slope_pct'] is not None else "N/A"
            sma20_val = f"{row['price_vs_sma20_pct']:.1f}%" if row['price_vs_sma20_pct'] is not None else "N/A"
            sma50_val = f"{row['price_vs_sma50_pct']:.1f}%" if row['price_vs_sma50_pct'] is not None else "N/A"
            sma200_val = f"{row['price_vs_sma200_pct']:.1f}%" if row['price_vs_sma200_pct'] is not None else "N/A"
            md += f"| **{row['ticker']}** | {row['name']} | {row['primary_exchange']} | ${row['close']:.2f} | {row['date']} | Stage 1 | {row['weeks_in_stage']} | {pct_val} | {slope_val} | {sma20_val} | {sma50_val} | {sma200_val} | {row['volume_ma_20']:,} | {row['volume_ratio']:.2f}x |\n"
    else:
        md += "\n*No watchlist candidates found matching criteria.*\n"

    md += """
---

## 📖 Understanding the Metrics

- **Stage 2 (Advancing):** Price >2% above 30-week MA with rising slope - primary buy zone
- **Stage 1 (Basing):** Price below 30-week MA - potential future breakouts
- **Price vs MA:** Distance from 30-week moving average (positive = above MA)
- **MA Slope:** Rate of change in 30-week MA over 4 weeks (positive = rising)
- **vs SMA20/50/200:** Distance from shorter-term moving averages
- **Vol Ratio:** Current volume divided by 20-day average
"""

    md_path.write_text(md, encoding="utf-8")
    print(f"   ✅ Markdown report: {md_path}")


def main() -> None:
    """Main entry point for Weinstein buy report generation."""
    print("=" * 80)
    print("📊 Weinstein Buy Candidates Report Generator")
    print("=" * 80)

    # Get database path
    db_path = get_duckdb_path()
    if not Path(db_path).exists():
        print(f"\n❌ Error: Gold database not found at {db_path}")
        print("   💡 Run 'uv run gold' first to create the database.")
        sys.exit(1)

    print(f"\n📂 Using database: {db_path}")

    # Connect to database
    con = duckdb.connect(db_path, read_only=True)

    try:
        # Get buy candidates and watchlist
        print("\n🔍 Querying buy candidates (Early Stage 2)...")
        buy_df = get_buy_candidates(con)
        print(f"   ✅ Found {len(buy_df)} buy candidates")

        print("\n🔍 Querying watchlist (Stage 1 near breakout)...")
        watchlist_df = get_watchlist_candidates(con)
        print(f"   ✅ Found {len(watchlist_df)} watchlist candidates")

        # Generate timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Export to all formats
        output_dir = Path(__file__).parent
        export_to_csv(buy_df, watchlist_df, output_dir)
        export_to_html(buy_df, watchlist_df, output_dir, timestamp)
        export_to_markdown(buy_df, watchlist_df, output_dir, timestamp)

        print("\n" + "=" * 80)
        print("✅ Report generation complete! 🎉")
        print("=" * 80)
        print("\n📊 Summary:")
        print(f"   Buy Candidates (Stage 2): {len(buy_df)}")
        print(f"   Watchlist (Stage 1): {len(watchlist_df)}")
        print("\n💡 Tip: Open the HTML report in your browser for the best viewing experience!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()


if __name__ == "__main__":
    main()
