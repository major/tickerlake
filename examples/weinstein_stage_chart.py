"""Generate a stacked area chart showing Weinstein stage distribution over time."""

import sys
from datetime import datetime
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import polars as pl

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tickerlake.gold.main import get_duckdb_path


def get_filtered_tickers(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Get CS tickers meeting volume criteria based on most recent data.

    Args:
        con: DuckDB connection to gold database.

    Returns:
        List of ticker symbols meeting criteria.
    """
    print("🔍 Finding CS stocks with 20-day avg volume >= 200K...")

    # Get most recent date from daily data
    most_recent_query = """
        SELECT MAX(date) as max_date
        FROM daily_enriched
        WHERE type = 'CS'
    """
    most_recent_date = con.execute(most_recent_query).fetchone()[0]
    print(f"   📅 Most recent daily data: {most_recent_date}")

    # Get tickers meeting criteria on that date
    filter_query = f"""
        SELECT DISTINCT ticker
        FROM daily_enriched
        WHERE type = 'CS'
          AND date = '{most_recent_date}'
          AND volume_ma_20 >= 200000
        ORDER BY ticker
    """

    result = con.execute(filter_query).pl()
    tickers = result['ticker'].to_list()

    print(f"   ✅ Found {len(tickers)} tickers meeting criteria")
    return tickers


def get_stage_distribution(con: duckdb.DuckDBPyConnection, tickers: list[str]) -> pl.DataFrame:
    """Get historical stage distribution for filtered tickers.

    Args:
        con: DuckDB connection to gold database.
        tickers: List of tickers to include.

    Returns:
        DataFrame with weekly stage distribution percentages.
    """
    print("\n📊 Calculating historical stage distribution...")

    # Create ticker list for SQL IN clause
    ticker_list = "','".join(tickers)

    # Query all weekly stage data for these tickers
    stage_query = f"""
        SELECT
            date,
            stage,
            COUNT(*) as count
        FROM weekly_enriched
        WHERE ticker IN ('{ticker_list}')
          AND stage IS NOT NULL
        GROUP BY date, stage
        ORDER BY date, stage
    """

    stage_counts = con.execute(stage_query).pl()

    if len(stage_counts) == 0:
        print("   ❌ No stage data found for filtered tickers!")
        sys.exit(1)

    # Calculate total stocks per week
    totals_by_week = stage_counts.group_by('date').agg(
        pl.col('count').sum().alias('total')
    )

    # Join totals and calculate percentages
    stage_pct = (
        stage_counts
        .join(totals_by_week, on='date')
        .with_columns(
            (pl.col('count') / pl.col('total') * 100).alias('percentage')
        )
        .select(['date', 'stage', 'percentage'])
    )

    # Pivot to get one column per stage
    stage_pivot = stage_pct.pivot(
        on='stage',
        values='percentage',
        index='date',
        aggregate_function='first'
    )

    # Ensure all 4 stages exist as strings (pivot creates string column names)
    # Fill missing stages with 0
    for stage in ['1', '2', '3', '4']:
        if stage not in stage_pivot.columns:
            stage_pivot = stage_pivot.with_columns(
                pl.lit(0.0).alias(stage)
            )

    # Sort by date and select columns in order
    stage_pivot = stage_pivot.sort('date').select(['date', '1', '2', '3', '4'])

    print(f"   ✅ Processed {len(stage_pivot)} weeks of data")
    print(f"   📅 Date range: {stage_pivot['date'].min()} to {stage_pivot['date'].max()}")

    return stage_pivot


def create_stacked_area_chart(df: pl.DataFrame, output_path: Path, ticker_count: int) -> None:
    """Create and save a stacked area chart of stage distribution.

    Args:
        df: DataFrame with columns: date, 1, 2, 3, 4 (stage percentages).
        output_path: Path to save the chart PNG.
        ticker_count: Number of tickers included in analysis.
    """
    print("\n🎨 Creating stacked area chart...")

    # Extract data as numpy arrays for matplotlib
    dates = [datetime.strptime(str(d), '%Y-%m-%d') for d in df['date'].to_list()]
    stage_1 = df['1'].to_numpy()
    stage_2 = df['2'].to_numpy()
    stage_3 = df['3'].to_numpy()
    stage_4 = df['4'].to_numpy()

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(14, 8))

    # Define colors for each stage
    colors = {
        1: '#87CEEB',  # Sky blue - Stage 1 (Basing)
        2: '#32CD32',  # Lime green - Stage 2 (Advancing)
        3: '#FFD700',  # Gold - Stage 3 (Topping)
        4: '#DC143C',  # Crimson - Stage 4 (Declining)
    }

    # Create stacked area chart
    ax.stackplot(
        dates,
        stage_1, stage_2, stage_3, stage_4,
        labels=[
            'Stage 1 (Basing)',
            'Stage 2 (Advancing)',
            'Stage 3 (Topping)',
            'Stage 4 (Declining)'
        ],
        colors=[colors[1], colors[2], colors[3], colors[4]],
        alpha=0.8
    )

    # Formatting
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Percentage of Stocks (%)', fontsize=12, fontweight='bold')
    ax.set_title(
        f'Weinstein Stage Distribution Over Time\n'
        f'CS Stocks with 20-day Avg Volume ≥ 200K shares (n={ticker_count})',
        fontsize=14, fontweight='bold', pad=20
    )

    # Legend
    ax.legend(loc='upper left', framealpha=0.95, fontsize=10, edgecolor='black')

    # Grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)

    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45, ha='right')

    # Y-axis from 0 to 100%
    ax.set_ylim(0, 100)
    ax.set_yticks(range(0, 101, 10))

    # Add percentage symbol to y-axis
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{int(y)}%'))

    # Tight layout
    plt.tight_layout()

    # Save
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   ✅ Chart saved to: {output_path}")

    # Display
    plt.show()
    print("   📺 Chart displayed (close window to continue)")


def main() -> None:
    """Main entry point for stage distribution analysis."""
    print("=" * 70)
    print("📊 Weinstein Stage Distribution Analysis")
    print("=" * 70)

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
        # Get filtered tickers
        tickers = get_filtered_tickers(con)

        if not tickers:
            print("\n❌ No tickers found meeting criteria!")
            sys.exit(1)

        # Get stage distribution
        stage_data = get_stage_distribution(con, tickers)

        # Create chart
        output_path = Path(__file__).parent / 'weinstein_stage_distribution.png'
        create_stacked_area_chart(stage_data, output_path, len(tickers))

        print("\n" + "=" * 70)
        print("✅ Analysis complete! 🎉")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        con.close()


if __name__ == "__main__":
    main()
