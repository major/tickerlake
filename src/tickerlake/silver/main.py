"""Silver layer processing to adjust historical stock data for splits."""

from pathlib import Path

import polars as pl

from tickerlake.config import settings
from tickerlake.logging_config import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

pl.Config.set_verbose(False)


def read_splits() -> pl.DataFrame:
    """Read splits data from bronze layer."""
    logger.info("Reading splits data...")
    splits_parquet = f"{settings.bronze_storage_path}/splits/splits.parquet"
    df = pl.read_parquet(splits_parquet)
    return df


def read_tickers() -> pl.DataFrame:
    """Read tickers data from bronze layer."""
    logger.info("Reading tickers data...")
    tickers_parquet = f"{settings.bronze_storage_path}/tickers/tickers.parquet"
    df = pl.read_parquet(tickers_parquet)
    return df


def read_stocks_lazy() -> pl.LazyFrame:
    """Read stock data lazily from bronze layer."""
    logger.info("Reading stocks data lazily...")
    stocks_parquet = f"{settings.bronze_storage_path}/stocks/date=*/*.parquet"
    lf = pl.scan_parquet(stocks_parquet)
    return lf


def apply_splits_lazy(
    stocks_lf: pl.LazyFrame,
    splits_df: pl.DataFrame,
    output_path: str,
) -> None:
    """Apply split adjustments to stock data.

    For each stock date, multiplies prices by the ratio (split_from/split_to) for
    all splits that occurred AFTER that date. This adjusts historical prices to
    match current split-adjusted prices.

    Args:
        stocks_lf: LazyFrame with stock data.
        splits_df: DataFrame with splits data.
        output_path: Path to write adjusted data.
    """
    logger.info("Applying split adjustments...")

    # Join stocks with all splits for that ticker
    # Then calculate adjustment factor: for each date, apply split_from/split_to
    # if the split occurred AFTER that date
    adjusted_lazy = (
        stocks_lf
        .join(
            splits_df.select(["ticker", "execution_date", "split_from", "split_to"]).lazy(),
            on="ticker",
            how="left",
        )
        # Calculate adjustment factor for each split
        .with_columns([
            pl.when(pl.col("date") < pl.col("execution_date"))
            .then(pl.col("split_from") / pl.col("split_to"))
            .otherwise(1.0)
            .alias("adjustment_factor")
        ])
        # Calculate total adjustment by multiplying all factors for this ticker+date
        .group_by(["ticker", "date"])
        .agg([
            pl.col("adjustment_factor").product().alias("total_adjustment"),
            pl.col("open").first(),
            pl.col("high").first(),
            pl.col("low").first(),
            pl.col("close").first(),
            pl.col("volume").first(),
            pl.col("transactions").first(),
        ])
        # Apply adjustments
        .with_columns([
            (pl.col("open") * pl.col("total_adjustment")).alias("open"),
            (pl.col("high") * pl.col("total_adjustment")).alias("high"),
            (pl.col("low") * pl.col("total_adjustment")).alias("low"),
            (pl.col("close") * pl.col("total_adjustment")).alias("close"),
            (pl.col("volume") / pl.col("total_adjustment"))
            .cast(pl.UInt64)
            .alias("volume"),
            (pl.col("transactions") / pl.col("total_adjustment"))
            .cast(pl.UInt32)
            .alias("transactions"),
        ])
        .drop("total_adjustment")
        .select(["ticker", "date", "open", "high", "low", "close", "volume", "transactions"])
    )

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Stream to output without collecting
    logger.info("Writing adjusted stock data to Parquet...")
    adjusted_lazy.sink_parquet(output_path)


def main() -> None:  # pragma: no cover
    """Main function to adjust historical stock data for splits."""
    # Read tickers and filter to only CS (Common Stock) and ETF types
    tickers_df = read_tickers()
    valid_tickers = tickers_df.filter(
        pl.col("type").is_in(["CS", "ETF"])
    ).select("ticker")

    logger.info(
        f"Filtering to {len(valid_tickers)} tickers with type CS or ETF "
        f"(from {len(tickers_df)} total tickers)"
    )

    # Read splits and stocks
    splits_df = read_splits()

    # Filter splits to only CS/ETF tickers to avoid applying incorrect splits
    # from warrants, delisted tickers, or other instrument types
    original_splits_count = len(splits_df)
    splits_df = splits_df.join(valid_tickers, on="ticker", how="semi")
    logger.info(
        f"Filtered splits to {len(splits_df)} splits for CS/ETF tickers "
        f"(from {original_splits_count} total splits)"
    )

    stocks_lf = read_stocks_lazy()

    # Filter stocks to only valid ticker types using semi-join
    stocks_lf = stocks_lf.join(valid_tickers.lazy(), on="ticker", how="semi")

    # Process all years at once with partitioning
    apply_splits_lazy(
        stocks_lf=stocks_lf,
        splits_df=splits_df,
        output_path=f"{settings.silver_storage_path}/stocks/adjusted.parquet",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
