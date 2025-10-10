"""SilverLayer class for data processing."""

from datetime import date
from typing import Literal

import polars as pl
import structlog

from tickerlake.config import s3_storage_options, settings
from tickerlake.delta_utils import (
    delta_table_exists,
    get_max_date_from_delta,
    merge_to_delta_table,
    scan_delta_table,
    write_delta_table,
)
from tickerlake.schemas import (
    validate_daily_aggregates,
    validate_etf_holdings,
    validate_splits,
    validate_ticker_details,
)

logger = structlog.get_logger()


class SilverLayer:
    """Silver layer data processing with Delta Lake."""

    def __init__(self):
        """Initialize Silver layer with storage configuration."""
        self.bucket_name = settings.s3_bucket_name
        self.storage_options = s3_storage_options

    def read_ticker_details(self) -> pl.DataFrame:
        """Read ticker details from bronze layer and filter for stocks and ETFs.

        Returns:
            pl.DataFrame: DataFrame with ticker, name, and ticker_type columns for CS and ETF types.
        """
        logger.info("Reading ticker details")
        df = (
            pl.scan_parquet(
                f"s3://{self.bucket_name}/bronze/tickers/data.parquet",
                storage_options=self.storage_options,
            )
            .select(["ticker", "name", "type"])
            .filter(pl.col("type").is_in(["CS", "ETF"]))
            .collect()
            .select(["ticker", "name", pl.col("type").alias("ticker_type")])
            .sort("ticker")
        )
        return validate_ticker_details(df)

    def write_ticker_details(self, df: pl.DataFrame) -> None:
        """Write ticker details to silver layer Delta table.

        Args:
            df: DataFrame containing ticker details to write.
        """
        logger.info("Writing ticker details to Delta table")
        if delta_table_exists("tickers"):
            merge_to_delta_table(df, "tickers", merge_keys=["ticker"])
        else:
            write_delta_table(df, "tickers", mode="overwrite")

    def read_split_details(self, valid_tickers: list = []) -> pl.DataFrame:
        """Read split details from bronze layer for specified tickers.

        Args:
            valid_tickers: List of ticker symbols to filter for.

        Returns:
            pl.DataFrame: DataFrame with split details for valid tickers.
        """
        logger.info("Reading split details")
        df = (
            pl.scan_parquet(
                f"s3://{self.bucket_name}/bronze/splits/data.parquet",
                storage_options=self.storage_options,
            )
            .filter(pl.col("ticker").is_in(valid_tickers))
            .select(["ticker", "execution_date", "split_from", "split_to"])
            .collect()
            .sort("execution_date")
        )
        return validate_splits(df)

    def write_split_details(self, df: pl.DataFrame) -> None:
        """Write split details to silver layer Delta table.

        Args:
            df: DataFrame containing split details to write.
        """
        logger.info("Writing split details to Delta table")
        if delta_table_exists("splits"):
            merge_to_delta_table(df, "splits", merge_keys=["ticker", "execution_date"])
        else:
            write_delta_table(df, "splits", mode="overwrite")

    def read_etf_holdings(self, etf_ticker: str) -> pl.DataFrame:
        """Read ETF holdings data from bronze layer.

        Args:
            etf_ticker: ETF ticker symbol to read holdings for.

        Returns:
            pl.DataFrame: DataFrame with ETF holdings including ticker and weight.
        """
        path = f"s3://{self.bucket_name}/bronze/holdings/{etf_ticker.lower()}/data.parquet"
        df = (
            pl.read_parquet(path, storage_options=self.storage_options)
            .sort("ticker")
            .with_columns(
                pl.lit(etf_ticker.lower()).alias("etf"),
            )
        )
        return validate_etf_holdings(df)

    def read_all_etf_holdings(self) -> pl.DataFrame:
        """Read and aggregate holdings for all configured ETFs.

        Returns:
            pl.DataFrame: DataFrame with ticker and list of ETFs holding each ticker.
        """
        all_etfs = pl.concat([self.read_etf_holdings(x) for x in settings.etfs])

        etf_membership = all_etfs.group_by("ticker").agg(pl.col("etf").alias("etfs"))
        return etf_membership

    def add_volume_ratio(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds a 'volume_avg_ratio' column to the given DataFrame, representing the ratio of the current 'volume' to its 20-period rolling mean per 'ticker'.

        Parameters:
            df (pl.DataFrame): Input DataFrame containing at least 'volume' and 'ticker' columns.

        Returns:
            pl.DataFrame: DataFrame with two additional columns:
                - 'volume_avg': 20-period rolling mean of 'volume' per 'ticker'.
                - 'volume_avg_ratio': Ratio of 'volume' to 'volume_avg'.
        """
        logger.info("Calculating volume average ratio")
        result = df.with_columns(
            pl.col("volume")
            .rolling_mean(window_size=20)
            .over("ticker")
            .cast(pl.UInt64)
            .alias("volume_avg")
        ).with_columns(
            pl.when(pl.col("volume_avg").is_not_null())
            .then(pl.col("volume") / pl.col("volume_avg"))
            .otherwise(None)
            .alias("volume_avg_ratio")
        )
        return validate_daily_aggregates(result, include_volume_ratio=True)

    def read_daily_aggs(
        self,
        valid_tickers: list = [],
        ticker_details: pl.DataFrame | None = None,
        start_date: date | None = None,
    ) -> pl.DataFrame:
        """Read daily aggregates from bronze layer for specified tickers.

        Args:
            valid_tickers: List of ticker symbols to filter for.
            ticker_details: DataFrame with ticker details including ticker_type.
            start_date: Optional start date to filter data (for incremental loading).

        Returns:
            pl.DataFrame: DataFrame with daily OHLCV data, date column, and ticker_type.
        """
        if start_date:
            logger.info(f"Reading daily aggregates from {start_date}")
        else:
            logger.info("Reading all daily aggregates")

        path = f"s3://{self.bucket_name}/bronze/daily/*/data.parquet"
        df = (
            pl.scan_parquet(path, storage_options=self.storage_options)
            .filter(pl.col("ticker").is_in(valid_tickers))
            .collect()
            .with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms")
                .cast(pl.Date)
                .alias("date"),
                pl.col("volume").cast(pl.UInt64),
                pl.col("open").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("transactions").cast(pl.UInt64),
            )
        )

        # Filter by start_date if provided (incremental mode)
        if start_date:
            df = df.filter(pl.col("date") > start_date)

        df = df.sort(["date", "ticker"])

        # Join with ticker details to add ticker_type
        if ticker_details is not None:
            df = df.join(
                ticker_details.select(["ticker", "ticker_type"]),
                on="ticker",
                how="left",
            )

        return validate_daily_aggregates(df)

    def apply_splits(self, daily_aggs: pl.DataFrame, splits: pl.DataFrame) -> pl.DataFrame:
        """Apply stock split adjustments to daily aggregate data.

        Args:
            daily_aggs: DataFrame with daily OHLCV data.
            splits: DataFrame with split details including execution dates.

        Returns:
            pl.DataFrame: Split-adjusted daily aggregates.
        """
        logger.info("Applying splits to daily aggregates")

        # Ensure ticker columns have the same dtype for joining
        # Convert both to strings to avoid categorical/string mismatch
        if splits["ticker"].dtype == pl.Categorical:
            splits = splits.with_columns(pl.col("ticker").cast(pl.String))
        if daily_aggs["ticker"].dtype == pl.Categorical:
            daily_aggs = daily_aggs.with_columns(pl.col("ticker").cast(pl.String))

        result = daily_aggs.join(splits, on="ticker", how="left")

        result = result.with_columns(
            pl.when(pl.col("date") < pl.col("execution_date"))
            .then(pl.col("split_from") / pl.col("split_to"))
            .otherwise(1.0)
            .alias("adjustment_factor")
        )

        # Include ticker_type in group_by if it exists
        group_cols = [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "transactions",
        ]
        if "ticker_type" in daily_aggs.columns:
            group_cols.append("ticker_type")

        result = result.group_by(
            group_cols,
        ).agg(pl.col("adjustment_factor").product().alias("total_adjustment"))

        result = result.with_columns(
            (pl.col("close") * pl.col("total_adjustment")).cast(pl.Float64).alias("close"),
            (pl.col("open") * pl.col("total_adjustment")).cast(pl.Float64).alias("open"),
            (pl.col("high") * pl.col("total_adjustment")).cast(pl.Float64).alias("high"),
            (pl.col("low") * pl.col("total_adjustment")).cast(pl.Float64).alias("low"),
            (pl.col("volume") / pl.col("total_adjustment")).cast(pl.UInt64).alias("volume"),
        ).drop("total_adjustment")

        return validate_daily_aggregates(result)

    def write_daily_aggs(
        self, df: pl.DataFrame, mode: Literal["error", "append", "overwrite", "ignore"] = "overwrite"
    ) -> None:
        """Write daily aggregates to silver layer Delta table.

        Args:
            df: DataFrame containing daily aggregates to write.
            mode: Write mode - 'overwrite' for full rebuild or 'append' for incremental.
        """
        logger.info(f"Writing daily aggregates ({df.shape[0]:,} rows) in {mode} mode")
        write_delta_table(df, "daily", mode=mode)

    def write_time_aggs(self, df: pl.DataFrame, period: str, table_name: str) -> None:
        """Write time-aggregated data to Delta table.

        Args:
            df: DataFrame with daily data to aggregate.
            period: Time period for aggregation (e.g., '1w', '1mo').
            table_name: Delta table name (e.g., 'weekly', 'monthly').
        """
        higher_timeframe_df = (
            df.sort(["ticker", "date"])
            .group_by_dynamic(
                "date",
                every=period,
                group_by="ticker",
                label="left",
                start_by="monday",
            )
            .agg([
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
            ])
            .sort(["date", "ticker"])
        )

        logger.info(f"Writing {period} aggregates ({higher_timeframe_df.shape[0]:,} rows)")
        write_delta_table(higher_timeframe_df, table_name, mode="overwrite")

    def write_weekly_aggs(self, df: pl.DataFrame) -> None:
        """Write weekly aggregates to silver layer Delta table.

        Args:
            df: DataFrame containing daily data to aggregate weekly.
        """
        self.write_time_aggs(df, "1w", "weekly")

    def write_monthly_aggs(self, df: pl.DataFrame) -> None:
        """Write monthly aggregates to silver layer Delta table.

        Args:
            df: DataFrame containing daily data to aggregate monthly.
        """
        self.write_time_aggs(df, "1mo", "monthly")

    def process_reference_data(self) -> tuple[pl.DataFrame, list[str], pl.DataFrame]:
        """Load and process ticker and split reference data.

        Returns:
            Tuple of (ticker_details, valid_tickers, split_details).
        """
        ticker_details = self.read_ticker_details()
        etf_holdings = self.read_all_etf_holdings()
        ticker_details = ticker_details.join(etf_holdings, on="ticker", how="left")
        self.write_ticker_details(ticker_details)

        valid_tickers = ticker_details["ticker"].to_list()

        split_details = self.read_split_details(valid_tickers)
        self.write_split_details(split_details)

        return ticker_details, valid_tickers, split_details

    def process_incremental_data(
        self,
        valid_tickers: list[str],
        ticker_details: pl.DataFrame,
        split_details: pl.DataFrame,
        last_processed_date: date,
    ) -> None:
        """Process new data incrementally and update Delta tables.

        Args:
            valid_tickers: List of valid ticker symbols.
            ticker_details: DataFrame with ticker details.
            split_details: DataFrame with split details.
            last_processed_date: Last date already processed in Delta table.
        """
        logger.info(f"Incremental mode: processing data after {last_processed_date}")

        # Read only new data from bronze layer
        unadjusted_daily_aggs = self.read_daily_aggs(
            valid_tickers, ticker_details, start_date=last_processed_date
        )

        if unadjusted_daily_aggs.height == 0:
            logger.info("No new data to process")
            return

        logger.info(f"Processing {unadjusted_daily_aggs.height:,} new rows")
        adjusted_daily_aggs = self.apply_splits(unadjusted_daily_aggs, split_details)

        # For volume ratios, we need full history to calculate 20-day rolling average
        logger.info("Reading existing data to calculate volume ratios")
        existing_daily_data = scan_delta_table("daily").collect()

        # Cast existing data to match new data types (UInt64 for volume/transactions)
        existing_daily_data = existing_daily_data.with_columns(
            pl.col("volume").cast(pl.UInt64),
            pl.col("transactions").cast(pl.UInt64),
        )

        # Combine existing and new data, sort by ticker and date
        combined_data = pl.concat(
            [existing_daily_data.drop(["volume_avg", "volume_avg_ratio"]), adjusted_daily_aggs]
        ).sort(["ticker", "date"])

        # Calculate volume ratios on full dataset
        combined_with_volume_ratio = self.add_volume_ratio(combined_data)

        # Filter to only the new rows we want to append
        new_rows_with_ratio = combined_with_volume_ratio.filter(pl.col("date") > last_processed_date)

        # Append new data to Delta table
        self.write_daily_aggs(new_rows_with_ratio, mode="append")

        # For weekly/monthly, we need to rebuild from the full dataset
        logger.info("Rebuilding weekly and monthly aggregates from full dataset")
        full_daily_data = scan_delta_table("daily").collect()
        self.write_weekly_aggs(full_daily_data)
        self.write_monthly_aggs(full_daily_data)

    def process_full_rebuild(
        self,
        valid_tickers: list[str],
        ticker_details: pl.DataFrame,
        split_details: pl.DataFrame,
    ) -> None:
        """Process all data from scratch and overwrite Delta tables.

        Args:
            valid_tickers: List of valid ticker symbols.
            ticker_details: DataFrame with ticker details.
            split_details: DataFrame with split details.
        """
        logger.info("Full rebuild mode: processing all data")
        unadjusted_daily_aggs = self.read_daily_aggs(valid_tickers, ticker_details)
        adjusted_daily_aggs = self.apply_splits(unadjusted_daily_aggs, split_details)

        adjusted_daily_aggs_with_volume_ratio = self.add_volume_ratio(adjusted_daily_aggs)

        self.write_daily_aggs(adjusted_daily_aggs_with_volume_ratio, mode="overwrite")
        self.write_weekly_aggs(adjusted_daily_aggs)
        self.write_monthly_aggs(adjusted_daily_aggs)

    def run(self, full_rebuild: bool = False) -> None:
        """Execute silver layer data processing pipeline.

        Args:
            full_rebuild: If True, rebuild all data from scratch. If False, process incrementally.
        """
        # Always update ticker details and splits (reference data)
        ticker_details, valid_tickers, split_details = self.process_reference_data()

        # Determine if we should process incrementally
        incremental_mode = not full_rebuild and delta_table_exists("daily")

        if incremental_mode:
            last_processed_date = get_max_date_from_delta("daily")

            if last_processed_date:
                self.process_incremental_data(
                    valid_tickers, ticker_details, split_details, last_processed_date
                )
            else:
                logger.info("Delta table exists but is empty, performing full rebuild")
                self.process_full_rebuild(valid_tickers, ticker_details, split_details)
        else:
            self.process_full_rebuild(valid_tickers, ticker_details, split_details)
