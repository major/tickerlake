"""Gold medallion layer for TickerLake."""

from datetime import date, timedelta

import polars as pl

from tickerlake.delta_utils import scan_delta_table
from tickerlake.logging_config import get_logger, setup_logging
from tickerlake.schemas import validate_high_volume_closes

pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_float="full")

setup_logging()
logger = get_logger(__name__)


class GoldLayer:
    """Gold layer analytics and exports."""

    def get_latest_closing_prices(self) -> pl.DataFrame:
        """Get the latest closing price for each ticker from Delta table.

        Returns:
            pl.DataFrame: DataFrame with ticker and latest_close columns.
        """
        logger.info("Fetching latest closing prices for each ticker")
        latest_prices = (
            scan_delta_table("daily")
            .group_by("ticker")
            .agg(
                pl.col("close").last().alias("latest_close"),
                pl.col("date").max().alias("latest_date"),
            )
            .select(["ticker", "latest_close"])
            .collect()
        )

        return latest_prices

    def get_high_volume_closes(self) -> pl.DataFrame:
        """Get days with unusually high trading volume from Delta table.

        Only includes data from the past 5 years to focus on recent patterns.

        Returns:
            pl.DataFrame: DataFrame with ticker, date, close, volume, and volume_avg_ratio.
        """
        logger.info("Extracting high volume closes from daily data (past 5 years)")

        # Calculate 5 years ago from today
        five_years_ago = date.today() - timedelta(days=5 * 365)

        df = (
            scan_delta_table("daily")
            .filter(
                pl.col("date") >= five_years_ago,  # Only past 5 years
                pl.col("volume_avg_ratio") >= 3,
                (
                    (
                        (pl.col("ticker_type") == "CS")
                        & (pl.col("volume_avg") >= 200000)
                        & (pl.col("close") >= 5)
                    )
                    | (
                        (pl.col("ticker_type") == "ETF")
                        & (pl.col("volume_avg") >= 50000)
                    )
                ),
            )
            .collect()
            .sort(["date", "ticker"], descending=[True, False])
        )

        return df

    def _get_consecutive_hvc_patterns(
        self,
        direction: str,
        min_steps: int = 3,
        hvcs_df: pl.DataFrame | None = None,
        latest_prices: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Generic method to identify consecutive HVC patterns (ascending or descending).

        Args:
            direction: Either "ascending" (stair-stepping up) or "descending" (falling down).
            min_steps: Minimum number of consecutive HVCs required (default: 3).
            hvcs_df: Optional precomputed HVCs DataFrame. If None, will fetch from Delta table.
            latest_prices: Optional precomputed latest prices. If None, will fetch from Delta table.

        Returns:
            pl.DataFrame: DataFrame with ticker, pattern details, and sequence of HVCs.
        """
        if direction not in ["ascending", "descending"]:
            raise ValueError("direction must be 'ascending' or 'descending'")

        pattern_name = "stair-stepping" if direction == "ascending" else "falling down stairs"
        logger.info(f"Identifying {pattern_name} HVC patterns with min {min_steps} steps")

        # If HVCs not provided, fetch them
        if hvcs_df is None:
            logger.info("HVCs not provided, fetching from Delta table")
            hvcs_df = self.get_high_volume_closes()
        else:
            logger.info("Using precomputed HVCs (optimization)")

        # Extract just the columns needed for pattern analysis
        hvcs = hvcs_df.select([
            "ticker",
            "ticker_type",
            "date",
            "close",
            "volume",
            "volume_avg_ratio",
        ]).sort(["ticker", "date"])

        # Define comparison based on direction
        if direction == "ascending":
            comparison = pl.col("close") > pl.col("close").shift(1).over("ticker")
        else:  # descending
            comparison = pl.col("close") < pl.col("close").shift(1).over("ticker")

        # For each ticker, identify consecutive price sequences
        patterns = (
            hvcs.with_columns(
                # Get previous close price for this ticker
                pl.col("close").shift(1).over("ticker").alias("prev_close"),
                # Check if current close matches the direction
                comparison.alias("matches_direction"),
            )
            .with_columns(
                # Create groups: new group starts when matches_direction becomes False or is null
                (~pl.col("matches_direction").fill_null(False))
                .cum_sum()
                .over("ticker")
                .alias("sequence_group"),
            )
            .filter(
                # Only keep sequences matching direction
                pl.col("matches_direction") == True  # noqa: E712
            )
            .group_by(["ticker", "ticker_type", "sequence_group"])
            .agg(
                pl.col("date").min().alias("pattern_start_date"),
                pl.col("date").max().alias("pattern_end_date"),
                pl.col("date").alias("dates"),
                pl.col("close").alias("closes"),
                pl.col("volume_avg_ratio").alias("volume_ratios"),
                pl.col("close").first().alias("first_close"),
                pl.col("close").last().alias("last_close"),
                pl.len().alias("step_count"),
            )
            .filter(
                # Filter for patterns with at least min_steps
                pl.col("step_count")
                >= (min_steps - 1)  # -1 because we're counting transitions
            )
            .with_columns(
                # Calculate percent change from first to last close in the pattern
                (
                    (pl.col("last_close") - pl.col("first_close"))
                    / pl.col("first_close")
                    * 100
                ).alias("pattern_change_pct"),
                # Actual number of HVCs in the pattern (+1 for the initial step)
                (pl.col("step_count") + 1).alias("hvc_count"),
            )
            .sort(["pattern_end_date", "hvc_count"], descending=[True, True])
            .drop("sequence_group")
        )

        # Get latest closing prices if not provided
        if latest_prices is None:
            logger.info("Latest prices not provided, fetching from Delta table")
            latest_prices = self.get_latest_closing_prices()
        else:
            logger.info("Using precomputed latest prices (optimization)")

        patterns = patterns.join(latest_prices, on="ticker", how="left")

        # Check if pattern is still "active" (no contrary HVCs after pattern ended)
        if direction == "ascending":
            # For ascending: broken if any HVCs after pattern are below last_close
            broken_condition = pl.col("hvc_close") < pl.col("last_close")
            trend_check = pl.col("latest_close") > pl.col("last_close")
            logger.info("Checking for broken patterns (HVCs below pattern high after pattern ended)")
        else:  # descending
            # For descending: broken if any HVCs after pattern are above last_close
            broken_condition = pl.col("hvc_close") > pl.col("last_close")
            trend_check = pl.col("latest_close") < pl.col("last_close")
            logger.info("Checking for broken patterns (HVCs above pattern low after pattern ended)")

        # Join patterns with all HVCs to find any HVCs after the pattern
        patterns_with_future_hvcs = patterns.join(
            hvcs.select(["ticker", "date", "close"]).rename({"date": "hvc_date", "close": "hvc_close"}),
            on="ticker",
            how="left"
        )

        # Filter to HVCs that occurred after the pattern ended
        patterns_with_future_hvcs = patterns_with_future_hvcs.filter(
            pl.col("hvc_date") > pl.col("pattern_end_date")
        )

        # Check if any future HVCs break the pattern
        broken_patterns = (
            patterns_with_future_hvcs
            .filter(broken_condition)
            .select(["ticker", "pattern_start_date", "pattern_end_date"])
            .unique()
            .with_columns(pl.lit(True).alias("is_broken"))
        )

        # Left join to mark broken patterns
        patterns = patterns.join(
            broken_patterns,
            on=["ticker", "pattern_start_date", "pattern_end_date"],
            how="left"
        ).with_columns(
            pl.col("is_broken").fill_null(False).alias("pattern_broken")
        ).drop("is_broken")

        # Add analysis columns
        patterns = patterns.with_columns(
            # Days since pattern ended
            (pl.lit(date.today()) - pl.col("pattern_end_date"))
            .dt.total_days()
            .alias("days_since_pattern"),
            # Current price vs last HVC in pattern
            (
                (pl.col("latest_close") - pl.col("last_close"))
                / pl.col("last_close")
                * 100
            ).alias("price_change_since_pct"),
            # Is current price continuing the trend?
            trend_check.alias("continuing_trend"),
        )

        logger.info(f"Found {patterns.height} {pattern_name} patterns")

        # Filter out broken patterns
        active_patterns = patterns.filter(~pl.col("pattern_broken"))
        logger.info(f"Filtered to {active_patterns.height} active patterns (excluding broken patterns)")

        return active_patterns

    def get_stairstepping_hvcs(
        self,
        min_steps: int = 3,
        hvcs_df: pl.DataFrame | None = None,
        latest_prices: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Identify stocks with consecutive HVCs showing stair-stepping price increases.

        A stair-step pattern occurs when a stock has consecutive HVCs where each
        subsequent HVC has a higher closing price than the previous one. This pattern
        often indicates sustained institutional accumulation.

        Only includes data from the past 5 years to focus on recent patterns.

        Args:
            min_steps: Minimum number of consecutive ascending HVCs required (default: 3).
            hvcs_df: Optional precomputed HVCs DataFrame. If None, will fetch from Delta table.
            latest_prices: Optional precomputed latest prices. If None, will fetch from Delta table.

        Returns:
            pl.DataFrame: DataFrame with ticker, pattern details, and sequence of HVCs
                         showing stair-stepping behavior.
        """
        patterns = self._get_consecutive_hvc_patterns(
            direction="ascending",
            min_steps=min_steps,
            hvcs_df=hvcs_df,
            latest_prices=latest_prices
        )

        # Rename pattern_change_pct to pattern_gain_pct for backward compatibility
        patterns = patterns.rename({"pattern_change_pct": "pattern_gain_pct"})
        patterns = patterns.rename({"continuing_trend": "above_pattern_high"})

        return patterns

    def get_falling_down_stairs_hvcs(
        self,
        min_steps: int = 3,
        hvcs_df: pl.DataFrame | None = None,
        latest_prices: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Identify stocks with consecutive HVCs showing falling price patterns.

        A falling down stairs pattern occurs when a stock has consecutive HVCs where
        each subsequent HVC has a lower closing price than the previous one. This pattern
        often indicates sustained institutional distribution or selling pressure.

        Only includes data from the past 5 years to focus on recent patterns.

        Args:
            min_steps: Minimum number of consecutive descending HVCs required (default: 3).
            hvcs_df: Optional precomputed HVCs DataFrame. If None, will fetch from Delta table.
            latest_prices: Optional precomputed latest prices. If None, will fetch from Delta table.

        Returns:
            pl.DataFrame: DataFrame with ticker, pattern details, and sequence of HVCs
                         showing falling down stairs behavior.
        """
        patterns = self._get_consecutive_hvc_patterns(
            direction="descending",
            min_steps=min_steps,
            hvcs_df=hvcs_df,
            latest_prices=latest_prices
        )

        # Rename pattern_change_pct to pattern_loss_pct (will be negative)
        patterns = patterns.rename({"pattern_change_pct": "pattern_loss_pct"})
        patterns = patterns.rename({"continuing_trend": "below_pattern_low"})

        return patterns

    def _get_pattern_summary(
        self,
        patterns: pl.DataFrame,
        pattern_type: str,
    ) -> pl.DataFrame:
        """Generic method to create a summary of patterns with one row per ticker.

        For tickers with multiple patterns, shows the most recent one (by pattern_end_date).

        Args:
            patterns: DataFrame with pattern data.
            pattern_type: Either "ascending" (stair-stepping) or "descending" (falling).

        Returns:
            pl.DataFrame: Summary with ticker, step count, dates, and price changes.
        """
        if pattern_type not in ["ascending", "descending"]:
            raise ValueError("pattern_type must be 'ascending' or 'descending'")

        pattern_name = "stair-stepping" if pattern_type == "ascending" else "falling down stairs"
        logger.info(f"Creating {pattern_name} summary (most recent pattern per ticker)")

        if patterns.height == 0:
            return pl.DataFrame()

        # Determine which columns to use based on pattern type
        if pattern_type == "ascending":
            change_col = "pattern_gain_pct"
            change_alias = "gain_pct"
            trend_col = "above_pattern_high"
            bottom_col = "first_close"
            top_col = "last_close"
        else:  # descending
            change_col = "pattern_loss_pct"
            change_alias = "loss_pct"
            trend_col = "below_pattern_low"
            # For descending, first is top and last is bottom
            bottom_col = "last_close"
            top_col = "first_close"

        # For each ticker, keep only the most recent pattern (by pattern_end_date)
        # If tied, keep the one with the most steps
        summary = (
            patterns.sort(["pattern_end_date", "hvc_count"], descending=[True, True])
            .group_by("ticker")
            .agg(
                pl.col("ticker_type").first(),
                pl.col("hvc_count").first().alias("steps"),
                pl.col("pattern_start_date").first().alias("first_hvc"),
                pl.col("pattern_end_date").first().alias("last_hvc"),
                pl.col(top_col).first().round(2).alias("top_price"),
                pl.col(bottom_col).first().round(2).alias("bottom_price"),
                pl.col(change_col).first().round(1).alias(change_alias),
                pl.col("latest_close").first().round(2),
                pl.col("price_change_since_pct")
                .first()
                .round(1)
                .alias("change_since_pct"),
                pl.col("days_since_pattern").first(),
                pl.col(trend_col).first().alias("still_trending"),
            )
            .sort(["last_hvc", "steps"], descending=[True, True])
        )

        logger.info(f"Created summary with {summary.height} unique tickers")
        return summary

    def get_stairstepping_summary(
        self, patterns: pl.DataFrame | None = None
    ) -> pl.DataFrame:
        """Get a summary of stair-stepping patterns with one row per ticker showing their most recent pattern.

        For tickers with multiple patterns, shows the most recent one (by pattern_end_date).

        Args:
            patterns: Optional precomputed patterns DataFrame. If None, will compute patterns.

        Returns:
            pl.DataFrame: Summary with ticker, step count, dates, and gains.
        """
        # Get all patterns if not provided
        if patterns is None:
            logger.info("Patterns not provided, computing from scratch")
            patterns = self.get_stairstepping_hvcs(min_steps=3)
        else:
            logger.info("Using precomputed patterns (optimization)")

        return self._get_pattern_summary(patterns, pattern_type="ascending")

    def get_falling_down_stairs_summary(
        self, patterns: pl.DataFrame | None = None
    ) -> pl.DataFrame:
        """Get a summary of falling down stairs patterns with one row per ticker showing their most recent pattern.

        For tickers with multiple patterns, shows the most recent one (by pattern_end_date).

        Args:
            patterns: Optional precomputed patterns DataFrame. If None, will compute patterns.

        Returns:
            pl.DataFrame: Summary with ticker, step count, dates, and losses.
        """
        # Get all patterns if not provided
        if patterns is None:
            logger.info("Patterns not provided, computing from scratch")
            patterns = self.get_falling_down_stairs_hvcs(min_steps=3)
        else:
            logger.info("Using precomputed patterns (optimization)")

        return self._get_pattern_summary(patterns, pattern_type="descending")

    def get_weekly_high_volume_closes(self) -> pl.DataFrame:
        """Get weeks with unusually high trading volume from weekly Delta table.

        Only includes data from the past 5 years to focus on recent patterns.

        Returns:
            pl.DataFrame: DataFrame with ticker, date, close, volume, and volume_avg_ratio.
        """
        logger.info("Extracting high volume closes from weekly data (past 5 years)")

        # Calculate 5 years ago from today
        five_years_ago = date.today() - timedelta(days=5 * 365)

        # Read weekly data and join with ticker details for ticker_type
        weekly_df = scan_delta_table("weekly").collect()
        ticker_details = scan_delta_table("tickers").select(["ticker", "ticker_type"]).collect()

        weekly_df = weekly_df.join(ticker_details, on="ticker", how="left")

        # Calculate 20-period volume average for weekly data
        weekly_with_volume_ratio = (
            weekly_df.sort(["ticker", "date"])
            .with_columns(
                pl.col("volume")
                .rolling_mean(window_size=20)
                .shift(1)  # Use previous 20 weeks, not including current week
                .over("ticker")
                .cast(pl.UInt64)
                .alias("volume_avg")
            )
            .with_columns(
                pl.when(pl.col("volume_avg").is_not_null())
                .then(pl.col("volume") / pl.col("volume_avg"))
                .otherwise(None)
                .alias("volume_avg_ratio")
            )
        )

        df = (
            weekly_with_volume_ratio.filter(
                pl.col("date") >= five_years_ago,  # Only past 5 years
                pl.col("volume_avg_ratio") >= 3,
                (
                    (
                        (pl.col("ticker_type") == "CS")
                        & (pl.col("volume_avg") >= 200000)
                        & (pl.col("close") >= 5)
                    )
                    | (
                        (pl.col("ticker_type") == "ETF")
                        & (pl.col("volume_avg") >= 50000)
                    )
                ),
            )
            .sort(["date", "ticker"], descending=[True, False])
        )

        return df

    def get_monthly_high_volume_closes(self) -> pl.DataFrame:
        """Get months with unusually high trading volume from monthly Delta table.

        Only includes data from the past 5 years to focus on recent patterns.

        Returns:
            pl.DataFrame: DataFrame with ticker, date, close, volume, and volume_avg_ratio.
        """
        logger.info("Extracting high volume closes from monthly data (past 5 years)")

        # Calculate 5 years ago from today
        five_years_ago = date.today() - timedelta(days=5 * 365)

        # Read monthly data and join with ticker details for ticker_type
        monthly_df = scan_delta_table("monthly").collect()
        ticker_details = scan_delta_table("tickers").select(["ticker", "ticker_type"]).collect()

        monthly_df = monthly_df.join(ticker_details, on="ticker", how="left")

        # Calculate 20-period volume average for monthly data
        monthly_with_volume_ratio = (
            monthly_df.sort(["ticker", "date"])
            .with_columns(
                pl.col("volume")
                .rolling_mean(window_size=20)
                .shift(1)  # Use previous 20 months, not including current month
                .over("ticker")
                .cast(pl.UInt64)
                .alias("volume_avg")
            )
            .with_columns(
                pl.when(pl.col("volume_avg").is_not_null())
                .then(pl.col("volume") / pl.col("volume_avg"))
                .otherwise(None)
                .alias("volume_avg_ratio")
            )
        )

        df = (
            monthly_with_volume_ratio.filter(
                pl.col("date") >= five_years_ago,  # Only past 5 years
                pl.col("volume_avg_ratio") >= 3,
                (
                    (
                        (pl.col("ticker_type") == "CS")
                        & (pl.col("volume_avg") >= 200000)
                        & (pl.col("close") >= 5)
                    )
                    | (
                        (pl.col("ticker_type") == "ETF")
                        & (pl.col("volume_avg") >= 50000)
                    )
                ),
            )
            .sort(["date", "ticker"], descending=[True, False])
        )

        return df

    def run(self) -> None:
        """Execute gold layer analytics and export to SQLite.

        Optimized to minimize Delta table scans:
        - Scans Delta table once for daily HVCs
        - Scans Delta table once for weekly HVCs
        - Scans Delta table once for monthly HVCs
        - Scans Delta table once for latest prices
        - Reuses these DataFrames for all downstream analytics
        """
        # SCAN 1: Get all daily HVCs (used by multiple downstream operations)
        logger.info("Starting daily high volume closes extraction")
        hvcs_raw = self.get_high_volume_closes()

        # Prepare daily HVCs for database export
        hvcs = hvcs_raw.with_columns(
            pl.col("close").round(2).alias("close"),
            pl.col("volume_avg_ratio").round(2).alias("volume_avg_ratio")
        ).select([
            "date",
            "ticker",
            "ticker_type",
            "close",
            "volume_avg_ratio",
            "volume",
            "volume_avg",
        ])

        # Validate schema before writing
        hvcs = validate_high_volume_closes(hvcs)

        logger.info(f"Extracted {hvcs.height} daily high volume close records")

        logger.info("Writing daily high volume closes to SQLite database")
        hvcs.filter(pl.col("ticker_type") == "CS").drop("ticker_type").write_database(
            table_name="daily_high_volume_closes_stocks",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )
        hvcs.filter(pl.col("ticker_type") == "ETF").drop("ticker_type").write_database(
            table_name="daily_high_volume_closes_etfs",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )

        # SCAN 2: Get all weekly HVCs
        logger.info("Starting weekly high volume closes extraction")
        weekly_hvcs_raw = self.get_weekly_high_volume_closes()

        # Prepare weekly HVCs for database export
        weekly_hvcs = weekly_hvcs_raw.with_columns(
            pl.col("close").round(2).alias("close"),
            pl.col("volume_avg_ratio").round(2).alias("volume_avg_ratio")
        ).select([
            "date",
            "ticker",
            "ticker_type",
            "close",
            "volume_avg_ratio",
            "volume",
            "volume_avg",
        ])

        logger.info(f"Extracted {weekly_hvcs.height} weekly high volume close records")

        logger.info("Writing weekly high volume closes to SQLite database")
        weekly_hvcs.filter(pl.col("ticker_type") == "CS").drop("ticker_type").write_database(
            table_name="weekly_high_volume_closes_stocks",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )
        weekly_hvcs.filter(pl.col("ticker_type") == "ETF").drop("ticker_type").write_database(
            table_name="weekly_high_volume_closes_etfs",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )

        # SCAN 3: Get all monthly HVCs
        logger.info("Starting monthly high volume closes extraction")
        monthly_hvcs_raw = self.get_monthly_high_volume_closes()

        # Prepare monthly HVCs for database export
        monthly_hvcs = monthly_hvcs_raw.with_columns(
            pl.col("close").round(2).alias("close"),
            pl.col("volume_avg_ratio").round(2).alias("volume_avg_ratio")
        ).select([
            "date",
            "ticker",
            "ticker_type",
            "close",
            "volume_avg_ratio",
            "volume",
            "volume_avg",
        ])

        logger.info(f"Extracted {monthly_hvcs.height} monthly high volume close records")

        logger.info("Writing monthly high volume closes to SQLite database")
        monthly_hvcs.filter(pl.col("ticker_type") == "CS").drop("ticker_type").write_database(
            table_name="monthly_high_volume_closes_stocks",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )
        monthly_hvcs.filter(pl.col("ticker_type") == "ETF").drop("ticker_type").write_database(
            table_name="monthly_high_volume_closes_etfs",
            connection="sqlite:///hvcs.db",
            if_table_exists="replace",
        )

        # SCAN 4: Get latest prices (used by stairstepping analysis)
        latest_prices = self.get_latest_closing_prices()

        # Compute stair-stepping patterns (reusing hvcs_raw and latest_prices)
        logger.info("Starting stair-stepping HVC pattern analysis")
        stairstepping = self.get_stairstepping_hvcs(
            min_steps=3, hvcs_df=hvcs_raw, latest_prices=latest_prices
        )

        if stairstepping.height > 0:
            # Extract and write stair-stepping summary (reusing stairstepping patterns)
            logger.info("Creating stair-stepping summary")
            summary = self.get_stairstepping_summary(patterns=stairstepping)

            if summary.height > 0:
                logger.info(
                    f"Writing {summary.height} tickers to stair-stepping summary tables"
                )
                summary.filter(pl.col("ticker_type") == "CS").drop(
                    "ticker_type"
                ).write_database(
                    table_name="stairstepping_summary_stocks",
                    connection="sqlite:///hvcs.db",
                    if_table_exists="replace",
                )
                summary.filter(pl.col("ticker_type") == "ETF").drop(
                    "ticker_type"
                ).write_database(
                    table_name="stairstepping_summary_etfs",
                    connection="sqlite:///hvcs.db",
                    if_table_exists="replace",
                )
            else:
                logger.info("No stair-stepping patterns to summarize")
        else:
            logger.info("No stair-stepping patterns found")

        # Compute falling down stairs patterns (reusing hvcs_raw and latest_prices)
        logger.info("Starting falling down stairs HVC pattern analysis")
        falling_patterns = self.get_falling_down_stairs_hvcs(
            min_steps=3, hvcs_df=hvcs_raw, latest_prices=latest_prices
        )

        if falling_patterns.height > 0:
            # Extract and write falling down stairs summary (reusing falling patterns)
            logger.info("Creating falling down stairs summary")
            falling_summary = self.get_falling_down_stairs_summary(patterns=falling_patterns)

            if falling_summary.height > 0:
                logger.info(
                    f"Writing {falling_summary.height} tickers to falling down stairs summary tables"
                )
                falling_summary.filter(pl.col("ticker_type") == "CS").drop(
                    "ticker_type"
                ).write_database(
                    table_name="falling_down_stairs_summary_stocks",
                    connection="sqlite:///hvcs.db",
                    if_table_exists="replace",
                )
                falling_summary.filter(pl.col("ticker_type") == "ETF").drop(
                    "ticker_type"
                ).write_database(
                    table_name="falling_down_stairs_summary_etfs",
                    connection="sqlite:///hvcs.db",
                    if_table_exists="replace",
                )
            else:
                logger.info("No falling down stairs patterns to summarize")
        else:
            logger.info("No falling down stairs patterns found")


def main():
    """Execute gold layer analytics pipeline."""
    gold = GoldLayer()
    gold.run()


if __name__ == "__main__":  # pragma: no cover
    main()
