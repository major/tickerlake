"""Databento API client for TickerLake bronze layer."""

from datetime import date, datetime, timedelta

import databento as db
import polars as pl
import structlog

logger = structlog.get_logger()


class DatabentoClient:
    """Databento API client for TickerLake bronze layer.

    Handles data downloads from Databento Historical API with:
    - Automatic dataset selection based on date
    - Symbology mapping (instrument_id → ticker)
    - Schema transformation to TickerLake format
    - Cost tracking
    """

    def __init__(self, api_key: str, enable_cost_tracking: bool = True):
        """Initialize Databento client.

        Args:
            api_key: Databento API key.
            enable_cost_tracking: Whether to track actual costs using Databento API.
        """
        self.client = db.Historical(api_key)
        self.enable_cost_tracking = enable_cost_tracking
        self.session_cost_usd = 0.0  # Track actual costs reported by Databento API

    def get_stock_daily_ohlcv_bulk(
        self,
        start_date: str,
        end_date: str,
        dataset: str | None = None
    ) -> pl.DataFrame:
        """Download daily stock OHLCV for a date range from Databento (BULK).

        More efficient than downloading day-by-day. Downloads entire range in one API call.

        Automatically selects appropriate dataset based on date:
        - EQUS.SUMMARY for 2025+
        - XNYS.PILLAR for historical (2018-2024)

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive).
            end_date: End date in YYYY-MM-DD format (exclusive).
            dataset: Override dataset selection (optional).

        Returns:
            DataFrame with schema matching DailyAggregatesSchema:
            - ticker: str
            - date: date
            - o, h, l, c: float (open, high, low, close)
            - v: int (volume)
            - transactions: int
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Auto-select dataset based on start date if not specified
        if dataset is None:
            if start >= date(2025, 1, 1):
                dataset = "EQUS.SUMMARY"
            else:
                dataset = "XNYS.PILLAR"

        # Get cost estimate before downloading
        if self.enable_cost_tracking:
            try:
                cost_usd = self.client.metadata.get_cost(
                    dataset=dataset,
                    schema='ohlcv-1d',
                    symbols='ALL_SYMBOLS',
                    start=start,
                    end=end
                )
                self.session_cost_usd += cost_usd
                logger.info(f"Estimated cost for bulk stock download ({start_date} to {end_date}): ${cost_usd:.6f}")
            except Exception as e:
                logger.warning(f"Failed to get cost estimate: {e}")

        logger.info(f"Downloading bulk stock data from {start_date} to {end_date} ({dataset})")

        # Download data for entire range
        data = self.client.timeseries.get_range(
            dataset=dataset,
            schema='ohlcv-1d',
            symbols='ALL_SYMBOLS',
            start=start,
            end=end
        )

        # Request symbology mapping
        logger.debug("Requesting symbology mapping")
        symbology = data.request_symbology(self.client)
        data.insert_symbology_json(symbology)

        # Convert to pandas, then polars
        df_pandas = data.to_df()
        df = pl.from_pandas(df_pandas)

        logger.info(f"Downloaded {df.height} stock records for date range")

        # Transform to TickerLake schema (with date from ts_event)
        return self._transform_stock_schema_bulk(df)

    def get_stock_daily_ohlcv(
        self,
        date_str: str,
        dataset: str | None = None
    ) -> pl.DataFrame:
        """Download daily stock OHLCV from Databento for a single day.

        Note: For multiple days, use get_stock_daily_ohlcv_bulk() instead for better performance.

        Args:
            date_str: Trading day in YYYY-MM-DD format.
            dataset: Override dataset selection (optional).

        Returns:
            DataFrame with schema matching DailyAggregatesSchema.
        """
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        next_date = target_date + timedelta(days=1)

        # Use bulk method for single day
        return self.get_stock_daily_ohlcv_bulk(date_str, next_date.strftime("%Y-%m-%d"), dataset)

    def get_options_daily_ohlcv_bulk(
        self,
        start_date: str,
        end_date: str,
        underlyings: list[str] | None = None,
        dataset: str = "OPRA.PILLAR"
    ) -> pl.DataFrame:
        """Download daily options OHLCV for a date range from Databento (BULK).

        More efficient than downloading day-by-day. Can download multiple underlyings
        in a single API call.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive).
            end_date: End date in YYYY-MM-DD format (exclusive).
            underlyings: List of underlying tickers to filter (e.g., SP500).
                        If None, downloads ALL options (expensive!).
            dataset: Options dataset (default: OPRA.PILLAR).

        Returns:
            DataFrame with schema matching OptionsDailyAggregatesSchema.
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        if underlyings:
            logger.info(f"Downloading bulk options for {len(underlyings)} underlyings from {start_date} to {end_date}")

            # Get cost estimate
            if self.enable_cost_tracking:
                try:
                    cost_usd = self.client.metadata.get_cost(
                        dataset=dataset,
                        schema='ohlcv-1d',
                        symbols=underlyings,
                        start=start,
                        end=end
                    )
                    self.session_cost_usd += cost_usd
                    logger.info(f"Estimated cost for bulk options download: ${cost_usd:.6f}")
                except Exception as e:
                    logger.warning(f"Failed to get cost estimate: {e}")

            # Download all underlyings in one call
            data = self.client.timeseries.get_range(
                dataset=dataset,
                schema='ohlcv-1d',
                symbols=underlyings,
                start=start,
                end=end
            )

            symbology = data.request_symbology(self.client)
            data.insert_symbology_json(symbology)
            df_pandas = data.to_df()

            if len(df_pandas) == 0:
                logger.warning(f"No options data downloaded for date range")
                return self._get_empty_options_df_bulk()

            df = pl.from_pandas(df_pandas)
        else:
            logger.warning(f"Downloading ALL options for {start_date} to {end_date} (this may be expensive!)")

            # Get cost estimate
            if self.enable_cost_tracking:
                try:
                    cost_usd = self.client.metadata.get_cost(
                        dataset=dataset,
                        schema='ohlcv-1d',
                        symbols='ALL_SYMBOLS',
                        start=start,
                        end=end
                    )
                    self.session_cost_usd += cost_usd
                    logger.warning(f"Estimated cost for ALL options: ${cost_usd:.6f}")
                except Exception as e:
                    logger.warning(f"Failed to get cost estimate: {e}")

            # Download all options
            data = self.client.timeseries.get_range(
                dataset=dataset,
                schema='ohlcv-1d',
                symbols='ALL_SYMBOLS',
                start=start,
                end=end
            )

            symbology = data.request_symbology(self.client)
            data.insert_symbology_json(symbology)
            df_pandas = data.to_df()
            df = pl.from_pandas(df_pandas)

        logger.info(f"Downloaded {df.height} options contracts for date range")

        # Transform to TickerLake schema (with date from ts_event)
        return self._transform_options_schema_bulk(df)

    def get_options_daily_ohlcv(
        self,
        date_str: str,
        underlyings: list[str] | None = None,
        dataset: str = "OPRA.PILLAR"
    ) -> pl.DataFrame:
        """Download daily options OHLCV from Databento.

        Args:
            date_str: Trading day in YYYY-MM-DD format.
            underlyings: List of underlying tickers to filter (e.g., SP500).
                        If None, downloads ALL options (expensive!).
            dataset: Options dataset (default: OPRA.PILLAR).

        Returns:
            DataFrame with schema matching OptionsDailyAggregatesSchema:
            - ticker: str (option ticker, e.g., O:SPY251219P00500000)
            - underlying_ticker: str
            - expiration_date: date
            - option_type: str ("call" or "put")
            - strike_price: float
            - date: date
            - open, high, low, close: float
            - volume, transactions: int
        """
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        next_date = target_date + timedelta(days=1)

        if underlyings:
            logger.info(f"Downloading options for {len(underlyings)} underlyings on {date_str}")
            # Filtered approach (cost-effective)
            all_data = []
            for ticker in underlyings:
                try:
                    # Get cost estimate for this underlying
                    if self.enable_cost_tracking:
                        try:
                            cost_usd = self.client.metadata.get_cost(
                                dataset=dataset,
                                schema='ohlcv-1d',
                                symbols=[ticker],
                                start=target_date,
                                end=next_date
                            )
                            self.session_cost_usd += cost_usd
                            logger.debug(f"Estimated cost for {ticker} options: ${cost_usd:.6f}")
                        except Exception as e:
                            logger.debug(f"Failed to get cost estimate for {ticker}: {e}")

                    data = self.client.timeseries.get_range(
                        dataset=dataset,
                        schema='ohlcv-1d',
                        symbols=[ticker],
                        start=target_date,
                        end=next_date
                    )

                    symbology = data.request_symbology(self.client)
                    data.insert_symbology_json(symbology)
                    df_pandas = data.to_df()

                    if len(df_pandas) > 0:
                        df = pl.from_pandas(df_pandas)
                        all_data.append(df)
                except Exception as e:
                    logger.warning(f"Failed to download options for {ticker}: {e}")
                    continue

            if not all_data:
                logger.warning(f"No options data downloaded for {date_str}")
                return self._get_empty_options_df(date_str)

            df = pl.concat(all_data)
        else:
            logger.info(f"Downloading ALL options for {date_str} (this may be expensive!)")

            # Get cost estimate for ALL_SYMBOLS
            if self.enable_cost_tracking:
                try:
                    cost_usd = self.client.metadata.get_cost(
                        dataset=dataset,
                        schema='ohlcv-1d',
                        symbols='ALL_SYMBOLS',
                        start=target_date,
                        end=next_date
                    )
                    self.session_cost_usd += cost_usd
                    logger.warning(f"Estimated cost for ALL options: ${cost_usd:.6f}")
                except Exception as e:
                    logger.warning(f"Failed to get cost estimate: {e}")

            # Full market (expensive but comprehensive)
            data = self.client.timeseries.get_range(
                dataset=dataset,
                schema='ohlcv-1d',
                symbols='ALL_SYMBOLS',
                start=target_date,
                end=next_date
            )

            symbology = data.request_symbology(self.client)
            data.insert_symbology_json(symbology)
            df_pandas = data.to_df()
            df = pl.from_pandas(df_pandas)

        logger.info(f"Downloaded {df.height} options contracts for {date_str}")

        # Transform to TickerLake schema
        return self._transform_options_schema(df, date_str)

    def get_session_cost(self) -> float:
        """Get total cost for this session in USD.

        Returns:
            Total cost in USD for all API calls made during this session.

        """
        return self.session_cost_usd

    def log_cost_summary(self) -> None:
        """Log cost summary for this session."""
        if not self.enable_cost_tracking:
            return

        logger.info(f"Session cost: ${self.session_cost_usd:.6f}")

    def _transform_stock_schema_bulk(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform Databento stock schema to TickerLake DailyAggregatesSchema (bulk).

        For bulk downloads, extracts date from ts_event field.

        Databento fields → TickerLake fields:
        - symbol → ticker
        - ts_event → date
        - open/high/low/close/volume → o/h/l/c/v
        - Estimate transactions from volume
        """
        return df.select([
            pl.col("symbol").alias("ticker"),
            pl.col("ts_event").cast(pl.Date).alias("date"),
            pl.col("open").alias("o"),
            pl.col("high").alias("h"),
            pl.col("low").alias("l"),
            pl.col("close").alias("c"),
            pl.col("volume").alias("v"),
            # Estimate transactions from volume (rough heuristic: volume / 100)
            (pl.col("volume") / 100).cast(pl.Int64).alias("transactions")
        ]).sort("date", "ticker")

    def _transform_stock_schema(self, df: pl.DataFrame, date_str: str) -> pl.DataFrame:
        """Transform Databento stock schema to TickerLake DailyAggregatesSchema (single day).

        Databento fields → TickerLake fields:
        - symbol → ticker
        - open/high/low/close/volume → o/h/l/c/v
        - ts_event → date
        - Estimate transactions from volume (Databento may not provide)
        """
        return df.select([
            pl.col("symbol").alias("ticker"),
            pl.lit(date_str).str.to_date().alias("date"),
            pl.col("open").alias("o"),
            pl.col("high").alias("h"),
            pl.col("low").alias("l"),
            pl.col("close").alias("c"),
            pl.col("volume").alias("v"),
            # Estimate transactions from volume (rough heuristic: volume / 100)
            (pl.col("volume") / 100).cast(pl.Int64).alias("transactions")
        ]).sort("ticker")

    def _transform_options_schema(self, df: pl.DataFrame, date_str: str) -> pl.DataFrame:
        """Transform Databento options schema to TickerLake OptionsDailyAggregatesSchema.

        Parses option tickers and extracts:
        - underlying_ticker
        - expiration_date
        - option_type (call/put)
        - strike_price
        """
        # Note: Databento option ticker format may need parsing
        # This is a placeholder - actual implementation depends on Databento's format
        # For now, assume symbol contains the option ticker

        # Parse option ticker components from symbol
        # Format example: "O:SPY251219P00500000" → SPY, 2025-12-19, put, 500.00
        from tickerlake.utils import parse_option_ticker

        transformed = df.with_columns([
            # Apply parsing to extract components
            pl.col("symbol").alias("ticker"),
            pl.lit(date_str).str.to_date().alias("date"),
        ])

        # Parse each option ticker to extract components
        # This will be done row-by-row for now (can optimize later)
        parsed_data = []
        for row in transformed.iter_rows(named=True):
            ticker = row["ticker"]
            try:
                parsed = parse_option_ticker(ticker)
                parsed_data.append({
                    "ticker": ticker,
                    "underlying_ticker": parsed["underlying_ticker"],
                    "expiration_date": parsed["expiration_date"],
                    "option_type": parsed["option_type"],
                    "strike_price": parsed["strike_price"],
                    "date": row["date"],
                    "open": row.get("open", 0.0),
                    "high": row.get("high", 0.0),
                    "low": row.get("low", 0.0),
                    "close": row.get("close", 0.0),
                    "volume": row.get("volume", 0),
                    "transactions": row.get("volume", 0) // 100  # Estimate
                })
            except Exception as e:
                logger.warning(f"Failed to parse option ticker {ticker}: {e}")
                continue

        if not parsed_data:
            return self._get_empty_options_df(date_str)

        return pl.DataFrame(parsed_data)

    def _transform_options_schema_bulk(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform Databento options schema to TickerLake OptionsDailyAggregatesSchema (bulk).

        Parses option tickers and extracts date from ts_event.
        """
        from tickerlake.utils import parse_option_ticker

        # Parse each option ticker to extract components
        parsed_data = []
        for row in df.iter_rows(named=True):
            ticker = row["symbol"]
            try:
                parsed = parse_option_ticker(ticker)
                parsed_data.append({
                    "ticker": ticker,
                    "underlying_ticker": parsed["underlying_ticker"],
                    "expiration_date": parsed["expiration_date"],
                    "option_type": parsed["option_type"],
                    "strike_price": parsed["strike_price"],
                    "date": row["ts_event"].date() if hasattr(row["ts_event"], "date") else row["ts_event"],
                    "open": row.get("open", 0.0),
                    "high": row.get("high", 0.0),
                    "low": row.get("low", 0.0),
                    "close": row.get("close", 0.0),
                    "volume": row.get("volume", 0),
                    "transactions": row.get("volume", 0) // 100  # Estimate
                })
            except Exception as e:
                logger.debug(f"Failed to parse option ticker {ticker}: {e}")
                continue

        if not parsed_data:
            return self._get_empty_options_df_bulk()

        return pl.DataFrame(parsed_data).sort("date", "ticker")

    def _get_empty_options_df_bulk(self) -> pl.DataFrame:
        """Return empty DataFrame matching OptionsDailyAggregatesSchema (bulk)."""
        return pl.DataFrame({
            "ticker": [],
            "underlying_ticker": [],
            "expiration_date": [],
            "option_type": [],
            "strike_price": [],
            "date": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
            "transactions": []
        }, schema={
            "ticker": pl.String,
            "underlying_ticker": pl.String,
            "expiration_date": pl.Date,
            "option_type": pl.String,
            "strike_price": pl.Float64,
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "transactions": pl.Int64
        })

    def _get_empty_options_df(self, date_str: str) -> pl.DataFrame:
        """Return empty DataFrame matching OptionsDailyAggregatesSchema (single day)."""
        return pl.DataFrame({
            "ticker": [],
            "underlying_ticker": [],
            "expiration_date": [],
            "option_type": [],
            "strike_price": [],
            "date": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
            "transactions": []
        }, schema={
            "ticker": pl.String,
            "underlying_ticker": pl.String,
            "expiration_date": pl.Date,
            "option_type": pl.String,
            "strike_price": pl.Float64,
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "transactions": pl.Int64
        })
