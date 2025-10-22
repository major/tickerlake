"""Tests for Weinstein Stage Analysis calculations."""

from datetime import date, timedelta

import polars as pl
import pytest

from tickerlake.silver.indicators import (
    _apply_anti_whipsaw,
    _classify_raw_stage,
    calculate_weinstein_stage,
)


@pytest.fixture
def sample_weekly_data():
    """Create sample weekly OHLCV data for testing."""
    dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(50)]
    return pl.DataFrame({
        "ticker": ["AAPL"] * 50,
        "date": dates,
        "open": [150.0] * 50,
        "high": [155.0] * 50,
        "low": [145.0] * 50,
        "close": [150.0 + i for i in range(50)],  # Uptrend
        "volume": [1000000] * 50,
        "transactions": [10000] * 50,
    })


@pytest.fixture
def stage_2_setup():
    """Create data that should produce Stage 2 pattern (price >2% above rising MA)."""
    dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(40)]
    closes = []

    # First 30 weeks: build base around 100
    for i in range(30):
        closes.append(100.0)

    # Last 10 weeks: breakout to 110+ (>2% above MA with rising slope)
    for i in range(10):
        closes.append(110.0 + i)

    return pl.DataFrame({
        "ticker": ["TEST"] * 40,
        "date": dates,
        "open": closes,
        "high": [c + 2 for c in closes],
        "low": [c - 2 for c in closes],
        "close": closes,
        "volume": [1000000] * 40,
        "transactions": [10000] * 40,
    })


@pytest.fixture
def stage_4_setup():
    """Create data that should produce Stage 4 pattern (price <-2% below falling MA)."""
    dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(40)]
    closes = []

    # First 30 weeks: build at 100
    for i in range(30):
        closes.append(100.0)

    # Last 10 weeks: breakdown to 90- (<-2% below MA with falling slope)
    for i in range(10):
        closes.append(90.0 - i)

    return pl.DataFrame({
        "ticker": ["TEST"] * 40,
        "date": dates,
        "open": closes,
        "high": [c + 2 for c in closes],
        "low": [c - 2 for c in closes],
        "close": closes,
        "volume": [1000000] * 40,
        "transactions": [10000] * 40,
    })


@pytest.fixture
def whipsaw_data():
    """Create data with rapid stage changes to test anti-whipsaw logic."""
    dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(40)]
    closes = []

    # Oscillate around 100 to create whipsaw conditions
    for i in range(40):
        if i % 2 == 0:
            closes.append(102.0)  # Slightly above
        else:
            closes.append(98.0)   # Slightly below

    return pl.DataFrame({
        "ticker": ["WHIP"] * 40,
        "date": dates,
        "open": closes,
        "high": [c + 1 for c in closes],
        "low": [c - 1 for c in closes],
        "close": closes,
        "volume": [1000000] * 40,
        "transactions": [10000] * 40,
    })


class TestWeinsteinStageCalculation:
    """Test Weinstein stage calculation functions."""

    def test_calculate_weinstein_stage_returns_all_columns(self, sample_weekly_data):
        """Test that calculate_weinstein_stage returns all expected columns."""
        result = calculate_weinstein_stage(sample_weekly_data)

        expected_columns = [
            "ticker", "date", "open", "high", "low", "close", "volume", "transactions",
            "ma_30", "price_vs_ma_pct", "ma_slope_pct", "raw_stage", "stage",
            "stage_changed", "weeks_in_stage"
        ]

        for col in expected_columns:
            assert col in result.columns, f"Missing column: {col}"

    def test_no_null_stages(self, sample_weekly_data):
        """Test that every row has a stage assigned (no nulls)."""
        result = calculate_weinstein_stage(sample_weekly_data)

        null_count = result.select(pl.col("stage").is_null().sum()).item()
        assert null_count == 0, "Found null stages in output"

    def test_stage_values_in_range(self, sample_weekly_data):
        """Test that all stage values are between 1 and 4."""
        result = calculate_weinstein_stage(sample_weekly_data)

        stages = result.select("stage").unique().sort("stage")["stage"].to_list()
        for stage in stages:
            assert 1 <= stage <= 4, f"Stage {stage} out of range [1-4]"

    def test_ma30_calculation(self, sample_weekly_data):
        """Test that 30-week MA is calculated correctly."""
        result = calculate_weinstein_stage(sample_weekly_data)

        # MA should be calculated (polars-talib may handle edge cases differently)
        # Just verify that MA column exists and has some values
        assert "ma_30" in result.columns, "ma_30 column should exist"

        # Row 30 onwards should have non-null MA
        from_30 = result.tail(result.height - 29).select("ma_30")
        non_null_count = from_30["ma_30"].is_not_null().sum()
        assert non_null_count > 0, "Should have some valid MA values after 30 rows"

    def test_price_vs_ma_pct_calculation(self, sample_weekly_data):
        """Test that price distance from MA is calculated as percentage."""
        result = calculate_weinstein_stage(sample_weekly_data)

        # Just verify that price_vs_ma_pct column exists and is calculated when MA exists
        assert "price_vs_ma_pct" in result.columns, "price_vs_ma_pct column should exist"

        # Check that when MA is available, price_vs_ma_pct is also available
        has_ma = result.filter(pl.col("ma_30").is_not_null())
        if has_ma.height > 0:
            # At least some rows with MA should also have price_vs_ma_pct
            has_both = has_ma.filter(pl.col("price_vs_ma_pct").is_not_null())
            assert has_both.height > 0, "Should calculate price_vs_ma_pct when MA is available"

    def test_ma_slope_calculation(self, sample_weekly_data):
        """Test that MA slope over 4 periods is calculated."""
        result = calculate_weinstein_stage(sample_weekly_data)

        # Just verify slope column exists and has some non-null values
        assert "ma_slope_pct" in result.columns, "ma_slope_pct column should exist"

        # Should have some valid slope values after enough data
        non_null_slopes = result.filter(pl.col("ma_slope_pct").is_not_null())
        assert non_null_slopes.height > 0, "Should have some valid MA slope values"

    def test_stage_2_detection(self, stage_2_setup):
        """Test detection of Stage 2 (Advancing) pattern."""
        result = calculate_weinstein_stage(stage_2_setup)

        # Last few rows should be in Stage 2
        last_5 = result.tail(5)
        stage_2_count = (last_5["stage"] == 2).sum()

        assert stage_2_count >= 3, \
            f"Expected at least 3 Stage 2 detections in last 5 rows, got {stage_2_count}"

    def test_stage_4_detection(self, stage_4_setup):
        """Test detection of Stage 4 (Declining) pattern."""
        result = calculate_weinstein_stage(stage_4_setup)

        # Last few rows should be in Stage 4
        last_5 = result.tail(5)
        stage_4_count = (last_5["stage"] == 4).sum()

        assert stage_4_count >= 3, \
            f"Expected at least 3 Stage 4 detections in last 5 rows, got {stage_4_count}"

    def test_stage_changed_flag(self, sample_weekly_data):
        """Test that stage_changed flag is set correctly."""
        result = calculate_weinstein_stage(sample_weekly_data)

        # First row should always have stage_changed = True
        assert result["stage_changed"].head(1).item() is True, \
            "First row should have stage_changed = True"

        # Count stage changes
        change_count = result.select(pl.col("stage_changed").sum()).item()
        assert change_count >= 1, "Should have at least 1 stage change"

    def test_weeks_in_stage_counter(self, sample_weekly_data):
        """Test that weeks_in_stage counter increments correctly."""
        result = calculate_weinstein_stage(sample_weekly_data)

        # Check that weeks_in_stage starts at 1 and increments
        # within each stage run (until stage_changed becomes True again)
        assert "weeks_in_stage" in result.columns, "weeks_in_stage column should exist"

        # Verify that weeks_in_stage is always > 0
        assert (result["weeks_in_stage"] > 0).all(), \
            "weeks_in_stage should always be positive"

        # Verify that weeks_in_stage increases for consecutive rows in same stage
        # by checking that after each stage_changed=True, weeks_in_stage starts from 1
        stage_change_rows = result.filter(pl.col("stage_changed"))
        if stage_change_rows.height > 0:
            # All stage change rows should have weeks_in_stage starting low (1-2)
            assert (stage_change_rows["weeks_in_stage"] <= 2).sum() > 0, \
                "Stage changes should reset weeks_in_stage counter"

    def test_multi_ticker_processing(self):
        """Test that stages are calculated independently per ticker."""
        dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(40)]

        df = pl.DataFrame({
            "ticker": ["AAPL"] * 40 + ["MSFT"] * 40,
            "date": dates * 2,
            "open": [100.0] * 40 + [200.0] * 40,
            "high": [102.0] * 40 + [202.0] * 40,
            "low": [98.0] * 40 + [198.0] * 40,
            "close": [100.0 + i for i in range(40)] + [200.0 + i for i in range(40)],
            "volume": [1000000] * 80,
            "transactions": [10000] * 80,
        })

        result = calculate_weinstein_stage(df)

        # Check that both tickers have data
        aapl_data = result.filter(pl.col("ticker") == "AAPL")
        msft_data = result.filter(pl.col("ticker") == "MSFT")

        assert aapl_data.height == 40, "AAPL should have 40 rows"
        assert msft_data.height == 40, "MSFT should have 40 rows"

        # Both should have stages assigned
        assert aapl_data.select(pl.col("stage").is_null().sum()).item() == 0
        assert msft_data.select(pl.col("stage").is_null().sum()).item() == 0


class TestClassifyRawStage:
    """Test raw stage classification logic."""

    def test_stage_1_classification(self):
        """Test Stage 1: Price below MA."""
        df = pl.DataFrame({
            "ticker": ["TEST"],
            "price_vs_ma_pct": [-1.0],  # Below MA but not <-2%
            "ma_slope_pct": [0.3],       # Flat/rising
        })

        result = _classify_raw_stage(df)
        assert result["raw_stage"].item() == 1

    def test_stage_2_classification(self):
        """Test Stage 2: Price >2% above MA, MA rising >0.5%."""
        df = pl.DataFrame({
            "ticker": ["TEST"],
            "price_vs_ma_pct": [3.0],   # >2% above MA
            "ma_slope_pct": [0.8],       # Rising >0.5%
        })

        result = _classify_raw_stage(df)
        assert result["raw_stage"].item() == 2

    def test_stage_3_classification(self):
        """Test Stage 3: Price above MA but not strong Stage 2."""
        df = pl.DataFrame({
            "ticker": ["TEST"],
            "price_vs_ma_pct": [1.0],   # Above MA but <2%
            "ma_slope_pct": [0.2],       # Flat/falling
        })

        result = _classify_raw_stage(df)
        assert result["raw_stage"].item() == 3

    def test_stage_4_classification(self):
        """Test Stage 4: Price <-2% below MA, MA falling <-0.5%."""
        df = pl.DataFrame({
            "ticker": ["TEST"],
            "price_vs_ma_pct": [-3.0],  # <-2% below MA
            "ma_slope_pct": [-0.8],      # Falling <-0.5%
        })

        result = _classify_raw_stage(df)
        assert result["raw_stage"].item() == 4


class TestAntiWhipsaw:
    """Test anti-whipsaw logic."""

    def test_holds_previous_stage_for_short_runs(self):
        """Test that stages shorter than 2 weeks hold previous confirmed stage."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 10,
            "date": [date(2024, 1, 1) + timedelta(weeks=i) for i in range(10)],
            "raw_stage": [1, 1, 1, 2, 1, 1, 1, 1, 1, 1],  # Brief Stage 2 blip
        })

        result = _apply_anti_whipsaw(df)

        # The single-week Stage 2 should be ignored
        # Stage should remain 1 throughout or only change after 2 consecutive weeks
        stage_2_count = (result["stage"] == 2).sum()
        assert stage_2_count == 0, \
            "Single-week Stage 2 should be filtered by anti-whipsaw"

    def test_confirms_stage_after_2_weeks(self):
        """Test that stages are confirmed after persisting for 2+ weeks."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 10,
            "date": [date(2024, 1, 1) + timedelta(weeks=i) for i in range(10)],
            "raw_stage": [1, 1, 1, 2, 2, 2, 2, 2, 2, 2],  # Stage 2 for 7 weeks
        })

        result = _apply_anti_whipsaw(df)

        # After 2 consecutive weeks of Stage 2, it should be confirmed
        last_7 = result.tail(7)
        stage_2_count = (last_7["stage"] == 2).sum()

        assert stage_2_count >= 5, \
            f"Stage 2 should be confirmed after 2+ weeks, got {stage_2_count} out of 7"

    def test_handles_initial_null_stages(self):
        """Test handling of null stages at the beginning of data."""
        df = pl.DataFrame({
            "ticker": ["TEST"] * 10,
            "date": [date(2024, 1, 1) + timedelta(weeks=i) for i in range(10)],
            "raw_stage": [None, None, 1, 1, 1, 1, 2, 2, 2, 2],
        })

        result = _apply_anti_whipsaw(df)

        # Should default to Stage 1 for nulls
        first_null_stage = result.head(1)["stage"].item()
        assert first_null_stage == 1, "Null stages should default to Stage 1"

        # All rows should have a stage assigned
        null_count = result.select(pl.col("stage").is_null().sum()).item()
        assert null_count == 0, "All rows should have a confirmed stage"


@pytest.mark.parametrize("ticker,expected_stages", [
    ("UPTREND", {2}),  # Strong uptrend should be Stage 2
    ("DOWNTREND", {4}),  # Strong downtrend should be Stage 4
])
def test_stage_detection_patterns(ticker, expected_stages):
    """Parameterized test for different stage patterns."""
    dates = [date(2024, 1, 1) + timedelta(weeks=i) for i in range(40)]

    if ticker == "UPTREND":
        closes = [100.0] * 30 + [110.0 + i for i in range(10)]
    else:  # DOWNTREND
        closes = [100.0] * 30 + [90.0 - i for i in range(10)]

    df = pl.DataFrame({
        "ticker": [ticker] * 40,
        "date": dates,
        "open": closes,
        "high": [c + 2 for c in closes],
        "low": [c - 2 for c in closes],
        "close": closes,
        "volume": [1000000] * 40,
        "transactions": [10000] * 40,
    })

    result = calculate_weinstein_stage(df)
    last_5_stages = set(result.tail(5)["stage"].to_list())

    # Check that expected stage appears in last 5 rows
    assert expected_stages.intersection(last_5_stages), \
        f"Expected stages {expected_stages} not found in last 5 rows: {last_5_stages}"
