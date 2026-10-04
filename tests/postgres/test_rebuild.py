"""PostgreSQL rebuild persistence and bounded-batch equivalence."""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

import polars as pl
import pytest

from tickerlake.extract import DAILY_AGGS_SCHEMA, SPLITS_SCHEMA, TICKERS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.raw import store_daily_outcome
from tickerlake.postgres.rebuild import rebuild_cache
from tickerlake.postgres.references import store_split_outcome, store_ticker_outcome
from tickerlake.postgres.state import start_run

if TYPE_CHECKING:
    from uuid import UUID

    import psycopg

START = date(2024, 1, 2)
TARGET = date(2024, 4, 30)
TYPES = ("CS",)
WEEKDAY_COUNT = 5
ROLLING_WINDOW = 20


def _run(connection: psycopg.Connection) -> UUID:
    return start_run(
        connection,
        RunSpec(
            target=TARGET,
            requested_start=None,
            requested_end=None,
            code_version="test",
            schema_version="1",
            transform_version="test",
        ),
    )


def _seed(connection: psycopg.Connection) -> UUID:
    run_id = _run(connection)
    sessions = [
        START + timedelta(days=index)
        for index in range(120)
        if (START + timedelta(days=index)).weekday() < WEEKDAY_COUNT
    ]
    sessions.append(date(2024, 5, 1))
    for index, day in enumerate(sessions):
        bars = [
            {
                "date": day,
                "ticker": symbol,
                "open": 10.0 + index,
                "high": 12.0 + index,
                "low": 9.0 + index,
                "close": 11.0 + index,
                "volume": 20.5 + index,
                "transactions": 2**34 + index,
            }
            for symbol in ("ACTIVE", "INACTIVE", "UNKNOWN")
        ]
        request = FetchRequest(run_id=run_id, source="daily", requested_date=day)
        store_daily_outcome(
            connection, request, FetchOutcome(FetchStatus.populated, pl.DataFrame(bars, schema=DAILY_AGGS_SCHEMA), day)
        )
        if day == date(2024, 3, 1):
            splits = pl.DataFrame(
                [
                    {
                        "ticker": symbol,
                        "execution_date": day,
                        "split_from": 2.0,
                        "split_to": 1.0,
                        "adjustment_factor": 0.5,
                        "adjustment_type": "split",
                    }
                    for symbol in ("ACTIVE", "INACTIVE", "UNKNOWN")
                ],
                schema=SPLITS_SCHEMA,
            )
            store_split_outcome(
                connection,
                FetchRequest(
                    run_id=run_id,
                    source="splits",
                    requested_start=START,
                    requested_end=TARGET,
                ),
                FetchOutcome(FetchStatus.populated, splits),
            )
    references = pl.DataFrame(
        [
            {"ticker": "ACTIVE", "name": "Active", "type": "CS", "primary_exchange": "X", "cik": None, "active": True},
            {
                "ticker": "INACTIVE",
                "name": "Inactive",
                "type": "CS",
                "primary_exchange": "X",
                "cik": None,
                "active": False,
            },
        ],
        schema=TICKERS_SCHEMA,
    )
    store_ticker_outcome(
        connection,
        FetchRequest(run_id=run_id, source="tickers", ticker_types=TYPES),
        FetchOutcome(FetchStatus.populated, references),
    )
    return run_id


def _snapshot(connection: psycopg.Connection) -> tuple[list[tuple[object, ...]], ...]:
    return tuple(
        connection.execute(query).fetchall()
        for query in (
            "SELECT * FROM market.adjusted_daily ORDER BY ticker_id, date",
            "SELECT * FROM market.adjusted_weekly ORDER BY ticker_id, date",
            "SELECT * FROM market.adjusted_monthly ORDER BY ticker_id, date",
        )
    )


def test_rebuild_persists_golden_products_independent_of_batch_size(pg_migrated_database) -> None:
    """All durable identities, including inactive and unknown symbols, retain complete products."""
    outputs = []
    for batch_size in (1, 10):
        with writer_connection(pg_migrated_database.owner_dsn) as connection:
            run_id = _seed(connection)
            result = rebuild_cache(connection, run_id, ticker_types=TYPES, batch_size=batch_size)
            assert result.run_id == run_id
            snapshot = _snapshot(connection)
            outputs.append(snapshot)
            tickers = connection.execute(
                "SELECT t.ticker_id, t.symbol, t.active FROM market.ticker t ORDER BY t.symbol"
            ).fetchall()
            symbols = {symbol: (ticker_id, active) for ticker_id, symbol, active in tickers}
            assert set(symbols) == {"ACTIVE", "INACTIVE", "UNKNOWN"}
            counts = connection.execute(
                "SELECT t.symbol, count(*) FROM market.adjusted_daily d "
                "JOIN market.ticker t USING (ticker_id) GROUP BY t.symbol ORDER BY t.symbol"
            ).fetchall()
            assert counts == [(symbol, 87) for symbol in ("ACTIVE", "INACTIVE", "UNKNOWN")]
            assert connection.execute(
                "SELECT transactions FROM market.adjusted_daily WHERE ticker_id = %s AND date = %s",
                (symbols["ACTIVE"][0], START),
            ).fetchone() == (2**34,)
            assert connection.execute(
                "SELECT calendar_closed FROM market.adjusted_monthly WHERE ticker_id = %s AND date = %s",
                (symbols["ACTIVE"][0], date(2024, 5, 1)),
            ).fetchone() == (False,)
            assert connection.execute("SELECT count(*) FROM market.latest_daily").fetchone()[0] == 1
    assert outputs[0] == outputs[1]


def test_rebuild_persists_independent_numeric_goldens_for_each_frequency(pg_migrated_database) -> None:
    """Persisted products match independent split, aggregate, and metric arithmetic."""
    collection_start = date(2022, 1, 3)
    target = date(2023, 11, 30)
    split_date = date(2023, 1, 3)
    sessions = [
        collection_start + timedelta(days=offset)
        for offset in range((target - collection_start).days + 1)
        if (collection_start + timedelta(days=offset)).weekday() < WEEKDAY_COUNT
    ]
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id = start_run(
            connection,
            RunSpec(
                target=target,
                requested_start=collection_start,
                requested_end=target,
                code_version="test",
                schema_version="1",
                transform_version="test",
            ),
        )
        for day in sessions:
            before_split = day < split_date
            bar = {
                "date": day,
                "ticker": "GOLDEN",
                "open": 20.0 if before_split else 10.0,
                "high": 24.0 if before_split else 12.0,
                "low": 16.0 if before_split else 8.0,
                "close": 20.0 if before_split else 10.0,
                "volume": 50.25 if before_split else 100.5,
                "transactions": 2**34,
            }
            store_daily_outcome(
                connection,
                FetchRequest(run_id=run_id, source="daily", requested_date=day),
                FetchOutcome(
                    FetchStatus.populated,
                    pl.DataFrame([bar], schema=DAILY_AGGS_SCHEMA),
                    day,
                ),
            )
        split = pl.DataFrame(
            [
                {
                    "ticker": "GOLDEN",
                    "execution_date": split_date,
                    "split_from": 2.0,
                    "split_to": 1.0,
                    "adjustment_factor": 0.5,
                    "adjustment_type": "split",
                }
            ],
            schema=SPLITS_SCHEMA,
        )
        store_split_outcome(
            connection,
            FetchRequest(
                run_id=run_id,
                source="splits",
                requested_start=collection_start,
                requested_end=target,
            ),
            FetchOutcome(FetchStatus.populated, split),
        )
        reference = pl.DataFrame(
            [
                {
                    "ticker": "GOLDEN",
                    "name": "Golden",
                    "type": "CS",
                    "primary_exchange": "X",
                    "cik": None,
                    "active": True,
                }
            ],
            schema=TICKERS_SCHEMA,
        )
        store_ticker_outcome(
            connection,
            FetchRequest(run_id=run_id, source="tickers", ticker_types=TYPES),
            FetchOutcome(FetchStatus.populated, reference),
        )
        rebuild_cache(connection, run_id, ticker_types=TYPES, batch_size=1)

        ticker_id = connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol='GOLDEN'").fetchone()[0]
        daily = connection.execute(
            "SELECT date, open, high, low, close, volume, transactions, sma_20, sma_50, sma_200, "
            "atr_14, atr_pct, adr_pct, volume_sma_20 FROM market.adjusted_daily "
            "WHERE ticker_id=%s ORDER BY date",
            (ticker_id,),
        ).fetchall()
        assert [row[0] for row in daily] == sessions
        for row in daily:
            assert row[1:5] == pytest.approx((10.0, 12.0, 8.0, 10.0))
            assert row[5:7] == pytest.approx((100.5, 2**34))
        for column, warmup, value in (
            (7, 19, 10.0),
            (8, 49, 10.0),
            (9, 199, 10.0),
            (10, 13, 4.0),
            (11, 13, 0.4),
            (12, 19, 0.4),
            (13, 19, 100.5),
        ):
            assert all(row[column] is None for row in daily[:warmup])
            assert all(row[column] == pytest.approx(value) for row in daily[warmup:])

        grouped_by_week: dict[date, list[date]] = {}
        grouped_by_month: dict[date, list[date]] = {}
        for day in sessions:
            grouped_by_week.setdefault(day - timedelta(days=day.weekday()), []).append(day)
            grouped_by_month.setdefault(day.replace(day=1), []).append(day)
        for grouped, labels, query in (
            (
                grouped_by_week,
                sorted(grouped_by_week),
                (
                    "SELECT date, open, high, low, close, volume, transactions, sma_20, sma_50, "
                    "sma_200, atr_14, atr_pct, adr_pct, volume_sma_20 FROM market.adjusted_weekly "
                    "WHERE ticker_id=%s ORDER BY date"
                ),
            ),
            (
                grouped_by_month,
                [max(grouped_by_month[key]) for key in sorted(grouped_by_month)],
                (
                    "SELECT date, open, high, low, close, volume, transactions, sma_20, sma_50, "
                    "sma_200, atr_14, atr_pct, adr_pct, volume_sma_20 FROM market.adjusted_monthly "
                    "WHERE ticker_id=%s ORDER BY date"
                ),
            ),
        ):
            counts = [len(grouped[key]) for key in sorted(grouped)]
            period_rows = connection.execute(query, (ticker_id,)).fetchall()
            assert [row[0] for row in period_rows] == labels
            assert len(period_rows) > ROLLING_WINDOW
            for row, count in zip(period_rows, counts, strict=True):
                assert row[1:5] == pytest.approx((10.0, 12.0, 8.0, 10.0))
                assert row[5] == pytest.approx(100.5 * count)
                assert row[6] == 2**34 * count
            for column, warmup, value in (
                (7, 19, 10.0),
                (8, 49, 10.0),
                (9, 199, 10.0),
                (10, 13, 4.0),
                (11, 13, 0.4),
                (12, 19, 0.4),
            ):
                assert all(row[column] is None for row in period_rows[:warmup])
                assert all(row[column] == pytest.approx(value) for row in period_rows[warmup:])
            expected_volume_average = [
                100.5 * sum(counts[index - 19 : index + 1]) / 20 for index in range(19, len(counts))
            ]
            assert all(row[13] is None for row in period_rows[:19])
            assert [row[13] for row in period_rows[19:]] == pytest.approx(expected_volume_average)


def test_rebuild_publishes_zero_history_identity_without_fabricating_products(pg_migrated_database) -> None:
    """An accepted cache retains durable identities without creating synthetic bars."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        first_run = _seed(connection)
        rebuild_cache(connection, first_run, ticker_types=TYPES, batch_size=10)
        connection.execute("INSERT INTO market.ticker (symbol) VALUES ('NO_HISTORY')")
        run_id = _run(connection)
        rebuild_cache(connection, run_id, ticker_types=TYPES, batch_size=1)
        empty_id = connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol='NO_HISTORY'").fetchone()[0]
        assert all(empty_id not in {row[0] for row in period} for period in _snapshot(connection))
        assert connection.execute(
            "SELECT published_session, run_id, ticker_count FROM market.publication_state"
        ).fetchone() == (TARGET, run_id, 4)


def test_rebuild_rejects_empty_cache_without_changing_publication(pg_migrated_database) -> None:
    """A cache without accepted target data is rejected without publication state changes."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id = _run(connection)
        before = _snapshot(connection)
        old_state = connection.execute(
            "SELECT published_session, run_id, ticker_count FROM market.publication_state"
        ).fetchone()
        with pytest.raises(PostgresWriterError, match="Target session has no accepted populated raw evidence"):
            rebuild_cache(connection, run_id, ticker_types=TYPES, batch_size=1)
        assert _snapshot(connection) == before
        assert (
            connection.execute(
                "SELECT published_session, run_id, ticker_count FROM market.publication_state"
            ).fetchone()
            == old_state
        )
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


def test_rebuild_rejects_invalid_preconditions_before_capturing_inputs(pg_migrated_database) -> None:
    """Empty type scopes and open transactions cannot mutate captured run inputs."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id = _seed(connection)
        before = connection.execute("SELECT input_revision FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone()
        with pytest.raises(PostgresWriterError, match="Invalid ticker types"):
            rebuild_cache(connection, run_id, ticker_types=())
        assert (
            connection.execute("SELECT input_revision FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == before
        )

        with connection.transaction():
            revision_in_transaction = connection.execute(
                "SELECT input_revision FROM ingest.run WHERE run_id=%s", (run_id,)
            ).fetchone()
            with pytest.raises(PostgresWriterError, match="not idle"):
                rebuild_cache(connection, run_id, ticker_types=TYPES)
            assert (
                connection.execute("SELECT input_revision FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone()
                == revision_in_transaction
            )
