"""Publication input and target acceptance guarantees."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import polars as pl
import pytest
from psycopg import sql

from tickerlake.extract import DAILY_AGGS_SCHEMA, TICKERS_SCHEMA
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.products import build_products
from tickerlake.postgres.publication import prepare_publication, publish_staged, stage_batch
from tickerlake.postgres.raw import store_daily_outcome
from tickerlake.postgres.reading import read_raw_history, read_split_history, read_ticker_batch
from tickerlake.postgres.references import store_ticker_outcome
from tickerlake.postgres.state import capture_run_inputs, start_run


def _start(connection, target: date):
    return start_run(
        connection,
        RunSpec(
            target=target,
            requested_start=None,
            requested_end=None,
            version="test",
        ),
    )


def _stage_one(connection, target: date, *, future: bool = False, prior_history: bool = True, name: str = "Test"):
    run_id = _start(connection, target)
    store_ticker_outcome(
        connection,
        FetchRequest(run_id=run_id, source="tickers", ticker_types=("CS", "ETF")),
        FetchOutcome(
            FetchStatus.populated,
            pl.DataFrame(
                [
                    {
                        "ticker": "TEST",
                        "name": name,
                        "type": "CS",
                        "primary_exchange": "X",
                        "cik": None,
                        "active": True,
                    },
                    {
                        "ticker": "INACTIVE",
                        "name": "Inactive",
                        "type": "CS",
                        "primary_exchange": "X",
                        "cik": None,
                        "active": False,
                    },
                    {
                        "ticker": "OUTSIDE",
                        "name": "Outside",
                        "type": "ETF",
                        "primary_exchange": "X",
                        "cik": None,
                        "active": True,
                    },
                ],
                schema=TICKERS_SCHEMA,
            ),
        ),
    )
    prior = date.fromordinal(target.toordinal() - 1)
    if prior_history:
        store_daily_outcome(
            connection,
            FetchRequest(run_id=run_id, source="daily", requested_date=prior),
            FetchOutcome(
                FetchStatus.populated,
                pl.DataFrame(
                    [
                        {
                            "date": prior,
                            "ticker": symbol,
                            "open": 9.0,
                            "high": 11.0,
                            "low": 8.0,
                            "close": 10.0,
                            "volume": 10.0,
                        }
                        for symbol in ("TEST", "INACTIVE", "OUTSIDE")
                    ],
                    schema=DAILY_AGGS_SCHEMA,
                ),
                prior,
            ),
        )
    if future:
        future_date = date(2024, 2, 1)
        store_daily_outcome(
            connection,
            FetchRequest(run_id=run_id, source="daily", requested_date=future_date),
            FetchOutcome(
                FetchStatus.populated,
                pl.DataFrame(
                    [
                        {
                            "date": future_date,
                            "ticker": "TEST",
                            "open": 11.0,
                            "high": 13.0,
                            "low": 10.0,
                            "close": 12.0,
                            "volume": 30.0,
                        }
                    ],
                    schema=DAILY_AGGS_SCHEMA,
                ),
                future_date,
            ),
        )
    store_daily_outcome(
        connection,
        FetchRequest(run_id=run_id, source="daily", requested_date=target),
        FetchOutcome(
            FetchStatus.populated,
            pl.DataFrame(
                [
                    {
                        "date": target,
                        "ticker": "TEST",
                        "open": 10.0,
                        "high": 12.0,
                        "low": 9.0,
                        "close": 11.0,
                        "volume": 20.0,
                    }
                ],
                schema=DAILY_AGGS_SCHEMA,
            ),
            target,
        ),
    )
    capture_run_inputs(connection, run_id)
    context = prepare_publication(connection, run_id, ticker_types=("CS",))
    identities = read_ticker_batch(connection)
    products = build_products(
        read_raw_history(connection, identities["ticker_id"].to_list()),
        read_split_history(connection, identities["ticker_id"].to_list()),
        identities,
        collection_start=context.retained_start or target,
        target=target,
    )
    stage_batch(connection, context, identities, products)
    return run_id, context


def test_prepare_requires_accepted_populated_target_session(pg_migrated_database) -> None:
    """A run cannot use an absent or empty target as evidence for publication."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id = _start(connection, date(2024, 1, 2))
        capture_run_inputs(connection, run_id)
        with pytest.raises(PostgresWriterError, match="accepted populated raw evidence"):
            prepare_publication(connection, run_id, ticker_types=("CS",))


def test_prepare_requires_run_and_cache_revision_to_match(pg_migrated_database) -> None:
    """A stale run snapshot is rejected before temporary stages are created."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id = _start(connection, date(2024, 1, 2))
        connection.execute("UPDATE ingest.cache_state SET input_revision=input_revision+1 WHERE cache_state_id=1")
        with pytest.raises(PostgresWriterError, match="revisions do not agree"):
            prepare_publication(connection, run_id, ticker_types=("CS",))


def test_invalid_staged_product_rolls_back_every_publication_write(pg_migrated_database) -> None:
    """A late validation failure leaves product tables and publication state unchanged."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id, context = _stage_one(connection, target)
        connection.execute("UPDATE pg_temp.publication_daily_stage SET close='NaN'::real")

        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context)

        for period in ("daily", "weekly", "monthly"):
            assert connection.execute(
                "SELECT count(*) FROM market.adjusted_bars WHERE period = %s", (period,)
            ).fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM market.latest_daily").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM market.publication_state").fetchone() == (0,)
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


def test_incomplete_scope_cannot_delete_existing_history(pg_migrated_database) -> None:
    """An unfinished identity scope never authorizes replacement or deletion."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id, context = _stage_one(connection, date(2024, 1, 2))
        ticker_id = connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol='TEST'").fetchone()[0]
        connection.execute(
            """INSERT INTO market.adjusted_bars
               (period,ticker_id,date,open,high,low,close,volume,left_truncated,calendar_closed)
               VALUES ('monthly',%s,'2024-01-03',1,1,1,1,0,false,false)""",
            (ticker_id,),
        )
        connection.execute("UPDATE pg_temp.publication_scope SET complete=false WHERE ticker_id=%s", (ticker_id,))
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='monthly' AND ticker_id=%s AND date='2024-01-03'",
            (ticker_id,),
        ).fetchone() == (1,)
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


@pytest.mark.parametrize(
    ("period", "period_key"),
    [("weekly", date(2024, 1, 1)), ("monthly", date(2024, 1, 1))],
)
def test_missing_period_product_is_rejected_without_removing_published_rows(
    pg_migrated_database, period: str, period_key: date
) -> None:
    """Period products must exactly reflect raw-derived dates before replacement."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        _first_run, first_context = _stage_one(connection, target)
        publish_staged(connection, first_context)
        stage = sql.Identifier(f"publication_{period}_stage")
        select = "SELECT * FROM market.adjusted_bars WHERE period = %s ORDER BY ticker_id,date"
        before = connection.execute(select, (period,)).fetchall()

        run_id, context = _stage_one(connection, target)
        connection.execute(sql.SQL("DELETE FROM pg_temp.{} WHERE date=%s").format(stage), (period_key,))
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context)

        assert connection.execute(select, (period,)).fetchall() == before
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


def test_reprepared_staging_context_cannot_publish_another_run(pg_migrated_database) -> None:
    """Preparing a new build binds its temporary stages to that build only."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        _first_run, context_a = _stage_one(connection, date(2024, 1, 2))
        _run_b, context_b = _stage_one(connection, date(2024, 1, 2))

        identities = read_ticker_batch(connection)
        products = build_products(
            read_raw_history(connection, identities["ticker_id"].to_list()),
            read_split_history(connection, identities["ticker_id"].to_list()),
            identities,
            collection_start=context_a.retained_start or context_a.target_session,
            target=context_a.target_session,
        )
        with pytest.raises(PostgresWriterError):
            stage_batch(connection, context_a, identities, products)
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context_a)
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, replace(context_b, target_session=date(2024, 1, 3)))


def test_tampered_publication_scope_rejects_publish_and_preserves_state(pg_migrated_database) -> None:
    """The publication rejects a tampered scope table without changing durable state."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        _first_run, first_context = _stage_one(connection, target)
        publish_staged(connection, first_context)
        run_id, context = _stage_one(connection, target, name="Legitimate refresh")
        before = (
            connection.execute("SELECT * FROM market.ticker ORDER BY ticker_id").fetchall(),
            tuple(
                connection.execute(
                    "SELECT * FROM market.adjusted_bars WHERE period = %s ORDER BY ticker_id, date", (kind,)
                ).fetchall()
                for kind in ("daily", "weekly", "monthly")
            ),
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
        )
        # Tamper: mark every staged ticker as NOT complete so the incomplete check fails.
        connection.execute("UPDATE pg_temp.publication_scope SET complete = false")
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context)
        after = (
            connection.execute("SELECT * FROM market.ticker ORDER BY ticker_id").fetchall(),
            tuple(
                connection.execute(
                    "SELECT * FROM market.adjusted_bars WHERE period = %s ORDER BY ticker_id, date", (kind,)
                ).fetchall()
                for kind in ("daily", "weekly", "monthly")
            ),
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
        )
        assert after == before
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


def test_new_validated_inputs_after_staging_reject_stale_generation(pg_migrated_database) -> None:
    """Accepted raw and reference writes invalidate a staged publication revision."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        _first_run, first_context = _stage_one(connection, target)
        publish_staged(connection, first_context)
        _stale_run, stale_context = _stage_one(connection, target)
        before = (
            connection.execute("SELECT * FROM market.adjusted_bars WHERE period='daily'").fetchall(),
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
        )

        revision_run = _start(connection, target)
        store_ticker_outcome(
            connection,
            FetchRequest(run_id=revision_run, source="tickers", ticker_types=("CS", "ETF")),
            FetchOutcome(
                FetchStatus.populated,
                pl.DataFrame(
                    [
                        {
                            "ticker": "TEST",
                            "name": "New input",
                            "type": "CS",
                            "primary_exchange": "X",
                            "cik": None,
                            "active": True,
                        }
                    ],
                    schema=TICKERS_SCHEMA,
                ),
            ),
        )
        store_daily_outcome(
            connection,
            FetchRequest(run_id=revision_run, source="daily", requested_date=target),
            FetchOutcome(
                FetchStatus.populated,
                pl.DataFrame(
                    [
                        {
                            "date": target,
                            "ticker": "TEST",
                            "open": 10.0,
                            "high": 13.0,
                            "low": 9.0,
                            "close": 12.0,
                            "volume": 25.0,
                        }
                    ],
                    schema=DAILY_AGGS_SCHEMA,
                ),
                target,
            ),
        )

        with pytest.raises(PostgresWriterError):
            publish_staged(connection, stale_context)

        assert (
            connection.execute("SELECT * FROM market.adjusted_bars WHERE period='daily'").fetchall(),
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
        ) == before


def test_late_failure_rolls_back_metadata_and_product_tables(pg_migrated_database) -> None:
    """A failed replacement preserves the complete previously published generation."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        first_run, first_context = _stage_one(connection, target)
        publish_staged(connection, first_context)
        run_id, context = _stage_one(connection, target, name="Changed")
        period_rows = tuple(
            connection.execute(
                "SELECT * FROM market.adjusted_bars WHERE period = %s ORDER BY ticker_id, date", (kind,)
            ).fetchall()
            for kind in ("daily", "weekly", "monthly")
        )
        before = (
            connection.execute("SELECT * FROM market.ticker ORDER BY ticker_id").fetchall(),
            period_rows,
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
            connection.execute("SELECT * FROM ingest.run ORDER BY started_at,run_id").fetchall(),
        )
        connection.execute(
            """CREATE FUNCTION pg_temp.reject_publication() RETURNS trigger LANGUAGE plpgsql AS $$
               BEGIN RAISE EXCEPTION 'injected publication failure'; END $$"""
        )
        connection.execute(
            "CREATE TRIGGER reject_publication BEFORE INSERT OR UPDATE ON market.publication_state "
            "FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_publication()"
        )
        with pytest.raises(PostgresWriterError):
            publish_staged(connection, context)
        after = (
            connection.execute("SELECT * FROM market.ticker ORDER BY ticker_id").fetchall(),
            tuple(
                connection.execute(
                    "SELECT * FROM market.adjusted_bars WHERE period = %s ORDER BY ticker_id, date", (kind,)
                ).fetchall()
                for kind in ("daily", "weekly", "monthly")
            ),
            connection.execute("SELECT * FROM market.latest_daily").fetchall(),
            connection.execute("SELECT * FROM market.publication_state").fetchall(),
            connection.execute("SELECT * FROM ingest.run ORDER BY started_at,run_id").fetchall(),
        )
        assert after == before
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (first_run,)).fetchone() == (
            "published",
        )
        assert connection.execute("SELECT state FROM ingest.run WHERE run_id=%s", (run_id,)).fetchone() == ("running",)


def test_period_keys_before_and_after_target_are_retained_and_obsolete_month_removed(pg_migrated_database) -> None:
    """Product keys can span period boundaries and stale monthly keys are deleted."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        _run_id, _context = _stage_one(connection, target, future=True, prior_history=False)
        ticker_id = connection.execute("SELECT ticker_id FROM market.ticker WHERE symbol='TEST'").fetchone()[0]
        connection.execute(
            """INSERT INTO market.adjusted_bars
               (period,ticker_id,date,open,high,low,close,volume,left_truncated,calendar_closed)
               VALUES ('monthly',%s,'2024-01-01',1,1,1,1,0,false,false),
                      ('monthly',%s,'2024-02-01',1,1,1,1,0,false,false),
                      ('monthly',%s,'2090-01-01',1,1,1,1,0,false,false)""",
            (ticker_id, ticker_id, ticker_id),
        )
        publish_staged(connection, _context)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='weekly' AND ticker_id=%s AND date='2024-01-01'",
            (ticker_id,),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='monthly' AND ticker_id=%s AND date='2024-02-01'",
            (ticker_id,),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='monthly' AND ticker_id=%s AND date='2024-01-03'",
            (ticker_id,),
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='monthly' AND ticker_id=%s AND date='2024-01-01'",
            (ticker_id,),
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='monthly' AND ticker_id=%s AND date='2090-01-01'",
            (ticker_id,),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM market.adjusted_bars WHERE period='daily' AND ticker_id=%s AND date='2024-02-01'",
            (ticker_id,),
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT date FROM market.latest_daily WHERE ticker_id=%s", (ticker_id,)
        ).fetchone() == (target,)


def test_latest_is_exact_target_eligible_projection_but_history_keeps_other_symbols(pg_migrated_database) -> None:
    """Only active requested-type target symbols appear in latest, while histories persist."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        connection.execute("INSERT INTO market.ticker(symbol) VALUES('UNKNOWN')")
        _run_id, context = _stage_one(connection, date(2024, 1, 2))
        connection.execute(
            """INSERT INTO market.latest_daily
               (ticker_id,date,open,high,low,close,volume)
               SELECT ticker_id,'2024-01-01',9,11,8,10,10 FROM market.ticker
               WHERE symbol IN ('INACTIVE','OUTSIDE','UNKNOWN')"""
        )
        publish_staged(connection, context)
        latest_symbols = connection.execute(
            "SELECT t.symbol FROM market.latest_daily l JOIN market.ticker t USING(ticker_id) ORDER BY t.symbol"
        ).fetchall()
        assert latest_symbols == [("TEST",)]
        assert connection.execute("SELECT active FROM market.ticker WHERE symbol='UNKNOWN'").fetchone() == (None,)
        histories = connection.execute(
            "SELECT t.symbol,count(*) FROM market.adjusted_bars d JOIN market.ticker t USING(ticker_id) "
            "WHERE d.period='daily' GROUP BY t.symbol ORDER BY t.symbol"
        ).fetchall()
        assert histories == [("INACTIVE", 1), ("OUTSIDE", 1), ("TEST", 2)]
