"""PostgreSQL migration integration tests."""

from __future__ import annotations

import hashlib
from importlib import resources

import psycopg
import pytest
from psycopg import sql

from tickerlake.postgres import migrations
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.migrations import apply_migrations


def test_apply_migrations_requires_live_locked_autocommit_connection(pg_owner_dsn: str) -> None:
    """Reject connections that do not own the writer lock."""
    with (
        psycopg.connect(pg_owner_dsn, autocommit=True) as connection,
        pytest.raises(PostgresWriterError, match="writer lock"),
    ):
        apply_migrations(connection)


def test_migrations_apply_repeat_and_ledger_checksums(pg_owner_dsn: str) -> None:
    """Apply packaged migrations once and preserve their checksums on repeat."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        before = connection.execute(
            "SELECT version, filename, checksum FROM ingest.schema_migration ORDER BY version"
        ).fetchall()
        apply_migrations(connection)
        after = connection.execute(
            "SELECT version, filename, checksum FROM ingest.schema_migration ORDER BY version"
        ).fetchall()
        assert after == before
        assert before == [
            (
                1,
                "0001_foundation.sql",
                hashlib.sha256(
                    resources.files("tickerlake.migrations").joinpath("0001_foundation.sql").read_bytes()
                ).hexdigest(),
            ),
            (
                2,
                "0002_publication.sql",
                hashlib.sha256(
                    resources.files("tickerlake.migrations").joinpath("0002_publication.sql").read_bytes()
                ).hexdigest(),
            ),
            (
                3,
                "0003_domains.sql",
                hashlib.sha256(
                    resources.files("tickerlake.migrations").joinpath("0003_domains.sql").read_bytes()
                ).hexdigest(),
            ),
        ]


def test_schema_contract_and_access_grants(pg_owner_dsn: str, pg_etl_dsn: str, pg_reader_dsn: str) -> None:
    """Check the foundation columns and least-privilege role access."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'ingest' AND table_name = 'raw_daily' ORDER BY ordinal_position"
        ).fetchall() == [
            ("date", "date", "NO"),
            ("ticker_id", "integer", "NO"),
            ("open", "real", "NO"),
            ("high", "real", "NO"),
            ("low", "real", "NO"),
            ("close", "real", "NO"),
            ("volume", "double precision", "NO"),
        ]
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'ingest' AND table_name = 'fetch_manifest' "
            "AND column_name = 'requested_ticker_types'"
        ).fetchone() == ("requested_ticker_types", "ARRAY", "YES")
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'ingest' AND table_name = 'raw_session' ORDER BY ordinal_position"
        ).fetchall() == [
            ("date", "date", "NO"),
            ("input_revision", "bigint", "NO"),
            ("manifest_id", "bigint", "NO"),
            ("row_count", "bigint", "NO"),
        ]
        expected_public_columns = [
            ("ticker_id", "integer", "NO"),
            ("date", "date", "NO"),
            ("open", "real", "NO"),
            ("high", "real", "NO"),
            ("low", "real", "NO"),
            ("close", "real", "NO"),
            ("volume", "double precision", "NO"),
            ("sma_20", "real", "YES"),
            ("sma_50", "real", "YES"),
            ("sma_200", "real", "YES"),
            ("atr_14", "real", "YES"),
            ("atr_pct", "real", "YES"),
            ("adr_pct", "real", "YES"),
            ("volume_sma_20", "double precision", "YES"),
        ]
        for table in ("adjusted_daily", "adjusted_weekly", "adjusted_monthly", "latest_daily"):
            columns = connection.execute(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'market' AND table_name = %s ORDER BY ordinal_position",
                (table,),
            ).fetchall()
            expected = expected_public_columns.copy()
            if table in {"adjusted_weekly", "adjusted_monthly"}:
                expected.extend([("left_truncated", "boolean", "NO"), ("calendar_closed", "boolean", "NO")])
            assert columns == expected
        assert connection.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = 'market' AND table_name = 'publication_state' ORDER BY ordinal_position"
        ).fetchall() == [
            ("singleton", "boolean", "NO"),
            ("published_session", "date", "NO"),
            ("published_at", "timestamp with time zone", "NO"),
            ("run_id", "uuid", "NO"),
            ("ticker_count", "bigint", "NO"),
        ]
        assert connection.execute("SELECT count(*) FROM ingest.raw_session").fetchone() == (0,)
        assert (
            connection.execute(
                "SELECT conname FROM pg_constraint WHERE conrelid = 'market.publication_state'::regclass "
                "AND contype = 'p'"
            ).fetchone()
            is not None
        )
        assert (
            connection.execute(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'ingest' "
                "AND indexname = 'raw_daily_ticker_date_idx'"
            )
            .fetchone()[0]
            .endswith("(ticker_id, date)")
        )
        assert (
            connection.execute(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'ingest' AND indexname = 'split_event_ticker_idx'"
            )
            .fetchone()[0]
            .endswith("(ticker_id)")
        )
    with psycopg.connect(pg_etl_dsn, autocommit=True) as etl:
        assert etl.execute("SELECT count(*) FROM ingest.cache_state").fetchone() == (1,)
        etl.execute("CREATE TEMP TABLE migration_temp_check (value integer)")
        # Exercise DML privileges using a private ticker, then remove it.
        ticker = etl.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('grant-check') RETURNING ticker_id"
        ).fetchone()[0]
        etl.execute(
            "INSERT INTO market.adjusted_daily (ticker_id, date, open, high, low, close, volume) "
            "VALUES (%s, '2025-01-02', 1, 1, 1, 1, 0)",
            (ticker,),
        )
        etl.execute("UPDATE market.adjusted_daily SET close = 2, high = 2 WHERE ticker_id = %s", (ticker,))
        etl.execute("DELETE FROM market.adjusted_daily WHERE ticker_id = %s", (ticker,))
        etl.execute("DELETE FROM market.ticker WHERE ticker_id = %s", (ticker,))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
                ("0" * 64,),
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute("UPDATE ingest.schema_migration SET filename = 'bad.sql' WHERE version = 1")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            etl.execute("DELETE FROM ingest.schema_migration WHERE version = 1")
    with psycopg.connect(pg_reader_dsn, autocommit=True) as reader:
        assert reader.execute("SELECT count(*) FROM market.ticker").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.raw_daily")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("INSERT INTO market.ticker (symbol) VALUES ('reader-write')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(
                "INSERT INTO market.publication_state (singleton, published_session, published_at, run_id) "
                "VALUES (true, '2025-01-02', now(), '00000000-0000-0000-0000-000000000000')"
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.schema_migration")


def test_numeric_domains_and_ohlc_function_back_affected_columns(pg_owner_dsn: str) -> None:
    """The numeric domains and OHLC function replace per-column inline checks."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        assert connection.execute(
            "SELECT domain_name, data_type FROM information_schema.domains "
            "WHERE domain_schema = 'market' AND domain_name IN ('finite_real', 'finite_volume') "
            "ORDER BY domain_name"
        ).fetchall() == [("finite_real", "real"), ("finite_volume", "double precision")]
        assert connection.execute(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'market' AND p.proname = 'is_valid_ohlc'"
        ).fetchone() == (1,)
        expected = {
            "open": "finite_real",
            "high": "finite_real",
            "low": "finite_real",
            "close": "finite_real",
            "volume": "finite_volume",
        }
        for schema, table in (
            ("ingest", "raw_daily"),
            ("market", "adjusted_daily"),
            ("market", "adjusted_weekly"),
            ("market", "adjusted_monthly"),
            ("market", "latest_daily"),
        ):
            columns = connection.execute(
                "SELECT column_name, domain_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s AND column_name = ANY(%s)",
                (schema, table, list(expected)),
            ).fetchall()
            assert dict(columns) == expected, f"{schema}.{table} uses {columns}"


def test_migration_revokes_default_acl_grants(pg_owner_dsn: str, pg_etl_dsn: str, pg_reader_dsn: str) -> None:
    """Remove effective default grants before assigning the foundation ACLs."""
    with writer_connection(pg_owner_dsn) as connection:
        connection.execute("CREATE SCHEMA ingest")
        connection.execute("CREATE SCHEMA market")
        for schema in ("ingest", "market"):
            connection.execute(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL ON TABLES TO tickerlake_etl, tickerlake_reader"
            )
        apply_migrations(connection)
    with psycopg.connect(pg_etl_dsn, autocommit=True) as etl:
        for statement in (
            "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
            "UPDATE ingest.schema_migration SET filename = 'bad.sql' WHERE version = 1",
            "DELETE FROM ingest.schema_migration WHERE version = 1",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                etl.execute(statement, ("0" * 64,) if statement.startswith("INSERT") else None)
    with psycopg.connect(pg_reader_dsn, autocommit=True) as reader:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM ingest.raw_daily")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("INSERT INTO market.ticker (symbol) VALUES ('reader-write')")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(
                "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (99, 'bad.sql', %s)",
                ("0" * 64,),
            )


@pytest.mark.parametrize("files", [[], [("0001_empty.sql", b" \n\t")]])
def test_empty_migration_set_fails_before_schema_bootstrap(
    pg_owner_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    files: list[tuple[str, bytes]],
) -> None:
    """Reject empty packaged migration resources before bootstrapping schemas."""

    class Resource:
        def __init__(self, name: str, data: bytes) -> None:
            self.name = name
            self._data = data

        def read_bytes(self) -> bytes:
            return self._data

    class Directory:
        def iterdir(self) -> list[Resource]:
            return [Resource(name, data) for name, data in files]

    monkeypatch.setattr(migrations.resources, "files", lambda _: Directory())
    with writer_connection(pg_owner_dsn) as connection:
        with pytest.raises(PostgresWriterError, match="migration"):
            apply_migrations(connection)
        assert connection.execute("SELECT to_regnamespace('ingest')").fetchone() == (None,)
        assert connection.execute("SELECT to_regnamespace('market')").fetchone() == (None,)


def test_failing_migration_rolls_back_ddl_and_ledger_row(pg_owner_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Roll back a relation created by a migration that later fails."""

    class Resource:
        name = "0001_atomic_failure.sql"

        def read_bytes(self) -> bytes:
            return b"CREATE TABLE ingest.partial_migration (id integer); SELECT 1 / 0;"

    class Directory:
        def iterdir(self) -> tuple[Resource, ...]:
            return (Resource(),)

    monkeypatch.setattr(migrations.resources, "files", lambda _: Directory())
    with writer_connection(pg_owner_dsn) as connection:
        with pytest.raises(PostgresWriterError, match="migrations"):
            apply_migrations(connection)

        assert connection.execute("SELECT to_regclass('ingest.partial_migration')").fetchone() == (None,)
        ledger_exists = connection.execute("SELECT to_regclass('ingest.schema_migration')").fetchone()[0]
        if ledger_exists is not None:
            assert connection.execute("SELECT count(*) FROM ingest.schema_migration WHERE version = 1").fetchone() == (
                0,
            )


def test_corrupt_applied_checksum_and_unknown_version_are_rejected(pg_owner_dsn: str) -> None:
    """Reject checksum changes and unknown applied migration versions."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        connection.execute("UPDATE ingest.schema_migration SET checksum = repeat('0', 64) WHERE version = 1")
        with pytest.raises(PostgresWriterError, match="checksum"):
            apply_migrations(connection)
        connection.execute("UPDATE ingest.schema_migration SET checksum = %s WHERE version = 1", ("0" * 64,))
        connection.execute(
            "INSERT INTO ingest.schema_migration (version, filename, checksum) VALUES (4, '0004_unknown.sql', %s)",
            ("1" * 64,),
        )
        with pytest.raises(PostgresWriterError, match="unknown"):
            apply_migrations(connection)


def test_invalid_raw_values_and_split_nullsafe_natural_key_are_rejected(pg_owner_dsn: str) -> None:
    """Enforce raw-data constraints and the split null-safe natural key."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        ticker_id = connection.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('TST') RETURNING ticker_id"
        ).fetchone()[0]
        raw_insert = """INSERT INTO ingest.raw_daily
            (date, ticker_id, open, high, low, close, volume)
            VALUES ('2025-01-02', %s, 1, 1, 2, 1, 0)"""
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(raw_insert, (ticker_id,))
        split_insert = """INSERT INTO ingest.split_event
            (ticker_id, execution_date, split_from, split_to, adjustment_factor, adjustment_type)
            VALUES (%s, '2025-01-02', 1, 2, 2, NULL)"""
        connection.execute(split_insert, (ticker_id,))
        with pytest.raises(psycopg.errors.UniqueViolation):
            connection.execute(split_insert, (ticker_id,))


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("open", "'NaN'::real"),
        ("high", "'Infinity'::real"),
        ("volume", "-1::double precision"),
        ("sma_20", "'NaN'::real"),
        ("atr_pct", "'Infinity'::real"),
        ("volume_sma_20", "-1::double precision"),
    ],
)
def test_adjusted_products_reject_invalid_numeric_values(pg_owner_dsn: str, column: str, value: str) -> None:
    """Database constraints reject invalid bar and metric numbers in every product."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        ticker_id = connection.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('CHECK') RETURNING ticker_id"
        ).fetchone()[0]
        for table in ("adjusted_daily", "adjusted_weekly", "adjusted_monthly", "latest_daily"):
            columns = "ticker_id, date, open, high, low, close, volume"
            values = "%s, '2025-01-02', 9, 11, 8, 10, 1"
            if table in {"adjusted_weekly", "adjusted_monthly"}:
                columns += ", left_truncated, calendar_closed"
                values += ", false, false"
            connection.execute(
                sql.SQL("INSERT INTO market.{} ({}) VALUES ({})").format(
                    sql.Identifier(table), sql.SQL(columns), sql.SQL(values)
                ),
                (ticker_id,),
            )
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(
                    sql.SQL("UPDATE market.{} SET {} = {} WHERE ticker_id = %s").format(
                        sql.Identifier(table), sql.Identifier(column), sql.SQL(value)
                    ),
                    (ticker_id,),
                )
            connection.execute(
                sql.SQL("DELETE FROM market.{} WHERE ticker_id = %s").format(sql.Identifier(table)),
                (ticker_id,),
            )


@pytest.mark.parametrize("table", ["adjusted_daily", "adjusted_weekly", "adjusted_monthly", "latest_daily"])
def test_adjusted_products_reject_invalid_ohlc_relationships(pg_owner_dsn: str, table: str) -> None:
    """Each published table enforces high and low OHLC bounds."""
    with writer_connection(pg_owner_dsn) as connection:
        apply_migrations(connection)
        ticker_id = connection.execute(
            "INSERT INTO market.ticker (symbol) VALUES ('OHLC') RETURNING ticker_id"
        ).fetchone()[0]
        columns = "ticker_id, date, open, high, low, close, volume"
        values = "%s, '2025-01-02', 9, 11, 8, 10, 1"
        if table in {"adjusted_weekly", "adjusted_monthly"}:
            columns += ", left_truncated, calendar_closed"
            values += ", false, false"
        connection.execute(
            sql.SQL("INSERT INTO market.{} ({}) VALUES ({})").format(
                sql.Identifier(table), sql.SQL(columns), sql.SQL(values)
            ),
            (ticker_id,),
        )
        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                sql.SQL("UPDATE market.{} SET high = 9 WHERE ticker_id = %s").format(sql.Identifier(table)),
                (ticker_id,),
            )
