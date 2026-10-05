"""Stage complete product batches and atomically publish a cache revision."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, LiteralString, NoReturn

import polars as pl
import psycopg
from psycopg import sql

from tickerlake.postgres._schema import (
    PERIOD_FLAG_NULL_CHECK,
    PERIOD_TRUNC_SQL,
    PRODUCT_COLUMNS,
    PUBLICATION_TABLES,
    STAGE_KINDS,
    STAGE_NAMES,
)
from tickerlake.postgres._validation import require_unique_nonempty_strings
from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection, writer_connection
from tickerlake.postgres.copying import copy_frame

if TYPE_CHECKING:
    import datetime
    from collections.abc import Sequence
    from uuid import UUID

    from tickerlake.postgres.products import ProductBatch

_STAGES: dict[str, str] = {kind: f"publication_{kind}_stage" for kind in STAGE_KINDS}
_STAGE_COLUMNS: dict[str, tuple[str, ...]] = dict.fromkeys(STAGE_KINDS, PRODUCT_COLUMNS)
_KEY_COLUMNS: tuple[str, ...] = ("period", "ticker_id", "date")
_SAFE = "Invalid PostgreSQL publication staging data"
_IDLE_REQUIRED = "PostgreSQL publication requires an idle writer connection"
_CONTEXT_MISMATCH = "PostgreSQL publication context does not match its staged build"
_TYPES_SEQUENCE = "Ticker types must be a sequence"
_TYPES_INVALID = "Ticker types must be unique nonempty strings"
_REVISION_MISMATCH = "PostgreSQL run and cache revisions do not agree"
_TARGET_UNACCEPTED = "Target session has no accepted populated raw evidence"
_PREPARE_FAILED = "Could not prepare PostgreSQL publication stages"
_STAGE_FAILED = "Could not stage PostgreSQL product batch"
_PUBLISH_FAILED = "Could not publish PostgreSQL products"
_PRODUCT_TYPES: dict[str, LiteralString] = {
    "period": "text",
    "ticker_id": "integer",
    "date": "date",
    "open": "real",
    "high": "real",
    "low": "real",
    "close": "real",
    "volume": "double precision",
    "left_truncated": "boolean",
    "calendar_closed": "boolean",
}


@dataclass(frozen=True, slots=True, kw_only=True)
class BuildContext:
    """Captured revision and retained scope bound to temporary writer stages."""

    run_id: UUID
    input_revision: int
    retained_start: datetime.date | None
    retained_end: datetime.date | None
    target_session: datetime.date
    ticker_types: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationResult:
    """Durable result of an atomically completed product publication."""

    run_id: UUID
    published_session: datetime.date
    input_revision: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PublicationResolution:
    """Whether a run published and whether it owns the current marker."""

    published: bool
    is_current: bool


class PublicationOutcomeUnknownError(PostgresWriterError):
    """The commit outcome cannot be determined from the failed connection."""

    def __init__(self, run_id: UUID) -> None:
        """Create an unknown-outcome error that identifies the affected run."""
        self.run_id = run_id
        super().__init__("PostgreSQL publication outcome is unknown")


def _fail() -> NoReturn:
    raise PostgresWriterError(_SAFE)


def _require_idle_writer(connection: psycopg.Connection) -> None:
    require_writer_connection(connection)
    if connection.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise PostgresWriterError(_IDLE_REQUIRED)


def _verify_staged_context(connection: psycopg.Connection, context: BuildContext) -> None:
    try:
        row = connection.execute(
            """SELECT run_id,input_revision,retained_start,retained_end,target_session,ticker_types
               FROM pg_temp.publication_context WHERE context_id=1"""
        ).fetchone()
    except psycopg.Error:
        raise PostgresWriterError(_CONTEXT_MISMATCH) from None
    if (
        row is None
        or row[0] != context.run_id
        or row[1] != context.input_revision
        or row[2] != context.retained_start
        or row[3] != context.retained_end
        or row[4] != context.target_session
        or tuple(row[5]) != context.ticker_types
    ):
        raise PostgresWriterError(_CONTEXT_MISMATCH)


def prepare_publication(
    connection: psycopg.Connection,
    run_id: UUID,
    *,
    ticker_types: Sequence[str],
) -> BuildContext:
    """Capture run/cache inputs and create reusable transaction-preserving stages."""
    _require_idle_writer(connection)
    if isinstance(ticker_types, (str, bytes)):
        raise PostgresWriterError(_TYPES_SEQUENCE)
    types = require_unique_nonempty_strings(
        ticker_types,
        field="ticker types",
        message=_TYPES_INVALID,
    )
    row = connection.execute(
        """SELECT r.target_date, r.input_revision, c.input_revision, c.retained_start, c.retained_end
           FROM ingest.run r CROSS JOIN ingest.cache_state c
           WHERE r.run_id = %s AND r.state = 'running' AND c.cache_state_id = 1""",
        (run_id,),
    ).fetchone()
    if row is None or row[1] is None or row[1] != row[2]:
        raise PostgresWriterError(_REVISION_MISMATCH)
    target, revision, _, retained_start, retained_end = row
    accepted = connection.execute(
        """SELECT 1 FROM ingest.raw_session s
           JOIN ingest.fetch_manifest m USING (manifest_id)
           WHERE s.date = %s
             AND m.source = 'daily' AND m.status = 'populated'
             AND m.requested_date = s.date""",
        (target,),
    ).fetchone()
    if accepted is None:
        raise PostgresWriterError(_TARGET_UNACCEPTED)
    try:
        for name in (*STAGE_NAMES, "publication_ticker_stage", "publication_scope", "publication_context"):
            connection.execute(sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(sql.Identifier(name)))
        connection.execute(
            """CREATE TEMP TABLE publication_ticker_stage (
                   ticker_id integer PRIMARY KEY, symbol text NOT NULL UNIQUE,
                   name text, ticker_type text, primary_exchange text, cik text, active boolean
               ) ON COMMIT PRESERVE ROWS"""
        )
        connection.execute(
            """INSERT INTO pg_temp.publication_ticker_stage
               (ticker_id,symbol,name,ticker_type,primary_exchange,cik,active)
               SELECT t.ticker_id,t.symbol,r.name,r.ticker_type,r.primary_exchange,r.cik,r.active
               FROM market.ticker t LEFT JOIN ingest.ticker_reference r USING(ticker_id)
               ORDER BY t.ticker_id"""
        )
        connection.execute(
            "CREATE TEMP TABLE publication_scope (ticker_id integer PRIMARY KEY, complete boolean NOT NULL) "
            "ON COMMIT PRESERVE ROWS"
        )
        connection.execute(
            """CREATE TEMP TABLE publication_context (
                   context_id integer PRIMARY KEY DEFAULT 1 CHECK (context_id = 1),
                   run_id uuid NOT NULL, input_revision bigint NOT NULL,
                   retained_start date, retained_end date, target_session date NOT NULL,
                   ticker_types text[] NOT NULL
               ) ON COMMIT PRESERVE ROWS"""
        )
        connection.execute(
            """INSERT INTO pg_temp.publication_context
               (context_id,run_id,input_revision,retained_start,retained_end,target_session,ticker_types)
               VALUES (1,%s,%s,%s,%s,%s,%s)""",
            (run_id, revision, retained_start, retained_end, target, list(types)),
        )
        for kind, name in _STAGES.items():
            columns = _STAGE_COLUMNS[kind]
            definitions = sql.SQL(", ").join(
                sql.SQL("{} {}{} ").format(
                    sql.Identifier(column),
                    sql.SQL(_PRODUCT_TYPES[column]),
                    sql.SQL(" NOT NULL") if column in {"period", "ticker_id", "date"} else sql.SQL(""),
                )
                for column in columns
            )
            connection.execute(
                sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT PRESERVE ROWS").format(sql.Identifier(name), definitions)
            )
        return BuildContext(
            run_id=run_id,
            input_revision=revision,
            retained_start=retained_start,
            retained_end=retained_end,
            target_session=target,
            ticker_types=tuple(types),
        )
    except psycopg.Error:
        raise PostgresWriterError(_PREPARE_FAILED) from None


def stage_batch(
    connection: psycopg.Connection,
    context: BuildContext,
    identities: pl.DataFrame,
    products: ProductBatch,
) -> None:
    """Copy one bounded identity and complete-history product batch into stages."""
    _require_idle_writer(connection)
    _verify_staged_context(connection, context)
    ids, copied = _checked_batch(connection, identities, products)
    try:
        with connection.transaction():
            connection.execute(
                """INSERT INTO pg_temp.publication_scope (ticker_id, complete)
                   SELECT ticker_id, false FROM pg_temp.publication_ticker_stage WHERE ticker_id = ANY(%s)
                   ON CONFLICT (ticker_id) DO UPDATE SET complete = false""",
                (ids,),
            )
            for kind, frame in copied:
                copy_frame(connection, _STAGES[kind], frame, _STAGE_COLUMNS[kind])
                flags = (
                    sql.SQL(" OR left_truncated IS NULL OR calendar_closed IS NULL") if kind != "daily" else sql.SQL("")
                )
                invalid = connection.execute(
                    sql.SQL(
                        """SELECT 1 FROM {} WHERE ticker_id=ANY(%s) AND
                           (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                            OR volume IS NULL OR volume < 0
                            {flags}) LIMIT 1"""
                    ).format(sql.Identifier("pg_temp", _STAGES[kind]), flags=flags),
                    (ids,),
                ).fetchone()
                if invalid is not None:
                    _fail()
            connection.execute("UPDATE pg_temp.publication_scope SET complete=true WHERE ticker_id=ANY(%s)", (ids,))
    except psycopg.Error:
        raise PostgresWriterError(_STAGE_FAILED) from None


def _checked_batch(
    connection: psycopg.Connection,
    identities: pl.DataFrame,
    products: ProductBatch,
) -> tuple[list[int], list[tuple[str, pl.DataFrame]]]:
    """Check durable identities and batch product columns before mutation."""
    if identities.schema != {"ticker_id": pl.Int32, "symbol": pl.String} or identities.is_empty():
        _fail()
    if identities["ticker_id"].n_unique() != identities.height or identities["symbol"].n_unique() != identities.height:
        _fail()
    ids = identities["ticker_id"].to_list()
    if any(type(value) is not int or value <= 0 for value in ids):
        _fail()
    durable = connection.execute(
        "SELECT ticker_id, symbol FROM market.ticker WHERE ticker_id = ANY(%s) ORDER BY ticker_id", (ids,)
    ).fetchall()
    if durable != sorted(zip(ids, identities["symbol"].to_list(), strict=True)):
        _fail()
    copied: list[tuple[str, pl.DataFrame]] = []
    for kind in ("daily", "weekly", "monthly"):
        frame = getattr(products, kind)
        columns = _STAGE_COLUMNS[kind]
        if tuple(frame.columns) != columns or frame["ticker_id"].dtype != pl.Int32:
            _fail()
        if frame.height and not set(frame["ticker_id"].to_list()).issubset(ids):
            _fail()
        copied.append((kind, frame))
    return ids, copied


def _require_key_match(
    connection: psycopg.Connection,
    stage_name: str,
    desired_keys: sql.Composable,
    params: tuple[datetime.date, datetime.date, datetime.date, datetime.date],
) -> None:
    staged_keys = sql.SQL("SELECT ticker_id,date FROM {}").format(sql.Identifier("pg_temp", stage_name))
    statement = sql.SQL("SELECT 1 FROM (({} EXCEPT {}) UNION ALL ({} EXCEPT {})) AS differences LIMIT 1").format(
        desired_keys, staged_keys, staged_keys, desired_keys
    )
    if connection.execute(statement, params).fetchone() is not None:
        _fail()


def _validate_product_stage(
    connection: psycopg.Connection,
    kind: str,
    context: BuildContext,
) -> None:
    stage = sql.Identifier("pg_temp", _STAGES[kind])
    period_start = sql.SQL(PERIOD_TRUNC_SQL[kind])
    flags = sql.SQL("") if kind == "daily" else sql.SQL(PERIOD_FLAG_NULL_CHECK)
    invalid_stage = sql.SQL(
        """SELECT 1 FROM {stage} WHERE
             ticker_id IS NULL OR date IS NULL OR ticker_id NOT IN
               (SELECT ticker_id FROM pg_temp.publication_scope WHERE complete)
             UNION ALL SELECT 1 FROM {stage} GROUP BY ticker_id,date HAVING count(*) > 1
             UNION ALL SELECT 1 FROM {stage} WHERE
               (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
OR volume IS NULL
                 OR ticker_id <= 0
                 OR NOT market.is_valid_ohlc(open, high, low, close)
                {flags} OR date < {period_start} OR date > %s)
             LIMIT 1"""
    ).format(stage=stage, flags=flags, period_start=period_start)
    start = context.retained_start or context.target_session
    end = context.retained_end or context.target_session
    if connection.execute(invalid_stage, (start, end)).fetchone() is not None:
        _fail()


def _validate_staged_metadata(connection: psycopg.Connection) -> None:
    difference = connection.execute(
        """WITH current_metadata AS (
               SELECT t.ticker_id,t.symbol,r.name,r.ticker_type,r.primary_exchange,r.cik,r.active
               FROM market.ticker t LEFT JOIN ingest.ticker_reference r USING(ticker_id)
           ), staged_metadata AS (
               SELECT ticker_id,symbol,name,ticker_type,primary_exchange,cik,active
               FROM pg_temp.publication_ticker_stage
           )
           SELECT 1 FROM (
               (SELECT * FROM current_metadata EXCEPT SELECT * FROM staged_metadata)
               UNION ALL
               (SELECT * FROM staged_metadata EXCEPT SELECT * FROM current_metadata)
           ) AS differences LIMIT 1"""
    ).fetchone()
    if difference is not None:
        _fail()


def _validate_stages(connection: psycopg.Connection, context: BuildContext) -> None:
    incomplete = connection.execute("SELECT 1 FROM pg_temp.publication_scope WHERE NOT complete LIMIT 1").fetchone()
    if incomplete is not None:
        _fail()
    missing_identity = connection.execute(
        """SELECT 1 FROM pg_temp.publication_ticker_stage i WHERE NOT EXISTS
           (SELECT 1 FROM pg_temp.publication_scope s WHERE s.complete AND s.ticker_id=i.ticker_id)
           LIMIT 1"""
    ).fetchone()
    if missing_identity is not None:
        _fail()
    _validate_product_stage(connection, "daily", context)
    _validate_product_stage(connection, "weekly", context)
    _validate_product_stage(connection, "monthly", context)
    start = context.retained_start or context.target_session
    end = context.retained_end or context.target_session
    raw = sql.SQL("SELECT ticker_id,date FROM ingest.raw_daily WHERE date BETWEEN %s AND %s")
    _require_key_match(connection, "publication_daily_stage", raw, (start, end, start, end))
    weekly = sql.SQL(
        "SELECT ticker_id,date_trunc('week',date)::date AS date FROM ingest.raw_daily "
        "WHERE date BETWEEN %s AND %s GROUP BY ticker_id,date_trunc('week',date)"
    )
    _require_key_match(connection, "publication_weekly_stage", weekly, (start, end, start, end))
    monthly = sql.SQL(
        "SELECT ticker_id,max(date) AS date FROM ingest.raw_daily "
        "WHERE date BETWEEN %s AND %s GROUP BY ticker_id,date_trunc('month',date)"
    )
    _require_key_match(connection, "publication_monthly_stage", monthly, (start, end, start, end))
    _validate_staged_metadata(connection)
    target_rows = connection.execute(
        """SELECT count(*) FROM ingest.raw_daily r
           JOIN ingest.raw_session s ON s.date=r.date
           WHERE r.date=%s""",
        (context.target_session,),
    ).fetchone()
    if target_rows is None or target_rows[0] <= 0:
        _fail()


def _commit_is_known_rollback(connection: psycopg.Connection, run_id: UUID) -> bool:
    """Prove rollback on the same still-locked connection after a COMMIT error."""
    try:
        if connection.closed or connection.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            return False
        require_writer_connection(connection)
        row = connection.execute(
            """SELECT r.state,p.run_id FROM ingest.run r
               LEFT JOIN market.publication_state p ON p.publication_state_id=1
               WHERE r.run_id=%s""",
            (run_id,),
        ).fetchone()
    except psycopg.Error, PostgresWriterError:
        return False
    return row is not None and row[0] == "running" and row[1] != run_id


def _verify_run_state(connection: psycopg.Connection, context: BuildContext) -> None:
    """Validate the locked cache, run, and accepted raw session state."""
    row = connection.execute(
        """SELECT c.input_revision, r.input_revision, r.state, r.target_date FROM ingest.cache_state c
           JOIN ingest.run r ON r.run_id=%s WHERE c.cache_state_id=1 FOR UPDATE OF c,r""",
        (context.run_id,),
    ).fetchone()
    accepted = connection.execute(
        """SELECT 1 FROM ingest.raw_session s JOIN ingest.fetch_manifest m USING(manifest_id)
           WHERE s.date=%s AND m.status='populated'
             AND m.source='daily' AND m.requested_date=s.date""",
        (context.target_session,),
    ).fetchone()
    if (
        row is None
        or row[0] != context.input_revision
        or row[1] != context.input_revision
        or row[2] != "running"
        or row[3] != context.target_session
        or accepted is None
    ):
        _fail()


def _publish_kind(connection: psycopg.Connection, kind: str, table: str, context: BuildContext) -> None:
    """Copy one product kind into its production table and prune out-of-scope rows."""
    columns = _STAGE_COLUMNS[kind]
    mutable = tuple(column for column in columns if column not in _KEY_COLUMNS)
    table_identifier = sql.Identifier(*table.split("."))
    stage_identifier = sql.Identifier("pg_temp", _STAGES[kind])
    names = sql.SQL(",").join(sql.Identifier(column) for column in columns)
    key = sql.SQL(",").join(sql.Identifier(column) for column in _KEY_COLUMNS)
    assignments = sql.SQL(",").join(
        sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(column), sql.Identifier(column)) for column in mutable
    )
    prior_values = sql.SQL(",").join(sql.SQL("target.{}").format(sql.Identifier(column)) for column in mutable)
    staged_values = sql.SQL(",").join(sql.SQL("EXCLUDED.{}").format(sql.Identifier(column)) for column in mutable)
    connection.execute(
        sql.SQL(
            """INSERT INTO {table} AS target ({columns}) SELECT {columns} FROM {stage}
               ON CONFLICT ({key}) DO UPDATE SET {assignments}
               WHERE ROW({prior}) IS DISTINCT FROM ROW({desired})"""
        ).format(
            table=table_identifier,
            columns=names,
            stage=stage_identifier,
            key=key,
            assignments=assignments,
            prior=prior_values,
            desired=staged_values,
        )
    )
    connection.execute(
        sql.SQL(
            """DELETE FROM {table} AS target USING pg_temp.publication_scope AS scope
               WHERE scope.complete AND target.ticker_id=scope.ticker_id
                 AND target.period=%s
                 AND target.date BETWEEN {lower} AND %s
                 AND NOT EXISTS (SELECT 1 FROM {stage} AS staged
                                 WHERE staged.ticker_id=target.ticker_id AND staged.date=target.date)"""
        ).format(table=table_identifier, lower=sql.SQL(PERIOD_TRUNC_SQL[kind]), stage=stage_identifier),
        (kind, context.retained_start or context.target_session, context.retained_end or context.target_session),
    )


def _refresh_latest_daily(connection: psycopg.Connection, context: BuildContext) -> None:
    """Upsert the current session into latest_daily and drop stale scoped rows."""
    connection.execute(
        """INSERT INTO market.latest_daily
           SELECT d.ticker_id,d.date,d.open,d.high,d.low,d.close,d.volume
           FROM pg_temp.publication_daily_stage d
           JOIN pg_temp.publication_scope s USING(ticker_id)
           JOIN pg_temp.publication_ticker_stage i USING(ticker_id)
           WHERE s.complete AND d.date=%s AND i.active IS TRUE
             AND i.ticker_type=ANY(%s)
           ON CONFLICT(ticker_id) DO UPDATE SET date=EXCLUDED.date, open=EXCLUDED.open,
             high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close,
             volume=EXCLUDED.volume
           WHERE ROW(market.latest_daily.date, market.latest_daily.open, market.latest_daily.high,
             market.latest_daily.low, market.latest_daily.close,
             market.latest_daily.volume)
             IS DISTINCT FROM ROW(EXCLUDED.date, EXCLUDED.open, EXCLUDED.high, EXCLUDED.low,
             EXCLUDED.close, EXCLUDED.volume)""",
        (context.target_session, list(context.ticker_types)),
    )
    connection.execute(
        """DELETE FROM market.latest_daily l USING pg_temp.publication_scope s
           WHERE s.complete AND l.ticker_id=s.ticker_id AND NOT EXISTS
              (SELECT 1 FROM pg_temp.publication_daily_stage d
               JOIN pg_temp.publication_ticker_stage i USING(ticker_id)
              WHERE d.ticker_id=l.ticker_id AND d.date=%s AND i.active IS TRUE
                AND i.ticker_type=ANY(%s))""",
        (context.target_session, list(context.ticker_types)),
    )


def _record_publication(connection: psycopg.Connection, context: BuildContext) -> None:
    """Record the publication marker and mark the run as published."""
    count = connection.execute("SELECT count(*) FROM pg_temp.publication_scope WHERE complete").fetchone()
    connection.execute(
        """INSERT INTO market.publication_state(publication_state_id,published_session,published_at,run_id,ticker_count)
           VALUES(1,%s,statement_timestamp(),%s,%s)
           ON CONFLICT(publication_state_id) DO UPDATE SET published_session=EXCLUDED.published_session,
             published_at=EXCLUDED.published_at,run_id=EXCLUDED.run_id,ticker_count=EXCLUDED.ticker_count""",
        (context.target_session, context.run_id, count[0] if count else 0),
    )
    connection.execute(
        """UPDATE ingest.run SET state='published', ended_at=statement_timestamp(),
           published_at=statement_timestamp() WHERE run_id=%s AND state='running'""",
        (context.run_id,),
    )


def publish_staged(connection: psycopg.Connection, context: BuildContext) -> PublicationResult:
    """Atomically validate, replace scoped products, and record the publication."""
    _require_idle_writer(connection)
    _verify_staged_context(connection, context)
    commit_started = False
    try:
        with connection.transaction():
            _verify_run_state(connection, context)
            _validate_stages(connection, context)
            connection.execute(
                """UPDATE market.ticker t SET name=i.name,ticker_type=i.ticker_type,
                     primary_exchange=i.primary_exchange,cik=i.cik,active=i.active
                   FROM pg_temp.publication_ticker_stage i WHERE t.ticker_id=i.ticker_id
                     AND ROW(t.name,t.ticker_type,t.primary_exchange,t.cik,t.active)
                         IS DISTINCT FROM ROW(i.name,i.ticker_type,i.primary_exchange,i.cik,i.active)""",
            )
            for kind, table in PUBLICATION_TABLES.items():
                _publish_kind(connection, kind, table, context)
            _refresh_latest_daily(connection, context)
            _record_publication(connection, context)
            commit_started = True
        return PublicationResult(
            run_id=context.run_id, published_session=context.target_session, input_revision=context.input_revision
        )
    except psycopg.Error as error:
        if commit_started:
            if _commit_is_known_rollback(connection, context.run_id):
                raise PostgresWriterError(_PUBLISH_FAILED) from None
            raise PublicationOutcomeUnknownError(context.run_id) from error
        raise PostgresWriterError(_PUBLISH_FAILED) from None


def resolve_publication(database_url: str, run_id: UUID) -> PublicationResolution:
    """Resolve a commit acknowledgement using durable state on a fresh writer."""
    try:
        with writer_connection(database_url) as connection:
            run = connection.execute(
                "SELECT state FROM ingest.run WHERE run_id=%s",
                (run_id,),
            ).fetchone()
            if run is None:
                raise PublicationOutcomeUnknownError(run_id)
            marker = connection.execute(
                "SELECT run_id FROM market.publication_state WHERE publication_state_id=1"
            ).fetchone()
            return PublicationResolution(
                published=run[0] == "published", is_current=bool(marker and marker[0] == run_id)
            )
    except PostgresWriterError as error:
        if isinstance(error, PublicationOutcomeUnknownError):
            raise
        raise PublicationOutcomeUnknownError(run_id) from error
