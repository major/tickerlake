"""Run, cache revision, and fetch manifest persistence."""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Final, TypeGuard
from uuid import UUID, uuid4

import psycopg

from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection
from tickerlake.postgres.models import CacheState, FetchRequest, RunSpec

# Safe errors intentionally carry fixed caller-facing messages.
# ruff: noqa: TRY003

if TYPE_CHECKING:
    from tickerlake.outcomes import FetchOutcome

_DIAGNOSTICS: Final = frozenset(
    {
        "transport_error",
        "invalid_envelope",
        "invalid_record",
        "reference_shrink",
        "reference_change",
        "invalid_canonical_frame",
    }
)
_FAILURE_CODES: Final = frozenset(
    {
        "storage_error",
        "connection_lost",
        "incomplete_fetch",
        "validation_error",
        "operator_cancelled",
    }
)


def _is_date(value: object) -> TypeGuard[date]:
    return isinstance(value, date) and not isinstance(value, datetime)


def _valid_range(start: object, end: object, *, optional: bool) -> bool:
    if start is None or end is None:
        return optional and start is None and end is None
    return _is_date(start) and _is_date(end) and start <= end


def start_run(connection: psycopg.Connection, spec: RunSpec) -> UUID:
    """Create a running run with the current input revision."""
    require_writer_connection(connection)
    if not _is_date(spec.target) or not _valid_range(spec.requested_start, spec.requested_end, optional=True):
        raise PostgresWriterError("Invalid PostgreSQL run date range")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (spec.code_version, spec.schema_version, spec.transform_version)
    ):
        raise PostgresWriterError("Invalid PostgreSQL run version")
    run_id = uuid4()
    try:
        with connection.transaction():
            row = connection.execute("SELECT input_revision FROM ingest.cache_state WHERE singleton = true").fetchone()
            if row is None:
                raise PostgresWriterError("PostgreSQL cache state is unavailable")
            connection.execute(
                """INSERT INTO ingest.run
                   (run_id, target_date, requested_start, requested_end, input_revision,
                    code_version, schema_version, transform_version, state, started_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'running', now())""",
                (
                    run_id,
                    spec.target,
                    spec.requested_start,
                    spec.requested_end,
                    row[0],
                    spec.code_version,
                    spec.schema_version,
                    spec.transform_version,
                ),
            )
    except psycopg.Error:
        raise PostgresWriterError("Could not start PostgreSQL run") from None
    return run_id


def capture_run_inputs(connection: psycopg.Connection, run_id: UUID) -> int:
    """Capture the latest cache revision for a still-running run."""
    require_writer_connection(connection)
    try:
        with connection.transaction():
            row = connection.execute("SELECT input_revision FROM ingest.cache_state WHERE singleton = true").fetchone()
            if row is None:
                raise PostgresWriterError("PostgreSQL cache state is unavailable")
            result = connection.execute(
                "UPDATE ingest.run SET input_revision = %s WHERE run_id = %s AND state = 'running'",
                (row[0], run_id),
            )
            if result.rowcount != 1:
                raise PostgresWriterError("PostgreSQL run is not running")
            return int(row[0])
    except psycopg.Error:
        raise PostgresWriterError("Could not capture PostgreSQL run inputs") from None


def finish_run(connection: psycopg.Connection, run_id: UUID) -> None:
    """Mark a running run completed."""
    _set_terminal(connection, run_id, "completed", None)


def fail_run(connection: psycopg.Connection, run_id: UUID, reason_code: str) -> None:
    """Mark a running run failed using a fixed safe reason code."""
    if reason_code not in _FAILURE_CODES:
        raise PostgresWriterError("Unsupported PostgreSQL run failure code")
    _set_terminal(connection, run_id, "failed", reason_code)


def _set_terminal(connection: psycopg.Connection, run_id: UUID, state: str, code: str | None) -> None:
    require_writer_connection(connection)
    try:
        with connection.transaction():
            result = connection.execute(
                """UPDATE ingest.run SET state = %s, failure_code = %s, ended_at = now()
                   WHERE run_id = %s AND state = 'running'""",
                (state, code, run_id),
            )
            if result.rowcount != 1:
                raise PostgresWriterError("PostgreSQL run is not running")
    except psycopg.Error:
        raise PostgresWriterError("Could not finish PostgreSQL run") from None


def read_cache_state(connection: psycopg.Connection) -> CacheState:
    """Read cache state without requiring writer-lock ownership."""
    try:
        row = connection.execute(
            "SELECT input_revision, retained_start, retained_end FROM ingest.cache_state WHERE singleton = true"
        ).fetchone()
    except psycopg.Error:
        raise PostgresWriterError("Could not read PostgreSQL cache state") from None
    if row is None:
        raise PostgresWriterError("PostgreSQL cache state is unavailable")
    return CacheState(input_revision=row[0], retained_start=row[1], retained_end=row[2])


def advance_cache_revision(connection: psycopg.Connection, accepted_date: date | None = None) -> int:
    """Advance input revision once and expand retained date bounds."""
    require_writer_connection(connection)
    if accepted_date is not None and not _is_date(accepted_date):
        raise PostgresWriterError("Invalid PostgreSQL accepted date")
    try:
        with connection.transaction():
            row = connection.execute(
                """UPDATE ingest.cache_state SET input_revision = input_revision + 1,
                   retained_start = CASE WHEN %s::date IS NULL THEN retained_start
                     WHEN retained_start IS NULL THEN %s::date ELSE LEAST(retained_start, %s::date) END,
                   retained_end = CASE WHEN %s::date IS NULL THEN retained_end
                     WHEN retained_end IS NULL THEN %s::date ELSE GREATEST(retained_end, %s::date) END
                   WHERE singleton = true RETURNING input_revision""",
                (accepted_date, accepted_date, accepted_date, accepted_date, accepted_date, accepted_date),
            ).fetchone()
            if row is None:
                raise PostgresWriterError("PostgreSQL cache state is unavailable")
            return int(row[0])
    except psycopg.Error:
        raise PostgresWriterError("Could not advance PostgreSQL cache revision") from None


def record_fetch_outcome(connection: psycopg.Connection, request: FetchRequest, outcome: FetchOutcome) -> int:
    """Record request scope and a sanitized fetch outcome; return the manifest ID."""
    require_writer_connection(connection)
    if not isinstance(request.source, str) or request.source not in {"daily", "tickers", "splits"}:
        raise PostgresWriterError("Invalid PostgreSQL fetch source")
    if request.source == "daily":
        valid_scope = (
            _is_date(request.requested_date)
            and request.requested_start is None
            and request.requested_end is None
            and not request.ticker_types
        )
    elif request.source == "splits":
        valid_scope = (
            request.requested_date is None
            and _valid_range(request.requested_start, request.requested_end, optional=False)
            and not request.ticker_types
        )
    else:
        valid_scope = (
            request.requested_date is None
            and request.requested_start is None
            and request.requested_end is None
            and isinstance(request.ticker_types, tuple)
            and bool(request.ticker_types)
            and all(isinstance(value, str) and value.strip() for value in request.ticker_types)
            and len(set(request.ticker_types)) == len(request.ticker_types)
        )
    started_at = request.started_at
    if (
        not valid_scope
        or (
            started_at is not None
            and (not isinstance(started_at, datetime) or started_at.tzinfo is None or started_at.utcoffset() is None)
        )
        or (
            outcome.requested_date is not None
            and (
                not _is_date(outcome.requested_date)
                or request.source != "daily"
                or outcome.requested_date != request.requested_date
            )
        )
    ):
        raise PostgresWriterError("Invalid PostgreSQL fetch request scope")
    diagnostic = outcome.diagnostic
    if diagnostic is not None and diagnostic not in _DIAGNOSTICS:
        raise PostgresWriterError("Unsupported PostgreSQL fetch diagnostic")
    row_count = outcome.frame.height
    try:
        with connection.transaction():
            running = connection.execute(
                "SELECT 1 FROM ingest.run WHERE run_id = %s AND state = 'running'", (request.run_id,)
            ).fetchone()
            if running is None:
                raise PostgresWriterError("PostgreSQL run is not running")
            result = connection.execute(
                """INSERT INTO ingest.fetch_manifest
                   (run_id, source, requested_date, requested_start, requested_end, requested_ticker_types,
                    started_at, finished_at, status, row_count, diagnostic_code)
                   VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, statement_timestamp()),
                           statement_timestamp(), %s, %s, %s)
                     RETURNING manifest_id, started_at, finished_at""",
                (
                    request.run_id,
                    request.source,
                    request.requested_date,
                    request.requested_start,
                    request.requested_end,
                    list(request.ticker_types) if request.source == "tickers" else None,
                    request.started_at,
                    outcome.status.value,
                    row_count,
                    diagnostic,
                ),
            )
            timestamps = result.fetchone()
            if timestamps is None or timestamps[2] < timestamps[1]:
                raise PostgresWriterError("Invalid PostgreSQL fetch timestamps")
            return int(timestamps[0])
    except psycopg.Error:
        raise PostgresWriterError("Could not record PostgreSQL fetch outcome") from None
