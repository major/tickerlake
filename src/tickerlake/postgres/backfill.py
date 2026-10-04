"""Orchestrated Massive-to-PostgreSQL fresh backfills and corrections.

The entry point owns one writer connection for the whole run. Network calls to
Massive happen while that connection is idle (autocommit, no open transaction);
every storage step opens its own short transaction. The run is published by the
chunk 4 rebuild, which captures the input revision once and publishes atomically.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import psycopg
from psycopg.pq import TransactionStatus

from tickerlake.calendar import get_closed_sessions, resolve_closed_target
from tickerlake.client import MassiveClient
from tickerlake.extract import extract_daily_aggs, extract_splits, extract_tickers
from tickerlake.outcomes import FetchOutcome, FetchStatus
from tickerlake.postgres._validation import is_date, require_unique_nonempty_strings
from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection, writer_connection
from tickerlake.postgres.models import FetchRequest, RunSpec
from tickerlake.postgres.publication import PublicationOutcomeUnknownError
from tickerlake.postgres.raw import store_daily_outcome
from tickerlake.postgres.reading import (
    read_latest_raw_dates,
    read_raw_date,
    read_split_bounds,
    read_split_range,
    read_ticker_reference,
)
from tickerlake.postgres.rebuild import rebuild_cache
from tickerlake.postgres.references import store_split_outcome, store_ticker_outcome
from tickerlake.postgres.state import fail_run, read_cache_state, start_run

# Safe errors intentionally carry fixed or summary-only caller-facing messages.
# ruff: noqa: TRY003

if TYPE_CHECKING:
    from uuid import UUID

    from tickerlake.config import Config
    from tickerlake.postgres.publication import PublicationResult

# Only documented safe run failure codes may be written.
_KNOWN_FAILURE_CODE: Final = "validation_error"
_INCOMPLETE_FAILURE_CODE: Final = "incomplete_fetch"
_ACCEPTED_SPLITS: Final = frozenset({FetchStatus.populated, FetchStatus.successful_empty})

_SAFE_NO_SESSIONS: Final = "Backfill has no closed sessions to fetch"
_SAFE_DATABASE_URL: Final = "PostgreSQL database URL must be configured"
_SAFE_TICKER_TYPES: Final = "Backfill ticker types must be unique nonempty strings"
_SAFE_CONFIG_BOUNDS: Final = "Backfill configured bounds must be ordered dates"
_SAFE_VERSIONS: Final = "Backfill run versions must be nonempty strings"
_SAFE_TARGET: Final = "Backfill target must be a date"
_SAFE_CORRECTION: Final = "Backfill correction range must be two ordered dates"
_SAFE_BATCH: Final = "Backfill batch size must be a positive integer"
_SAFE_NOW: Final = "Backfill now must be a timezone-aware datetime"
_SAFE_UPDATE: Final = "Update has no closed sessions to fetch"
_SAFE_TICKERS_UNPOPULATED: Final = "Backfill ticker reference did not populate"
_SAFE_SPLITS_UNPOPULATED: Final = "Backfill split coverage did not populate"
_CORRECTION_PAIR_SIZE: Final = 2
_MAX_TICKER_TYPES: Final = 20
_MAX_BATCH_SIZE: Final = 1000


@dataclass(frozen=True, slots=True, kw_only=True)
class BackfillRequest:
    """Frozen provenance and optional scope for one PostgreSQL backfill."""

    code_version: str
    schema_version: str
    transform_version: str
    target: datetime.date | None = None
    correction_range: tuple[datetime.date, datetime.date] | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class UpdateRequest:
    """Frozen provenance and optional target for one PostgreSQL update."""

    code_version: str
    schema_version: str
    transform_version: str
    target: datetime.date | None = None


class BackfillError(PostgresWriterError):
    """A safe error raised by the PostgreSQL backfill orchestration."""


class BackfillIncompleteError(BackfillError):
    """Raised when requested closed sessions did not all return populated data."""

    def __init__(self, outcomes: list[FetchOutcome]) -> None:
        """Build a safe summary from the daily outcomes that blocked publication."""
        self.outcomes = tuple(outcomes)
        super().__init__(f"Backfill closed sessions did not all populate: {_outcome_summary(outcomes)}")


def _outcome_summary(outcomes: list[FetchOutcome]) -> str:
    """Describe non-populated outcomes without exposing source records or secrets."""
    return "; ".join(
        f"{outcome.requested_date or 'reference'}={outcome.status.value}"
        + (f" ({outcome.diagnostic})" if outcome.diagnostic else "")
        for outcome in outcomes
    )


def _validate_config(config: Config) -> str:
    """Validate credentials and scope before any write; return the database URL."""
    if not isinstance(config.api_key, str) or not config.api_key.strip():
        msg = "MASSIVE_API_KEY environment variable is required"
        raise ValueError(msg)
    database_url = config.database_url
    if not isinstance(database_url, str) or not database_url.strip():
        raise BackfillError(_SAFE_DATABASE_URL)
    if not is_date(config.start_date) or not is_date(config.end_date) or config.start_date > config.end_date:
        raise BackfillError(_SAFE_CONFIG_BOUNDS)
    require_unique_nonempty_strings(
        config.ticker_types,
        field="ticker types",
        max_len=_MAX_TICKER_TYPES,
        message=_SAFE_TICKER_TYPES,
        error=BackfillError,
    )
    return database_url


def _validate_request(request: BackfillRequest | UpdateRequest, *, now: datetime.datetime, batch_size: int) -> None:
    """Validate frozen provenance, dates, batch size, and the frozen clock."""
    if not isinstance(now, datetime.datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise BackfillError(_SAFE_NOW)
    if type(batch_size) is not int or not 1 <= batch_size <= _MAX_BATCH_SIZE:
        raise BackfillError(_SAFE_BATCH)
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (request.code_version, request.schema_version, request.transform_version)
    ):
        raise BackfillError(_SAFE_VERSIONS)
    if request.target is not None and not is_date(request.target):
        raise BackfillError(_SAFE_TARGET)
    if isinstance(request, BackfillRequest) and request.correction_range is not None:
        correction = request.correction_range
        if not isinstance(correction, tuple) or len(correction) != _CORRECTION_PAIR_SIZE:
            raise BackfillError(_SAFE_CORRECTION)
        start, end = correction
        if not is_date(start) or not is_date(end) or start > end:
            raise BackfillError(_SAFE_CORRECTION)


def _selected_dates(
    config: Config,
    request: BackfillRequest | UpdateRequest,
    target: datetime.date,
    *,
    now: datetime.datetime,
) -> list[datetime.date]:
    """Return correction sessions when supplied, otherwise every configured closed session.

    A correction run fetches only its explicit bounds. It never adds the
    configured history or the resolved target, and an empty correction stays
    empty even when the configured range has sessions.
    """
    correction = request.correction_range if isinstance(request, BackfillRequest) else None
    if correction is not None:
        start, end = correction
        return get_closed_sessions(start, end, now=now)
    if config.start_date > target:
        return []
    return get_closed_sessions(config.start_date, target, now=now)


def _split_bounds(
    config: Config,
    request: BackfillRequest | UpdateRequest,
    target: datetime.date,
    connection: psycopg.Connection,
) -> tuple[datetime.date, datetime.date]:
    """Union configured, requested, target, retained raw, and stored split bounds."""
    starts: list[datetime.date] = [config.start_date, config.end_date, target]
    ends: list[datetime.date] = [config.start_date, config.end_date, target]
    if isinstance(request, BackfillRequest) and request.correction_range is not None:
        start, end = request.correction_range
        starts.append(start)
        ends.append(end)
    cache = read_cache_state(connection)
    if cache.retained_start is not None:
        starts.append(cache.retained_start)
    if cache.retained_end is not None:
        ends.append(cache.retained_end)
    existing_start, existing_end = read_split_bounds(connection)
    if existing_start is not None:
        starts.append(existing_start)
    if existing_end is not None:
        ends.append(existing_end)
    return min(starts), max(ends)


def _calendar_year_windows(
    coverage_start: datetime.date,
    coverage_end: datetime.date,
) -> list[tuple[datetime.date, datetime.date]]:
    """Split a range into non-overlapping inclusive calendar-year windows."""
    windows: list[tuple[datetime.date, datetime.date]] = []
    cursor = coverage_start
    while cursor <= coverage_end:
        window_end = min(datetime.date(cursor.year, 12, 31), coverage_end)
        windows.append((cursor, window_end))
        if window_end >= coverage_end:
            break
        cursor = window_end + datetime.timedelta(days=1)
    return windows


def _fail_known(connection: psycopg.Connection, run_id: UUID, code: str) -> None:
    """Best-effort fail_run only while the original connection is alive, idle, and locked."""
    try:
        if connection.closed or connection.info.transaction_status != TransactionStatus.IDLE:
            return
        require_writer_connection(connection)
        fail_run(connection, run_id, code)
    except PostgresWriterError, psycopg.Error:
        # A failed connection or unsuccessful fail_run must not mask the original error.
        return


def _fetch_daily(
    connection: psycopg.Connection,
    client: MassiveClient,
    run_id: UUID,
    dates: list[datetime.date],
) -> None:
    """Fetch, validate, and store every selected closed session, then block if any failed."""
    rejected: list[FetchOutcome] = []
    for day in dates:
        previous = read_raw_date(connection, day)
        outcome = extract_daily_aggs(client, [day], previous=previous)[0]
        store_daily_outcome(connection, FetchRequest(run_id=run_id, source="daily", requested_date=day), outcome)
        if outcome.status is not FetchStatus.populated:
            rejected.append(outcome)
        del previous, outcome
    if rejected:
        _fail_known(connection, run_id, _INCOMPLETE_FAILURE_CODE)
        raise BackfillIncompleteError(rejected)


def _store_tickers(
    connection: psycopg.Connection,
    client: MassiveClient,
    run_id: UUID,
    ticker_types: list[str],
) -> None:
    """Fetch and store the requested metadata scope, requiring a populated catalog."""
    types = tuple(ticker_types)
    previous = read_ticker_reference(connection, types)
    outcome = extract_tickers(client, list(types), previous=previous)
    store_ticker_outcome(connection, FetchRequest(run_id=run_id, source="tickers", ticker_types=types), outcome)
    if outcome.status is not FetchStatus.populated:
        _fail_known(connection, run_id, _KNOWN_FAILURE_CODE)
        raise BackfillError(f"{_SAFE_TICKERS_UNPOPULATED}: {outcome.status.value}")
    del previous, outcome


def _store_splits(
    connection: psycopg.Connection,
    client: MassiveClient,
    run_id: UUID,
    windows: list[tuple[datetime.date, datetime.date]],
) -> None:
    """Refresh each calendar-year window of the union split coverage independently."""
    for window_start, window_end in windows:
        previous = read_split_range(connection, window_start, window_end)
        outcome = extract_splits(client, window_start, window_end, previous=previous)
        store_split_outcome(
            connection,
            FetchRequest(run_id=run_id, source="splits", requested_start=window_start, requested_end=window_end),
            outcome,
        )
        if outcome.status not in _ACCEPTED_SPLITS:
            _fail_known(connection, run_id, _KNOWN_FAILURE_CODE)
            raise BackfillError(f"{_SAFE_SPLITS_UNPOPULATED}: {outcome.status.value}")
        del previous, outcome


def _fetch_reference_and_publish(
    config: Config,
    request: BackfillRequest | UpdateRequest,
    context: tuple[psycopg.Connection, MassiveClient],
    scope: tuple[list[datetime.date], datetime.date],
    *,
    batch_size: int,
) -> PublicationResult:
    """Persist one selected scope, refresh references, and publish it."""
    connection, client = context
    selected, target = scope
    spec = RunSpec(
        target=target,
        requested_start=selected[0],
        requested_end=selected[-1],
        code_version=request.code_version,
        schema_version=request.schema_version,
        transform_version=request.transform_version,
    )
    run_id = start_run(connection, spec)
    try:
        _fetch_daily(connection, client, run_id, selected)
        _store_tickers(connection, client, run_id, config.ticker_types)
        coverage_start, coverage_end = _split_bounds(config, request, target, connection)
        _store_splits(connection, client, run_id, _calendar_year_windows(coverage_start, coverage_end))
        return rebuild_cache(connection, run_id, ticker_types=config.ticker_types, batch_size=batch_size)
    except PublicationOutcomeUnknownError:
        raise
    except BackfillError:
        raise
    except PostgresWriterError:
        _fail_known(connection, run_id, _KNOWN_FAILURE_CODE)
        raise


def backfill(
    config: Config,
    request: BackfillRequest,
    *,
    now: datetime.datetime,
    batch_size: int = 100,
) -> PublicationResult:
    """Fetch configured history or a correction range from Massive, then rebuild and publish.

    Args:
        config: Validated application configuration, including credentials and DSN.
        request: Frozen provenance plus optional target and correction range.
        now: Timezone-aware instant used to resolve and bound closed sessions.
        batch_size: Positive identity page size for the publication rebuild.

    Returns:
        The durable publication result from the chunk 4 rebuild.

    Raises:
        ValueError: If Massive credentials are missing.
        BackfillError: If inputs are invalid or the actionable range is empty.
        BackfillIncompleteError: If any requested closed session is not populated.
        PublicationOutcomeUnknownError: Propagated unchanged when a COMMIT is unresolved.
    """
    database_url = _validate_config(config)
    _validate_request(request, now=now, batch_size=batch_size)

    base_target = request.target if request.target is not None else config.end_date
    target = resolve_closed_target(base_target, now=now)
    selected = _selected_dates(config, request, target, now=now)
    if not selected:
        raise BackfillError(_SAFE_NO_SESSIONS)

    client = MassiveClient(config)

    with writer_connection(database_url) as connection:
        return _fetch_reference_and_publish(
            config, request, (connection, client), (selected, target), batch_size=batch_size
        )


def update(
    config: Config,
    request: UpdateRequest,
    *,
    now: datetime.datetime,
    batch_size: int = 100,
) -> PublicationResult:
    """Refresh recent raw revisions and references, then publish the cache."""
    database_url = _validate_config(config)
    _validate_request(request, now=now, batch_size=batch_size)

    target = resolve_closed_target(request.target if request.target is not None else config.end_date, now=now)
    client = MassiveClient(config)
    with writer_connection(database_url) as connection:
        # Scope depends on durable raw history, so choose it only after taking the
        # same writer lock used by backfill and publication.
        recent_dates = read_latest_raw_dates(connection, target)
        if recent_dates:
            selected = get_closed_sessions(min(recent_dates), target, now=now)
        else:
            selected = _selected_dates(config, request, target, now=now)
        if not selected:
            raise BackfillError(_SAFE_UPDATE)

        return _fetch_reference_and_publish(
            config, request, (connection, client), (selected, target), batch_size=batch_size
        )
