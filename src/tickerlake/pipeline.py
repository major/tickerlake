"""ETL pipeline orchestration: backfill and update commands."""

from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING

from tickerlake.postgres.backfill import BackfillRequest
from tickerlake.postgres.backfill import backfill as postgres_backfill
from tickerlake.postgres.backfill import update as postgres_update
from tickerlake.postgres.info import collect_info

if TYPE_CHECKING:
    from collections.abc import Callable

    from tickerlake.config import Config
    from tickerlake.postgres.publication import PublicationResult

logger = logging.getLogger(__name__)

# Provenance recorded on every PostgreSQL run. The DuckDB pipeline did not track
# versions, so seed a placeholder value until real version tracking lands.
_VERSION = "tickerlake"


def _require_api_key(config: Config) -> None:
    """Raise a clear error when a Massive API command lacks credentials."""
    if not config.api_key:
        msg = "MASSIVE_API_KEY environment variable is required"
        raise ValueError(msg)


def _log_run_summary(
    action: str,
    config: Config,
    request: BackfillRequest,
    result: PublicationResult,
) -> None:
    """Log the run target and published cache state after a postgres run."""
    target = request.target if request.target is not None else config.end_date
    logger.info(
        "%s complete: target=%s published_session=%s input_revision=%s run_id=%s",
        action,
        target,
        result.published_session,
        result.input_revision,
        result.run_id,
    )


def _run(
    config: Config,
    *,
    action: str,
    runner: Callable[..., PublicationResult],
) -> None:
    """Run a postgres-backed action and log the summary."""
    request = BackfillRequest(version=_VERSION)
    result = runner(config, request, now=datetime.datetime.now(tz=datetime.UTC))
    _log_run_summary(action, config, request, result)


def backfill(config: Config) -> None:
    """Run a full backfill through the PostgreSQL backend."""
    _require_api_key(config)
    _run(config, action="Backfill", runner=postgres_backfill)


def update(config: Config) -> None:
    """Refresh recent revisions through the PostgreSQL backend."""
    _require_api_key(config)
    _run(config, action="Update", runner=postgres_update)


def info(config: Config) -> None:
    """Log a read-only summary of the PostgreSQL database."""
    database_url = config.database_url
    if not isinstance(database_url, str) or not database_url.strip():
        msg = "DATABASE_URL is required for info"
        raise ValueError(msg)
    result = collect_info(database_url)
    logger.info(
        "Database info: schemas=%s tables=%d tickers=%d splits=%d raw_daily=%d publication=%s",
        ",".join(result.schemas),
        len(result.tables),
        result.counts.tickers,
        result.counts.splits,
        result.counts.raw_daily,
        result.publication.target_session if result.publication is not None else "none",
    )
