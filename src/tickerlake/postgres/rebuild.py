"""Bounded orchestration for rebuilding staged PostgreSQL products."""

from __future__ import annotations

from typing import TYPE_CHECKING

from psycopg.pq import TransactionStatus

from tickerlake.postgres.connection import PostgresWriterError, require_writer_connection
from tickerlake.postgres.products import build_products
from tickerlake.postgres.publication import prepare_publication, publish_staged, stage_batch
from tickerlake.postgres.reading import read_raw_history, read_split_history, read_ticker_batch
from tickerlake.postgres.state import capture_run_inputs

if TYPE_CHECKING:
    from collections.abc import Sequence
    from uuid import UUID

    import psycopg

    from tickerlake.postgres.publication import PublicationResult

_MAX_BATCH = 1000


def rebuild_cache(
    connection: psycopg.Connection,
    run_id: UUID,
    *,
    ticker_types: Sequence[str],
    batch_size: int = 100,
) -> PublicationResult:
    """Rebuild all products from durable identities on the writer connection."""
    require_writer_connection(connection)
    if connection.info.transaction_status != TransactionStatus.IDLE:
        raise PostgresWriterError("Writer connection is not idle")  # noqa: TRY003
    if type(batch_size) is not int or not 1 <= batch_size <= _MAX_BATCH:
        raise PostgresWriterError("Invalid batch size")  # noqa: TRY003
    if isinstance(ticker_types, (str, bytes)):
        raise PostgresWriterError("Invalid ticker types")  # noqa: TRY003
    types = tuple(ticker_types)
    if (
        not types
        or len(set(types)) != len(types)
        or any(not isinstance(value, str) or not value.strip() for value in types)
    ):
        raise PostgresWriterError("Invalid ticker types")  # noqa: TRY003

    capture_run_inputs(connection, run_id)
    context = prepare_publication(connection, run_id, ticker_types=types)
    after_id = 0
    found_identities = False
    while True:
        identities = read_ticker_batch(connection, after_id=after_id, limit=batch_size)
        if identities.is_empty():
            break
        found_identities = True
        raw = read_raw_history(connection, identities["ticker_id"].to_list())
        splits = read_split_history(connection, identities["ticker_id"].to_list())
        products = build_products(
            raw,
            splits,
            identities,
            collection_start=context.retained_start or context.target_session,
            target=context.target_session,
        )
        stage_batch(connection, context, identities, products)
        after_id = identities["ticker_id"][-1]
        del products, splits, raw, identities
    if not found_identities:
        raise PostgresWriterError("No durable identities")  # noqa: TRY003
    return publish_staged(connection, context)
