"""Resolve publication acknowledgements from durable writer state."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from uuid import uuid4

import psycopg
import pytest

from tests.postgres.test_publication import _stage_one
from tickerlake.postgres import connection as writer_module
from tickerlake.postgres.connection import PostgresWriterError, writer_connection
from tickerlake.postgres.publication import (
    PublicationOutcomeUnknownError,
    publish_staged,
    resolve_publication,
)


class _ConnectionFault:
    """Inject a connection-boundary failure while forwarding the real locked connection."""

    def __init__(self, connection: psycopg.Connection, *, lose_ack: bool = False, fail_before_commit: bool = False):
        self.connection = connection
        self.lose_ack = lose_ack
        self.fail_before_commit = fail_before_commit

    def __getattr__(self, name: str):
        return getattr(self.connection, name)

    def transaction(self):
        @contextmanager
        def transaction_scope():
            with self.connection.transaction():
                yield
                if self.fail_before_commit:
                    raise psycopg.OperationalError
            if self.lose_ack:
                raise psycopg.OperationalError

        return transaction_scope()


@pytest.fixture
def register_proxy_writer():
    """Register a forwarding proxy while its underlying writer lock remains held."""
    proxies: list[_ConnectionFault] = []

    def register(connection: psycopg.Connection, **faults: bool) -> _ConnectionFault:
        proxy = _ConnectionFault(connection, **faults)
        vars(writer_module)["_ACTIVE_WRITERS"].add(id(proxy))
        proxies.append(proxy)
        return proxy

    yield register
    for proxy in proxies:
        vars(writer_module)["_ACTIVE_WRITERS"].discard(id(proxy))


def test_lost_commit_ack_resolves_to_published(pg_migrated_database, register_proxy_writer) -> None:
    """A lost acknowledgement after PostgreSQL commits resolves as published."""
    target = date(2024, 1, 2)
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id, context = _stage_one(connection, target)
        proxy = register_proxy_writer(connection, lose_ack=True)
        with pytest.raises(PublicationOutcomeUnknownError) as raised:
            publish_staged(proxy, context)
        assert raised.value.run_id == run_id

    assert resolve_publication(pg_migrated_database.owner_dsn, run_id).published


def test_failure_before_commit_resolves_as_not_published(pg_migrated_database, register_proxy_writer) -> None:
    """A write failure before COMMIT rolls back and remains a known non-publication."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        run_id, context = _stage_one(connection, date(2024, 1, 2))
        proxy = register_proxy_writer(connection, fail_before_commit=True)
        with pytest.raises(PostgresWriterError) as raised:
            publish_staged(proxy, context)
        assert not isinstance(raised.value, PublicationOutcomeUnknownError)

    result = resolve_publication(pg_migrated_database.owner_dsn, run_id)
    assert not result.published
    assert not result.is_current


def test_superseded_publication_still_resolves_as_published(pg_migrated_database) -> None:
    """A later exact-session publication replaces the marker but not prior run evidence."""
    with writer_connection(pg_migrated_database.owner_dsn) as connection:
        first, first_context = _stage_one(connection, date(2024, 1, 2))
        publish_staged(connection, first_context)
        second, second_context = _stage_one(connection, date(2024, 1, 2))
        publish_staged(connection, second_context)

    old = resolve_publication(pg_migrated_database.owner_dsn, first)
    current = resolve_publication(pg_migrated_database.owner_dsn, second)
    assert old.published
    assert not old.is_current
    assert current.published
    assert current.is_current


def test_resolution_contention_and_unknown_run_remain_unresolved(pg_migrated_database) -> None:
    """A missing run or unavailable writer lock is not reported as a rollback."""
    with pytest.raises(PublicationOutcomeUnknownError):
        resolve_publication(pg_migrated_database.owner_dsn, uuid4())
    with writer_connection(pg_migrated_database.owner_dsn), pytest.raises(PublicationOutcomeUnknownError):
        # The live advisory lock prevents a fresh resolver from reading a possibly changing marker.
        resolve_publication(pg_migrated_database.owner_dsn, uuid4())
