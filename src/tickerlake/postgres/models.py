"""Shared immutable PostgreSQL lane request and state models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import date
    from uuid import UUID


@dataclass(frozen=True, slots=True, kw_only=True)
class RunSpec:
    """Versions and date scope for one run."""

    target: date
    requested_start: date | None
    requested_end: date | None
    version: str


@dataclass(frozen=True, slots=True, kw_only=True)
class CacheState:
    """Input revision and retained date bounds."""

    input_revision: int
    retained_start: date | None
    retained_end: date | None


@dataclass(frozen=True, slots=True, kw_only=True)
class FetchRequest:
    """Source and requested scope for one fetch."""

    run_id: UUID
    source: Literal["daily", "tickers", "splits"]
    requested_date: date | None = None
    requested_start: date | None = None
    requested_end: date | None = None
    ticker_types: tuple[str, ...] = ()
