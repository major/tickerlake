"""Typed results from external data fetch and validation."""

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime

    import polars as pl


class FetchStatus(StrEnum):
    """Outcome category for a requested extraction."""

    failed = "failed"
    quarantined = "quarantined"
    populated = "populated"
    successful_empty = "successful_empty"


@dataclass(frozen=True)
class FetchOutcome:
    """A fetch result, including safe diagnostics for non-successful data."""

    status: FetchStatus
    frame: pl.DataFrame
    requested_date: datetime.date | None = None
    diagnostic: str | None = None
