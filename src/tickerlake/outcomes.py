"""Typed results from external data fetch and validation."""

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    import datetime

    import polars as pl

    from tickerlake.postgres.models import FetchRequest


class FetchStatus(StrEnum):
    """Outcome category for a requested extraction."""

    failed = "failed"
    quarantined = "quarantined"
    populated = "populated"
    successful_empty = "successful_empty"


# Statuses that count as a successful fetch outcome.
_ACCEPTED_STATUSES: Final = frozenset({FetchStatus.populated, FetchStatus.successful_empty})


@dataclass(frozen=True)
class FetchOutcome:
    """A fetch result, including safe diagnostics for non-successful data."""

    status: FetchStatus
    frame: pl.DataFrame
    requested_date: datetime.date | None = None
    diagnostic: str | None = None

    def is_populated(self) -> bool:
        """True iff this outcome carries new validated rows."""
        return self.status is FetchStatus.populated

    def is_accepted(self) -> bool:
        """True iff this outcome counts as a successful fetch."""
        return self.status in _ACCEPTED_STATUSES

    def matches_daily_request(self, request: FetchRequest) -> bool:
        """True iff this outcome's requested_date matches a daily request's date.

        An outcome without a requested_date is trivially matching. A populated
        requested_date only matches when the request is a daily source and the
        dates are equal.
        """
        if self.requested_date is None:
            return True
        return request.source == "daily" and self.requested_date == request.requested_date
