"""Shared schema constants for the postgres backend.

Single source of truth for product column lists and publication stage metadata,
so drift between caller modules produces a single, easy-to-find edit site.
"""

from __future__ import annotations

from typing import LiteralString

# Product columns. Every product kind (daily, weekly, monthly) shares this one
# column set in market.adjusted_bars; the leading period discriminator selects
# the kind. Daily bars are never truncated or rolled up, so their
# left_truncated/calendar_closed flags are false.
PRODUCT_COLUMNS: tuple[str, ...] = (
    "period",
    "ticker_id",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "left_truncated",
    "calendar_closed",
)

# Publication kinds (the table kinds) and the matching pg_temp stage names.
# Publication stages are created in `prepare_publication` and consumed by
# `publish_staged`. The copying allowlist enforces that arbitrary tables
# cannot be loaded.
STAGE_KINDS: tuple[str, ...] = ("daily", "weekly", "monthly")
STAGE_NAMES: frozenset[str] = frozenset(
    {"publication_daily_stage", "publication_weekly_stage", "publication_monthly_stage"}
)
PUBLICATION_TABLES: dict[str, str] = {
    "daily": "market.adjusted_bars",
    "weekly": "market.adjusted_bars",
    "monthly": "market.adjusted_bars",
}

# SQL fragments used by publication._validate_product_stage to build the per-kind
# SQL literal. PERIOD_TRUNC_SQL wraps the bound :lower parameter in a date_trunc
# function appropriate for the kind. PERIOD_FLAG_NULL_CHECK is the extra null check
# appended for non-daily kinds (daily flags are always false and need no check).
PERIOD_TRUNC_SQL: dict[str, LiteralString] = {
    "daily": "%s",
    "weekly": "date_trunc('week', %s)::date",
    "monthly": "date_trunc('month', %s)::date",
}
PERIOD_FLAG_NULL_CHECK: LiteralString = " OR left_truncated IS NULL OR calendar_closed IS NULL"
