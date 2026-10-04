"""Shared schema constants for the postgres backend.

Single source of truth for product column lists and publication stage metadata,
so drift between caller modules produces a single, easy-to-find edit site.
"""

from __future__ import annotations

# Product columns. BASE_COLUMNS are common to daily, weekly, and monthly.
# PERIOD_COLUMNS adds the two flags that only exist on weekly/monthly rolls.
BASE_COLUMNS: tuple[str, ...] = (
    "ticker_id",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "sma_20",
    "sma_50",
    "sma_200",
    "atr_14",
    "atr_pct",
    "adr_pct",
    "volume_sma_20",
)
PERIOD_COLUMNS: tuple[str, ...] = (*BASE_COLUMNS, "left_truncated", "calendar_closed")

# Publication kinds (the table kinds) and the matching pg_temp stage names.
# Publication stages are created in `prepare_publication` and consumed by
# `publish_staged`. The copying allowlist enforces that arbitrary tables
# cannot be loaded.
STAGE_KINDS: tuple[str, ...] = ("daily", "weekly", "monthly")
STAGE_NAMES: frozenset[str] = frozenset(
    {"publication_daily_stage", "publication_weekly_stage", "publication_monthly_stage"}
)
PUBLICATION_TABLES: dict[str, str] = {
    "daily": "market.adjusted_daily",
    "weekly": "market.adjusted_weekly",
    "monthly": "market.adjusted_monthly",
}
