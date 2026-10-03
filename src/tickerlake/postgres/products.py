"""Deterministic products built from canonical PostgreSQL input frames."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Mapping

from tickerlake.extract import DAILY_AGGS_SCHEMA, SPLITS_SCHEMA
from tickerlake.transform import (
    adjust_splits,
    aggregate_to_monthly,
    aggregate_to_weekly,
    compute_metrics,
)

_DAILY_COLUMNS = (
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
_PERIOD_COLUMNS = (
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
    "left_truncated",
    "calendar_closed",
)
_METRIC_SCHEMA = {
    "sma_20": pl.Float32,
    "sma_50": pl.Float32,
    "sma_200": pl.Float32,
    "atr_14": pl.Float32,
    "atr_pct": pl.Float32,
    "adr_pct": pl.Float32,
    "volume_sma_20": pl.Float64,
}


@dataclass(frozen=True, slots=True, kw_only=True)
class ProductBatch:
    """One batch of daily, weekly, and monthly consumer products."""

    daily: pl.DataFrame
    weekly: pl.DataFrame
    monthly: pl.DataFrame


def _require_frame(
    frame: pl.DataFrame,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
) -> None:
    if not isinstance(frame, pl.DataFrame) or frame.schema != schema:
        raise ValueError("Invalid")


def _validate_split_factors(splits: pl.DataFrame) -> pl.DataFrame:
    if splits.is_empty():
        return splits
    conflicts = (
        splits.group_by("ticker", "execution_date")
        .agg(pl.col("adjustment_factor").n_unique().alias("factors"))
        .filter(pl.col("factors") > 1)
    )
    if conflicts.height:
        raise ValueError("Conflict")
    return splits.unique(maintain_order=True)


def _finite_outputs(frame: pl.DataFrame) -> None:
    for name, dtype in frame.schema.items():
        if dtype.is_float() and frame.filter(pl.col(name).is_not_null() & ~pl.col(name).is_finite()).height:
            raise ValueError("Nonfinite")


def _with_ticker_ids(frame: pl.DataFrame, identity_map: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.join(identity_map.rename({"symbol": "ticker"}), on="ticker", how="inner", validate="m:1")
        .drop("ticker")
        .select("ticker_id", pl.exclude("ticker_id"))
    )


def _empty(
    columns: tuple[str, ...],
    base_schema: Mapping[str, pl.DataType | type[pl.DataType]],
) -> pl.DataFrame:
    fields = {"ticker_id": pl.Int32, **base_schema}
    fields.update(_METRIC_SCHEMA)
    fields = {name: fields[name] for name in columns}
    return pl.DataFrame(schema=fields)


def _validate_inputs(
    raw: pl.DataFrame,
    splits: pl.DataFrame,
    identities: pl.DataFrame,
) -> pl.DataFrame:
    _require_frame(raw, DAILY_AGGS_SCHEMA)
    _require_frame(splits, SPLITS_SCHEMA)
    if identities.schema != {"ticker_id": pl.Int32, "symbol": pl.String}:
        raise ValueError("Invalid")
    identity_rows = identities.select("ticker_id", "symbol")
    if identity_rows.filter((pl.col("ticker_id") <= 0) | (pl.col("symbol").str.strip_chars() == "")).height:
        raise ValueError("Invalid")
    if (
        identity_rows.get_column("ticker_id").n_unique() != identity_rows.height
        or identity_rows.get_column("symbol").n_unique() != identity_rows.height
    ):
        raise ValueError("Invalid")
    symbols = set(identity_rows["symbol"].to_list())
    if not set(raw["ticker"].to_list()).issubset(symbols) or not set(splits["ticker"].to_list()).issubset(symbols):
        raise ValueError("Invalid")
    return identity_rows


def build_products(
    raw: pl.DataFrame,
    splits: pl.DataFrame,
    identities: pl.DataFrame,
    *,
    collection_start: datetime.date,
    target: datetime.date,
) -> ProductBatch:
    """Build complete-history consumer products for a durable identity batch.

    Identity frames contain ``ticker_id`` (positive Int32) and ``symbol``. Raw
    and split frames use the canonical extraction schemas and symbol column.
    """
    if not isinstance(collection_start, datetime.date) or isinstance(collection_start, datetime.datetime):
        raise TypeError
    if not isinstance(target, datetime.date) or isinstance(target, datetime.datetime) or collection_start > target:
        raise ValueError("Invalid")
    identity_rows = _validate_inputs(raw, splits, identities)
    if raw.is_empty():
        return ProductBatch(
            daily=_empty(_DAILY_COLUMNS, DAILY_AGGS_SCHEMA),
            weekly=_empty(
                _PERIOD_COLUMNS, DAILY_AGGS_SCHEMA | {"left_truncated": pl.Boolean, "calendar_closed": pl.Boolean}
            ),
            monthly=_empty(
                _PERIOD_COLUMNS, DAILY_AGGS_SCHEMA | {"left_truncated": pl.Boolean, "calendar_closed": pl.Boolean}
            ),
        )
    canonical_splits = _validate_split_factors(splits)
    adjusted = adjust_splits(raw, canonical_splits).sort(["ticker", "date"])
    duplicate_bars = adjusted.select(pl.struct("ticker", "date").is_duplicated().any()).item()
    if duplicate_bars:
        raise ValueError("Duplicate")
    _finite_outputs(adjusted)
    metrics = compute_metrics(adjusted)
    daily = _with_ticker_ids(adjusted.join(metrics, on=["ticker", "date"], how="inner", validate="1:1"), identity_rows)
    weekly = _with_ticker_ids(
        aggregate_to_weekly(adjusted, collection_start=collection_start, target=target), identity_rows
    )
    monthly = _with_ticker_ids(
        aggregate_to_monthly(adjusted, collection_start=collection_start, target=target), identity_rows
    )
    for period_name, period_frame in (("weekly", weekly), ("monthly", monthly)):
        period_metrics = compute_metrics(period_frame.rename({"ticker_id": "ticker"})).rename({"ticker": "ticker_id"})
        measured_frame = period_frame.join(period_metrics, on=["ticker_id", "date"], how="inner", validate="1:1")
        if period_name == "weekly":
            weekly = measured_frame
        else:
            monthly = measured_frame
    for result in (daily, weekly, monthly):
        _finite_outputs(result)
    return ProductBatch(
        daily=daily.select(_DAILY_COLUMNS),
        weekly=weekly.select(_PERIOD_COLUMNS),
        monthly=monthly.select(_PERIOD_COLUMNS),
    )
