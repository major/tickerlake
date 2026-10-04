"""Validate raw Massive API objects and convert them into typed frames."""

import datetime
import logging
import math
from typing import TYPE_CHECKING, Any

import polars as pl

from tickerlake.outcomes import FetchOutcome, FetchStatus

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tickerlake.client import MassiveClient

logger = logging.getLogger(__name__)

DAILY_AGGS_SCHEMA = {
    "date": pl.Date,
    "ticker": pl.Utf8,
    "open": pl.Float32,
    "high": pl.Float32,
    "low": pl.Float32,
    "close": pl.Float32,
    "volume": pl.Float64,
    "vwap": pl.Float32,
    "transactions": pl.Int64,
}
SPLITS_SCHEMA = {
    "ticker": pl.Utf8,
    "execution_date": pl.Date,
    "split_from": pl.Float32,
    "split_to": pl.Float32,
    "adjustment_factor": pl.Float64,
    "adjustment_type": pl.Utf8,
}
TICKERS_SCHEMA = {
    "ticker": pl.Utf8,
    "name": pl.Utf8,
    "type": pl.Utf8,
    "primary_exchange": pl.Utf8,
    "cik": pl.Utf8,
    "active": pl.Boolean,
}


def _value(record: Any, key: str) -> Any:
    if isinstance(record, dict):
        return record.get(key)
    return getattr(record, key)


def _frame(rows: list[dict[str, Any]], schema: Mapping[str, Any]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(rows).cast(schema)  # ty: ignore[invalid-argument-type]
    return frame.select(list(schema))


def _canonical_finite(frame: pl.DataFrame, columns: tuple[str, ...]) -> bool:
    """Check cast floating values for infinities while allowing nullable fields."""
    for column in columns:
        values = frame[column].drop_nulls().to_list()
        if not all(math.isfinite(value) for value in values):
            return False
    return True


def _canonical_positive(frame: pl.DataFrame, columns: tuple[str, ...]) -> bool:
    """Ensure positive source ratios remain positive after canonical casts."""
    return all((frame[column] > 0).all() for column in columns)


def _outcome(
    status: FetchStatus,
    schema: Mapping[str, Any],
    *,
    date: datetime.date | None = None,
    diagnostic: str | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> FetchOutcome:
    try:
        frame = _frame(rows or [], schema)
    except pl.exceptions.PolarsError, OverflowError, TypeError, ValueError:
        return FetchOutcome(FetchStatus.quarantined, pl.DataFrame(schema=schema), date, "invalid_record")
    if schema is DAILY_AGGS_SCHEMA:
        float_columns = ("open", "high", "low", "close", "volume", "vwap")
    elif schema is SPLITS_SCHEMA:
        float_columns = ("split_from", "split_to", "adjustment_factor")
    else:
        float_columns = ()
    if float_columns and not _canonical_finite(frame, float_columns):
        return FetchOutcome(FetchStatus.quarantined, pl.DataFrame(schema=schema), date, "invalid_record")
    if schema is SPLITS_SCHEMA and not _canonical_positive(frame, ("split_from", "split_to", "adjustment_factor")):
        return FetchOutcome(FetchStatus.quarantined, pl.DataFrame(schema=schema), date, "invalid_record")
    return FetchOutcome(status, frame, date, diagnostic)


def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _strings_valid(values: tuple[Any, ...]) -> bool:
    return all(value is None or isinstance(value, str) for value in values)


def _daily_rows(records: list[Any], date: datetime.date) -> list[dict[str, Any]] | None:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        try:
            ticker = _value(record, "ticker")
            stamp = _value(record, "timestamp")
            if not _finite(stamp) or not stamp.is_integer():
                return None
            record_date = datetime.datetime.fromtimestamp(stamp / 1000, tz=datetime.UTC).date()
            o, h, low, close = (_value(record, key) for key in ("open", "high", "low", "close"))
            volume = _value(record, "volume")
            try:
                vwap = _value(record, "vwap")
            except AttributeError:
                vwap = None
            transactions = _value(record, "transactions")
            numeric = (o, h, low, close, volume) + (() if vwap is None else (vwap,))
            if (
                not isinstance(ticker, str)
                or not ticker.strip()
                or record_date != date
                or not all(_finite(value) for value in numeric)
                or not low <= min(o, close) <= max(o, close) <= h
                or volume < 0
                or not isinstance(transactions, int)
                or isinstance(transactions, bool)
                or not 0 <= transactions <= 2**63 - 1
                or ticker in seen
            ):
                return None
            seen.add(ticker)
            rows.append(
                {
                    "date": record_date,
                    "ticker": ticker,
                    "open": o,
                    "high": h,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "vwap": vwap,
                    "transactions": transactions,
                }
            )
        except AttributeError, TypeError, ValueError, OverflowError, OSError:
            return None
    return rows


def _previous_daily(previous: pl.DataFrame | None, date: datetime.date) -> pl.DataFrame:
    if previous is None or previous.is_empty():
        return pl.DataFrame(schema=DAILY_AGGS_SCHEMA)
    return previous.filter(pl.col("date") == date)


def extract_daily_aggs(
    client: MassiveClient, dates: list[datetime.date], *, previous: pl.DataFrame | None = None
) -> list[FetchOutcome]:
    """Fetch and validate each requested daily bar date independently."""
    outcomes: list[FetchOutcome] = []
    for date in dates:
        prior = _previous_daily(previous, date)
        try:
            records = client.fetch_daily_aggs(date)
        except Exception:  # noqa: BLE001
            logger.warning("Daily aggregate fetch failed for %s", date)
            outcomes.append(_outcome(FetchStatus.failed, DAILY_AGGS_SCHEMA, date=date, diagnostic="transport_error"))
            continue
        if not isinstance(records, (list, tuple)):
            outcomes.append(_outcome(FetchStatus.failed, DAILY_AGGS_SCHEMA, date=date, diagnostic="invalid_envelope"))
            continue
        rows = _daily_rows(records, date)
        if rows is None:
            outcomes.append(
                _outcome(FetchStatus.quarantined, DAILY_AGGS_SCHEMA, date=date, diagnostic="invalid_record")
            )
        elif not rows:
            status = FetchStatus.quarantined if prior.height else FetchStatus.successful_empty
            reason = "reference_shrink" if prior.height else None
            outcomes.append(_outcome(status, DAILY_AGGS_SCHEMA, date=date, diagnostic=reason))
        elif prior.height and not set(prior["ticker"].to_list()).issubset({row["ticker"] for row in rows}):
            outcomes.append(
                _outcome(FetchStatus.quarantined, DAILY_AGGS_SCHEMA, date=date, diagnostic="reference_shrink")
            )
        else:
            outcomes.append(_outcome(FetchStatus.populated, DAILY_AGGS_SCHEMA, date=date, rows=rows))
    return outcomes


def _split_rows(records: list[Any], start_date: datetime.date, end_date: datetime.date) -> list[dict[str, Any]] | None:
    rows = []
    seen: set[tuple[Any, ...]] = set()
    try:
        for record in records:
            row = {
                "ticker": _value(record, "ticker"),
                "execution_date": datetime.date.fromisoformat(_value(record, "execution_date")),
                "split_from": _value(record, "split_from"),
                "split_to": _value(record, "split_to"),
                "adjustment_factor": _value(record, "historical_adjustment_factor"),
                "adjustment_type": _value(record, "adjustment_type"),
            }
            factors = tuple(_value(record, key) for key in ("split_from", "split_to", "historical_adjustment_factor"))
            if (
                not isinstance(row["ticker"], str)
                or not row["ticker"].strip()
                or not start_date <= row["execution_date"] <= end_date
                or (row["adjustment_type"] is not None and not isinstance(row["adjustment_type"], str))
                or not all(_finite(value) and value > 0 for value in factors)
            ):
                return None
            key = tuple(row.values())
            if key in seen:
                return None
            seen.add(key)
            rows.append(row)
    except AttributeError, TypeError, ValueError, OverflowError:
        return None
    return rows


def _split_identity(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row[key] for key in SPLITS_SCHEMA)


def _validated_split_frame(rows: list[dict[str, Any]]) -> pl.DataFrame | None:
    """Return canonical split rows only when casts remain finite, positive, and unique."""
    outcome = _outcome(FetchStatus.populated, SPLITS_SCHEMA, rows=rows)
    if outcome.status is FetchStatus.quarantined:
        return None
    frame = outcome.frame
    identities = [_split_identity(row) for row in frame.to_dicts()]
    return frame if len(identities) == len(set(identities)) else None


def extract_splits(
    client: MassiveClient, start_date: datetime.date, end_date: datetime.date, *, previous: pl.DataFrame | None = None
) -> FetchOutcome:
    """Fetch and validate stock split events in an inclusive date range."""
    try:
        records = client.fetch_splits(start_date, end_date)
    except Exception:  # noqa: BLE001
        logger.warning("Split fetch failed")
        return _outcome(FetchStatus.failed, SPLITS_SCHEMA, diagnostic="transport_error")
    if not isinstance(records, (list, tuple)):
        return _outcome(FetchStatus.failed, SPLITS_SCHEMA, diagnostic="invalid_envelope")
    rows = _split_rows(records, start_date, end_date)
    frame = _validated_split_frame(rows) if rows is not None else None
    if frame is None:
        return _outcome(FetchStatus.quarantined, SPLITS_SCHEMA, diagnostic="invalid_record")
    rows = frame.to_dicts()
    if previous is not None and not previous.is_empty():
        scoped = previous.filter(pl.col("execution_date").is_between(start_date, end_date, closed="both"))
        old = {_split_identity(row) for row in scoped.to_dicts()}
        try:
            current = _frame(rows, SPLITS_SCHEMA)
        except pl.exceptions.PolarsError, OverflowError, TypeError, ValueError:
            return _outcome(FetchStatus.quarantined, SPLITS_SCHEMA, diagnostic="invalid_record")
        new = {_split_identity(row) for row in current.to_dicts()}
        if not old.issubset(new):
            return _outcome(FetchStatus.quarantined, SPLITS_SCHEMA, diagnostic="reference_change")
    return _outcome(FetchStatus.populated if rows else FetchStatus.successful_empty, SPLITS_SCHEMA, rows=rows)


def _ticker_rows(records: list[Any]) -> list[dict[str, Any]] | None:
    rows = []
    seen: set[str] = set()
    try:
        for record in records:
            row = {key: _value(record, key) for key in TICKERS_SCHEMA if key not in {"name", "cik"}}
            for optional in ("name", "cik"):
                try:
                    row[optional] = _value(record, optional)
                except AttributeError:
                    row[optional] = None
            ticker = row["ticker"]
            if not isinstance(ticker, str) or not ticker.strip() or ticker in seen:
                return None
            if not isinstance(row["active"], (bool, type(None))) or not _strings_valid(
                tuple(row[key] for key in ("name", "type", "primary_exchange", "cik"))
            ):
                return None
            seen.add(ticker)
            rows.append(row)
    except AttributeError, TypeError, ValueError:
        return None
    return rows


def extract_tickers(client: MassiveClient, types: list[str], *, previous: pl.DataFrame | None = None) -> FetchOutcome:
    """Fetch and validate ticker reference records."""
    try:
        records = client.fetch_tickers(types)
    except Exception:  # noqa: BLE001
        logger.warning("Ticker fetch failed")
        return _outcome(FetchStatus.failed, TICKERS_SCHEMA, diagnostic="transport_error")
    if not isinstance(records, (list, tuple)):
        return _outcome(FetchStatus.failed, TICKERS_SCHEMA, diagnostic="invalid_envelope")
    rows = _ticker_rows(records)
    if rows is None:
        return _outcome(FetchStatus.quarantined, TICKERS_SCHEMA, diagnostic="invalid_record")
    if (
        previous is not None
        and not previous.is_empty()
        and not set(previous["ticker"].to_list()).issubset({row["ticker"] for row in rows})
    ):
        return _outcome(FetchStatus.quarantined, TICKERS_SCHEMA, diagnostic="reference_shrink")
    return _outcome(FetchStatus.populated if rows else FetchStatus.successful_empty, TICKERS_SCHEMA, rows=rows)
