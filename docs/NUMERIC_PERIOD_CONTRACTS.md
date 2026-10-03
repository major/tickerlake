# Numeric and period contracts

## Numeric precision

Daily extraction retains the source contract. Aggregate volume is stored as
Float64 so summing fractional volumes does not discard fractional values, and
transaction counts are Int64. Transaction totals outside the Int64 range are
rejected rather than wrapped or truncated. Price fields keep their existing
Float32 contract; Float64 computations can reduce additional rounding during
aggregation but cannot restore precision already lost when source prices were
stored as Float32. Results may differ slightly from older Float32 accumulation.

## Period rows and flags

Weekly and monthly aggregates use the retained raw history and are anchored by
an explicit collection lower bound and configured target date. The earliest
retained raw session is only a conservative lower bound: the database does not
persist the original requested collection bound. It cannot establish whether
earlier history was never collected, removed, or omitted for a ticker. A period
row is not a completeness claim, and ticker IPO gaps are not collection
metadata.

`left_truncated` is true when the collection lower bound is later than the
period's first scheduled XNYS session. `calendar_closed` is true when the
period's final scheduled XNYS session is on or before the configured target.
Future periods are not calendar-closed. Cached future rows are retained and
aggregated; the target controls period status, not history retention. Neither
flag proves that the vendor supplied every expected observation.

VWAP is the volume-weighted mean of daily VWAP values. It is null when the
period's total volume is zero or any positive-volume input has missing VWAP.
Missing VWAP on zero-volume inputs does not affect the weighted mean. It is
not inferred from OHLC prices.
