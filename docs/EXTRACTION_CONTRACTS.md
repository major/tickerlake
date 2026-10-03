# Extraction outcome and publication contracts

## Fetch results

Every extraction reports an explicit outcome. `populated` means a validated
non-empty frame; `successful_empty` means a validated empty response; `failed`
means the request or decoding failed; and `quarantined` means decoded data did
not pass validation. Failed and quarantined outcomes include safe diagnostic
evidence. An outcome is scoped to its requested date for daily bars and to the
whole request for reference data.

An anomaly is evidence for review, not proof that source data is incomplete.
Row-count changes, missing tickers, and empty responses do not alone establish
completeness or incompleteness. No universal thresholds are implied by these
contracts.

## Cache and consumer behavior

Only populated daily results can replace cached dates, and the replacement
uses the explicit requested date, not dates inferred from rows. Failed,
quarantined, and successful-empty results preserve any prior cached date.
Every expected trading date must have an acceptable result before rebuilding
the consumer database. If a run has failures or suspicious results, any
successful raw replacements are persisted first, then the run fails without
publishing a new consumer database. This interim DuckDB workflow has separate
raw and consumer files and is not a cross-file atomic transaction.

Ticker and split outcomes are both validated before split cache writes or
consumer publication. Failed or quarantined reference results block a rebuild;
a successful-empty ticker result also blocks it. Successful-empty splits are
valid for bootstrap, but do not clear a previously non-empty split cache.
Ticker anomaly checks compare only with previously published metadata in the
requested ticker-type scope, so deliberately excluded types do not trigger a
shrink decision. A detected metadata omission leaves the complete existing
consumer generation and split cache intact.

Split fetch coverage for a rebuild spans retained raw history and any relevant
cached split dates, as well as the configured range. A narrower correction
range must not cause older split events to disappear while all retained bars
are transformed. Changes to a cumulative split factor are conservatively
quarantined; this contract defines no validation bypass or approval workflow.

## Future PostgreSQL contract

Raw bar replacements and validated ticker-reference or split-content changes
must advance a cache `input_revision` atomically with those changes. A run
captures its input revision and checks it again before publication. All public
products and a single run-ID generation publication marker become visible in
one transaction. Staging can be disposable and progressively loaded, but
publication is all-or-nothing. Fetch outcomes are operational evidence, not
retry checkpoints. This design does not promise exact replay of past cache or
reference versions.

There is no raw/reference archive, approval workflow, or operator adjudication
machinery in this contract. The current DuckDB implementation is an interim
flow and cannot provide PostgreSQL's atomic cache revision and publication
guarantees.
