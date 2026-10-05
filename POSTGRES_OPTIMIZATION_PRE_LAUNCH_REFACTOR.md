# PostgreSQL Pre-Launch Simplification & Optimization Refactor 🐙

**Status:** Pre-deployment, no live systems. The full migration history can be rewritten wholesale; any breaking change is on the table.

**Reviewer:** @oracle
**Date:** 2026-10-04
**Scope:** `src/tickerlake/postgres/` (~2,100 lines Python) + `src/tickerlake/migrations/` (3 SQL files)
**Scale context:** ~12k tickers, 10y history → `ingest.raw_daily` ≈ 30M rows. Every `update` run re-stages and re-publishes complete history (daily+weekly+monthly ≈ 35-40M rows through Python per run). That fact drives most of the performance section.

---

## Top 5 wins 🏆

### 1. Collapse the three publication stage tables into one `publication_bars_stage`, and publish in one INSERT + one DELETE

All three stages have byte-identical columns (`_schema.py:32-40` maps daily/weekly/monthly to the same target table, `PRODUCT_COLUMNS` already carries the `period` discriminator). Yet `_publish_kind` (`publication.py:432-470`) runs three separate `INSERT ... ON CONFLICT` + three `DELETE ... NOT EXISTS` passes against `market.adjusted_bars`, `stage_batch` copies and SQL-scans three tables per batch, and `_validate_product_stage` runs three times.

With one merged stage:

- One upsert (period is just a PK column)
- One DELETE with a `CASE period` lower bound
- One validation query with a `CASE` for the date_trunc bound and the flag null-check
- `STAGE_NAMES`, `STAGE_KINDS`, `PUBLICATION_TABLES`, `_STAGES`, `_STAGE_COLUMNS`, and most of the `sql.SQL` composition machinery in `_schema.py`/`publication.py` shrink or vanish

**Effort:** Medium (a focused day, mostly mechanical; the per-kind SQL fragments fold into CASE expressions).

---

### 2. Wrap the whole rebuild + publish in a single transaction; delete `pg_temp.publication_context` and `publication_scope.complete`

Nothing outside the session ever reads the stages, temp-table writes generate no WAL, and the per-batch transaction (`publication.py:234`) therefore protects nothing: a mid-rebuild failure has no retry path, so partial staged state has no value.

One transaction from `prepare_publication` through `publish_staged` means:

- Stages become `ON COMMIT DROP`
- `_verify_staged_context` (run per batch today, ~120+ pointless round-trips at batch_size=100) disappears because `BuildContext` lives and dies inside one atomic unit, and durable re-verification already happens in `_verify_run_state` (`publication.py:408-429`)
- The `complete` boolean dies because rows inserted `false` and flipped `true` in the same transaction can never persist as `false` (`publication.py:234-257`). Keep `publication_scope` as a bare `ticker_id` set (genuinely needed to represent "processed, zero bars" tickers that drive DELETE scoping).
- The `PublicationOutcomeUnknown` machinery stays, guarding the now-single COMMIT.

**Effort:** Medium; depends on win #1 landing first for a clean diff.

---

### 3. Strip the redundant validation layers; keep exactly one validation boundary inside the publish transaction

The same invariants are checked three to four times:

- Polars-side in `products.py:79-99` (`_validate_inputs` subset/uniqueness)
- Per batch in `_checked_batch` (`publication.py:275-279`, an extra `market.ticker` round-trip per batch re-proving what `read_ticker_batch` just read)
- Per batch again in SQL (`publication.py:246-256` null/negative scans of freshly copied rows)
- Set-based at publish in `_validate_stages` (`publication.py:352-388`) which subsumes all of it, including the `_require_key_match` EXCEPT against `ingest.raw_daily`

Only the publish-time check matters: it is inside the atomic transaction and set-based. Value-level checks (OHLC, nulls, volume >= 0) are additionally redundant with the `market.finite_real`/`finite_volume` domains and `adjusted_bars_ohlc_check`. Once the safe-error wrapper is thinned (win #4), a CHECK violation surfaces with a precise constraint name anyway.

Keep the integrity checks (key match vs raw, scope completeness, metadata drift); delete the value checks and the per-batch round-trips.

Same for the `type(x) is not int` guards (`reading.py:56,96,98`, `publication.py:273`, `rebuild.py:38`, `backfill.py:124`): these values come from the database or from already-validated config inside one process. Validate at real boundaries (CLI, DB reads), trust internal types.

**Effort:** Medium, large line reduction, no loss of atomicity.

---

### 4. Thin the "safe error" wrapper to a single boundary

There are ~30 sites of `except psycopg.Error: raise PostgresWriterError("...") from None` across `state.py`, `raw.py`, `references.py`, `reading.py`, `publication.py`, `migrations.py`. The `from None` destroys the root cause even in logs, so a production failure yields "Could not store daily fetch outcome" with zero Postgres detail. psycopg error messages do not carry the DSN or API key, so the scrubbing buys almost nothing.

**Recommendation:** Keep `PostgresWriterError` as the public exception type, but wrap once at the `backfill`/`pipeline` boundary with `raise PostgresWriterError(context) from error`, and let inner layers propagate `psycopg.Error` directly. This also unblocks win #3 (CHECK violations become self-describing).

**Effort:** Small-medium, mostly deletions; biggest debuggability gain in the codebase.

---

### 5. Schema pruning batch (pre-deployment, so rewrite migrations wholesale): fold `ingest.ticker_reference` into `market.ticker`, drop `adjustment_type`, drop `split_event_ticker_idx`, kill the dead `'completed'` state

- **`market.ticker`** already carries name/ticker_type/primary_exchange/cik/active (`0001:10-19`), mirrored from `ticker_reference` (`0001:34-41`) at publication (`publication.py:530-536`) and policed by `_validate_staged_metadata` (`publication.py:333-349`) plus a 7-column temp mirror in `publication_ticker_stage`. Writing reference data straight into `market.ticker` in `references._store_tickers` removes a table, the mirror columns, the metadata-drift validation, and the EXCEPT-diff ceremony.

- **`adjustment_type`** admits only NULL or `'forward'` (`0001:52`) and is never read by `transform.adjust_splits` (it uses `execution_date` + `adjustment_factor` only). It is the sole reason the natural key needs `UNIQUE NULLS NOT DISTINCT`. Dropping it yields a plain all-NOT-NULL `UNIQUE(ticker_id, execution_date, split_from, split_to, adjustment_factor)`.

- **`split_event_ticker_idx`** (`0002:2`) is a strict prefix duplicate of that unique index's leading column: pure write amplification.

- **`'completed'` in `ingest.run.state`** (`0001:78,86`) is never written by any code path.

Since no deployment exists, squash all of this into rewritten 0001-0003 rather than stacking ALTERs.

**Effort:** Medium (touches extract/reading/references schemas and tests).

---

## Schema simplification opportunities 🗄️

### HIGH severity

- **Drop `split_event_ticker_idx`** (`0002:2`): redundant with the natural-key UNIQUE index prefix (`0001:56-57`). Queries `ticker_id = ANY(...)` use the unique index.
- **Drop `adjustment_type`** everywhere (schema `0001:52,57`, `extract.py:34,197,204`, `references.py:26-28,80-97,131`, `reading.py:21,132,152`). Never semantically consumed; forces `NULLS NOT DISTINCT`; complicates dedup identity in three places.
- **Replace `market.latest_daily`** (`0002:21-28`) with a view over `adjusted_bars JOIN market.ticker` (`DISTINCT ON`/LATERAL, backward index scan on PK `(period, ticker_id, date)` is index-supported). Removes: the table, its grants, its entire 0003 domain-migration section (`0003:60-78`), `_refresh_latest_daily` (`publication.py:473-501`, one UPSERT + one scope-joined DELETE per publication), and its validation surface. No consumer exists yet; if dashboard latency ever matters, promote to a materialized view refreshed by one statement inside the publish transaction. Do not store a "latest pointer" on `market.ticker`, that is the same denormalization with worse ergonomics.
- **Fold `ingest.ticker_reference` into `market.ticker`** (see Top 5 #5). Trade-off to accept: metadata becomes visible at store time rather than publication-atomic. For name/exchange/active that is fine; bars remain revision-atomic. The per-type replacement becomes "upsert staged rows + null out metadata for in-scope-type tickers absent from the stage" (today the DELETE-by-type + LEFT JOIN achieves the same via `references.py:246-252` and `publication.py:176-177`). The type-change conflict guard (`references.py:218-226`) ports directly to `market.ticker.ticker_type`.

### MED severity

- **Remove dead `'completed'`** from `ingest.run.state` CHECK and its quad-state timestamp CHECK branch (`0001:78,85-88`). States actually used: running/failed/published. This also simplifies the `ended_at`/`published_at`/`failure_code` tri-state reasoning: `published_at` becomes redundant with `ended_at` (they are set to the same `statement_timestamp()` in `publication.py:515-516`). Drop `published_at`, keep `ended_at` + `failure_code`.
- **Keep `ingest.raw_session`** but stop re-joining `fetch_manifest` to re-prove acceptance (`publication.py:154-161` and `415-420`): `raw.py:181-187` only writes `raw_session` in the same transaction as a `populated` daily manifest, so `EXISTS (SELECT 1 FROM raw_session WHERE date=%s)` carries the same information. The table itself earns its keep; existence in `raw_daily` is not equivalent (stale rows survive a non-populated refetch).
- **`ingest.cache_state` single-row sentinel**: keep (idiomatic, cheap). But the double revision capture, `start_run` reads it (`state.py:126-144`), `capture_run_inputs` re-reads and updates (`state.py:150-168`), `prepare_publication` and `_verify_run_state` both check equality (`publication.py:151`, `421-424`), is ceremony under the global advisory lock: no other process can advance the revision between capture and publish. Capture once inside the publish transaction (which already takes `FOR UPDATE`), and keep `input_revision` on `run` purely as recorded provenance. Same for `market.publication_state`: keep the sentinel, it is the recovery anchor for `resolve_publication`.
- **GRANT/REVOKE** (`0001:110-137`, `0002:38-42`): migrations hardcode three role names and fail if the roles do not exist (deployment coupling baked into schema). Postgres tables are default-deny for non-owners, so the REVOKE wall only matters against pre-existing grants. No consumer exists, so `tickerlake_reader` is speculative (YAGNI). Recommend: grants for one ETL role only, guarded by a `DO` block checking `pg_roles`, or managed out-of-band entirely. Reintroduce a reader role when a reader exists.

### LOW severity

- **`market.ticker.cik` CHECK** (`0001:17`): `'^[0-9]+$' OR '^[A-Za-z0-9-]+$'`, the second regex subsumes the first. Dead disjunct; simplify to the length + `[A-Za-z0-9-]` check.
- **Keep `market.finite_real`/`finite_volume`/`is_valid_ohlc`** (`0003:6-22`). This is the rare helper that pays: applied uniformly to three tables and reused by stage validation (`publication.py:323`). Raw CHECKs vs domains is a wash; domains win on DRY here.
- **Keep the `period` discriminator on `adjusted_bars`**. Separate tables would triple publish code for zero query gain; a parent-row layout is over-engineering. If a consumer ever queries "all tickers, one date," add `(period, date)` then, not now.
- **`left_truncated`/`calendar_closed` always-false on daily rows**: ~2 bytes/row, and the uniform column set is what enables the merged single-stage publish (win #1). Keep, and make the invariant durable with a CHECK: `(period <> 'daily') OR (NOT left_truncated AND NOT calendar_closed)`, free enforcement of what `products.py:132-136` asserts in Python today.
- **Indexes on the publication hot path are actually right**: `raw_daily` PK `(date, ticker_id)` serves `read_raw_date` + the date-window validation scans; `raw_daily_ticker_date_idx (ticker_id, date)` (`0002:1`) serves `read_raw_history`'s per-ticker range scans. Both orders are used; keep both. No index is missing for the publish DELETE pattern (PK leads with `period`).

---

## Code simplification opportunities 🧹

### HIGH severity

- **Safe-error wrapper thinning** (Top 5 #4). Single wrap point at the boundary, `from error` chaining, inner layers propagate.
- **Merge the three publication stages** (Top 5 #1) and fold `_validate_product_stage` ×3 into one query; the `PERIOD_TRUNC_SQL`/`PERIOD_FLAG_NULL_CHECK` fragment machinery in `_schema.py:46-51` collapses into CASE expressions.
- **Drop per-batch defenses**: `_checked_batch`'s durable-identity round-trip (`publication.py:275-279`) and the post-copy SQL null scans (`publication.py:243-256`); `_verify_staged_context` per `stage_batch` call (`publication.py:231`, `rebuild.py:64`). Publish-time `_validate_stages` remains the single gate.

### MED severity

- **Single transaction for rebuild+publish** (Top 5 #2): removes `publication_context` creation/verification (`publication.py:184-197`, `110-127`), `ON COMMIT PRESERVE ROWS` bookkeeping, and the `_require_idle_writer` status dance between batches.
- **Delete `_calendar_year_windows`** (`backfill.py:187-200`): windowing does not reduce total bytes fetched (the client paginates anyway, `client.py:27-65`), split volume over a decade is tiny, and `_store_splits` replaces each window wholesale regardless. One window over the `_split_bounds` union; delete the slicer and loop. Revisit only if the API imposes per-request range caps.
- **`_split_bounds`** (`backfill.py:161-184`): the starts/ends union is clear Python and does two cheap reads; it could be one SQL query, but the win is cosmetic. Low priority; simplify only while touching the file.
- **`require_writer_connection`** (`connection.py:69-88`): the `pg_locks` EXISTS round-trip runs before every storage operation. An advisory lock cannot be lost while the session lives, `connection.closed` covers death, and `_ACTIVE_WRITERS` membership proves this process took the lock. Keep the three cheap checks; drop the pg_locks query.
- **`BackfillIncompleteError.outcomes`** (`backfill.py:85-88`): `fetch_manifest` already persists status + diagnostic per date durably (`state.py:241-260`). The exception pins whole `FetchOutcome`s (including Polars frames) for a log message. Carry a precomputed summary string (dates + statuses) instead; `_outcome_summary` becomes the constructor of that string and nothing else.

### LOW severity

- **`del previous, outcome`** (`backfill.py:229,249,270`) and **`del products, splits, raw, identities`** (`rebuild.py:66`): noise. Loop variables rebind each iteration; CPython frees eagerly. Not hiding a leak. Delete the dels.
- **`state.py` `_SCOPE_OK` dict-of-callables** (`state.py:81-85`): fine as a dispatch, but `_tickers_scope_ok` raises (via `require_unique_nonempty_strings`) while its siblings return bool, a predicate with inconsistent failure modes. Make all three raise with the specific message, or all three return bool.
- **`record_fetch_outcome`'s RETURNING + `timestamps[2] < timestamps[1]` check** (`state.py:245-263`): redundant with the `finished_at >= started_at` CHECK (`0001:107`). Return `manifest_id` only. Also `FetchRequest.started_at` (`models.py:42`) is never set by any caller (always COALESCEd to `statement_timestamp()`); drop the field.
- **`PublicationOutcomeUnknown = PublicationOutcomeUnknownError` alias "for API compatibility"** (`publication.py:96-97`): pre-deployment, there is no external API. Pick one name, delete the alias.
- **`info.py`**: it is used (`__init__.py:45` registers the subcommand and `pipeline.info` dispatches it); AGENTS.md's claim that `info` was removed is stale (fix the docs). If it stays: `_read_table_counts` issues nine sequential `count(*)` queries (`info.py:131-147`), two of which are full scans of the biggest tables at scale. One UNION ALL query, or `reltuples` estimates, or drop the big-table counts.
- **`reading.py` `_frame()` empty-DataFrame handling** (`reading.py:44-49`): keep as-is. Every consumer is Polars-native (`.is_empty()`, joins in `products.py`); returning bare lists would push schema reconstruction onto each caller.

---

## Performance opportunities ⚡

### HIGH severity

- **The COPY path is protocol-correct but row-by-row**: `copying.py:51-53` iterates `frame.iter_rows()` + `write_row` per row in Python. At publication scale (~35-40M staged rows per rebuild) this is the dominant serial cost on the write side. Pragmatic fix: serialize with Polars at Rust speed and hand bytes to text-mode COPY (`copy.write(buffer)` from `write_csv`), expecting a 5-10× improvement. Caveat: set CSV float precision high enough for `double precision` volume round-trips, or use `FORMAT binary` with an Arrow-backed encoder for lossless + faster. Benchmark before/after; this is the first thing to profile.
- **The read side has the same shape**: `_query` (`reading.py:33-41`) does `fetchall()` → list of Python tuples → `pl.DataFrame(orient="row")`, i.e. 30M rows materialized through Python tuples per rebuild for `read_raw_history`. Chunked fetch into Arrow or `write_database`-style columnar transfer would cut this substantially. Measure alongside the COPY fix; they are the same pipeline.

### MED severity

- **The real algorithmic win**: `update` refetches ~5 raw days but `rebuild_cache` re-reads, re-transforms, and re-upserts complete history for every ticker every run (`rebuild.py:50-65`; stages carry the full retained window). Incremental publication (re-stage only tickers/dates affected since the last published revision: a split change invalidates one ticker's full history; a daily change invalidates recent daily rows plus the containing week/month) turns a daily O(universe × decade) job into O(changed). This is genuine design work and the current full-rebuild is what makes correctness obvious, so: measure first, then pursue. Everything else on this list is small next to it.
- **Publication statements are already set-based** (good): `INSERT ... ON CONFLICT DO UPDATE ... WHERE ROW(...) IS DISTINCT FROM ROW(...)` avoids no-op tuple kills, DELETE is a scoped `NOT EXISTS`. `MERGE` would not beat it here. With win #1 the statement count drops from 6 to 2 per publish. Keep the DISTINCT-guard upsert pattern.
- **Global advisory lock** (`connection.py:12,49`): keep it. It is the correct KISS posture for a single-writer cron ETL, and the alternatives do not actually buy concurrency today: `advance_cache_revision` is a single hot row (`state.py:213-221`) and `store_daily_outcome` does date-scoped delete+insert, so parallel ingesters would serialize on `cache_state` anyway. If parallelism is ever needed, the revision-equality gate (`publication.py:421-424`) is already the optimistic-concurrency hook; per-revision advisory locks would slot in then. Do not pre-build it.

### LOW severity

- **`_require_key_match`** (`publication.py:292-303`): three EXCEPT-pair scans over the retained raw window per publish. Fine at current scale; at 30M+ rows the daily one is a large sort/hash. If it hurts, compare counts + a checksum aggregate for the daily key match and keep full EXCEPT only for the weekly/monthly derivations.
- **`_validate_stages` metadata EXCEPT** (`publication.py:333-349`) and the **`publication_ticker_stage` snapshot copy** (`publication.py:173-179`): ~12k rows, negligible. The metadata validation disappears entirely with the `ticker_reference` fold-in.
- **`info.py` `count(*)` on the two biggest tables** (`info.py:131-147`): minutes-long full scans at scale for a diagnostic. Use estimates or drop those counts.

---

## Risks and things to NOT change 🛡️

- **The autocommit + advisory-lock + "network calls with no open transaction" architecture** (`backfill.py:1-7`, `connection.py:31-66`). This is the core correctness insight: no idle-in-transaction across Massive API latency. Even with the single-transaction rebuild (which is pure DB+CPU work, no network), keep fetch/store phases exactly as they are.
- **Keyset paging in `read_ticker_batch`/`rebuild_cache`** (`reading.py:94-106`, `rebuild.py:48-66`): correct pattern. OFFSET degrades quadratically; a SQL cursor would pin a long transaction and fight the autocommit design. The `after_id` bookkeeping is one line, not a problem.
- **`_commit_is_known_rollback` / `PublicationOutcomeUnknownError` / `resolve_publication`** (`publication.py:391-405`, `521-572`): handles the genuinely ambiguous COMMIT (connection lost mid-commit). Subtle, tested, load-bearing for crash recovery. Keep even after everything else collapses to one transaction.
- **EXCEPT-based change detection in `references.py`** (`230-244`, `265-276`): it exists to avoid revision churn and needless DELETE+INSERT rewrite on unchanged refetches. MERGE would not simplify the change-detection decision. Keep (port it to `market.ticker` when folding).
- **`ingest.raw_session`**: existence in `raw_daily` is not equivalent. Stale rows survive a non-populated refetch, and the acceptance pointer is what makes `_TARGET_UNACCEPTED` meaningful. Keep the table; drop only the redundant manifest join.
- **The `previous=` refetch guards** (`extract.py` reference_shrink / reference_change quarantines): data-loss protection against API regressions. Keep.
- **Raw (`ingest.*`) vs published (`market.*`) separation**: this is what makes the atomic full rebuild safe. Never merge the lanes, even while folding `ticker_reference` into `market.ticker` (that fold moves published metadata earlier, it does not move bars).
- **Domains + `is_valid_ohlc`**: consistent, cheap, reused. Keep.

---

## Suggested migration plan order 📚

Pre-deployment means the checksum-verified migration history can be rewritten/squashed rather than extended (tests use fresh clusters; `migrations.py:78-83` only pins checksums of applied databases). Land as this stack, each step green on `make check`:

1. **Code thinning, behavior-neutral.** Safe-error boundary wrap (win #4), drop per-batch defenses + `_checked_batch` round-trip + per-batch context verify (win #3), `require_writer_connection` pg_locks drop, `del` noise, `BackfillIncompleteError` summary, `started_at` field, alias removal, `record_fetch_outcome` timestamp check. Pure deletions; existing tests characterize behavior.

2. **Merge publication stages** (win #1). `_schema.py`, `publication.py`, `copying.py` allowlist, `rebuild.py`. Still three temp tables → one; publish math unchanged semantically.

3. **Single rebuild+publish transaction** (win #2). Depends on step 2 for a clean diff; drops `publication_context`, `scope.complete`, PRESERVE ROWS.

4. **Schema rewrite (squash 0001-0003)**. Ticker_reference fold-in, `adjustment_type` removal, drop `split_event_ticker_idx`, drop `'completed'` + `published_at`, cik CHECK fix, raw_session join removal, single-role grants, daily-flags CHECK. One coherent DDL batch, since rewriting migrations is a pre-deployment-only privilege. Update `extract.py`/`reading.py`/`references.py` schemas in the same commit.

5. **`latest_daily` → view**. After step 4's grants cleanup; removes `_refresh_latest_daily` and the last per-publication maintenance write.

6. **`backfill.py` fetch simplifications**. `_calendar_year_windows` removal, `_split_bounds` tidy. Independent; can land anytime after step 1.

7. **Performance work, measure-first**. COPY bulk-bytes path and chunked/Arrow reads (profile with a realistic 30M-row fixture); then decide on incremental publication as its own design project. Fix `info.py` counts and AGENTS.md drift alongside.

Steps 1-3 shrink ~2,100 lines of Python by a rough 25-30% before the schema batch even lands; steps 4-5 remove one table, one denormalization, one index, one enum value, and one column. The stack never leaves the tree broken because each step preserves the atomic-publish invariant that the test suite asserts.

---

## Appendix: CI build cache from grep 🔍

Two grep facts confirmed during the review:

- Nothing ever writes `run.state = 'completed'`. The CHECK constraint admits it, no code path emits it.
- `adjustment_type` is stored and round-tripped through `references.py`, `raw.py`, and `reading.py` but never consumed by `transform.adjust_splits` (which only uses `execution_date` + `adjustment_factor`).

Both facts underpin the corresponding top-5 recommendations.