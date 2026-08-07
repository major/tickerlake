# etf-race table: emoji + color styling

**Date:** 2026-08-07
**Status:** Approved by user
**Scope:** `src/tickerlake/race.py` (renderer only) + `tests/test_race.py` + `docs/etf-race.md`

## Problem

The etf-race leaderboard is a plain Rich table: readable, but hard to scan when
the default dynamic list shows 50 ETFs.
There is no visual encoding of which horses are charging, fading, or simply
cruising.
This design adds emoji and color to make the leaderboard scannable at a glance.

## Decisions

- Styling is always on.
  No `--plain` flag: Rich already disables ANSI color when the console is not a
  TTY, and emoji are plain text, so piping stays safe.
- All styling lives in `race.py`, the existing pure data + Rich rendering layer.
  `pipeline.py` and the CLI are untouched.
- Discrete color buckets for the race score, not a continuous RGB gradient.
  Simpler, unit-testable, and crisper in terminals.

## Current behavior (context)

- `render_relative_leaderboard` (race.py:627) builds a Rich `Table` titled
  `🐎 vs {benchmark} Momentum` with columns Ticker, Pos, Places, Pace Short,
  Pace Medium, Pace Long, Race, Form.
- The table is sorted by `race_score` descending, nulls last (race.py:650).
  `race_score` = momentum_score * 0.45 + closing_score * 0.35 +
  staying_power * 0.20 (race.py:497-505).
- No per-row styling and no form emojis exist today.

## Changes

### 1. Form emoji + row color map

Module-level constants in `race.py`:

| Form | Emoji | Row style |
|------|-------|-----------|
| Charging | 🚀 | `green` |
| Front-runner | 🏆 | `cyan` |
| Closing ground | ⚡ | `yellow` |
| Steady | ➖ | default |
| Losing steam | 📉 | `red` |
| Fading | 🍂 | `orange` |
| Back of field | 🐢 | `dim red` |
| Unknown | ❔ | `dim` |

The row style tints the whole row (that is what makes a 50-row leaderboard
scannable).
The Form cell renders as `🚀 Charging` (emoji + label).

Implementation shape: a `_FORM_STYLE: dict[str, tuple[str, str]]` mapping
form -> (emoji, rich style), plus a small helper that returns `(emoji, style)`
and falls back to Unknown for unrecognized/null form values.
Tests cover every form label plus the fallback.

### 2. Pace sign coloring

Pace Short / Pace Medium / Pace Long cells are colored by sign:
`> 0` green, `< 0` red, `0`/`n/a` default.
Applied via a per-cell `rich.text.Text` object so the numeric value keeps its
`+1.2%` formatting.

Implementation shape: one pure helper that maps a float (or None) to a rich
style string; unit-testable without building a table.

### 3. Race score color buckets

The Race cell is colored by value:

| Range | Style |
|-------|-------|
| `>= 70` | green |
| `40–69` | yellow |
| `< 40` | red |
| null | default |

Implementation shape: one pure helper mapping a float (or None) to a style
string; thresholds live in module-level constants (project convention, also
keeps ruff magic-number checks quiet).

### 4. Docs

`docs/etf-race.md` gains a short "Reading the table" section describing the
form emoji/color mapping, pace sign coloring, and race score buckets.

## Testing

- Unit tests for the three pure style helpers: every form label + null
  fallback, pace sign boundaries (`>0`, `<0`, `0`, null), race score buckets
  and boundaries (70, 40, null).
- Table-level assertions in `test_race.py`: rendered rows carry the expected
  emoji in the Form cell and the expected style on the row, on at least one
  representative frame covering all form classes.
- Existing tests for `render_relative_leaderboard` keep passing unchanged
  where they assert column layout and sort order.

## Out of scope

- Podium highlighting (🥇🥈🥉) — user declined.
- Continuous score gradient.
- Any CLI flags, pipeline changes, or changes to the underlying metrics.

## Non-goals / constraints

- No new dependencies: styling uses Rich features already in the project.
- Emoji stay inside the rendered report layer; none are persisted to DuckDB.
