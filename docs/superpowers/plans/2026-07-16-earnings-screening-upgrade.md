# Earnings Screening Upgrade: score fix, reaction metrics, filters

## Context

The Quarterly Results tab (Q1FY27/Q4FY26 sub-tabs, shipped 2026-07-16) tracks price
reaction to results but has gaps as a *screener*:

1. **Score flaw** — the volume component of the Post-Result Strength Score uses the
   *latest* day's `volume_ratio`, not the announcement day's; the 200-DMA check mixes
   announcement-day price with today's DMA.
2. **Weak reaction measurement** — no excess return vs Nifty, no gap-held check,
   no at-a-glance reaction classification, no 52-week-high context.
3. **No filters** — the season table can't be narrowed by market cap, sector, or score.

User approved these packages (2026-07-16); the "actual reported numbers" and
"upcoming results calendar" packages were explicitly dropped. Recon verified:
- `^NSEI` daily closes are in `prices_daily` (current through 16 Jul) → excess
  return vs Nifty computable in SQL.
- `minervini_template_daily.pct_from_52w_high` exists daily → join at announcement date.
- No schema changes and no new backend scripts needed — **frontend-only change**.

## Design decisions

- **Score structure and weights unchanged** — only data sources fixed: the volume
  tier and the two trend checks (price > 200-DMA, slope) read the announcement-day
  technicals row (first row on/after `result_date`) instead of the latest row.
  Individual score values will shift — that is the fix, not a formula change.
  Minervini criteria/RS-rank components stay on the latest row by design (they
  measure current trend quality).
- New season columns: **Reaction** badge, **Excess Ret** (vs Nifty since
  announcement), **% off 52wH** (at announcement), **Gap Held**, **Repeat**
  (current quarter only: strong reaction in the previous quarter too).
- Reaction badge (pandas, from ann-day return + ann-day volume + drift):
  🔴 Rejected = ann ≤ −3%; 🟡 Faded = ann ≥ +2% but drift-since < 0;
  🟢 Strong = ann ≥ +4% on ≥2× volume; else —.
- Gap Held = no low after the announcement day has undercut the announcement-day
  low ("—" until at least one later session exists).
- Filters (season): min market cap, sector multiselect, min score, positive-only.
  Sector season summary table above the filters (count, % positive, avg ann-day
  return, avg return-since by sector).
- Today's Results keeps its current columns; only its score internals get the
  announcement-day fix (values fill in after the 4:30 PM refresh lands, same as
  its return column today).

## Steps — all in `frontend/app.py`

1. **Season query** (`_render_earnings_season`): change `td` lateral to `td_ann`
   (first technicals row ≥ `result_date`); add laterals `mt_ann` (minervini row ≥
   result_date → `pct_from_52w_high`), `ann_px` (prices_daily first row ≥
   result_date → date + low), `low_since` (MIN(low) after `ann_px.date`),
   `nifty_ann`/`nifty_latest` (^NSEI first close ≥ result_date / latest close),
   and, when a previous quarter is passed, `prevq` (that symbol's previous-quarter
   announcement-day return). SELECT adds `s.sector`, excess return, gap-held,
   pct-off-high, repeat flag. Score volume/DMA/slope terms read `td_ann`.
2. **Today query** (`_render_earnings_today`): same `td` → `td_ann` swap only.
3. **`_render_earnings_season(snap_date, quarter, prev_quarter=None)`**: badge
   column in pandas; sector summary table; filters row (unique widget keys per
   quarter); filtered caption.
4. **`_render_earnings_table`**: render the new columns when present (Reaction,
   Excess Ret, % off 52wH, Gap Held, Repeat) with number formats and return
   coloring; unchanged for today-mode.
5. **`_frag_quarterly_results`**: pass `prev_quarter="Q4FY26"` for the Q1FY27 view.
6. **`_TAB_DESCRIPTIONS["quarterly_results"]`**: document the new columns and filters.

## Verify

Playwright drive: Q1FY27 season shows badges, new columns, working filters and
sector summary; Q4FY26 archive renders with the new columns minus Repeat; zero
`stException`s; CSV download works. Commit + push to origin main.
