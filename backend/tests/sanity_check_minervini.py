"""
sanity_check_minervini.py — QA script for Minervini Trend Template data.

Prints:
  1. Row count and date coverage in minervini_template_daily
  2. RS Rank distribution check (should span 1–99)
  3. Pass count per day (should be 30–100 in normal markets)
  4. Top 10 stocks by template_score with all 8 criteria flags
  5. TradingView links for manual chart verification
  6. Edge case checks (None values, score bounds, criteria_count sanity)

Usage:
    python backend/tests/sanity_check_minervini.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from sqlalchemy import text
from db import get_engine

engine = get_engine()

SEP = "─" * 70


def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


# ── 1. Row count & date coverage ──────────────────────────────────────────────
section("1. Row count & date coverage")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT
            COUNT(*)            AS total_rows,
            COUNT(DISTINCT symbol) AS symbols,
            COUNT(DISTINCT date)   AS days,
            MIN(date)           AS earliest,
            MAX(date)           AS latest,
            SUM(CASE WHEN template_pass THEN 1 ELSE 0 END) AS total_passes
        FROM minervini_template_daily
    """)).fetchone()

print(f"  Total rows     : {result[0]:,}")
print(f"  Unique symbols : {result[1]:,}")
print(f"  Days covered   : {result[2]}")
print(f"  Date range     : {result[3]}  →  {result[4]}")
print(f"  Total passes   : {result[5]:,}")

if result[2] == 0:
    print("\n  ⚠️  No data found. Run backfill first:")
    print("     python backend/backfill_minervini_template.py")
    sys.exit(1)


# ── 2. Pass count per day ─────────────────────────────────────────────────────
section("2. Pass count per day (expect 30–100 in healthy market)")

with engine.connect() as conn:
    daily = pd.read_sql(text("""
        SELECT date,
               SUM(CASE WHEN template_pass THEN 1 ELSE 0 END) AS passes,
               COUNT(*) AS evaluated
        FROM minervini_template_daily
        GROUP BY date
        ORDER BY date DESC
        LIMIT 30
    """), conn)

for _, row in daily.iterrows():
    flag = ""
    if row["passes"] < 10:
        flag = "  ⚠️  very low (bear market?)"
    elif row["passes"] > 200:
        flag = "  ⚠️  very high (check logic)"
    print(f"  {row['date']}  passes={int(row['passes']):>3}  evaluated={int(row['evaluated']):>4}{flag}")


# ── 3. RS Rank distribution (latest date) ────────────────────────────────────
section("3. RS Rank distribution on latest date (should span ~1–99)")

with engine.connect() as conn:
    rs_dist = conn.execute(text("""
        SELECT
            MIN(rs_rank_12m)   AS min_rs,
            MAX(rs_rank_12m)   AS max_rs,
            AVG(rs_rank_12m)   AS avg_rs,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rs_rank_12m) AS p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY rs_rank_12m) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rs_rank_12m) AS p75
        FROM minervini_template_daily
        WHERE date = (SELECT MAX(date) FROM minervini_template_daily)
          AND rs_rank_12m IS NOT NULL
    """)).fetchone()

print(f"  Min  : {rs_dist[0]:.1f}  (expect ~1)")
print(f"  Max  : {rs_dist[1]:.1f}  (expect ~99)")
print(f"  Avg  : {rs_dist[2]:.1f}  (expect ~50)")
print(f"  P25  : {rs_dist[3]:.1f}")
print(f"  P50  : {rs_dist[4]:.1f}")
print(f"  P75  : {rs_dist[5]:.1f}")

if rs_dist[1] < 90:
    print("  ⚠️  Max RS Rank below 90 — rank computation may have an issue")
if rs_dist[0] > 20:
    print("  ⚠️  Min RS Rank above 20 — rank computation may have an issue")


# ── 4. Top 10 stocks by template_score ───────────────────────────────────────
section("4. Top 10 stocks by template_score (latest date)")

with engine.connect() as conn:
    top10 = pd.read_sql(text("""
        SELECT m.symbol, s.name, s.sector,
               m.template_score, m.criteria_count,
               m.rs_rank_12m, m.cmp,
               m.pct_from_52w_high, m.pct_above_52w_low,
               m.criterion_1_pass, m.criterion_2_pass, m.criterion_3_pass,
               m.criterion_4_pass, m.criterion_5_pass, m.criterion_6_pass,
               m.criterion_7_pass, m.criterion_8_pass,
               m.return_12m
        FROM minervini_template_daily m
        JOIN stocks s ON m.symbol = s.symbol
        WHERE m.date = (SELECT MAX(date) FROM minervini_template_daily)
          AND m.template_pass = true
        ORDER BY m.template_score DESC, m.rs_rank_12m DESC
        LIMIT 10
    """), conn)

if top10.empty:
    print("  No passing stocks found on latest date.")
else:
    criteria_cols = [f"criterion_{i}_pass" for i in range(1, 9)]
    for _, r in top10.iterrows():
        flags = "".join("✓" if r[c] else "✗" for c in criteria_cols)
        tv_sym = r["symbol"].replace(".NS", "").replace(".BO", "")
        tv_link = f"https://www.tradingview.com/chart/?symbol=NSE:{tv_sym}"
        print(f"\n  {r['symbol']:<14} {str(r['name'])[:28]:<28} [{r['sector'] or '—'}]")
        print(f"    Score={r['template_score']:.1f}/10  RS={r['rs_rank_12m']:.0f}  CMP=₹{r['cmp']:,.1f}")
        print(f"    12M={r['return_12m']:+.1f}%  From52wHigh={r['pct_from_52w_high']:+.1f}%  Above52wLow=+{r['pct_above_52w_low']:.1f}%")
        print(f"    Criteria [{flags}]  (C1–C8)")
        print(f"    TradingView → {tv_link}")


# ── 5. Edge case checks ───────────────────────────────────────────────────────
section("5. Edge case checks")

with engine.connect() as conn:
    checks = conn.execute(text("""
        SELECT
            SUM(CASE WHEN template_pass AND criteria_count < 8 THEN 1 ELSE 0 END) AS pass_but_not_8,
            SUM(CASE WHEN NOT template_pass AND template_score > 0 THEN 1 ELSE 0 END) AS fail_but_score,
            SUM(CASE WHEN template_score > 10 THEN 1 ELSE 0 END) AS score_over_10,
            SUM(CASE WHEN criteria_count < 0 OR criteria_count > 8 THEN 1 ELSE 0 END) AS bad_count,
            SUM(CASE WHEN cmp IS NULL THEN 1 ELSE 0 END) AS null_cmp
        FROM minervini_template_daily
        WHERE date = (SELECT MAX(date) FROM minervini_template_daily)
    """)).fetchone()

issues = 0
checks_map = [
    (checks[0], "template_pass=True but criteria_count<8"),
    (checks[1], "template_pass=False but template_score>0"),
    (checks[2], "template_score > 10"),
    (checks[3], "criteria_count out of 0–8 range"),
    (checks[4], "cmp is NULL"),
]
for count, label in checks_map:
    status = "✓ OK" if count == 0 else f"⚠️  {count} rows"
    print(f"  {status:<12}  {label}")
    if count > 0:
        issues += 1

print(f"\n  {'All checks passed ✓' if issues == 0 else f'{issues} issue(s) found — review above'}")

print(f"\n{SEP}")
print("  Sanity check complete.")
print(SEP)
