"""
Seed themes and theme_membership from data/themes/Custom_Indices_Tickers.xlsx.

Idempotent — safe to re-run whenever the Excel file is updated.

Usage:
    cd backend
    python seed_themes.py
"""

import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import get_engine

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EXCEL_PATH = Path(__file__).parent.parent / "data" / "themes" / "Custom_Indices_Tickers.xlsx"

# Regex that matches theme separator rows, e.g. "1. Affordable Housing Finance Index (5)"
THEME_ROW_RE = re.compile(r"^(\d+)\.\s+(.+?)\s*\((\d+)\)$")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def slugify(name: str) -> str:
    """Convert display name to URL-safe slug."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug


def parse_excel(path: Path) -> list[dict]:
    """
    Returns a list of theme dicts:
        {
            theme_order: int,
            theme_name: str,       # display name, no number or count
            theme_slug: str,
            stock_count: int,      # from Excel parentheses
            stocks: [(ticker, company_name), ...]
        }
    """
    df = pd.read_excel(path, sheet_name="Custom Indices", header=None, skiprows=1)
    # After skiprows=1 (title row), row 0 in df is the header "TICKER | COMPANY NAME" — skip it too
    df.columns = ["ticker", "company"]
    df = df.iloc[1:].reset_index(drop=True)

    themes = []
    current_theme = None

    for _, row in df.iterrows():
        ticker_val = str(row["ticker"]).strip() if pd.notna(row["ticker"]) else ""
        company_val = str(row["company"]).strip() if pd.notna(row["company"]) else ""

        if not ticker_val:
            continue

        m = THEME_ROW_RE.match(ticker_val)
        if m and not company_val:
            # Theme separator row
            order = int(m.group(1))
            raw_name = m.group(2).strip()
            count = int(m.group(3))
            # Strip trailing "Index" suffix if present (keep display name clean)
            display_name = raw_name
            current_theme = {
                "theme_order": order,
                "theme_name": display_name,
                "theme_slug": slugify(display_name),
                "stock_count": count,
                "stocks": [],
            }
            themes.append(current_theme)
        elif current_theme is not None and ticker_val and ticker_val != "TICKER":
            current_theme["stocks"].append((ticker_val, company_val))

    return themes


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate(themes: list[dict]) -> None:
    errors = []
    for t in themes:
        actual = len(t["stocks"])
        expected = t["stock_count"]
        if actual != expected:
            errors.append(
                f"  [{t['theme_order']}] {t['theme_name']}: "
                f"expected {expected} stocks, found {actual}"
            )
    if errors:
        print("VALIDATION WARNINGS (count mismatch):")
        for e in errors:
            print(e)
    else:
        print("Validation passed — all theme stock counts match Excel.")


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def seed(themes: list[dict]) -> None:
    engine = get_engine()

    # --- Collect all unique tickers ---
    all_tickers: dict[str, str] = {}  # symbol -> company_name
    for t in themes:
        for ticker, name in t["stocks"]:
            all_tickers.setdefault(ticker, name)

    print(f"\nTotal unique tickers in Excel: {len(all_tickers)}")

    # --- Add missing stocks to master ---
    with engine.begin() as conn:
        existing = {
            row[0]
            for row in conn.execute(text("SELECT symbol FROM stocks")).fetchall()
        }

    new_tickers = {sym: name for sym, name in all_tickers.items() if sym not in existing}
    already_count = len(all_tickers) - len(new_tickers)
    print(f"{already_count} tickers already in master, {len(new_tickers)} tickers newly added.")

    if new_tickers:
        new_rows = [
            {
                "symbol": sym,
                "name": name if name else sym,
                "yahoo_symbol": sym + ".NS",
                "screener_url": f"https://www.screener.in/company/{sym}/consolidated/",
                "tradingview_url": f"https://www.tradingview.com/chart/?symbol=NSE%3A{sym}",
            }
            for sym, name in new_tickers.items()
        ]
        BATCH = 50
        inserted = 0
        for i in range(0, len(new_rows), BATCH):
            batch = new_rows[i : i + BATCH]
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO stocks (symbol, name, yahoo_symbol, screener_url, tradingview_url, is_active, added_at)
                        VALUES (:symbol, :name, :yahoo_symbol, :screener_url, :tradingview_url, TRUE, NOW())
                        ON CONFLICT (symbol) DO NOTHING
                    """),
                    batch,
                )
            inserted += len(batch)
        print(f"Inserted {inserted} new stocks into master.")

    # --- Upsert themes ---
    theme_rows = [
        {
            "theme_slug": t["theme_slug"],
            "theme_name": t["theme_name"],
            "theme_order": t["theme_order"],
            "stock_count": t["stock_count"],
        }
        for t in themes
    ]
    BATCH = 50
    with engine.begin() as conn:
        for i in range(0, len(theme_rows), BATCH):
            conn.execute(
                text("""
                    INSERT INTO themes (theme_slug, theme_name, theme_order, stock_count)
                    VALUES (:theme_slug, :theme_name, :theme_order, :stock_count)
                    ON CONFLICT (theme_slug) DO UPDATE SET
                        theme_name  = EXCLUDED.theme_name,
                        theme_order = EXCLUDED.theme_order,
                        stock_count = EXCLUDED.stock_count
                """),
                theme_rows[i : i + BATCH],
            )
    print(f"Upserted {len(theme_rows)} themes.")

    # --- Rebuild theme_membership (truncate + re-insert = idempotent) ---
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE theme_membership"))

    membership_rows = [
        {"theme_slug": t["theme_slug"], "symbol": ticker}
        for t in themes
        for ticker, _ in t["stocks"]
    ]
    total_memberships = 0
    for i in range(0, len(membership_rows), BATCH):
        batch = membership_rows[i : i + BATCH]
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO theme_membership (theme_slug, symbol)
                    VALUES (:theme_slug, :symbol)
                    ON CONFLICT DO NOTHING
                """),
                batch,
            )
        total_memberships += len(batch)
    print(f"Inserted {total_memberships} rows into theme_membership.")

    # --- Final summary ---
    with engine.connect() as conn:
        theme_count = conn.execute(text("SELECT COUNT(*) FROM themes")).fetchone()[0]
        member_count = conn.execute(text("SELECT COUNT(*) FROM theme_membership")).fetchone()[0]
        drift_count = conn.execute(
            text("SELECT COUNT(*) FROM themes_with_counts WHERE actual_stock_count != stock_count")
        ).fetchone()[0]

    print(f"\n--- Final Summary ---")
    print(f"Themes in DB        : {theme_count}")
    print(f"Total memberships   : {member_count}")
    print(f"Count-drift rows    : {drift_count}  (should be 0)")

    if drift_count > 0:
        print("WARNING: Some themes have a mismatch between stock_count and actual membership.")
    else:
        print("All theme counts match. Seed complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if not EXCEL_PATH.exists():
        print(f"ERROR: Excel file not found at {EXCEL_PATH}")
        print("Place Custom_Indices_Tickers.xlsx in data/themes/ and re-run.")
        sys.exit(1)

    print(f"Reading: {EXCEL_PATH}")
    themes = parse_excel(EXCEL_PATH)
    print(f"Parsed {len(themes)} themes, {sum(len(t['stocks']) for t in themes)} total stock rows.")

    validate(themes)
    seed(themes)
