"""
seed_from_csv.py — Insert/upsert stocks from a flat CSV (Symbol, Name) into the stocks table.

Usage:
    python backend/seed_from_csv.py "Stocks to be Updated .csv"

Run from the repo root. Safe to re-run — uses ON CONFLICT DO NOTHING so existing
stocks are left untouched. After this, run backfill_new_stocks.py to fetch price history.
"""

import sys
import csv
from pathlib import Path
from sqlalchemy import text
from db import get_engine


def make_yahoo_symbol(symbol: str) -> str:
    return symbol.strip() + ".NS"


def make_screener_url(symbol: str) -> str:
    return f"https://www.screener.in/company/{symbol.strip()}/consolidated/"


def make_tradingview_url(symbol: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol.strip()}"


def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Strip whitespace from header keys
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        return [
            {k.strip(): v.strip() for k, v in row.items()}
            for row in reader
        ]


def run(csv_path: Path):
    rows = load_csv(csv_path)
    print(f"Loaded {len(rows)} rows from {csv_path.name}")

    stock_params = []
    for row in rows:
        symbol = row.get("Symbol", "").strip()
        name = row.get("Name", "").strip()
        if not symbol:
            continue
        stock_params.append({
            "symbol":          symbol,
            "name":            name,
            "yahoo_symbol":    make_yahoo_symbol(symbol),
            "screener_url":    make_screener_url(symbol),
            "tradingview_url": make_tradingview_url(symbol),
        })

    print(f"Upserting {len(stock_params)} stocks into Supabase...")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO stocks
                    (symbol, name, yahoo_symbol, screener_url, tradingview_url, is_active, added_at)
                VALUES
                    (:symbol, :name, :yahoo_symbol, :screener_url, :tradingview_url, TRUE, NOW())
                ON CONFLICT (symbol) DO NOTHING
            """),
            stock_params,
        )

    print(f"Done. {len(stock_params)} stocks upserted (existing ones untouched).")
    print("\nNext step: run   python backend/backfill_new_stocks.py   to fetch price history.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backend/seed_from_csv.py <path-to-csv>")
        sys.exit(1)

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"[ERROR] File not found: {path}")
        sys.exit(1)

    run(path)
