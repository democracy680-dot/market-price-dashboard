"""
seed_stocks.py — One-time (and re-runnable) setup script.

Reads every CSV in data/indexes/, inserts stocks into the `stocks` table,
and rebuilds `index_membership` from scratch.

Usage:
    python backend/seed_stocks.py

Run from the repo root. Requires SUPABASE_DB_URL in .env or environment.
Safe to re-run: upserts on stocks, deletes+reinserts on index_membership.
"""

import os
import sys
import csv
from pathlib import Path
from datetime import date

from sqlalchemy import text
from db import get_engine

# ── paths ──────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
INDEXES_DIR = REPO_ROOT / "data" / "indexes"

# CSV filename → index_name stored in the DB
INDEX_MAP = {
    "nifty_50.csv":   "NIFTY_50",
    "nifty_500.csv":  "NIFTY_500",
    "nifty_bank.csv": "NIFTY_BANK",
    "banks.csv":      "BANKS",
    "nbfcs.csv":      "NBFCS",
    "pharma.csv":     "PHARMA",
    "defence.csv":    "DEFENCE",
    "fno.csv":        "FNO",
}


def load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def make_yahoo_symbol(symbol: str) -> str:
    """Append .NS for NSE symbols. Handles edge cases like M&M → M%26M.NS."""
    return symbol + ".NS"


def make_screener_url(symbol: str) -> str:
    return f"https://www.screener.in/company/{symbol}/consolidated/"


def make_tradingview_url(symbol: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol}"


def seed(engine):
    today = date.today().isoformat()

    # Collect all stocks across all index files
    # stock_data: symbol → {name, indexes: set}
    stock_data: dict[str, dict] = {}
    index_rows: list[tuple[str, str]] = []  # (symbol, index_name)

    for filename, index_name in INDEX_MAP.items():
        csv_path = INDEXES_DIR / filename
        if not csv_path.exists():
            print(f"  [WARN] {filename} not found, skipping.")
            continue

        rows = load_csv(csv_path)
        for row in rows:
            symbol = row.get("symbol", "").strip()
            name = row.get("name", "").strip()
            if not symbol:
                continue

            if symbol not in stock_data:
                stock_data[symbol] = {"name": name}

            index_rows.append((symbol, index_name))

        print(f"  Loaded {len(rows):>4} rows from {filename} -> {index_name}")

    print(f"\nTotal unique stocks across all indexes: {len(stock_data)}")

    with engine.begin() as conn:
        # ── 1. Upsert into stocks (single batch call) ─────────────────────
        print("\nUpserting stocks...")
        stock_params = [
            {
                "symbol":          symbol,
                "name":            info["name"],
                "yahoo_symbol":    make_yahoo_symbol(symbol),
                "screener_url":    make_screener_url(symbol),
                "tradingview_url": make_tradingview_url(symbol),
            }
            for symbol, info in stock_data.items()
        ]
        conn.execute(
            text("""
                INSERT INTO stocks
                    (symbol, name, yahoo_symbol, screener_url, tradingview_url, is_active, added_at)
                VALUES
                    (:symbol, :name, :yahoo_symbol, :screener_url, :tradingview_url, TRUE, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    name            = EXCLUDED.name,
                    yahoo_symbol    = EXCLUDED.yahoo_symbol,
                    screener_url    = EXCLUDED.screener_url,
                    tradingview_url = EXCLUDED.tradingview_url
            """),
            stock_params,
        )
        print(f"  {len(stock_params)} stocks upserted.")

        # ── 2. Rebuild index_membership (single batch call) ───────────────
        print("\nRebuilding index_membership...")
        conn.execute(text("DELETE FROM index_membership"))

        membership_params = [
            {"symbol": symbol, "index_name": index_name, "added_at": today}
            for symbol, index_name in index_rows
        ]
        conn.execute(
            text("""
                INSERT INTO index_membership (symbol, index_name, added_at)
                VALUES (:symbol, :index_name, :added_at)
                ON CONFLICT (symbol, index_name) DO NOTHING
            """),
            membership_params,
        )
        print(f"  {len(membership_params)} index membership rows inserted.")

    print("\nSeed complete.")


if __name__ == "__main__":
    print("Connecting to Supabase...")
    try:
        engine = get_engine()
        # Quick connectivity check
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connected.\n")
    except Exception as e:
        print(f"[ERROR] Could not connect: {e}")
        sys.exit(1)

    seed(engine)
