"""
add_equity_l_stocks.py

1. Reads EQUITY_L (1).csv and all data/indexes/*.csv
2. Finds up to 500 stocks from EQUITY_L not yet in the webapp
   (ranked by market cap descending; #N/A cap stocks come last)
3. Inserts those 500 stocks into the DB via seed_from_csv logic
4. Rewrites EQUITY_L (1).csv keeping only stocks still NOT in the webapp

Usage (from repo root):
    python backend/add_equity_l_stocks.py
"""

import csv
import re
import sys
from pathlib import Path
from sqlalchemy import text
from db import get_engine

REPO_ROOT = Path(__file__).parent.parent
EQUITY_CSV = REPO_ROOT / "EQUITY_L (1).csv"
INDEXES_DIR = REPO_ROOT / "data" / "indexes"
SEED_LIMIT = 500


def parse_market_cap(raw: str) -> float:
    """Return numeric market cap or -1 if not available."""
    cleaned = re.sub(r"[,\s]", "", raw.strip())
    try:
        return float(cleaned)
    except ValueError:
        return -1.0


def load_webapp_stocks() -> set[str]:
    stocks: set[str] = set()
    for csv_file in INDEXES_DIR.glob("*.csv"):
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get("symbol", "").strip()
                if sym:
                    stocks.add(sym)
    return stocks


def load_equity_l() -> list[dict]:
    """Returns list of {symbol, name, market_cap_raw} preserving original order."""
    rows = []
    with open(EQUITY_CSV, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            if not row or not row[0].strip():
                continue
            symbol = row[0].strip()
            name = row[1].strip() if len(row) > 1 else ""
            cap_raw = row[2].strip() if len(row) > 2 else "#N/A"
            rows.append({"symbol": symbol, "name": name, "cap_raw": cap_raw})
    return rows


def make_yahoo_symbol(symbol: str) -> str:
    return symbol.strip() + ".NS"


def make_screener_url(symbol: str) -> str:
    return f"https://www.screener.in/company/{symbol.strip()}/consolidated/"


def make_tradingview_url(symbol: str) -> str:
    return f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol.strip()}"


def main():
    print("Loading webapp stocks from data/indexes/ ...")
    webapp_stocks = load_webapp_stocks()
    print(f"  {len(webapp_stocks)} unique stocks currently in webapp")

    print(f"\nLoading {EQUITY_CSV.name} ...")
    equity_rows = load_equity_l()
    print(f"  {len(equity_rows)} total rows")

    # Split into already-in-webapp and not-in-webapp
    not_in_webapp = [r for r in equity_rows if r["symbol"] not in webapp_stocks]
    in_webapp = [r for r in equity_rows if r["symbol"] in webapp_stocks]
    print(f"  {len(in_webapp)} already in webapp")
    print(f"  {len(not_in_webapp)} NOT in webapp")

    # Sort not_in_webapp by market cap descending (#N/A → -1, goes last)
    not_in_webapp.sort(key=lambda r: parse_market_cap(r["cap_raw"]), reverse=True)

    to_add = not_in_webapp[:SEED_LIMIT]
    remaining = not_in_webapp[SEED_LIMIT:]

    print(f"\nWill add top {len(to_add)} stocks (by market cap) to webapp")

    # ── Insert into DB ────────────────────────────────────────────────────────
    stock_params = [
        {
            "symbol":          r["symbol"],
            "name":            r["name"],
            "yahoo_symbol":    make_yahoo_symbol(r["symbol"]),
            "screener_url":    make_screener_url(r["symbol"]),
            "tradingview_url": make_tradingview_url(r["symbol"]),
        }
        for r in to_add
    ]

    print("Connecting to database (transaction pooler) ...")
    # Switch to transaction-mode pooler (port 6543) to avoid session-pool exhaustion
    import os
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    raw_url = os.environ.get("SUPABASE_DB_URL", "")
    tx_url = raw_url.replace(":5432/", ":6543/")
    engine = create_engine(tx_url, poolclass=NullPool, connect_args={"connect_timeout": 15})
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
    print(f"  Inserted {len(stock_params)} stocks into DB (skipped any duplicates)")

    # ── Rewrite EQUITY_L (1).csv ──────────────────────────────────────────────
    # Keep only stocks that are STILL not in the webapp after this addition
    added_symbols = {r["symbol"] for r in to_add}
    all_now_in_webapp = webapp_stocks | added_symbols

    with open(EQUITY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Ticker ", "Name ", "Market cap "])
        for r in equity_rows:
            if r["symbol"] not in all_now_in_webapp:
                writer.writerow([r["symbol"], r["name"], r["cap_raw"]])

    kept = len([r for r in equity_rows if r["symbol"] not in all_now_in_webapp])
    removed = len(equity_rows) - kept

    print(f"\nRewritten EQUITY_L (1).csv:")
    print(f"  Removed : {removed} stocks (now in webapp)")
    print(f"  Remaining: {kept} stocks (still NOT in webapp)")
    print(f"\nDone. Next step: run   python backend/backfill_new_stocks.py   to fetch price history for the {len(to_add)} new stocks.")


if __name__ == "__main__":
    main()
