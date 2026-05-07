"""Seed earnings_calendar from the quarterly result announcement Excel file.

Usage (from project root):
    python backend/seed_earnings_calendar.py

What it does:
1. Reads the Excel file, extracts symbol + result_date for all 351 entries
2. Inserts any symbols not already in the stocks table (name defaults to symbol;
   market cap and other fundamentals are populated by the next daily_refresh run)
3. Upserts all entries into earnings_calendar

Re-run safely whenever the user uploads a new Excel file with updated dates.
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Allow running from both project root and backend/
sys.path.insert(0, str(Path(__file__).parent))
from db import get_engine
from sqlalchemy import text

EXCEL_PATH = Path(__file__).parent.parent / "data" / "Quaterly Result Annoucement date.xlsx"
BATCH = 50


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def load_excel():
    df = pd.read_excel(EXCEL_PATH, header=None)

    # Keep rows where column 1 starts with "NSE:" and column 2 is a real datetime
    valid = df[df[1].notna() & df[1].astype(str).str.startswith("NSE:")]
    valid = valid[
        valid[2].apply(
            lambda x: isinstance(x, datetime)
            or (pd.notna(x) and str(x) not in ("Result Date", "NaT", ""))
        )
    ]
    valid = valid[valid[2].notna()].copy()

    valid["symbol"] = valid[1].str.replace("NSE:", "", regex=False).str.strip()
    valid["result_date"] = pd.to_datetime(valid[2]).dt.date
    # Use company name from column 3 when available
    valid["name"] = valid[3].where(valid[3].notna(), valid["symbol"])

    return valid[["symbol", "result_date", "name"]].drop_duplicates(
        subset=["symbol", "result_date"]
    )


def seed(engine, records):
    # ── Step 1: insert new stocks ──────────────────────────────────────────────
    new_stocks = 0
    for batch in chunks(records, BATCH):
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO stocks (symbol, name, yahoo_symbol, screener_url, tradingview_url, is_active, added_at)
                    VALUES (:symbol, :name, :yahoo_symbol, :screener_url, :tradingview_url, TRUE, NOW())
                    ON CONFLICT (symbol) DO NOTHING
                """),
                [
                    {
                        "symbol": r["symbol"],
                        "name": r["name"],
                        "yahoo_symbol": r["symbol"] + ".NS",
                        "screener_url": f"https://www.screener.in/company/{r['symbol']}/consolidated/",
                        "tradingview_url": f"https://www.tradingview.com/chart/?symbol=NSE%3A{r['symbol']}",
                    }
                    for r in batch
                ],
            )
            new_stocks += result.rowcount

    print(f"  Stocks: {new_stocks} new rows inserted (rest already existed)")

    # ── Step 2: upsert earnings_calendar ──────────────────────────────────────
    cal_rows = 0
    for batch in chunks(records, BATCH):
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO earnings_calendar (symbol, result_date)
                    VALUES (:symbol, :result_date)
                    ON CONFLICT (symbol, result_date) DO NOTHING
                """),
                [{"symbol": r["symbol"], "result_date": r["result_date"]} for r in batch],
            )
            cal_rows += result.rowcount

    print(f"  Earnings calendar: {cal_rows} new entries upserted")


def main():
    if not EXCEL_PATH.exists():
        print(f"ERROR: Excel file not found at {EXCEL_PATH}")
        print("Place 'Quaterly Result Annoucement date.xlsx' in the project root and re-run.")
        sys.exit(1)

    print(f"Reading {EXCEL_PATH.name} ...")
    df = load_excel()
    print(f"  Found {len(df)} valid entries across {df['result_date'].nunique()} dates "
          f"({df['result_date'].min()} to {df['result_date'].max()})")

    records = df.to_dict("records")
    engine = get_engine()
    print("Seeding database ...")
    seed(engine, records)
    print("Done.")


if __name__ == "__main__":
    main()
