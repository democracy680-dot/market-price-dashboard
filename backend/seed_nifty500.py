"""Seed all 500 Nifty 500 stocks and rebuild index_membership."""
from db import get_engine
from sqlalchemy import text
import csv
from pathlib import Path

engine = get_engine()
csv_path = Path(__file__).parent.parent / "data" / "indexes" / "nifty_500.csv"

rows = []
with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        symbol = row.get("symbol", "").strip()
        name = row.get("name", "").strip()
        if symbol:
            rows.append({
                "symbol": symbol,
                "name": name,
                "yahoo_symbol": symbol + ".NS",
                "screener_url": f"https://www.screener.in/company/{symbol}/consolidated/",
                "tradingview_url": f"https://www.tradingview.com/chart/?symbol=NSE%3A{symbol}",
            })

print(f"Loaded {len(rows)} stocks from nifty_500.csv")

BATCH = 50

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# Upsert stocks in batches
total = 0
for batch in chunks(rows, BATCH):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stocks (symbol, name, yahoo_symbol, screener_url, tradingview_url, is_active, added_at)
            VALUES (:symbol, :name, :yahoo_symbol, :screener_url, :tradingview_url, TRUE, NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                name=EXCLUDED.name,
                yahoo_symbol=EXCLUDED.yahoo_symbol,
                screener_url=EXCLUDED.screener_url,
                tradingview_url=EXCLUDED.tradingview_url
        """), batch)
        total += len(batch)
print(f"Upserted {total} stocks into stocks table")

# Rebuild NIFTY_500 membership in batches
with engine.begin() as conn:
    conn.execute(text("DELETE FROM index_membership WHERE index_name = 'NIFTY_500'"))

membership = [{"symbol": r["symbol"], "index_name": "NIFTY_500"} for r in rows]
total_m = 0
for batch in chunks(membership, BATCH):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO index_membership (symbol, index_name)
            VALUES (:symbol, :index_name)
            ON CONFLICT DO NOTHING
        """), batch)
        total_m += len(batch)
print(f"Inserted {total_m} rows into index_membership for NIFTY_500")

with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM index_membership WHERE index_name = 'NIFTY_500'")).fetchone()[0]
    stocks = conn.execute(text("SELECT COUNT(*) FROM stocks WHERE is_active=TRUE")).fetchone()[0]
    print(f"NIFTY_500 membership: {count}")
    print(f"Total active stocks:  {stocks}")

print("Done.")
