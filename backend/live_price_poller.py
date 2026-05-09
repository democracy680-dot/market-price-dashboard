"""
live_price_poller.py — Real-time price poller for quarterly result day stocks.

Runs as a standalone process (via scripts/run_live_poller.bat).
Polls Zerodha KiteConnect every 60 seconds for stocks that announce
quarterly results today, writing prices to data/live_prices.db (SQLite).

This script NEVER touches Supabase for writes. It reads earnings_calendar
once at startup to get today's symbols, then polls Zerodha only.

Market hours: 09:15–15:35 IST (Monday–Friday only).
"""

import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Allow running from project root or from backend/
sys.path.insert(0, str(Path(__file__).resolve().parent))
from zerodha_auth import get_kite_client

load_dotenv()

IST = ZoneInfo("Asia/Kolkata")
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "live_prices.db"

MARKET_OPEN  = (9, 15)   # 09:15 IST
MARKET_CLOSE = (15, 35)  # 15:35 IST (5 min after official close for stragglers)
POLL_INTERVAL = 60        # seconds

_running = True


def _signal_handler(sig, frame):
    global _running
    print("\n[Poller] Received stop signal. Shutting down gracefully...")
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def _ist_now() -> datetime:
    return datetime.now(IST)


def _is_market_open() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def _wait_for_market_open() -> bool:
    """Block until market opens or exit if today is a weekend. Returns False if should exit."""
    now = _ist_now()
    if now.weekday() >= 5:
        print(f"[Poller] Today is {'Saturday' if now.weekday() == 5 else 'Sunday'}. Market closed. Exiting.")
        return False

    t = (now.hour, now.minute)
    if t > MARKET_CLOSE:
        print(f"[Poller] Market already closed for today ({now.strftime('%H:%M')} IST). Exiting.")
        return False

    if t < MARKET_OPEN:
        open_dt = now.replace(hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0)
        wait_secs = (open_dt - now).seconds
        print(f"[Poller] Market opens at 09:15 IST. Waiting {wait_secs // 60}m {wait_secs % 60}s...")
        time.sleep(wait_secs)

    return True


def _init_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS live_prices (
            symbol     TEXT PRIMARY KEY,
            ltp        REAL NOT NULL,
            prev_close REAL,
            pct_change REAL,
            fetched_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def _fetch_todays_symbols() -> list[str]:
    """Read today's earnings symbols from Supabase (read-only)."""
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL not set in .env")

    engine = create_engine(db_url, connect_args={"connect_timeout": 10})
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol FROM earnings_calendar WHERE result_date = :today"),
            {"today": str(date.today())},
        ).fetchall()
    engine.dispose()
    return [row[0] for row in rows]


def _to_nse_instruments(symbols: list[str]) -> list[str]:
    """Convert bare NSE symbols to Zerodha instrument format: NSE:SYMBOL."""
    return [f"NSE:{s}" for s in symbols]


def _poll_once(kite, instruments: list[str], symbols: list[str], conn: sqlite3.Connection) -> None:
    """Fetch LTPs for all instruments and upsert into SQLite."""
    try:
        ltp_data = kite.ltp(instruments)
    except Exception as e:
        print(f"[Poller] Zerodha API error: {e} — skipping this cycle.")
        return

    fetched_at = datetime.utcnow().isoformat()
    rows = []
    for sym, instr in zip(symbols, instruments):
        info = ltp_data.get(instr)
        if not info:
            continue
        ltp = info["last_price"]
        prev_close = info.get("ohlc", {}).get("close")
        pct_change = ((ltp - prev_close) / prev_close * 100) if prev_close else None
        rows.append((sym, ltp, prev_close, pct_change, fetched_at))

    if rows:
        conn.executemany(
            "REPLACE INTO live_prices (symbol, ltp, prev_close, pct_change, fetched_at) VALUES (?,?,?,?,?)",
            rows,
        )
        conn.commit()
        now_ist = _ist_now().strftime("%H:%M:%S")
        print(f"[Poller] {now_ist} IST — polled {len(rows)}/{len(symbols)} symbols.")
    else:
        print(f"[Poller] No data returned from Zerodha for {len(symbols)} symbols.")


def main():
    global _running
    print("[Poller] Starting Zerodha live price poller...")

    if not _wait_for_market_open():
        sys.exit(0)

    # Authenticate once at startup
    try:
        kite = get_kite_client()
    except Exception as e:
        print(f"[Poller] Authentication failed: {e}")
        sys.exit(1)

    # Fetch today's earnings symbols (read-only Supabase query)
    try:
        symbols = _fetch_todays_symbols()
    except Exception as e:
        print(f"[Poller] Could not fetch today's symbols: {e}")
        sys.exit(1)

    if not symbols:
        print("[Poller] No earnings announcements scheduled for today. Exiting.")
        sys.exit(0)

    instruments = _to_nse_instruments(symbols)
    print(f"[Poller] Tracking {len(symbols)} symbols: {', '.join(symbols)}")

    conn = _init_db()

    # Main polling loop
    while _running and _is_market_open():
        _poll_once(kite, instruments, symbols, conn)
        # Sleep in 1s chunks so Ctrl+C is responsive
        for _ in range(POLL_INTERVAL):
            if not _running:
                break
            time.sleep(1)

    conn.close()
    print("[Poller] Market closed or stopped. Poller exited cleanly.")


if __name__ == "__main__":
    main()
