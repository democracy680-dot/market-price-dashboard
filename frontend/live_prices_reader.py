"""
live_prices_reader.py — Reads live intraday prices from data/live_prices.db (SQLite).

Used exclusively by the Quarterly Results tab in app.py when today's date is selected.
Returns an empty dict on any error — never raises, so the frontend always degrades
gracefully to DB prices if the poller is not running.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# Resolve relative to this file's location (frontend/) → project root → data/
_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "live_prices.db"

_MAX_STALENESS_SECONDS = 5 * 60  # 5 minutes


def get_live_prices(symbols: list[str]) -> dict:
    """
    Fetch latest live prices for the given symbols from SQLite.

    Returns:
        {
            "INFY": {
                "ltp": 1542.30,
                "prev_close": 1510.00,
                "pct_change": 2.14,
                "fetched_at": "2026-05-09T04:02:11.123456"  # UTC ISO string
            },
            ...
        }
        Empty dict if the DB file doesn't exist, no rows found, or any error.
    """
    if not _DB_PATH.exists():
        return {}

    if not symbols:
        return {}

    try:
        conn = sqlite3.connect(str(_DB_PATH), timeout=2)
        placeholders = ",".join("?" * len(symbols))
        rows = conn.execute(
            f"SELECT symbol, ltp, prev_close, pct_change, fetched_at "
            f"FROM live_prices WHERE symbol IN ({placeholders})",
            symbols,
        ).fetchall()
        conn.close()
    except Exception:
        return {}

    return {
        row[0]: {
            "ltp": row[1],
            "prev_close": row[2],
            "pct_change": row[3],
            "fetched_at": row[4],
        }
        for row in rows
    }


def is_live_data_fresh(fetched_at_utc: str, max_age_seconds: int = _MAX_STALENESS_SECONDS) -> bool:
    """
    Return True if the fetched_at UTC ISO timestamp is within max_age_seconds of now.
    Returns False on any parse error.
    """
    try:
        fetched = datetime.fromisoformat(fetched_at_utc).replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - fetched).total_seconds()
        return age <= max_age_seconds
    except Exception:
        return False


def get_live_status_text(live: dict) -> str | None:
    """
    Return a human-readable status string like '2 min ago' for the LIVE badge,
    based on the most recently fetched timestamp across all returned symbols.
    Returns None if live dict is empty.
    """
    if not live:
        return None
    try:
        timestamps = [
            datetime.fromisoformat(v["fetched_at"]).replace(tzinfo=timezone.utc)
            for v in live.values()
            if v.get("fetched_at")
        ]
        if not timestamps:
            return None
        latest = max(timestamps)
        age_secs = int((datetime.now(timezone.utc) - latest).total_seconds())
        if age_secs < 90:
            return "just now"
        return f"{age_secs // 60} min ago"
    except Exception:
        return None
