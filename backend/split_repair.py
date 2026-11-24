"""
split_repair.py — Detect and repair unadjusted splits / bonuses in prices_daily.

Why: the daily refresh only fetches recent rows. When a stock splits, Yahoo
re-adjusts its whole history (usually 3–9 days later), but our older rows keep
the pre-split prices, so the stock looks like it crashed 50–90% in one day.

How:
  * compare_closes()  — per symbol, compare our closes with Yahoo's current
                        (split-adjusted, auto_adjust=False) closes on common dates.
                        Any common date off by more than TOL means our history is stale.
  * repair_rows()     — Yahoo's rows are authoritative for every date Yahoo has;
                        our rows older than Yahoo's first date are scaled by the
                        factor observed on the first common date (prices × f,
                        volume ÷ f); our rows inside Yahoo's range that Yahoo no
                        longer has are deleted.
  * repair_symbols()  — IO: fetch 2y from Yahoo for the given symbols and write.

Used by the one-off repair (python backend/split_repair.py) and by daily_refresh.py,
which checks a ~20-day overlap window every day.

Demergers are NOT fixable this way (Yahoo shows the same drop); the theme index
guards against those separately (see compute_theme_index.MAX_ABS_RET).
"""
import logging
import sys

import pandas as pd
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

TOL = 0.03          # >3% disagreement on a common date = stale/unadjusted history
PRICE_COLS = ["open", "high", "low", "close"]


# ── Pure functions ────────────────────────────────────────────────────────────

def compare_closes(db: pd.DataFrame, yahoo: pd.DataFrame, tol: float = TOL) -> pd.DataFrame:
    """db: [symbol, date, close]; yahoo: [symbol, date, close].
    Returns one row per symbol with any mismatch:
    [symbol, common, n_bad, ratio_med, first_bad, last_bad] (ratio = ours / Yahoo)."""
    m = db[["symbol", "date", "close"]].merge(
        yahoo[["symbol", "date", "close"]], on=["symbol", "date"], suffixes=("_db", "_y"))
    m = m[(m["close_y"] > 0) & m["close_db"].notna()]
    if m.empty:
        return pd.DataFrame(columns=["symbol", "common", "n_bad", "ratio_med", "first_bad", "last_bad"])
    m["ratio"] = m["close_db"].astype(float) / m["close_y"].astype(float)
    m["bad"] = (m["ratio"] - 1).abs() > tol
    common = m.groupby("symbol").size().rename("common")
    bad = m[m["bad"]].groupby("symbol").agg(
        n_bad=("ratio", "size"), ratio_med=("ratio", "median"),
        first_bad=("date", "min"), last_bad=("date", "max"))
    return bad.join(common).reset_index()[["symbol", "common", "n_bad", "ratio_med", "first_bad", "last_bad"]]


def repair_rows(db_sym: pd.DataFrame, y_sym: pd.DataFrame):
    """For ONE symbol. db_sym / y_sym: [date, open, high, low, close, volume].
    Returns (upsert_df, delete_dates):
      upsert_df    — Yahoo's rows + our older rows rescaled to Yahoo's basis
      delete_dates — our dates inside Yahoo's range that Yahoo doesn't have."""
    y = y_sym.dropna(subset=["close"]).sort_values("date")
    d = db_sym.sort_values("date")
    if y.empty:
        return pd.DataFrame(columns=["date", *PRICE_COLS, "volume"]), []
    y_first, y_last = y["date"].iloc[0], y["date"].iloc[-1]

    common = d.merge(y[["date", "close"]], on="date", suffixes=("", "_y"))
    common = common[(common["close"] > 0) & (common["close_y"] > 0)]
    older = d[d["date"] < y_first]
    out = [y[["date", *PRICE_COLS, "volume"]]]
    if not older.empty and not common.empty:
        f = float(common["close_y"].iloc[0]) / float(common["close"].iloc[0])
        if abs(f - 1) > 1e-9:
            scaled = older.copy()
            scaled[PRICE_COLS] = scaled[PRICE_COLS].astype(float) * f
            scaled["volume"] = (scaled["volume"].astype(float) / f).round()
            out.append(scaled[["date", *PRICE_COLS, "volume"]])
    inside = d[(d["date"] >= y_first) & (d["date"] <= y_last)]
    delete_dates = sorted(set(inside["date"]) - set(y["date"]))
    return pd.concat(out, ignore_index=True).sort_values("date"), delete_dates


# ── IO ────────────────────────────────────────────────────────────────────────

_UPSERT = """
    INSERT INTO prices_daily (symbol, date, open, high, low, close, volume) VALUES %s
    ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume
"""


def _clean(v):
    return None if v is None or (isinstance(v, float) and pd.isna(v)) else v


def repair_symbols(symbols, conn, yahoo_df: pd.DataFrame | None = None) -> dict:
    """Re-download 2y of Yahoo history for `symbols` (NSE symbols) and rewrite
    their prices_daily rows. yahoo_df (optional) = pre-fetched fetch_prices()
    output with a 'symbol' column. Returns {symbol: rows_written}."""
    from fetcher import fetch_prices
    symbols = sorted(set(symbols))
    if not symbols:
        return {}
    cur = conn.cursor()
    cur.execute("SELECT symbol, yahoo_symbol FROM stocks WHERE symbol = ANY(%s)", (symbols,))
    ymap = {y: s for s, y in cur.fetchall() if y}
    if yahoo_df is None:
        yahoo_df = fetch_prices(list(ymap))                     # period=2y
        yahoo_df["symbol"] = yahoo_df["yahoo_symbol"].map(ymap)
    written = {}
    for sym in symbols:
        y = yahoo_df[yahoo_df["symbol"] == sym]
        if y.empty:
            logger.warning("  split repair: no Yahoo data for %s — skipped", sym)
            continue
        cur.execute("SELECT date, open, high, low, close, volume FROM prices_daily WHERE symbol = %s", (sym,))
        d = pd.DataFrame(cur.fetchall(), columns=["date", *PRICE_COLS, "volume"])
        for c in [*PRICE_COLS, "volume"]:
            d[c] = pd.to_numeric(d[c], errors="coerce")
        up, dele = repair_rows(d, y[["date", *PRICE_COLS, "volume"]])
        rows = [(sym, r.date, *[_clean(float(getattr(r, c))) if pd.notna(getattr(r, c)) else None
                                 for c in PRICE_COLS],
                 None if pd.isna(r.volume) else int(r.volume)) for r in up.itertuples(index=False)]
        if dele:
            cur.execute("DELETE FROM prices_daily WHERE symbol = %s AND date = ANY(%s)", (sym, dele))
        execute_values(cur, _UPSERT, rows, page_size=1000)
        conn.commit()
        written[sym] = len(rows)
        logger.info("  split repair: %s — %d rows rewritten, %d stale rows deleted", sym, len(rows), len(dele))
    cur.close()
    return written


def find_stale_symbols(prices_df: pd.DataFrame, symbol_map: dict, conn, tol: float = TOL) -> pd.DataFrame:
    """Daily check: prices_df is the incremental fetch (yahoo_symbol, date, close, ...)
    covering an overlap window with the DB. Returns compare_closes() rows."""
    y = prices_df.assign(symbol=prices_df["yahoo_symbol"].map(symbol_map)).dropna(subset=["symbol"])
    if y.empty:
        return compare_closes(pd.DataFrame(columns=["symbol", "date", "close"]), y)
    cur = conn.cursor()
    cur.execute("SELECT symbol, date, close FROM prices_daily WHERE date >= %s AND date <= %s",
                (y["date"].min(), y["date"].max()))
    db = pd.DataFrame(cur.fetchall(), columns=["symbol", "date", "close"])
    cur.close()
    db["close"] = pd.to_numeric(db["close"], errors="coerce")
    return compare_closes(db, y, tol)


if __name__ == "__main__":
    # Usage: python backend/split_repair.py SYMBOL [SYMBOL ...]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    from db import get_psycopg2_conn
    c = get_psycopg2_conn()
    try:
        print(repair_symbols(sys.argv[1:], c))
    finally:
        c.close()
