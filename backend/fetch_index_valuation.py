"""
fetch_index_valuation.py — Daily P/E, P/B and dividend yield for NSE indices.

Source: niftyindices.com historical P/E-P/B-Div data (the endpoint behind
https://www.niftyindices.com/reports/historical-data). The site serves at most
one year per request, so history is fetched in yearly windows.

Usage:
    python backend/fetch_index_valuation.py          # incremental (last ~2 weeks)
    python backend/fetch_index_valuation.py --full   # full backfill from 1999

Writes index_valuation_indices + index_valuation_daily (migrate_index_valuation.sql).
Called as a non-fatal step from daily_refresh.py.
"""
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

URL = "https://www.niftyindices.com/BackPage/getpepbHistoricaldataDBtoString"
REFERER = "https://www.niftyindices.com/reports/historical-data"
FULL_START = date(1999, 1, 1)
INCREMENTAL_OVERLAP_DAYS = 14
SLEEP = 0.4

# (api_name accepted by niftyindices, display name, group). Order = display order.
INDICES = [
    ("NIFTY 50", "Nifty 50", "Broad"),
    ("NIFTY NEXT 50", "Nifty Next 50", "Broad"),
    ("NIFTY 100", "Nifty 100", "Broad"),
    ("NIFTY 200", "Nifty 200", "Broad"),
    ("NIFTY 500", "Nifty 500", "Broad"),
    ("NIFTY500 MULTICAP", "Nifty500 Multicap 50:25:25", "Broad"),
    ("NIFTY LARGEMID250", "Nifty LargeMidcap 250", "Broad"),
    ("NIFTY MIDCAP 50", "Nifty Midcap 50", "Broad"),
    ("NIFTY MIDCAP 100", "Nifty Midcap 100", "Broad"),
    ("NIFTY MIDCAP 150", "Nifty Midcap 150", "Broad"),
    ("NIFTY MIDSML 400", "Nifty MidSmallcap 400", "Broad"),
    ("NIFTY SMLCAP 50", "Nifty Smallcap 50", "Broad"),
    ("NIFTY SMLCAP 100", "Nifty Smallcap 100", "Broad"),
    ("NIFTY SMLCAP 250", "Nifty Smallcap 250", "Broad"),
    ("NIFTY MICROCAP250", "Nifty Microcap 250", "Broad"),
    ("NIFTY AUTO", "Nifty Auto", "Sectoral"),
    ("NIFTY BANK", "Nifty Bank", "Sectoral"),
    ("NIFTY PVT BANK", "Nifty Private Bank", "Sectoral"),
    ("NIFTY PSU BANK", "Nifty PSU Bank", "Sectoral"),
    ("NIFTY FIN SERVICE", "Nifty Financial Services", "Sectoral"),
    ("NIFTY FINSRV25 50", "Nifty Financial Services 25/50", "Sectoral"),
    ("NIFTY CEMENT", "Nifty Cement", "Sectoral"),
    ("NIFTY CHEMICALS", "Nifty Chemicals", "Sectoral"),
    ("NIFTY CONSR DURBL", "Nifty Consumer Durables", "Sectoral"),
    ("NIFTY FMCG", "Nifty FMCG", "Sectoral"),
    ("NIFTY HEALTHCARE", "Nifty Healthcare", "Sectoral"),
    ("NIFTY MIDSML HLTH", "Nifty MidSmall Healthcare", "Sectoral"),
    ("NIFTY IT", "Nifty IT", "Sectoral"),
    ("NIFTY MEDIA", "Nifty Media", "Sectoral"),
    ("NIFTY METAL", "Nifty Metal", "Sectoral"),
    ("NIFTY OIL AND GAS", "Nifty Oil & Gas", "Sectoral"),
    ("NIFTY PHARMA", "Nifty Pharma", "Sectoral"),
    ("NIFTY REALTY", "Nifty Realty", "Sectoral"),
    ("NIFTY CAPITAL MKT", "Nifty Capital Markets", "Thematic"),
    ("NIFTY COMMODITIES", "Nifty Commodities", "Thematic"),
    ("NIFTY CONGLOMERATE 50", "Nifty Conglomerate 50", "Thematic"),
    ("NIFTY CPSE", "Nifty CPSE", "Thematic"),
    ("NIFTY ENERGY", "Nifty Energy", "Thematic"),
    ("NIFTY EV", "Nifty EV & New Age Automotive", "Thematic"),
    ("NIFTY HOUSING", "Nifty Housing", "Thematic"),
    ("NIFTY CONSUMPTION", "Nifty India Consumption", "Thematic"),
    ("NIFTY IND DEFENCE", "Nifty India Defence", "Thematic"),
    ("NIFTY IND DIGITAL", "Nifty India Digital", "Thematic"),
    ("NIFTY INDIA INTERNET", "Nifty India Internet", "Thematic"),
    ("NIFTY INDIA MFG", "Nifty India Manufacturing", "Thematic"),
    ("NIFTY INDIA RAILWAYS PSU", "Nifty India Railways PSU", "Thematic"),
    ("NIFTY IND TOURISM", "Nifty India Tourism", "Thematic"),
    ("NIFTY INFRA", "Nifty Infrastructure", "Thematic"),
    ("NIFTY IPO", "Nifty IPO", "Thematic"),
    ("NIFTY MNC", "Nifty MNC", "Thematic"),
    ("NIFTY MOBILITY", "Nifty Mobility", "Thematic"),
    ("NIFTY NONCYC CONS", "Nifty Non-Cyclical Consumer", "Thematic"),
    ("NIFTY PSE", "Nifty PSE", "Thematic"),
    ("NIFTY RURAL", "Nifty Rural", "Thematic"),
    ("NIFTY SERV SECTOR", "Nifty Services Sector", "Thematic"),
    ("NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP", "Nifty Tata Group", "Thematic"),
    ("NIFTY TRANS LOGIS", "Nifty Transportation & Logistics", "Thematic"),
]


# ── Pure helpers ─────────────────────────────────────────────────────────────

def _num(v):
    try:
        x = float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


def parse_rows(rows) -> pd.DataFrame:
    """API rows ({'DATE': '23 Nov 2026', 'pe': '20.65', 'pb': ..., 'divYield': ...})
    -> DataFrame[date, pe, pb, div_yield]; non-numeric / non-positive values -> None."""
    out = []
    for r in rows or []:
        try:
            d = datetime.strptime(str(r.get("DATE", "")).strip(), "%d %b %Y").date()
        except ValueError:
            continue
        out.append({"date": d, "pe": _num(r.get("pe")), "pb": _num(r.get("pb")),
                    "div_yield": _num(r.get("divYield"))})
    df = pd.DataFrame(out, columns=["date", "pe", "pb", "div_yield"])
    return df.drop_duplicates("date", keep="last").sort_values("date").reset_index(drop=True)


def year_windows(start: date, end: date):
    """Split [start, end] into consecutive windows of at most one year."""
    out, a = [], start
    while a <= end:
        b = min(a + timedelta(days=364), end)
        out.append((a, b))
        a = b + timedelta(days=1)
    return out


# ── IO ───────────────────────────────────────────────────────────────────────

def _session():
    from curl_cffi import requests as cr
    s = cr.Session(impersonate="chrome")
    s.get(REFERER, timeout=30)
    return s


def fetch_window(s, api_name: str, a: date, b: date):
    body = {"cinfo": "{'name':'%s','startDate':'%s','endDate':'%s','indexName':'%s'}"
            % (api_name, a.strftime("%d-%b-%Y"), b.strftime("%d-%b-%Y"), api_name)}
    headers = {"Content-Type": "application/json; charset=UTF-8", "X-Requested-With": "XMLHttpRequest",
               "Referer": REFERER, "Origin": "https://www.niftyindices.com"}
    for attempt in range(3):
        try:
            r = s.post(URL, json=body, headers=headers, timeout=60)
            data = json.loads(r.text)
            if isinstance(data, dict):
                data = json.loads(data.get("d") or "[]")
            return parse_rows(data)
        except Exception as exc:  # noqa: BLE001
            if attempt == 2:
                logger.warning("  %s %s..%s failed: %s", api_name, a, b, exc)
            time.sleep(2 * (attempt + 1))
    return parse_rows([])


def _write(sql, rows, retries: int = 3, **kw):
    """Short-lived connection per write: the pooler drops connections left idle
    while we spend ~20s per index fetching from niftyindices."""
    from db import get_psycopg2_conn
    for attempt in range(retries):
        conn = None
        try:
            conn = get_psycopg2_conn()
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, **kw)
            conn.commit()
            return
        except Exception:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            time.sleep(3 * (attempt + 1))
        finally:
            if conn is not None:
                conn.close()


def run(full: bool = False) -> int:
    from db import get_psycopg2_conn
    _write("""
        INSERT INTO index_valuation_indices (api_name, display_name, grp, sort_order) VALUES %s
        ON CONFLICT (api_name) DO UPDATE SET display_name = EXCLUDED.display_name,
            grp = EXCLUDED.grp, sort_order = EXCLUDED.sort_order""",
        [(a, d, g, i) for i, (a, d, g) in enumerate(INDICES)])
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT api_name, MAX(date) FROM index_valuation_daily GROUP BY api_name")
            last = dict(cur.fetchall())
    finally:
        conn.close()

    s = _session()
    today = date.today()
    total = 0
    for api_name, display, _ in INDICES:
        start = FULL_START if (full or api_name not in last) else last[api_name] - timedelta(days=INCREMENTAL_OVERLAP_DAYS)
        frames = []
        for a, b in year_windows(start, today):
            frames.append(fetch_window(s, api_name, a, b))
            time.sleep(SLEEP)
        df = pd.concat(frames, ignore_index=True).drop_duplicates("date", keep="last") if frames else parse_rows([])
        if df.empty:
            logger.info("  %s: no data", display)
            continue
        rows = [tuple(None if (isinstance(v, float) and pd.isna(v)) else v
                      for v in (api_name, r.date, r.pe, r.pb, r.div_yield)) for r in df.itertuples(index=False)]
        try:
            _write("""
                INSERT INTO index_valuation_daily (api_name, date, pe, pb, div_yield) VALUES %s
                ON CONFLICT (api_name, date) DO UPDATE SET
                    pe = EXCLUDED.pe, pb = EXCLUDED.pb, div_yield = EXCLUDED.div_yield""",
                rows, page_size=2000)
        except Exception as exc:  # noqa: BLE001 — one index failing must not stop the rest
            logger.warning("  %s: write failed: %s", display, exc)
            continue
        total += len(rows)
        logger.info("  %s: %d rows (%s → %s)", display, len(rows), df["date"].min(), df["date"].max())
    logger.info("  index valuation: %d rows upserted", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    run(full="--full" in sys.argv)
