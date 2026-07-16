"""Discover quarterly result announcements from BSE and add them to earnings_calendar.

Unlike the Q4FY26 season (seeded from a user-provided CSV of result dates), from
Q1FY27 onward result announcements are discovered automatically:

  1. Pull all "Financial Results" filings from BSE's bulk announcements API for a
     date window (default: last 3 days; --backfill: from SEASON_START).
  2. Map BSE scrip codes to NSE symbols via the scrip master, restricted to
     active stocks in our DB.
  3. Insert (symbol, result_date, quarter) into earnings_calendar. The existing
     fetch_bse_presentations.py run then enriches the new rows with PDF/PPT links.

Usage:
  python discover_bse_results.py               # daily mode (last 3 days)
  python discover_bse_results.py --backfill    # scan from SEASON_START to today
  python discover_bse_results.py --quarter Q1FY27   # override derived quarter
"""

import argparse
import logging
import math
import os
import time
from datetime import date, datetime, timedelta

import psycopg2
import requests
from dotenv import load_dotenv

from fetch_bse_presentations import (
    HEADERS,
    RESULTS_KEYWORDS,
    RESULTS_SUBCATS,
    fetch_scrip_master,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SEASON_START = date(2026, 7, 1)
DAILY_LOOKBACK_DAYS = 3  # overlap survives skipped runs/holidays; dedup makes it free

BSE_ANN_BULK_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?pageno={page}&strCat=Result&strPrevDate={from_dt}&strScrip=&strSearch=P"
    "&strToDate={to_dt}&strType=C&subcategory=Financial+Results"
)
PAGE_SIZE = 50
MAX_PAGES = 200


def derive_quarter(announce_date: date) -> str:
    """Map an announcement date to the fiscal quarter being reported.

    Results are announced in the quarter after the one they cover (Indian FY
    Apr-Mar), so shift back one quarter: Jul-Sep 2026 -> Q1FY27, Apr-Jun 2026
    -> Q4FY26 (fiscal year labelled by its March year-end).
    """
    ann_q = (announce_date.month - 4) % 12 // 3  # fiscal quarter index, Apr-Jun = 0
    fy_end_year = announce_date.year + 1 if announce_date.month >= 4 else announce_date.year
    report_q = ann_q - 1
    if report_q < 0:  # Apr-Jun announcement reports Q4 of the previous fiscal year
        report_q = 3
        fy_end_year -= 1
    return f"Q{report_q + 1}FY{fy_end_year % 100:02d}"


def parse_news_date(news_dt) -> date | None:
    """Parse BSE's NEWS_DT timestamp (e.g. '2026-07-16T16:22:05.53') to a date."""
    if not news_dt:
        return None
    try:
        return datetime.fromisoformat(str(news_dt)).date()
    except ValueError:
        return None


def fetch_result_announcements(from_dt: date, to_dt: date) -> list[dict]:
    """Fetch all Financial Results filings from BSE for the window, paginated."""
    announcements: list[dict] = []
    page = 1
    total_pages = None
    while page <= MAX_PAGES:
        url = BSE_ANN_BULK_URL.format(
            page=page,
            from_dt=from_dt.strftime("%Y%m%d"),
            to_dt=to_dt.strftime("%Y%m%d"),
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"BSE bulk API error on page {page}: {e}")
            return announcements

        rows = data.get("Table", []) if isinstance(data, dict) else []
        if not rows:
            break
        announcements.extend(rows)

        if total_pages is None:
            try:
                rowcnt = int(data["Table1"][0]["ROWCNT"])
                total_pages = math.ceil(rowcnt / PAGE_SIZE)
                log.info(f"BSE reports {rowcnt} result filings in window ({total_pages} pages)")
            except (KeyError, IndexError, TypeError, ValueError):
                log.warning("Could not read ROWCNT; paginating until an empty page")
                total_pages = MAX_PAGES
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.5)

    return announcements


def match_announcements(
    announcements: list[dict], scrip_to_symbol: dict[str, str]
) -> dict[str, tuple[date, str]]:
    """Filter filings to our universe -> {symbol: (earliest result_date, company name)}.

    The bulk API is already filtered to Financial Results, but the
    subcat/keyword check is kept as a safety net against category drift.
    """
    matches: dict[str, tuple[date, str]] = {}
    for ann in announcements:
        subcat = (ann.get("SUBCATNAME") or "").lower()
        headline = (ann.get("NEWSSUB") or "").lower()
        if subcat not in RESULTS_SUBCATS and not any(kw in headline for kw in RESULTS_KEYWORDS):
            continue

        symbol = scrip_to_symbol.get(str(ann.get("SCRIP_CD") or "").strip())
        if not symbol:
            continue

        result_date = parse_news_date(ann.get("NEWS_DT"))
        if result_date is None:
            continue

        name = (ann.get("SLONGNAME") or "").strip() or symbol
        if symbol not in matches or result_date < matches[symbol][0]:
            matches[symbol] = (result_date, name)
    return matches


def _get_conn(db_url: str):
    return psycopg2.connect(db_url, connect_timeout=15)


def load_active_symbols(db_url: str) -> set[str]:
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM stocks WHERE is_active = TRUE")
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def load_existing_for_quarter(db_url: str, quarter: str) -> set[str]:
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM earnings_calendar WHERE quarter = %s", (quarter,))
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def insert_results(db_url: str, matches: dict[str, tuple[date, str]], quarter: str) -> int:
    conn = _get_conn(db_url)
    inserted = 0
    try:
        with conn.cursor() as cur:
            for symbol, (result_date, name) in sorted(matches.items()):
                cur.execute(
                    """
                    INSERT INTO earnings_calendar (symbol, result_date, quarter)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol, result_date) DO NOTHING
                    """,
                    (symbol, result_date, quarter),
                )
                if cur.rowcount:
                    inserted += 1
                    log.info(f"  + {symbol} — {name} ({result_date})")
        conn.commit()
    finally:
        conn.close()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Discover BSE result announcements")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=f"scan from {SEASON_START} instead of the last {DAILY_LOOKBACK_DAYS} days",
    )
    parser.add_argument("--quarter", help="override the derived quarter label (e.g. Q1FY27)")
    args = parser.parse_args()

    db_url = os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")
    today = date.today()
    from_dt = SEASON_START if args.backfill else today - timedelta(days=DAILY_LOOKBACK_DAYS)
    quarter = args.quarter or derive_quarter(today)
    log.info(f"Discovering results {from_dt} → {today} (quarter: {quarter})")

    try:
        symbol_to_scrip = fetch_scrip_master()
    except Exception as e:
        log.error(f"Could not fetch BSE scrip master: {e}. Aborting.")
        return

    active = load_active_symbols(db_url)
    scrip_to_symbol = {
        scrip: symbol for symbol, scrip in symbol_to_scrip.items() if symbol in active
    }
    log.info(f"Universe: {len(active)} active stocks, {len(scrip_to_symbol)} with BSE scrip codes")

    announcements = fetch_result_announcements(from_dt, today)
    matches = match_announcements(announcements, scrip_to_symbol)

    existing = load_existing_for_quarter(db_url, quarter)
    new_matches = {sym: v for sym, v in matches.items() if sym not in existing}
    inserted = insert_results(db_url, new_matches, quarter)

    log.info(
        f"Done — filings fetched: {len(announcements)} | in universe: {len(matches)} | "
        f"already known: {len(matches) - len(new_matches)} | inserted: {inserted}"
    )


if __name__ == "__main__":
    main()
