"""Discover quarterly result announcements from BSE and add them to earnings_calendar.

Unlike the Q4FY26 season (seeded from a user-provided CSV of result dates), from
Q1FY27 onward result announcements are discovered automatically:

  1. Pull all "Financial Results" filings from BSE's bulk announcements API for a
     date window (default: last 3 days; --backfill: from the first day of the
     current calendar quarter, i.e. the start of the season being reported).
  2. Map BSE scrip codes to NSE symbols via the scrip master, restricted to
     active stocks in our DB.
  3. Label each filing with the fiscal quarter it reports (derived from its own
     announcement date, so a window spanning two seasons labels each correctly)
     and insert (symbol, result_date, quarter) into earnings_calendar. The
     existing fetch_bse_presentations.py run then enriches the new rows with
     PDF/PPT links.

Exits non-zero when BSE cannot be reached so the CI step turns red instead of
quietly "succeeding" with zero inserts.

Usage:
  python discover_bse_results.py                    # daily mode (last 3 days)
  python discover_bse_results.py --backfill         # scan from season start
  python discover_bse_results.py --from 2026-10-01  # scan from an explicit date
  python discover_bse_results.py --quarter Q2FY27   # force one quarter label
"""

import argparse
import logging
import math
import os
import sys
import time
from datetime import date, datetime, timedelta

import psycopg2
from dotenv import load_dotenv

from fetch_bse_presentations import (
    RESULTS_KEYWORDS,
    RESULTS_SUBCATS,
    bse_get,
    fetch_scrip_master,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

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


def season_start(today: date) -> date:
    """First day of the calendar quarter containing `today`.

    Results for a fiscal quarter start flowing in the first days of the next
    calendar quarter (Q2FY27 results from 1 Oct 2026), so this is the natural
    lower bound for a --backfill scan.
    """
    return date(today.year, 3 * ((today.month - 1) // 3) + 1, 1)


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
            resp = bse_get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            if page == 1:
                # Nothing fetched at all — almost always a block/outage. Fail
                # loudly so the CI step is red rather than "0 inserted".
                raise RuntimeError(f"BSE bulk API unreachable: {e}") from e
            log.error(f"BSE bulk API error on page {page}: {e}; keeping {len(announcements)} rows")
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
    announcements: list[dict],
    scrip_to_symbol: dict[str, str],
    quarter: str | None = None,
) -> dict[tuple[str, str], tuple[date, str]]:
    """Filter filings to our universe.

    Returns {(symbol, quarter): (earliest result_date, company name)}. The
    quarter is derived from each filing's own announcement date unless an
    explicit `quarter` override is given, so one scan can straddle two seasons
    and a company that reported in both keeps one row per season.

    The bulk API is already filtered to Financial Results, but the
    subcat/keyword check is kept as a safety net against category drift.
    """
    matches: dict[tuple[str, str], tuple[date, str]] = {}
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
        key = (symbol, quarter or derive_quarter(result_date))
        if key not in matches or result_date < matches[key][0]:
            matches[key] = (result_date, name)
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


def load_existing(db_url: str, quarters: set[str]) -> set[tuple[str, str]]:
    """(symbol, quarter) pairs already in earnings_calendar for the given quarters."""
    if not quarters:
        return set()
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, quarter FROM earnings_calendar WHERE quarter = ANY(%s)",
                (sorted(quarters),),
            )
            return {(row[0], row[1]) for row in cur.fetchall()}
    finally:
        conn.close()


def insert_results(db_url: str, matches: dict[tuple[str, str], tuple[date, str]]) -> int:
    conn = _get_conn(db_url)
    inserted = 0
    try:
        with conn.cursor() as cur:
            for (symbol, quarter), (result_date, name) in sorted(matches.items()):
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
                    log.info(f"  + {symbol} — {name} ({result_date}, {quarter})")
        conn.commit()
    finally:
        conn.close()
    return inserted


def main():
    parser = argparse.ArgumentParser(description="Discover BSE result announcements")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help=f"scan from the start of the current season instead of the last "
             f"{DAILY_LOOKBACK_DAYS} days",
    )
    parser.add_argument(
        "--from", dest="from_date", type=date.fromisoformat, metavar="YYYY-MM-DD",
        help="scan from this date (overrides --backfill)",
    )
    parser.add_argument(
        "--quarter",
        help="force this quarter label on every filing instead of deriving it "
             "from each announcement date (e.g. Q2FY27)",
    )
    args = parser.parse_args()

    db_url = os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")
    today = date.today()
    if args.from_date:
        from_dt = args.from_date
    elif args.backfill:
        from_dt = season_start(today)
    else:
        from_dt = today - timedelta(days=DAILY_LOOKBACK_DAYS)
    log.info(f"Discovering results {from_dt} → {today} "
             f"(quarter: {args.quarter or 'derived per filing'})")

    try:
        symbol_to_scrip = fetch_scrip_master()
    except Exception as e:
        log.error(f"Could not fetch BSE scrip master: {e}. Aborting.")
        sys.exit(1)

    active = load_active_symbols(db_url)
    scrip_to_symbol = {
        scrip: symbol for symbol, scrip in symbol_to_scrip.items() if symbol in active
    }
    log.info(f"Universe: {len(active)} active stocks, {len(scrip_to_symbol)} with BSE scrip codes")

    try:
        announcements = fetch_result_announcements(from_dt, today)
    except RuntimeError as e:
        log.error(f"{e}. Aborting.")
        sys.exit(1)
    matches = match_announcements(announcements, scrip_to_symbol, args.quarter)

    existing = load_existing(db_url, {q for _, q in matches})
    new_matches = {k: v for k, v in matches.items() if k not in existing}
    inserted = insert_results(db_url, new_matches)

    log.info(
        f"Done — filings fetched: {len(announcements)} | in universe: {len(matches)} | "
        f"already known: {len(matches) - len(new_matches)} | inserted: {inserted}"
    )


if __name__ == "__main__":
    main()
