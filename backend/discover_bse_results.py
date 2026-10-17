"""Discover quarterly result announcements from BSE and add them to earnings_calendar.

Unlike the Q4FY26 season (seeded from a user-provided CSV of result dates), from
Q1FY27 onward result announcements are discovered automatically:

  1. Pull all "Financial Results" filings from BSE's bulk announcements API for a
     date window (default: last 3 days; --backfill: from the first day of the
     current calendar quarter, i.e. the start of the season being reported).
     Many companies (E2E Networks, PNB, ...) file results only as an "Outcome
     of Board Meeting". That category is pulled too, but a board meeting can be
     about a QIP, an ESOP allotment or an AGM, so an outcome only counts when
     (a) the SEBI results deadline for the season is still open and (b) its
     headline, or failing that the first pages of its PDF, talk about
     financial results.
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
import io
import logging
import math
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Callable

import psycopg2
from dotenv import load_dotenv

from fetch_bse_presentations import (
    BSE_PDF_BASE,
    RESULTS_KEYWORDS,
    RESULTS_SUBCATS,
    bse_get,
    fetch_scrip_master,
)

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover
    PdfReader = None

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
    "?pageno={page}&strCat={cat}&strPrevDate={from_dt}&strScrip=&strSearch=P"
    "&strToDate={to_dt}&strType=C&subcategory={subcat}"
)
# (category, subcategory) pairs that carry quarterly results. The second is
# noisy outside results season, so match_announcements() gates it on the
# SEBI deadline window — see within_results_deadline().
RESULT_CATEGORIES = [
    ("Result", "Financial+Results"),
    ("Board+Meeting", "Outcome+of+Board+Meeting"),
]
OUTCOME_SUBCAT = "outcome of board meeting"
# Phrases that only appear when a filing actually carries quarterly numbers.
# (RESULTS_KEYWORDS is looser — it includes "outcome of board meeting" itself.)
STRICT_RESULT_PHRASES = [
    "financial results",
    "financial result",
    "results for the quarter",
    "result for the quarter",
    "quarter ended",
    "quarter and half year",
    "half year ended",
    "half-year ended",
    "nine months ended",
    "year ended",
    "unaudited",
    "audited results",
    "statement of profit",
    "total income",
    "revenue from operations",
]
STRICT_MIN_HITS = 2          # for PDF text; a single phrase is too easy to hit by accident
PDF_PAGES_TO_SCAN = 3        # the cover letter is always up front
BSE_PDF_ARCHIVE_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"
# SEBI LODR Reg. 33: quarterly results within 45 days of quarter end; the
# March quarter (audited annual) gets 60 days.
RESULTS_DEADLINE_DAYS = {1: 45, 2: 45, 3: 45, 4: 60}
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


def within_results_deadline(announce_date: date) -> bool:
    """True while the SEBI deadline for the quarter being reported is open.

    An "Outcome of Board Meeting" filed in this window is almost always the
    quarterly result; outside it, board meetings are about fundraises,
    dividends and the like, so those are not treated as results.
    """
    q_num = int(derive_quarter(announce_date)[1])
    deadline = season_start(announce_date) + timedelta(days=RESULTS_DEADLINE_DAYS[q_num])
    return announce_date <= deadline


def results_phrase_hits(text: str) -> int:
    """Number of distinct STRICT_RESULT_PHRASES found in `text` (case-insensitive)."""
    norm = re.sub(r"\s+", " ", (text or "").lower())
    return sum(1 for ph in STRICT_RESULT_PHRASES if ph in norm)


def outcome_pdf_is_results(attachment: str) -> bool:
    """Download a board-meeting outcome PDF and decide whether it carries results.

    Reads only the first PDF_PAGES_TO_SCAN pages (the cover letter). Returns
    False when the PDF is missing, unreadable or image-only — i.e. unverified
    outcomes are left out rather than guessed in.
    """
    if not attachment or PdfReader is None:
        return False
    for base in (BSE_PDF_BASE, BSE_PDF_ARCHIVE_BASE):
        try:
            resp = bse_get(base + attachment, timeout=60)
        except Exception as e:
            log.warning(f"  PDF fetch failed for {attachment}: {e}")
            continue
        if resp.status_code != 200 or not resp.content.startswith(b"%PDF"):
            continue
        try:
            reader = PdfReader(io.BytesIO(resp.content))
            text = " ".join(
                (reader.pages[i].extract_text() or "")
                for i in range(min(PDF_PAGES_TO_SCAN, len(reader.pages)))
            )
        except Exception as e:
            log.warning(f"  PDF parse failed for {attachment}: {e}")
            return False
        return results_phrase_hits(text) >= STRICT_MIN_HITS
    return False


def parse_news_date(news_dt) -> date | None:
    """Parse BSE's NEWS_DT timestamp (e.g. '2026-07-16T16:22:05.53') to a date."""
    if not news_dt:
        return None
    try:
        return datetime.fromisoformat(str(news_dt)).date()
    except ValueError:
        return None


def fetch_result_announcements(from_dt: date, to_dt: date) -> list[dict]:
    """Fetch every filing in RESULT_CATEGORIES for the window, paginated."""
    announcements: list[dict] = []
    for cat, subcat in RESULT_CATEGORIES:
        announcements.extend(_fetch_category(cat, subcat, from_dt, to_dt))
    return announcements


def _fetch_category(cat: str, subcat: str, from_dt: date, to_dt: date) -> list[dict]:
    announcements: list[dict] = []
    page = 1
    total_pages = None
    while page <= MAX_PAGES:
        url = BSE_ANN_BULK_URL.format(
            page=page,
            cat=cat,
            subcat=subcat,
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
                log.info(f"BSE reports {rowcnt} '{subcat.replace('+', ' ')}' filings "
                         f"in window ({total_pages} pages)")
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
    *,
    verify_outcome: Callable[[str], bool] | None = None,
    skip_keys: set[tuple[str, str]] | frozenset = frozenset(),
) -> dict[tuple[str, str], tuple[date, str]]:
    """Filter filings to our universe.

    Returns {(symbol, quarter): (earliest result_date, company name)}. The
    quarter is derived from each filing's own announcement date unless an
    explicit `quarter` override is given, so one scan can straddle two seasons
    and a company that reported in both keeps one row per season.

    "Financial Results" filings are taken as-is (the subcat/keyword check is a
    safety net against category drift). "Outcome of Board Meeting" filings
    are processed after them and must pass, in order:
      1. the SEBI deadline window for the season (within_results_deadline);
      2. not already matched for that (symbol, quarter), and not in skip_keys
         (rows already in the DB) — so no PDF is fetched needlessly;
      3. a results phrase in the headline, or else `verify_outcome(attachment)`
         returning True. With no verifier, keyword-less outcomes are dropped.
    """
    matches: dict[tuple[str, str], tuple[date, str]] = {}
    ordered = sorted(
        announcements, key=lambda a: (a.get("SUBCATNAME") or "").lower() == OUTCOME_SUBCAT
    )
    for ann in ordered:
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

        if subcat == OUTCOME_SUBCAT:
            if not within_results_deadline(result_date):
                continue
            if key in matches or key in skip_keys:
                continue
            title = f"{ann.get('NEWSSUB') or ''} {ann.get('HEADLINE') or ''}"
            if results_phrase_hits(title) == 0:
                if verify_outcome is None or not verify_outcome(ann.get("ATTACHMENTNAME") or ""):
                    continue

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
    quarters_in_window = {args.quarter} if args.quarter else {
        derive_quarter(from_dt), derive_quarter(today)
    }
    existing = load_existing(db_url, quarters_in_window)
    matches = match_announcements(
        announcements, scrip_to_symbol, args.quarter,
        verify_outcome=outcome_pdf_is_results, skip_keys=existing,
    )

    new_matches = {k: v for k, v in matches.items() if k not in existing}
    inserted = insert_results(db_url, new_matches)

    log.info(
        f"Done — filings fetched: {len(announcements)} | in universe: {len(matches)} | "
        f"already known: {len(matches) - len(new_matches)} | inserted: {inserted}"
    )


if __name__ == "__main__":
    main()
