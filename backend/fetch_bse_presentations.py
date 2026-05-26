"""Fetch BSE filing PDF links for earnings-season companies.

Runs after the daily refresh. For each company in earnings_calendar:
  - presentation_url: looks for an 'Investor Presentation' filing within
    result_date to result_date + 10 days.
  - result_pdf_url: looks for the 'Financial Results' quarterly filing within
    result_date to result_date + 7 days.
Both URLs are written to the earnings_calendar table.
"""

import os
import time
import logging
from datetime import date, timedelta

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BSE_SCRIP_MASTER_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
BSE_ANN_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?strCat=-1&strType=C&strScrip={scrip}&strSearch=P"
    "&strPrevDate={from_dt}&strToDate={to_dt}"
)
BSE_PDF_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}

# Primary: exact subcategory match. Fallback: keyword in headline.
PRESENTATION_SUBCATS = {"investor presentation"}
PRESENTATION_KEYWORDS = ["investor presentation", "corporate presentation"]

# Financial Results use the same BSE endpoint as presentations (strSearch=P is required)
# BSE typically returns SUBCATNAME = "Results" (not "financial results")
RESULTS_SUBCATS = {"financial results", "quarterly results", "results", "half yearly results", "annual results"}
RESULTS_KEYWORDS = [
    "unaudited financial results",
    "audited financial results",
    "quarterly financial results",
    "financial results for",
    "standalone financial results",
    "consolidated financial results",
    "outcome of board meeting",
    "board meeting outcome",
    "results for the quarter",
    "results for the half",
    "results for the year",
]


def fetch_scrip_master() -> dict[str, str]:
    """Return {nse_symbol_upper: bse_scrip_code} for all active BSE equities.

    BSE scrip_id matches NSE symbol for ~99% of stocks, so we map by symbol
    rather than ISIN (which is unpopulated in our DB).
    """
    log.info("Fetching BSE scrip master...")
    resp = requests.get(BSE_SCRIP_MASTER_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # API returns list directly or wrapped in a key — handle both
    items = data if isinstance(data, list) else data.get("Table", [])
    mapping = {}
    for item in items:
        scrip_id = (item.get("scrip_id") or "").strip().upper()
        scrip_cd = str(item.get("SCRIP_CD") or "").strip()
        if scrip_id and scrip_cd:
            mapping[scrip_id] = scrip_cd
    log.info(f"Scrip master loaded: {len(mapping)} entries")
    return mapping


def find_presentation_url(scrip_code: str, result_date: date) -> str | None:
    """Query BSE announcements API and return PDF URL if a presentation is found."""
    from_dt = result_date.strftime("%Y%m%d")
    to_dt = (result_date + timedelta(days=10)).strftime("%Y%m%d")
    url = BSE_ANN_URL.format(scrip=scrip_code, from_dt=from_dt, to_dt=to_dt)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"  BSE API error for scrip {scrip_code}: {e}")
        return None

    announcements = data if isinstance(data, list) else data.get("Table", [])
    # Prefer exact subcategory match, fall back to keyword in headline
    matches = []
    for ann in announcements:
        subcat = (ann.get("SUBCATNAME") or "").lower()
        headline = (ann.get("NEWSSUB") or "").lower()
        if subcat in PRESENTATION_SUBCATS or any(kw in headline for kw in PRESENTATION_KEYWORDS):
            matches.append(ann)

    if not matches:
        return None

    # Take the most recent match (announcements are typically newest-first, but sort to be safe)
    best = sorted(matches, key=lambda a: a.get("NEWS_DT", ""), reverse=True)[0]
    attachment = (best.get("ATTACHMENTNAME") or "").strip()
    if not attachment:
        return None

    return BSE_PDF_BASE + attachment


def find_result_pdf_url(scrip_code: str, result_date: date) -> str | None:
    """Return PDF URL of the quarterly Financial Results filing, if found."""
    # Start 2 days before result_date — some companies file just before the board meeting date
    from_dt = (result_date - timedelta(days=2)).strftime("%Y%m%d")
    to_dt = (result_date + timedelta(days=7)).strftime("%Y%m%d")
    url = BSE_ANN_URL.format(scrip=scrip_code, from_dt=from_dt, to_dt=to_dt)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"  BSE API error for scrip {scrip_code} (results): {e}")
        return None

    announcements = data if isinstance(data, list) else data.get("Table", [])
    matches = []
    for ann in announcements:
        subcat = (ann.get("SUBCATNAME") or "").lower()
        headline = (ann.get("NEWSSUB") or "").lower()
        if subcat in RESULTS_SUBCATS or any(kw in headline for kw in RESULTS_KEYWORDS):
            matches.append(ann)

    if not matches:
        # Log what BSE actually returned so we can tune keywords if needed
        seen = {(a.get("SUBCATNAME",""), a.get("NEWSSUB",""))[:80] for a in announcements[:5]}
        if seen:
            log.debug(f"  scrip {scrip_code}: no result match. BSE returned: {seen}")
        return None

    best = sorted(matches, key=lambda a: a.get("NEWS_DT", ""), reverse=True)[0]
    attachment = (best.get("ATTACHMENTNAME") or "").strip()
    if not attachment:
        return None

    return BSE_PDF_BASE + attachment


def main():
    db_url = os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")
    conn = psycopg2.connect(db_url, connect_timeout=15)
    conn.autocommit = False
    cur = conn.cursor()

    today = date.today()

    # Load NSE symbol → BSE scrip code mapping
    try:
        symbol_to_scrip = fetch_scrip_master()
    except Exception as e:
        log.error(f"Could not fetch BSE scrip master: {e}. Aborting.")
        conn.close()
        return

    # Fetch all season companies missing a presentation URL
    cur.execute(
        """
        SELECT ec.symbol, ec.result_date
        FROM earnings_calendar ec
        WHERE ec.result_date <= %s
          AND ec.presentation_url IS NULL
        ORDER BY ec.result_date DESC
        """,
        (today,),
    )
    rows = cur.fetchall()
    log.info(f"Companies to check: {len(rows)}")

    found = 0
    skipped = 0
    for symbol, result_date in rows:
        scrip_code = symbol_to_scrip.get(symbol.upper())
        if not scrip_code:
            log.debug(f"  {symbol}: no BSE scrip code")
            skipped += 1
            continue

        pdf_url = find_presentation_url(scrip_code, result_date)
        if pdf_url:
            cur.execute(
                "UPDATE earnings_calendar SET presentation_url = %s WHERE symbol = %s AND result_date = %s",
                (pdf_url, symbol, result_date),
            )
            log.info(f"  {symbol} ({result_date}): found → {pdf_url}")
            found += 1
        else:
            log.info(f"  {symbol} ({result_date}): no presentation yet")

        time.sleep(0.5)  # be polite to BSE

    # --- Fetch Financial Results PDFs ---
    cur.execute(
        """
        SELECT ec.symbol, ec.result_date
        FROM earnings_calendar ec
        WHERE ec.result_date <= %s
          AND ec.result_pdf_url IS NULL
        ORDER BY ec.result_date DESC
        """,
        (today,),
    )
    result_rows = cur.fetchall()
    log.info(f"Companies to check for result PDFs: {len(result_rows)}")

    found_pdf = 0
    skipped_pdf = 0
    for symbol, result_date in result_rows:
        scrip_code = symbol_to_scrip.get(symbol.upper())
        if not scrip_code:
            log.debug(f"  {symbol}: no BSE scrip code")
            skipped_pdf += 1
            continue

        pdf_url = find_result_pdf_url(scrip_code, result_date)
        if pdf_url:
            cur.execute(
                "UPDATE earnings_calendar SET result_pdf_url = %s WHERE symbol = %s AND result_date = %s",
                (pdf_url, symbol, result_date),
            )
            log.info(f"  {symbol} ({result_date}): result PDF → {pdf_url}")
            found_pdf += 1
        else:
            log.info(f"  {symbol} ({result_date}): no result PDF yet")

        time.sleep(0.5)

    not_found_pdf = len(result_rows) - found_pdf - skipped_pdf
    log.info(f"Result PDFs — Found: {found_pdf} | Not found: {not_found_pdf} | No scrip: {skipped_pdf}")

    conn.commit()
    conn.close()
    not_found = len(rows) - found - skipped
    log.info(f"Done. Found: {found} | Not found: {not_found} | No BSE scrip code: {skipped}")


if __name__ == "__main__":
    main()
