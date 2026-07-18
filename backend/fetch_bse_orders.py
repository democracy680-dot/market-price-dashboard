"""Track order wins & major business updates filed with BSE, for our dashboard stocks.

Pulls "Company Update" corporate announcements from BSE's bulk announcements API,
filters them to the ~1600 stocks listed on our webapp, classifies each into one of
four types (order / acquisition / jv / expansion), parses any ₹ order value from the
headline, and upserts into the bse_orders table. Rows older than 180 days are purged.

Reuses the proven BSE plumbing from fetch_bse_presentations.py
(fetch_scrip_master, HEADERS, BSE_PDF_BASE) and the pagination approach from
discover_bse_results.py.

Usage:
  python fetch_bse_orders.py             # daily mode (last 3 days)
  python fetch_bse_orders.py --backfill  # scan from BACKFILL_START to today

Requires SUPABASE_DB_URL in .env or environment.
"""

import argparse
import hashlib
import logging
import math
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from fetch_bse_presentations import HEADERS, BSE_PDF_BASE, fetch_scrip_master

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BACKFILL_START = date(2026, 1, 1)  # earliest date scanned in --backfill mode
DAILY_LOOKBACK_DAYS = 3            # overlap survives skipped runs; dedup makes it free
RETENTION_DAYS = 180
PAGE_SIZE = 50
MAX_PAGES = 400
# BSE's bulk-announcements API silently returns an empty Table for windows wider
# than ~1 month, so we split any request into sub-windows of at most CHUNK_DAYS.
CHUNK_DAYS = 25
USD_INR = 83.0                     # documented fallback FX rate for rare USD headlines

BSE_ANN_BULK_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
    "?pageno={page}&strCat={cat}&strPrevDate={from_dt}&strScrip=&strSearch=P"
    "&strToDate={to_dt}&strType=C&subcategory="
)

IST = timezone(timedelta(hours=5, minutes=30))

# ── Classification taxonomy (derived from live BSE SUBCATNAME values) ──────────
# Exact BSE subcategory names are the most reliable signal; headline keywords are a
# fallback for filings that land under the generic "General" subcategory (which is
# where BSE files most capacity/capex/expansion updates — it has no dedicated one).
ORDER_SUBCATS = {"award of order / receipt of order"}
ACQ_SUBCATS = {"acquisition", "amalgamation / merger", "amalgamation/ merger",
               "scheme of arrangement"}
JV_SUBCATS = {"joint venture"}
EXPANSION_SUBCATS = {"expansion"}  # none in BSE's list today; future-proof

ORDER_KEYWORDS = [
    "award of order", "receipt of order", "bags order", "bagged order",
    "bags an order", "order worth", "order valued", "secures order",
    "secured order", "receives order", "new order", "letter of award",
    "letter of intent", "work order", "purchase order",
]
ACQ_KEYWORDS = ["acquisition", "acquires", "acquire ", "acquired",
                "amalgamation", "merger", "scheme of arrangement"]
JV_KEYWORDS = ["joint venture", "partnership"]
EXPANSION_KEYWORDS = [
    "capacity addition", "capacity expansion", "capacity augmentation",
    "installed capacity", "capex", "commissioning", "new plant",
    "greenfield", "brownfield", "expansion", "setting up",
]


# ── Pure helpers (no network / no DB — covered by tests/test_bse_orders.py) ────

def classify_announcement(subcatname: str | None, headline: str | None) -> str | None:
    """Map a BSE filing to one of our 4 tracked types, or None to ignore it."""
    sc = (subcatname or "").strip().lower()
    h = (headline or "").lower()

    # 1. Authoritative exact-subcategory match
    if sc in ORDER_SUBCATS:
        return "order"
    if sc in ACQ_SUBCATS:
        return "acquisition"
    if sc in JV_SUBCATS:
        return "jv"
    if sc in EXPANSION_SUBCATS:
        return "expansion"

    # 2. Headline keyword fallback (priority order: order > acq > jv > expansion)
    if any(k in h for k in ORDER_KEYWORDS):
        return "order"
    if any(k in h for k in ACQ_KEYWORDS):
        return "acquisition"
    if any(k in h for k in JV_KEYWORDS):
        return "jv"
    if any(k in h for k in EXPANSION_KEYWORDS):
        return "expansion"
    return None


_VALUE_RE = re.compile(
    r"(?P<cur>Rs\.?|₹|INR|US\$|USD|\$)?\s*"
    r"(?P<num>\d[\d,]*(?:\.\d+)?)\s*"
    r"(?P<unit>crores?|cr|lakhs?|lacs?|lac|millions?|mn|mio|billions?|bn)\b",
    re.IGNORECASE,
)


def _to_crore(num: float, unit: str) -> float:
    unit = unit.lower()
    if unit in ("crore", "crores", "cr"):
        return num
    if unit in ("lakh", "lakhs", "lac", "lacs"):
        return num / 100.0
    if unit in ("million", "millions", "mn", "mio"):
        return num * 0.1
    if unit in ("billion", "billions", "bn"):
        return num * 100.0
    return num


def parse_order_value(text: str | None) -> tuple[float | None, str | None]:
    """Extract a monetary value from a headline, normalized to ₹ crore.

    Returns (value_in_crore, raw_matched_text). When several amounts appear, the
    largest is returned (orders are usually the biggest number in the sentence).
    Returns (None, None) when no amount is found.
    """
    if not text:
        return (None, None)
    best_val: float | None = None
    best_raw: str | None = None
    for m in _VALUE_RE.finditer(text):
        num = float(m.group("num").replace(",", ""))
        val = _to_crore(num, m.group("unit"))
        cur = (m.group("cur") or "").lower()
        if cur in ("usd", "us$", "$"):
            val *= USD_INR
        if best_val is None or val > best_val:
            best_val = val
            best_raw = m.group(0).strip()
    if best_val is None:
        return (None, None)
    return (round(best_val, 2), best_raw)


def build_order_id(row: dict) -> str:
    """BSE NEWSID when present, else a deterministic SHA-256[:40] of the filing."""
    newsid = str(row.get("NEWSID") or "").strip()
    if newsid:
        return newsid
    basis = f"{row.get('SCRIP_CD', '')}|{row.get('NEWS_DT', '')}|{row.get('ATTACHMENTNAME', '')}"
    return hashlib.sha256(basis.encode()).hexdigest()[:40]


def parse_announced_at(news_dt) -> datetime | None:
    """Parse BSE's NEWS_DT (e.g. '2026-07-16T16:22:05.53') to an IST-aware datetime."""
    if not news_dt:
        return None
    try:
        dt = datetime.fromisoformat(str(news_dt))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt


def clean_headline(newssub: str | None, scrip_code: str) -> str:
    """Strip BSE's redundant '{company} - {scrip_code} - ' prefix from NEWSSUB.

    NEWSSUB looks like 'Coal India Ltd - 533278 - Commissioning Of 200 MW ...';
    the company and code already live in their own columns, so we keep only the
    meaningful description. Underscores in BSE's canned subcategory labels
    (e.g. 'Award_of_Order_Receipt_of_Order') are turned back into spaces.
    """
    if not newssub:
        return ""
    text_out = newssub.strip()
    marker = f"- {scrip_code} -"
    idx = text_out.find(marker)
    if idx != -1:
        text_out = text_out[idx + len(marker):].strip(" -").strip()
    # Collapse underscores (BSE canned labels) and any whitespace/newlines to single spaces
    return re.sub(r"\s+", " ", text_out.replace("_", " ")).strip()


def build_order_rows(announcements: list[dict], scrip_to_symbol: dict[str, str]) -> list[dict]:
    """Filter filings to our universe, classify, parse value → order row dicts."""
    rows: list[dict] = []
    seen: set[str] = set()
    for ann in announcements:
        scrip = str(ann.get("SCRIP_CD") or "").strip()
        symbol = scrip_to_symbol.get(scrip)
        if not symbol:
            continue
        atype = classify_announcement(ann.get("SUBCATNAME"), ann.get("NEWSSUB"))
        if not atype:
            continue
        announced_at = parse_announced_at(ann.get("NEWS_DT"))
        if announced_at is None:
            continue
        order_id = build_order_id(ann)
        if order_id in seen:
            continue
        seen.add(order_id)

        headline = clean_headline(ann.get("NEWSSUB"), scrip)
        value_cr, value_raw = parse_order_value(headline)
        attachment = (ann.get("ATTACHMENTNAME") or "").strip()
        attachment_url = (BSE_PDF_BASE + attachment) if attachment else None
        company = (ann.get("SLONGNAME") or "").strip() or symbol

        rows.append({
            "order_id": order_id,
            "scrip_code": scrip,
            "symbol": symbol,
            "company_name": company[:200],
            "announcement_type": atype,
            "headline": headline[:1000],
            "order_value_cr": value_cr,
            "value_raw": value_raw,
            "attachment_url": attachment_url,
            "announced_at": announced_at,
        })
    return rows


# ── Network / DB ──────────────────────────────────────────────────────────────

def _fetch_window(from_dt: date, to_dt: date) -> list[dict]:
    """Fetch all 'Company Update' filings for a single (<= ~1 month) window, paginated."""
    announcements: list[dict] = []
    page = 1
    total_pages = None
    cat = requests.utils.quote("Company Update")
    while page <= MAX_PAGES:
        url = BSE_ANN_BULK_URL.format(
            page=page, cat=cat,
            from_dt=from_dt.strftime("%Y%m%d"),
            to_dt=to_dt.strftime("%Y%m%d"),
        )
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"BSE bulk API error on page {page} ({from_dt}->{to_dt}): {e}")
            return announcements

        rows = data.get("Table", []) if isinstance(data, dict) else []
        if not rows:
            break
        announcements.extend(rows)

        if total_pages is None:
            try:
                rowcnt = int(data["Table1"][0]["ROWCNT"])
                total_pages = math.ceil(rowcnt / PAGE_SIZE)
                log.info(f"  {from_dt} -> {to_dt}: {rowcnt} filings ({total_pages} pages)")
            except (KeyError, IndexError, TypeError, ValueError):
                log.warning("  Could not read ROWCNT; paginating until an empty page")
                total_pages = MAX_PAGES
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.4)

    return announcements


def fetch_company_updates(from_dt: date, to_dt: date) -> list[dict]:
    """Fetch all 'Company Update' filings for the window.

    BSE's bulk API silently returns an empty Table when the requested date range
    exceeds ~1 month, so wide windows (e.g. --backfill) are split into sub-windows
    of at most CHUNK_DAYS and concatenated. A narrow daily run is a single window.
    """
    announcements: list[dict] = []
    chunk_start = from_dt
    while chunk_start <= to_dt:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), to_dt)
        announcements.extend(_fetch_window(chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return announcements


def _get_conn(db_url: str):
    return psycopg2.connect(db_url, connect_timeout=15)


def load_active_symbols(db_url: str) -> set[str]:
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM stocks WHERE is_active = TRUE")
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


def upsert_orders(db_url: str, rows: list[dict]) -> int:
    """Bulk upsert; returns the count of genuinely new rows (via RETURNING)."""
    if not rows:
        return 0
    tuples = [
        (r["order_id"], r["scrip_code"], r["symbol"], r["company_name"],
         r["announcement_type"], r["headline"], r["order_value_cr"],
         r["value_raw"], r["attachment_url"], r["announced_at"])
        for r in rows
    ]
    sql = """
        INSERT INTO bse_orders
            (order_id, scrip_code, symbol, company_name, announcement_type,
             headline, order_value_cr, value_raw, attachment_url, announced_at)
        VALUES %s
        ON CONFLICT (order_id) DO NOTHING
        RETURNING order_id
    """
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            returned = execute_values(cur, sql, tuples, page_size=500, fetch=True)
        conn.commit()
        return len(returned)
    finally:
        conn.close()


def purge_old_orders(db_url: str, days: int = RETENTION_DAYS) -> int:
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM bse_orders WHERE announced_at < NOW() - make_interval(days => %s)",
                (int(days),),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()


def log_run(db_url, run_id, started_at, filings, orders_new, status, error=None):
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bse_order_refresh_log
                    (run_id, started_at, finished_at, filings_fetched,
                     orders_new, status, error_message)
                VALUES (%s, %s, NOW(), %s, %s, %s, %s)
                """,
                (str(run_id), started_at, filings, orders_new, status, error),
            )
        conn.commit()
    finally:
        conn.close()


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(backfill: bool = False):
    run_id = uuid.uuid4()
    started = datetime.now(timezone.utc)
    db_url = os.environ["SUPABASE_DB_URL"].replace(":5432/", ":6543/")
    today = date.today()
    from_dt = BACKFILL_START if backfill else today - timedelta(days=DAILY_LOOKBACK_DAYS)
    log.info(f"=== Order tracker refresh {from_dt} → {today} (backfill={backfill}) ===")

    try:
        symbol_to_scrip = fetch_scrip_master()
    except Exception as e:
        log.error(f"Could not fetch BSE scrip master: {e}. Aborting.")
        log_run(db_url, run_id, started, 0, 0, "failed", str(e))
        return

    active = load_active_symbols(db_url)
    scrip_to_symbol = {scrip: sym for sym, scrip in symbol_to_scrip.items() if sym in active}
    log.info(f"Universe: {len(active)} active stocks, {len(scrip_to_symbol)} with BSE scrip codes")

    try:
        announcements = fetch_company_updates(from_dt, today)
    except Exception as e:
        log.error(f"BSE fetch failed: {e}")
        log_run(db_url, run_id, started, 0, 0, "failed", str(e))
        return

    rows = build_order_rows(announcements, scrip_to_symbol)
    log.info(f"Filings fetched: {len(announcements)} | in universe & classified: {len(rows)}")

    new_count = upsert_orders(db_url, rows)
    log.info(f"  {new_count} new orders inserted")

    try:
        deleted = purge_old_orders(db_url)
        log.info(f"  Purged {deleted} orders older than {RETENTION_DAYS} days")
    except Exception as purge_err:
        log.warning(f"  Purge failed (non-fatal): {purge_err}")

    log_run(db_url, run_id, started, len(announcements), new_count, "success")
    log.info("=== Order tracker refresh complete ===")


def main():
    parser = argparse.ArgumentParser(description="Track BSE order wins & business updates")
    parser.add_argument(
        "--backfill", action="store_true",
        help=f"scan from {BACKFILL_START} instead of the last {DAILY_LOOKBACK_DAYS} days",
    )
    args = parser.parse_args()
    run(backfill=args.backfill)


if __name__ == "__main__":
    main()
