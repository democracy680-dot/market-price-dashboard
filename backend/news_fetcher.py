"""
news_fetcher.py — RSS feed parser, NSE symbol matcher, and Supabase upsert.

What it does:
  1. Reads active feed URLs from the news_sources table
  2. Parses each feed with feedparser (timeout + retry)
  3. Strips HTML from summaries using stdlib only
  4. Matches NSE company names / symbols against article text
  5. Bulk-upserts into news_articles and news_article_symbols
  6. Purges articles older than 30 days
  7. Logs the run to news_refresh_log

Usage:
    python backend/news_fetcher.py

Requires SUPABASE_DB_URL in .env or environment.
"""

import hashlib
import html
import logging
import re
import sys
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

import feedparser
import psycopg2
import requests
from psycopg2.extras import execute_values
from sqlalchemy import text

# ── Bootstrap path so `from db import` works when run from repo root ──────────
import os
sys.path.insert(0, os.path.dirname(__file__))
from db import get_engine, get_psycopg2_conn

logger = logging.getLogger(__name__)

FEED_TIMEOUT        = 15    # seconds per feed request
FEED_RETRIES        = 2     # attempts per feed before giving up
MIN_TOKEN_MATCHES   = 2     # name tokens needed for a "name" match
MAX_SYMBOLS_PER_ARTICLE = 10  # cap to avoid tagging broad roundups to 50+ stocks

# Words too generic to use as match tokens
_SKIP_WORDS = {
    "limited", "ltd", "india", "the", "and", "of", "for",
    "corporation", "corp", "enterprise", "enterprises",
    "industries", "industry", "services", "group", "bank",
    "company", "companies", "holdings", "finance",
}


# ── HTML stripping ─────────────────────────────────────────────────────────────

class _MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self._fed: list[str] = []

    def handle_data(self, d: str) -> None:
        self._fed.append(d)

    def get_data(self) -> str:
        return " ".join(self._fed).strip()


def strip_html(raw: str) -> str:
    if not raw:
        return ""
    s = _MLStripper()
    s.feed(raw)
    text_out = s.get_data()
    text_out = html.unescape(text_out)
    return re.sub(r"\s+", " ", text_out).strip()


# ── Article ID ─────────────────────────────────────────────────────────────────

def make_article_id(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:40]


# ── Feed loading ───────────────────────────────────────────────────────────────

def load_feeds(engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT source_id, display_name, feed_url FROM news_sources WHERE is_active = TRUE")
        ).fetchall()
    return [{"source_id": r[0], "display_name": r[1], "feed_url": r[2]} for r in rows]


_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; StockStack-NewsBot/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def _fetch_feed_bytes(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=FEED_TIMEOUT)
        r.raise_for_status()
        return r.content
    except Exception as exc:
        logger.warning(f"  HTTP fetch failed for {url}: {exc}")
        return None


def parse_feed(source: dict) -> list[dict]:
    articles = []
    for attempt in range(FEED_RETRIES):
        try:
            raw = _fetch_feed_bytes(source["feed_url"])
            if raw is None:
                continue
            # Pass raw bytes — feedparser auto-detects encoding from XML declaration
            parsed = feedparser.parse(raw)
            if parsed.bozo and not parsed.entries:
                logger.warning(f"  {source['source_id']}: bozo error — {parsed.bozo_exception}")
                continue

            for entry in parsed.entries:
                url = entry.get("link", "").strip()
                if not url:
                    continue

                title   = strip_html(entry.get("title", ""))
                summary = strip_html(
                    entry.get("summary", "") or entry.get("description", "")
                )

                raw_date = entry.get("published", "") or entry.get("updated", "")
                pub_date = None
                if raw_date:
                    try:
                        pub_date = parsedate_to_datetime(raw_date)
                        if pub_date.tzinfo is None:
                            pub_date = pub_date.replace(tzinfo=timezone.utc)
                    except Exception:
                        pub_date = datetime.now(timezone.utc)
                else:
                    pub_date = datetime.now(timezone.utc)

                articles.append({
                    "article_id":  make_article_id(url),
                    "source_id":   source["source_id"],
                    "title":       title[:500],
                    "summary":     summary[:2000],
                    "url":         url[:1000],
                    "published_at": pub_date,
                })
            break  # success

        except Exception as exc:
            logger.warning(f"  {source['source_id']} attempt {attempt + 1} failed: {exc}")

    return articles


# ── Stock lookup ───────────────────────────────────────────────────────────────

def load_stock_lookup(engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol, name FROM stocks WHERE is_active = TRUE AND name IS NOT NULL")
        ).fetchall()

    lookup = []
    for symbol, name in rows:
        name_clean = re.sub(r"[^a-zA-Z0-9\s]", " ", name)
        tokens = [
            t.lower()
            for t in name_clean.split()
            if len(t) >= 4 and t.lower() not in _SKIP_WORDS
        ]
        lookup.append({
            "symbol":       symbol,
            "name_lower":   name.lower(),
            "symbol_lower": symbol.lower(),
            "name_tokens":  tokens,
        })
    return lookup


# ── Symbol matching ────────────────────────────────────────────────────────────

def match_symbols(text_lower: str, lookup: list[dict]) -> list[dict]:
    matches: dict[str, str] = {}

    for stock in lookup:
        sym = stock["symbol"]
        if sym in matches:
            continue

        # Tier 1: whole-word NSE symbol (RELIANCE, HDFCBANK, etc.)
        if re.search(r"\b" + re.escape(stock["symbol_lower"]) + r"\b", text_lower):
            matches[sym] = "symbol"
            continue

        # Tier 2: ≥MIN_TOKEN_MATCHES distinct name tokens appear in text
        if stock["name_tokens"]:
            hit_count = sum(1 for tok in stock["name_tokens"] if tok in text_lower)
            if hit_count >= MIN_TOKEN_MATCHES:
                matches[sym] = "name"

    result = sorted(
        [{"symbol": sym, "match_type": mt} for sym, mt in matches.items()],
        key=lambda x: x["symbol"],
    )
    return result[:MAX_SYMBOLS_PER_ARTICLE]


# ── Upserts ────────────────────────────────────────────────────────────────────

def upsert_articles(articles: list[dict]) -> int:
    if not articles:
        return 0
    rows = [
        (a["article_id"], a["source_id"], a["title"],
         a["summary"], a["url"], a["published_at"])
        for a in articles
    ]
    sql = """
        INSERT INTO news_articles
            (article_id, source_id, title, summary, url, published_at)
        VALUES %s
        ON CONFLICT (article_id) DO NOTHING
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def upsert_article_symbols(symbol_rows: list[tuple]) -> int:
    if not symbol_rows:
        return 0
    sql = """
        INSERT INTO news_article_symbols (article_id, symbol, match_type)
        VALUES %s
        ON CONFLICT (article_id, symbol) DO NOTHING
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, symbol_rows, page_size=1000)
        conn.commit()
    finally:
        conn.close()
    return len(symbol_rows)


# ── Retention ──────────────────────────────────────────────────────────────────

def purge_old_articles(engine, days: int = 30) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM news_articles WHERE published_at < NOW() - INTERVAL '30 days'")
        )
    return result.rowcount


# ── Logging ────────────────────────────────────────────────────────────────────

def log_run(engine, run_id, started_at, fetched, new_count, failed, status, error=None):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO news_refresh_log
                    (run_id, started_at, finished_at, articles_fetched,
                     articles_new, articles_failed, status, error_message)
                VALUES
                    (:run_id, :started_at, NOW(), :fetched,
                     :new_count, :failed, :status, :error)
                ON CONFLICT (run_id) DO UPDATE SET
                    finished_at      = NOW(),
                    articles_fetched = EXCLUDED.articles_fetched,
                    articles_new     = EXCLUDED.articles_new,
                    articles_failed  = EXCLUDED.articles_failed,
                    status           = EXCLUDED.status,
                    error_message    = EXCLUDED.error_message
            """),
            {
                "run_id":    str(run_id),
                "started_at": started_at,
                "fetched":   fetched,
                "new_count": new_count,
                "failed":    failed,
                "status":    status,
                "error":     error,
            },
        )


# ── Orchestrator ───────────────────────────────────────────────────────────────

def run():
    run_id  = uuid.uuid4()
    started = datetime.now(timezone.utc)
    engine  = get_engine()

    logger.info(f"=== News refresh started  run_id={run_id} ===")

    sources = load_feeds(engine)
    lookup  = load_stock_lookup(engine)
    logger.info(f"  {len(sources)} feeds, {len(lookup)} stocks loaded")

    all_articles   = []
    failed_sources = 0

    for src in sources:
        articles = parse_feed(src)
        if not articles:
            failed_sources += 1
        all_articles.extend(articles)
        logger.info(f"  {src['source_id']}: {len(articles)} articles")

    logger.info(f"Total parsed: {len(all_articles)}")

    # Deduplicate within this batch (same URL appearing in two feeds)
    seen: dict[str, dict] = {}
    for a in all_articles:
        seen[a["article_id"]] = a
    deduped = list(seen.values())
    logger.info(f"After dedup: {len(deduped)}")

    n_upserted = upsert_articles(deduped)
    logger.info(f"  {n_upserted} articles submitted (new ones committed)")

    symbol_rows: list[tuple] = []
    for a in deduped:
        combined = (a["title"] + " " + (a["summary"] or "")).lower()
        for m in match_symbols(combined, lookup):
            symbol_rows.append((a["article_id"], m["symbol"], m["match_type"]))

    n_sym = upsert_article_symbols(symbol_rows)
    logger.info(f"  {n_sym} symbol links upserted")

    try:
        deleted = purge_old_articles(engine)
        logger.info(f"  Purged {deleted} articles older than 30 days")
    except Exception as purge_err:
        logger.warning(f"  Purge failed (non-fatal): {purge_err}")

    status = "success" if failed_sources == 0 else "partial"
    log_run(engine, run_id, started, len(all_articles), n_upserted, failed_sources, status)
    logger.info(f"=== News refresh complete: {status} ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    run()
