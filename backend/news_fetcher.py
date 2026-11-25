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
MAX_SYMBOLS_PER_ARTICLE = 8   # cap to avoid tagging broad roundups to 50+ stocks

# Corporate suffixes dropped before building a company's match phrase.
_SUFFIX_WORDS = {"limited", "ltd", "the", "co", "company", "corporation", "corp", "inc", "plc", "pvt"}

# Words too generic to identify a company on their own (single-word matches).
_GENERIC_WORDS = {
    "india", "indian", "global", "oil", "gas", "power", "energy", "steel", "bank", "banks", "finance",
    "financial", "capital", "share", "shares", "stock", "stocks", "market", "markets", "total", "focus",
    "take", "retail", "home", "gold", "silver", "star", "life", "general", "national", "united", "first",
    "new", "best", "great", "royal", "prime", "super", "standard", "premier", "classic", "modern",
    "international", "industries", "industry", "services", "systems", "technologies", "technology",
    "solutions", "infra", "infrastructure", "projects", "products", "chemicals", "pharma", "foods",
    "textiles", "motors", "auto", "metals", "cement", "cements", "paper", "sugar", "hotels", "power",
    "investments", "ventures", "holdings", "group", "enterprises", "trading", "exports", "engineering",
    "electric", "electricals", "dollar", "idea", "sun", "apollo", "tata", "bajaj", "reliance", "adani",
    "birla", "mahindra", "hindustan", "bharat", "state", "union", "central", "punjab", "indo", "asian",
    "tech", "techno", "digital", "smart", "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten", "hundred", "thousand", "million", "top", "big", "high", "low", "green", "blue", "red",
}

# Tickers that are also everyday words / exchange names: only matched when followed by
# a stock word ("OIL shares", "BSE stock", "MCX Ltd").
_AMBIGUOUS_SYMBOLS = {
    "OIL", "GLOBAL", "DOLLAR", "FOCUS", "TOTAL", "TAKE", "RETAIL", "BSE", "MCX", "APOLLO", "IDEA",
    "GOLD", "STAR", "SUN", "HOME", "POWER", "INDIA", "SAFE", "SMART", "SPECIAL", "AGI", "RAIN",
    "HERO", "EASY", "ELECTRIC", "JUST", "PRIME", "ORIENT", "SHARE", "STOCK", "MARKET", "TREND",
    "IPL", "NDTV", "CEO", "GDP", "FII", "DII", "IPO", "AI", "EV", "USA", "UK", "RBI", "SEBI",
}
_STOCK_WORDS = r"(?i:shares?|stocks?|ltd|limited|scrip|q[1-4]|results?|stake)"
_STOCK_WORDS_NORM = r"(?:shares?|stocks?|ltd|limited|scrip|q[1-4]|results?|stake|share price|stock price)"

# Common ways headlines name big companies (lower-case, matched as whole words).
_ALIASES = {
    "SBIN": ["sbi", "state bank of india"], "RELIANCE": ["reliance industries", "ril"],
    "LT": ["l&t", "larsen & toubro", "larsen and toubro"], "M&M": ["m&m", "mahindra & mahindra",
    "mahindra and mahindra"], "HDFCBANK": ["hdfc bank"], "ICICIBANK": ["icici bank"],
    "KOTAKBANK": ["kotak mahindra bank", "kotak bank"], "AXISBANK": ["axis bank"],
    "BAJFINANCE": ["bajaj finance"], "BAJAJFINSV": ["bajaj finserv"], "MARUTI": ["maruti suzuki", "maruti"],
    "TATASTEEL": ["tata steel"], "TCS": ["tata consultancy"], "INFY": ["infosys"],
    "HINDUNILVR": ["hindustan unilever", "hul"], "ADANIENT": ["adani enterprises"],
    "ADANIPORTS": ["adani ports"], "BHARTIARTL": ["bharti airtel", "airtel"], "ITC": ["itc"],
    "SUNPHARMA": ["sun pharma", "sun pharmaceutical"], "ASIANPAINT": ["asian paints"],
    "COALINDIA": ["coal india"], "ONGC": ["ongc"], "NTPC": ["ntpc"], "POWERGRID": ["power grid"],
    "HAL": ["hindustan aeronautics"], "BEL": ["bharat electronics"], "IRFC": ["irfc"],
    "LICI": ["lic", "life insurance corporation"], "ETERNAL": ["zomato", "eternal"],
    "TMPV": ["tata motors"], "ULTRACEMCO": ["ultratech"], "WIPRO": ["wipro"], "HCLTECH": ["hcltech", "hcl tech"],
}


# Two-word openings that are places / generic phrases, not company identifiers:
# such names use their first THREE words ("tamil nadu newsprint").
_PLACE_PHRASES = {
    "new delhi", "tamil nadu", "andhra pradesh", "uttar pradesh", "madhya pradesh", "west bengal",
    "himachal pradesh", "arunachal pradesh", "jammu kashmir", "south india", "north east", "new india",
    "south indian", "north india", "east india", "west coast", "great eastern", "indian oil", "indian bank",
    "state bank", "national fertilizers", "hindustan petroleum", "bharat petroleum",
}
_CONNECTORS = {"of", "and", "&", "for", "in", "on"}

# Institutions whose names contain a listed company's name ('Reserve Bank of India'
# contains 'Bank of India'): removed from the text before company-name matching.
_NON_COMPANY_PHRASES = [
    "reserve bank of india", "securities and exchange board of india", "government of india",
    "competition commission of india", "election commission of india", "comptroller and auditor general of india",
    "insurance regulatory and development authority of india", "national bank for agriculture",
    "bank of america", "bank of japan", "bank of england", "bank of canada", "bank of china",
    "people s bank of china", "state bank of pakistan",
]


def _name_tokens(name: str) -> list[str]:
    """Normalised name words without corporate suffixes; runs of initials merged ('s a l' -> 'sal')."""
    toks, out, run = [t for t in _norm(name).split() if t not in _SUFFIX_WORDS], [], ""
    for t in toks:
        if len(t) == 1 and t.isalpha():
            run += t
            continue
        if run:
            out.append(run); run = ""
        out.append(t)
    if run:
        out.append(run)
    return out


def _merge_initials(norm: str) -> str:
    """' s a l steel ' -> ' sal steel ' (matches how names with initials are stored)."""
    return re.sub(r"(?<= )((?:[a-z] ){2,})", lambda m: m.group(1).replace(" ", "") + " ", norm)


def _name_phrase(toks: list[str]) -> str:
    """Identifying phrase: first two words; three when word two is a connector
    ('bank of india') or the first two are a place / generic pair ('tamil nadu ...')."""
    if len(toks) < 2:
        return ""
    two = " ".join(toks[:2])
    if len(toks) >= 3 and (toks[1] in _CONNECTORS or two in _PLACE_PHRASES
                           or (toks[0] in _GENERIC_WORDS and toks[1] in _GENERIC_WORDS)):
        n = 4 if (len(toks) >= 4 and toks[2] in _CONNECTORS) else 3   # 'union bank of india'
        return " ".join(toks[:n])
    return two


def _norm(text: str) -> str:
    """lower-case, '&' kept, everything else non-alphanumeric -> space, collapsed."""
    return " " + re.sub(r"\s+", " ", re.sub(r"[^a-z0-9&]+", " ", text.lower())).strip() + " "


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


def parse_pub_date(entry) -> datetime:
    """Publish time from an RSS/Atom entry: RFC-822 ('Wed, 25 Nov 2026 14:25:41 +0530'),
    ISO-8601 ('2026-11-25T14:37:07+05:30') or feedparser's parsed struct. Falls back to
    now() only when the entry carries no date at all."""
    raw = (entry.get("published") or entry.get("updated") or "").strip()
    if raw:
        for parse in (parsedate_to_datetime, lambda v: datetime.fromisoformat(v.replace("Z", "+00:00"))):
            try:
                d = parse(raw)
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            except Exception:  # noqa: BLE001
                pass
    for key in ("published_parsed", "updated_parsed"):
        st_ = entry.get(key)
        if st_:
            try:
                return datetime(*st_[:6], tzinfo=timezone.utc)
            except Exception:  # noqa: BLE001
                pass
    return datetime.now(timezone.utc)


def is_real_headline(title: str) -> bool:
    """Drops feed junk such as bare dates ('2026-12-06') and very short titles."""
    t = (title or "").strip()
    return len(t) >= 15 and not re.fullmatch(r"[\d\s:/.\-]+", t)


def parse_feed(source: dict) -> tuple[list[dict], bool]:
    """Returns (articles, network_failed).

    network_failed=True only when we couldn't reach the server at all (HTTP/network
    error on every attempt). Malformed-XML (bozo) feeds from the upstream source are
    NOT counted as network failures — that's the feed provider's problem.
    """
    articles = []
    http_failures = 0
    for attempt in range(FEED_RETRIES):
        try:
            raw = _fetch_feed_bytes(source["feed_url"])
            if raw is None:
                http_failures += 1
                continue
            # Pass raw bytes — feedparser auto-detects encoding from XML declaration
            parsed = feedparser.parse(raw)
            if parsed.bozo and not parsed.entries:
                logger.warning(f"  {source['source_id']}: bozo feed (upstream XML error) — {parsed.bozo_exception}")
                return [], False  # upstream content problem, not a network failure

            for entry in parsed.entries:
                url = entry.get("link", "").strip()
                if not url:
                    continue

                title   = strip_html(entry.get("title", ""))
                summary = strip_html(
                    entry.get("summary", "") or entry.get("description", "")
                )

                if not is_real_headline(title):
                    continue
                pub_date = parse_pub_date(entry)

                articles.append({
                    "article_id":  make_article_id(url),
                    "source_id":   source["source_id"],
                    "title":       title[:500],
                    "summary":     summary[:2000],
                    "url":         url[:1000],
                    "published_at": pub_date,
                })
            return articles, False  # success

        except Exception as exc:
            logger.warning(f"  {source['source_id']} attempt {attempt + 1} failed: {exc}")
            http_failures += 1

    return articles, http_failures >= FEED_RETRIES


# ── Stock lookup ───────────────────────────────────────────────────────────────

def build_lookup(rows) -> list[dict]:
    """rows: (symbol, name). Builds the match rules for each stock:
    - symbol: matched case-sensitively as an UPPER-case whole word (ambiguous tickers
      need a following stock word);
    - phrases: the first two words of the company name (suffixes dropped), plus the
      first word alone when it is distinctive and no other company starts with it,
      plus known aliases. Phrases are matched as whole words on normalised text."""
    prepared = []
    for symbol, name in rows:
        if not symbol or str(symbol).startswith("^") or not name:
            continue
        prepared.append((symbol, _name_tokens(name)))
    first_counts: dict[str, int] = {}
    for _, toks in prepared:
        if toks:
            first_counts[toks[0]] = first_counts.get(toks[0], 0) + 1

    phrase_counts: dict[str, int] = {}
    for _, toks in prepared:
        ph = _name_phrase(toks)
        if ph:
            phrase_counts[ph] = phrase_counts.get(ph, 0) + 1

    lookup = []
    for symbol, toks in prepared:
        phrases = set()
        ph = _name_phrase(toks)
        if ph and phrase_counts.get(ph, 0) > 1 and len(toks) > len(ph.split()):
            ph = " ".join(toks[:len(ph.split()) + 1])    # 'dalmia bharat sugar' vs 'dalmia bharat'
        if ph:
            phrases.add(ph)
        first = toks[0] if toks else ""
        solo = None   # distinctive first word: only counts when followed by a stock word
        if (first and len(first) >= 4 and first not in _GENERIC_WORDS and first_counts.get(first) == 1
                and not first.isdigit()):
            solo = re.compile(r" " + re.escape(first) + r" " + _STOCK_WORDS_NORM + r" ")
        for al in _ALIASES.get(symbol, []):
            phrases.add(" ".join(_norm(al).split()))
        sym_ok = len(symbol) >= 3 and symbol.replace("&", "").isalnum()
        lookup.append({
            "symbol": symbol,
            "symbol_re": (re.compile(r"(?<![A-Za-z0-9&])" + re.escape(symbol) +
                                     (r"(?![A-Za-z0-9&])\s+" + _STOCK_WORDS + r"\b" if symbol in _AMBIGUOUS_SYMBOLS
                                      else r"(?![A-Za-z0-9&])"))
                          if sym_ok else None),
            "phrases": sorted(p for p in phrases if p),
            "solo_re": solo,
        })
    return lookup


def load_stock_lookup(engine) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT symbol, name FROM stocks WHERE is_active = TRUE AND name IS NOT NULL")
        ).fetchall()
    return build_lookup(rows)


# ── Symbol matching ────────────────────────────────────────────────────────────

def match_symbols(text: str, lookup: list[dict]) -> list[dict]:
    """text: article title + summary in ORIGINAL case."""
    norm = _merge_initials(_norm(text))
    for inst in _NON_COMPANY_PHRASES:
        norm = norm.replace(f" {inst} ", " | ")
    matches: dict[str, str] = {}
    name_hits: list[tuple[str, str]] = []          # (phrase, symbol)
    for stock in lookup:
        sym = stock["symbol"]
        rx = stock["symbol_re"]
        if rx is not None and rx.search(text):
            matches[sym] = "symbol"
            continue
        hit = next((ph for ph in stock["phrases"] if f" {ph} " in norm), None)
        if hit:
            name_hits.append((hit, sym))
        elif stock["solo_re"] is not None and stock["solo_re"].search(norm):
            matches[sym] = "name"
    # Longest phrases claim their words first, so 'dalmia bharat sugar' doesn't
    # also count as a mention of 'dalmia bharat'.
    work = norm
    for ph, sym in sorted(name_hits, key=lambda x: -len(x[0])):
        if f" {ph} " in work:
            matches[sym] = "name"
            work = work.replace(f" {ph} ", " | ")
    result = sorted(({"symbol": s, "match_type": m} for s, m in matches.items()), key=lambda x: x["symbol"])
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
        articles, network_failed = parse_feed(src)
        if network_failed:
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
        combined = a["title"] + " " + (a["summary"] or "")
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


def retag_recent(days: int = 30) -> int:
    """Re-run the symbol matcher over stored articles (e.g. after improving it).
    Replaces news_article_symbols for those articles in one transaction."""
    engine = get_engine()
    lookup = load_stock_lookup(engine)
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT article_id, title, COALESCE(summary, '') FROM news_articles "
                        "WHERE published_at >= NOW() - (%s || ' days')::interval", (str(days),))
            arts = cur.fetchall()
            rows = [(aid, m["symbol"], m["match_type"])
                    for aid, title, summary in arts for m in match_symbols(title + " " + summary, lookup)]
            cur.execute("DELETE FROM news_article_symbols WHERE article_id = ANY(%s)", ([a[0] for a in arts],))
            if rows:
                execute_values(cur, "INSERT INTO news_article_symbols (article_id, symbol, match_type) VALUES %s "
                                    "ON CONFLICT DO NOTHING", rows, page_size=2000)
        conn.commit()
    finally:
        conn.close()
    logger.info(f"  Re-tagged {len(arts)} articles -> {len(rows)} symbol links")
    return len(rows)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    if "--retag" in sys.argv:
        retag_recent()
    else:
        run()
