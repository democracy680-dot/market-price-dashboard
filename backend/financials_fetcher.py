"""
financials_fetcher.py — Weekly fundamental ratios fetcher.

What it does:
  1. Reads all active stocks from Supabase
  2. For each stock (in batches of 25), fetches 14 fundamental ratios from yfinance
  3. Computes derived ratios: ROCE, Interest Coverage, WC Days, EBITDA Growth
  4. Upserts into financials_snapshots with as_of_date = today
  5. Logs the run to financials_refresh_log

Usage:
    python backend/financials_fetcher.py

    # Test with a small universe (10 stocks):
    python backend/financials_fetcher.py --test

Run from the repo root. Requires SUPABASE_DB_URL in .env or environment.
"""

import sys
import uuid
import time
import math
import logging
import argparse
from datetime import datetime, date, timezone

import pandas as pd
import yfinance as yf
from psycopg2.extras import execute_values
from sqlalchemy import text

# Add backend/ to path when run from repo root
sys.path.insert(0, "backend")
from db import get_engine, get_psycopg2_conn

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 25          # smaller than price batches — .info calls are heavier
SLEEP_BETWEEN_BATCHES = 3  # seconds
RATE_LIMIT_SLEEP = 30    # seconds to wait on rate-limit errors
MAX_RETRIES = 2


# ── Helpers ───────────────────────────────────────────────────────────────────

def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _clean(v):
    """Convert float NaN/inf to None so psycopg2 sends NULL."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def safe_divide(numerator, denominator):
    """Return numerator/denominator, or None if either is None/zero."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    try:
        result = numerator / denominator
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except Exception:
        return None


def normalize_pct(value):
    """
    yfinance is inconsistent: some pct fields return 0.18 (decimal),
    others return 18.0 (percent). We always want decimal (0.18 = 18%).
    Heuristic: if abs(value) > 5, assume it's in percent form and divide by 100.
    """
    if value is None:
        return None
    try:
        v = float(value)
        if math.isnan(v) or math.isinf(v):
            return None
        if abs(v) > 5:
            return round(v / 100, 6)
        return round(v, 6)
    except Exception:
        return None


def get_scalar(df, row_key, col_idx=0):
    """
    Safely extract a scalar from a yfinance financial DataFrame.
    These DataFrames have dates as columns and line items as index.
    Returns None if the row or column doesn't exist.
    """
    if df is None or df.empty:
        return None
    try:
        # Find row by partial match on index labels (yfinance label formats vary)
        matches = [i for i in df.index if row_key.lower() in str(i).lower()]
        if not matches:
            return None
        row = df.loc[matches[0]]
        # col_idx 0 = most recent quarter
        if col_idx >= len(row):
            return None
        val = row.iloc[col_idx]
        if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
            return None
        return float(val)
    except Exception:
        return None


# ── Computed ratio functions ──────────────────────────────────────────────────

def compute_roce(balance_sheet, income_stmt) -> float | None:
    """
    ROCE = EBIT / Capital Employed
    Capital Employed = Total Assets - Current Liabilities
    """
    total_assets = get_scalar(balance_sheet, "Total Assets")
    current_liabilities = get_scalar(balance_sheet, "Current Liabilities")
    ebit = get_scalar(income_stmt, "EBIT")

    # Fallback: derive EBIT from Operating Income
    if ebit is None:
        ebit = get_scalar(income_stmt, "Operating Income")

    capital_employed = safe_divide(
        (total_assets - current_liabilities) if (total_assets and current_liabilities) else None,
        1
    )
    if total_assets is None or current_liabilities is None:
        return None
    capital_employed = total_assets - current_liabilities
    return safe_divide(ebit, capital_employed)


def compute_interest_coverage(income_stmt) -> float | None:
    """
    Interest Coverage = EBIT / Interest Expense
    """
    ebit = get_scalar(income_stmt, "EBIT")
    if ebit is None:
        ebit = get_scalar(income_stmt, "Operating Income")

    interest = get_scalar(income_stmt, "Interest Expense")
    if interest is None:
        interest = get_scalar(income_stmt, "Interest And Debt Expense")

    # Interest expense is typically negative in yfinance — take abs
    if interest is not None:
        interest = abs(interest)

    return safe_divide(ebit, interest)


def compute_working_capital_days(balance_sheet, income_stmt) -> float | None:
    """
    WC Days = (Receivables + Inventory - Payables) / (Annual Revenue / 365)
    Uses TTM revenue from income statement (most recent annual column).
    """
    receivables = get_scalar(balance_sheet, "Receivables")
    if receivables is None:
        receivables = get_scalar(balance_sheet, "Net Receivables")

    inventory = get_scalar(balance_sheet, "Inventory")
    if inventory is None:
        inventory = 0.0  # Many service/financial companies have no inventory

    payables = get_scalar(balance_sheet, "Payables")
    if payables is None:
        payables = get_scalar(balance_sheet, "Accounts Payable")
    if payables is None:
        payables = 0.0

    revenue = get_scalar(income_stmt, "Total Revenue")

    if receivables is None or revenue is None or revenue == 0:
        return None

    wc = receivables + inventory - payables
    return safe_divide(wc, revenue / 365)


def compute_ebitda_growth(quarterly_financials) -> float | None:
    """
    EBITDA Growth YoY = (TTM EBITDA this year - TTM EBITDA last year) / abs(TTM EBITDA last year)
    TTM = sum of 4 most recent quarters.
    Requires at least 8 quarters of data.
    """
    if quarterly_financials is None or quarterly_financials.empty:
        return None

    try:
        # Find EBITDA or derive it
        ebitda_row = None
        for label in quarterly_financials.index:
            if "ebitda" in str(label).lower():
                ebitda_row = quarterly_financials.loc[label]
                break

        if ebitda_row is None:
            # Try EBIT + D&A
            ebit_matches = [i for i in quarterly_financials.index if "ebit" in str(i).lower() and "da" not in str(i).lower()]
            da_matches = [i for i in quarterly_financials.index if "depreciation" in str(i).lower()]
            if not ebit_matches or not da_matches:
                return None
            ebit_row = quarterly_financials.loc[ebit_matches[0]]
            da_row = quarterly_financials.loc[da_matches[0]]
            ebitda_row = ebit_row + da_row

        # Need at least 8 quarters
        values = pd.to_numeric(ebitda_row, errors="coerce").dropna()
        if len(values) < 8:
            return None

        ttm_current = values.iloc[:4].sum()
        ttm_prior = values.iloc[4:8].sum()

        if ttm_prior == 0:
            return None

        growth = (ttm_current - ttm_prior) / abs(ttm_prior)
        if math.isnan(growth) or math.isinf(growth):
            return None
        return round(growth, 6)
    except Exception:
        return None


# ── Single stock fetcher ──────────────────────────────────────────────────────

def fetch_one_stock(yahoo_symbol: str) -> dict:
    """
    Fetch all 14 ratios for a single stock.
    Never raises — returns a dict with None for missing fields.
    """
    result = {
        "yahoo_symbol": yahoo_symbol,
        "pe_ttm": None,
        "pb": None,
        "ev_ebitda": None,
        "dividend_yield": None,
        "roe": None,
        "roce": None,
        "ebitda_margin": None,
        "pat_margin": None,
        "revenue_growth_yoy": None,
        "ebitda_growth_yoy": None,
        "pat_growth_yoy": None,
        "debt_to_equity": None,
        "interest_coverage": None,
        "working_capital_days": None,
    }

    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(yahoo_symbol)
            info = ticker.info

            if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
                # Likely delisted or invalid ticker
                logger.debug(f"  {yahoo_symbol}: no price in info, possibly delisted")
                return result

            # ── Market-derived ratios from .info (these are accurate) ────────
            result["pe_ttm"] = _clean(info.get("trailingPE"))
            result["pb"] = _clean(info.get("priceToBook"))
            result["ev_ebitda"] = _clean(info.get("enterpriseToEbitda"))
            result["revenue_growth_yoy"] = normalize_pct(info.get("revenueGrowth"))
            result["pat_growth_yoy"] = normalize_pct(info.get("earningsGrowth"))

            # Dividend yield — yfinance returns as decimal (0.006 = 0.6%)
            div_yield = info.get("dividendYield")
            result["dividend_yield"] = _clean(div_yield)

            # Debt/Equity — yfinance returns as percent (45.2 means 0.452), normalize
            de = info.get("debtToEquity")
            if de is not None:
                try:
                    de = float(de)
                    result["debt_to_equity"] = round(de / 100, 6) if not (math.isnan(de) or math.isinf(de)) else None
                except Exception:
                    pass

            # ── Statement-computed ratios (more accurate than .info TTM) ─────
            # Margins and ROE from .info use Yahoo TTM estimates which diverge
            # from screener.in. Compute directly from annual income_stmt instead.
            try:
                bs = ticker.balance_sheet          # Annual balance sheet
                inc = ticker.income_stmt           # Annual income statement
                q_fin = ticker.quarterly_financials

                # EBITDA Margin = EBITDA / Revenue (annual)
                ebitda_val = get_scalar(inc, "EBITDA")
                revenue_val = get_scalar(inc, "Total Revenue")
                result["ebitda_margin"] = safe_divide(ebitda_val, revenue_val)

                # PAT Margin = Net Income / Revenue (annual)
                net_income_val = get_scalar(inc, "Net Income")
                result["pat_margin"] = safe_divide(net_income_val, revenue_val)

                # ROE = Net Income / Stockholders Equity (annual)
                equity_val = get_scalar(bs, "Stockholders Equity")
                if equity_val is None:
                    equity_val = get_scalar(bs, "Common Stock Equity")
                result["roe"] = safe_divide(net_income_val, equity_val)

                # Fallback to .info if statements gave nothing
                if result["ebitda_margin"] is None:
                    result["ebitda_margin"] = normalize_pct(info.get("ebitdaMargins"))
                if result["pat_margin"] is None:
                    result["pat_margin"] = normalize_pct(info.get("profitMargins"))
                if result["roe"] is None:
                    result["roe"] = normalize_pct(info.get("returnOnEquity"))

                result["roce"] = compute_roce(bs, inc)
                result["interest_coverage"] = compute_interest_coverage(inc)
                result["working_capital_days"] = compute_working_capital_days(bs, inc)
                result["ebitda_growth_yoy"] = compute_ebitda_growth(q_fin)
            except Exception as e:
                logger.debug(f"  {yahoo_symbol}: financial statements error: {e}")
                # Fall back to .info for margins
                result["ebitda_margin"] = normalize_pct(info.get("ebitdaMargins"))
                result["pat_margin"] = normalize_pct(info.get("profitMargins"))
                result["roe"] = normalize_pct(info.get("returnOnEquity"))

            break  # success — exit retry loop

        except Exception as e:
            err_str = str(e).lower()
            if "rate" in err_str or "429" in err_str or "too many" in err_str:
                logger.warning(f"  {yahoo_symbol}: rate limited, sleeping {RATE_LIMIT_SLEEP}s...")
                time.sleep(RATE_LIMIT_SLEEP)
                if attempt == MAX_RETRIES - 1:
                    logger.warning(f"  {yahoo_symbol}: skipping after {MAX_RETRIES} rate-limit retries")
            else:
                logger.debug(f"  {yahoo_symbol}: fetch error: {e}")
                break  # non-rate-limit error, don't retry

    # Round numeric results to 4 decimal places
    for key in result:
        if key == "yahoo_symbol":
            continue
        v = result[key]
        if isinstance(v, float) and not (math.isnan(v) or math.isinf(v)):
            result[key] = round(v, 4)

    return result


def count_quality(row: dict) -> int:
    """Count non-null financial fields (0–14)."""
    fields = [
        "pe_ttm", "pb", "ev_ebitda", "dividend_yield",
        "roe", "roce", "ebitda_margin", "pat_margin",
        "revenue_growth_yoy", "ebitda_growth_yoy", "pat_growth_yoy",
        "debt_to_equity", "interest_coverage", "working_capital_days",
    ]
    return sum(1 for f in fields if row.get(f) is not None)


# ── DB write functions ────────────────────────────────────────────────────────

def upsert_financials(rows: list[dict], as_of_date: date):
    """Bulk upsert rows into financials_snapshots."""
    if not rows:
        return 0

    cols = [
        "symbol", "as_of_date",
        "pe_ttm", "pb", "ev_ebitda", "dividend_yield",
        "roe", "roce", "ebitda_margin", "pat_margin",
        "revenue_growth_yoy", "ebitda_growth_yoy", "pat_growth_yoy",
        "debt_to_equity", "interest_coverage", "working_capital_days",
        "data_quality_score", "fetched_at",
    ]

    tuples = []
    now = datetime.now(timezone.utc)
    for r in rows:
        tuples.append((
            r["symbol"],
            as_of_date,
            _clean(r.get("pe_ttm")),
            _clean(r.get("pb")),
            _clean(r.get("ev_ebitda")),
            _clean(r.get("dividend_yield")),
            _clean(r.get("roe")),
            _clean(r.get("roce")),
            _clean(r.get("ebitda_margin")),
            _clean(r.get("pat_margin")),
            _clean(r.get("revenue_growth_yoy")),
            _clean(r.get("ebitda_growth_yoy")),
            _clean(r.get("pat_growth_yoy")),
            _clean(r.get("debt_to_equity")),
            _clean(r.get("interest_coverage")),
            _clean(r.get("working_capital_days")),
            r.get("data_quality_score", 0),
            now,
        ))

    sql = """
        INSERT INTO financials_snapshots
            (symbol, as_of_date,
             pe_ttm, pb, ev_ebitda, dividend_yield,
             roe, roce, ebitda_margin, pat_margin,
             revenue_growth_yoy, ebitda_growth_yoy, pat_growth_yoy,
             debt_to_equity, interest_coverage, working_capital_days,
             data_quality_score, fetched_at)
        VALUES %s
        ON CONFLICT (symbol, as_of_date) DO UPDATE SET
            pe_ttm               = EXCLUDED.pe_ttm,
            pb                   = EXCLUDED.pb,
            ev_ebitda            = EXCLUDED.ev_ebitda,
            dividend_yield       = EXCLUDED.dividend_yield,
            roe                  = EXCLUDED.roe,
            roce                 = EXCLUDED.roce,
            ebitda_margin        = EXCLUDED.ebitda_margin,
            pat_margin           = EXCLUDED.pat_margin,
            revenue_growth_yoy   = EXCLUDED.revenue_growth_yoy,
            ebitda_growth_yoy    = EXCLUDED.ebitda_growth_yoy,
            pat_growth_yoy       = EXCLUDED.pat_growth_yoy,
            debt_to_equity       = EXCLUDED.debt_to_equity,
            interest_coverage    = EXCLUDED.interest_coverage,
            working_capital_days = EXCLUDED.working_capital_days,
            data_quality_score   = EXCLUDED.data_quality_score,
            fetched_at           = EXCLUDED.fetched_at
    """
    conn = get_psycopg2_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, tuples, page_size=200)
        conn.commit()
    finally:
        conn.close()
    return len(tuples)


def log_run(engine, run_id, started_at, total, success, zero_coverage, avg_quality, status, error=None):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO financials_refresh_log
                    (run_id, started_at, finished_at, stocks_total,
                     stocks_success, stocks_zero_coverage, avg_quality_score,
                     status, error_message)
                VALUES
                    (:run_id, :started_at, NOW(), :stocks_total,
                     :stocks_success, :stocks_zero_coverage, :avg_quality_score,
                     :status, :error_message)
                ON CONFLICT (run_id) DO UPDATE SET
                    finished_at          = NOW(),
                    stocks_total         = EXCLUDED.stocks_total,
                    stocks_success       = EXCLUDED.stocks_success,
                    stocks_zero_coverage = EXCLUDED.stocks_zero_coverage,
                    avg_quality_score    = EXCLUDED.avg_quality_score,
                    status               = EXCLUDED.status,
                    error_message        = EXCLUDED.error_message
            """),
            {
                "run_id":               str(run_id),
                "started_at":           started_at,
                "stocks_total":         total,
                "stocks_success":       success,
                "stocks_zero_coverage": zero_coverage,
                "avg_quality_score":    avg_quality,
                "status":               status,
                "error_message":        error,
            },
        )


# ── Main orchestrator ─────────────────────────────────────────────────────────

def run_weekly_refresh(test_mode: bool = False):
    run_id = uuid.uuid4()
    started_at = datetime.now(timezone.utc)
    as_of_date = date.today()

    logger.info(f"=== Financials refresh started  run_id={run_id}  as_of={as_of_date} ===")
    if test_mode:
        logger.info("  [TEST MODE] — limiting to 10 stocks")

    engine = get_engine()

    # ── 1. Load active stocks ─────────────────────────────────────────────────
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT symbol, yahoo_symbol FROM stocks WHERE is_active = TRUE AND yahoo_symbol IS NOT NULL")
        )
        stocks = pd.DataFrame(result.fetchall(), columns=["symbol", "yahoo_symbol"])

    total = len(stocks)
    logger.info(f"Loaded {total} active stocks")

    if total == 0:
        logger.error("No active stocks found.")
        log_run(engine, run_id, started_at, 0, 0, 0, None, "failed", "No active stocks")
        sys.exit(1)

    if test_mode:
        stocks = stocks.head(10)
        total = len(stocks)

    # Build reverse map: yahoo_symbol → symbol
    sym_map = dict(zip(stocks["yahoo_symbol"], stocks["symbol"]))

    # ── 2. Fetch in batches ───────────────────────────────────────────────────
    all_results = []
    batches = list(_chunks(stocks["yahoo_symbol"].tolist(), BATCH_SIZE))
    logger.info(f"Processing {total} stocks in {len(batches)} batches of {BATCH_SIZE}")

    for i, batch in enumerate(batches, 1):
        logger.info(f"  Batch {i}/{len(batches)} ({len(batch)} stocks)...")
        for yahoo_sym in batch:
            row = fetch_one_stock(yahoo_sym)
            row["symbol"] = sym_map.get(yahoo_sym, yahoo_sym)
            row["data_quality_score"] = count_quality(row)
            all_results.append(row)

        if i < len(batches):
            time.sleep(SLEEP_BETWEEN_BATCHES)

    # ── 3. Compute stats ──────────────────────────────────────────────────────
    quality_scores = [r["data_quality_score"] for r in all_results]
    stocks_success = sum(1 for s in quality_scores if s > 0)
    stocks_zero = sum(1 for s in quality_scores if s == 0)
    avg_quality = round(sum(quality_scores) / len(quality_scores), 2) if quality_scores else 0

    logger.info(f"  Results: {stocks_success} with data, {stocks_zero} zero-coverage, avg quality={avg_quality}/14")

    # ── 4. Upsert into Supabase ───────────────────────────────────────────────
    logger.info("Upserting into financials_snapshots...")
    n_rows = upsert_financials(all_results, as_of_date)
    logger.info(f"  {n_rows} rows upserted")

    # ── 5. Log the run ────────────────────────────────────────────────────────
    status = "success" if stocks_zero == 0 else ("failed" if stocks_success == 0 else "partial")
    log_run(engine, run_id, started_at, total, stocks_success, stocks_zero, avg_quality, status)

    logger.info(f"=== Financials refresh complete: {status} | "
                f"{stocks_success}/{total} stocks with data | avg quality={avg_quality}/14 ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly financials fetcher")
    parser.add_argument("--test", action="store_true", help="Run on 10 stocks only (sanity check)")
    args = parser.parse_args()

    run_weekly_refresh(test_mode=args.test)
