"""
fetcher.py — yfinance wrapper with batching and error handling.

Fetches OHLCV data for batches of NSE stocks. Never aborts on a single
bad ticker — logs and continues.
"""

import time
import logging
import concurrent.futures
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2  # seconds, to be polite to yfinance
FUNDAMENTALS_WORKERS = 10  # threads for parallel fast_info fetches


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_prices(yahoo_symbols: list[str]) -> pd.DataFrame:
    """
    Fetch 2 years of daily OHLCV for all yahoo_symbols.
    Uses unadjusted Close prices so returns match NSE / screener.in.

    Returns a DataFrame with columns:
        yahoo_symbol, date, open, high, low, close, volume
    """
    all_frames = []
    batches = list(_chunks(yahoo_symbols, BATCH_SIZE))
    logger.info(f"Fetching {len(yahoo_symbols)} symbols in {len(batches)} batches")

    for i, batch in enumerate(batches, 1):
        logger.info(f"  Batch {i}/{len(batches)} ({len(batch)} symbols)...")
        try:
            raw = yf.download(
                tickers=batch,
                period="2y",          # 2 years → ~500 trading days, enough for 200DMA + 365D return
                interval="1d",
                auto_adjust=False,    # unadjusted Close matches NSE/screener prices
                progress=False,
                threads=True,
            )
        except Exception as e:
            logger.warning(f"  Batch {i} failed entirely: {e}")
            continue

        if raw.empty:
            logger.warning(f"  Batch {i} returned empty data")
            continue

        # yfinance returns MultiIndex columns (Price, Ticker) for multi-ticker downloads
        if isinstance(raw.columns, pd.MultiIndex):
            raw = raw.stack(level="Ticker", future_stack=True).reset_index()
            raw.columns.name = None
            raw = raw.rename(columns={"Date": "date", "Ticker": "yahoo_symbol"})
        else:
            raw = raw.reset_index().rename(columns={"Date": "date"})
            raw["yahoo_symbol"] = batch[0]

        # Normalise column names to lowercase
        raw.columns = [c.lower() for c in raw.columns]

        # With auto_adjust=False yfinance gives both 'close' and 'adj close'
        # Rename 'adj close' if present, we use raw 'close'
        if "adj close" in raw.columns:
            raw = raw.drop(columns=["adj close"])

        needed = {"yahoo_symbol", "date", "open", "high", "low", "close", "volume"}
        missing = needed - set(raw.columns)
        if missing:
            logger.warning(f"  Batch {i} missing columns {missing}, skipping")
            continue

        raw = raw[list(needed)].dropna(subset=["close"])
        all_frames.append(raw)

        if i < len(batches):
            time.sleep(SLEEP_BETWEEN_BATCHES)

    if not all_frames:
        logger.error("All batches failed — no data fetched.")
        return pd.DataFrame()

    df = pd.concat(all_frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info(f"Fetched {len(df)} rows across {df['yahoo_symbol'].nunique()} symbols")
    return df


def _fetch_one_fundamental(yahoo_symbol: str) -> dict:
    """Fetch market_cap, pe_ratio, and sector for a single ticker. Never raises."""
    result = {"yahoo_symbol": yahoo_symbol, "market_cap_cr": None, "pe_ratio": None, "sector": None}
    try:
        t = yf.Ticker(yahoo_symbol)
        fi = t.fast_info

        market_cap = getattr(fi, "market_cap", None)
        if market_cap and market_cap > 0:
            # NSE stocks (.NS) → yfinance returns market_cap in INR already.
            # If the ticker reports a non-INR currency, convert to INR first
            # using a rough exchange rate before dividing by 1e7 (1 Cr = 10^7 INR).
            currency = getattr(fi, "currency", "INR") or "INR"
            if currency.upper() != "INR":
                # Fallback exchange-rate approximation (USD/GBP/etc → INR)
                try:
                    fx_ticker = yf.Ticker(f"{currency.upper()}INR=X")
                    fx_rate = fx_ticker.fast_info.last_price or 84.0
                except Exception:
                    fx_rate = 84.0          # conservative fallback: 1 USD ≈ ₹84
                market_cap = market_cap * fx_rate
            result["market_cap_cr"] = round(market_cap / 1e7, 2)  # Rupees → Crore

        # PE and sector from info (slower but only called once per ticker per day)
        info = t.info
        pe = info.get("trailingPE") or info.get("forwardPE")
        if pe and pe > 0:
            result["pe_ratio"] = round(float(pe), 2)

        sector = info.get("sector")
        if sector:
            result["sector"] = sector
    except Exception as e:
        logger.debug(f"  fundamentals failed for {yahoo_symbol}: {e}")
    return result


def fetch_fundamentals(yahoo_symbols: list[str]) -> pd.DataFrame:
    """
    Fetch market_cap_cr and pe_ratio for all symbols using parallel threads.

    Returns a DataFrame with columns:
        yahoo_symbol, market_cap_cr, pe_ratio
    """
    logger.info(f"Fetching fundamentals for {len(yahoo_symbols)} symbols...")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=FUNDAMENTALS_WORKERS) as pool:
        futures = {pool.submit(_fetch_one_fundamental, sym): sym for sym in yahoo_symbols}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            results.append(future.result())
            if i % 50 == 0:
                logger.info(f"  fundamentals: {i}/{len(yahoo_symbols)} done")

    df = pd.DataFrame(results)
    filled = df["market_cap_cr"].notna().sum()
    logger.info(f"  fundamentals fetched: {filled}/{len(yahoo_symbols)} with market cap")
    return df
