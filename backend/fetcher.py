"""
fetcher.py — yfinance wrapper with batching and error handling.

Fetches OHLCV data for batches of NSE stocks. Never aborts on a single
bad ticker — logs and continues.
"""

import time
import logging
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
LOOKBACK_DAYS = 250   # enough for 200 DMA + returns
SLEEP_BETWEEN_BATCHES = 2  # seconds, to be polite to yfinance


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def fetch_prices(yahoo_symbols: list[str]) -> pd.DataFrame:
    """
    Fetch up to LOOKBACK_DAYS of daily OHLCV for all yahoo_symbols.

    Returns a DataFrame with columns:
        yahoo_symbol, date, open, high, low, close, volume

    Stocks that fail are logged and excluded — never raises.
    """
    all_frames = []
    batches = list(_chunks(yahoo_symbols, BATCH_SIZE))
    logger.info(f"Fetching {len(yahoo_symbols)} symbols in {len(batches)} batches")

    for i, batch in enumerate(batches, 1):
        logger.info(f"  Batch {i}/{len(batches)} ({len(batch)} symbols)...")
        try:
            raw = yf.download(
                tickers=batch,
                period=f"{LOOKBACK_DAYS}d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            logger.warning(f"  Batch {i} failed entirely: {e}")
            continue

        if raw.empty:
            logger.warning(f"  Batch {i} returned empty data")
            continue

        # yfinance 1.x returns MultiIndex columns (Price, Ticker) for multi-ticker downloads
        if isinstance(raw.columns, pd.MultiIndex):
            # Stack ticker level into rows
            raw = raw.stack(level="Ticker", future_stack=True).reset_index()
            raw.columns.name = None
            # After stack+reset_index: columns are Date, Ticker, Close, Open, ...
            raw = raw.rename(columns={"Date": "date", "Ticker": "yahoo_symbol"})
        else:
            raw = raw.reset_index().rename(columns={"Date": "date"})
            raw["yahoo_symbol"] = batch[0]

        # Normalise column names to lowercase
        raw.columns = [c.lower() for c in raw.columns]
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
