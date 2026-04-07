"""
compute.py — Returns and DMA calculations from a price history DataFrame.

All functions operate on a DataFrame that has columns:
    symbol, date, close

sorted by (symbol, date) ascending.

Return methodology: calendar-day lookback (matches screener.in).
For each window we find the last available trading day on or before
(latest_date - N calendar days), then compute (latest / that_close) - 1.
This gives numbers consistent with screener.in and moneycontrol.
"""

import pandas as pd
from datetime import timedelta


# Calendar-day lookback windows (matches screener.in methodology)
RETURN_WINDOWS = {
    "ret_1d":   1,
    "ret_1w":   7,
    "ret_30d":  30,
    "ret_60d":  60,
    "ret_180d": 180,
    "ret_365d": 365,
}


def _past_close(dates: pd.Series, closes: pd.Series, latest_date, days: int):
    """
    Find the close price of the last available trading day that is
    at least `days` calendar days before `latest_date`.
    Returns (price, date) or (None, None) if not enough history.
    """
    cutoff = latest_date - timedelta(days=days)
    mask = dates <= cutoff
    if not mask.any():
        return None, None
    idx = mask[mask].index[-1]
    return closes.loc[idx], dates.loc[idx]


def compute_snapshots(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame of (symbol, date, close) sorted by (symbol, date),
    return a snapshot DataFrame for the LATEST date of each symbol with:
        symbol, date, cmp,
        ret_1d, ret_1w, ret_30d, ret_60d, ret_180d, ret_365d,
        dma_50, dma_200,
        status_50dma, status_200dma

    Stocks with insufficient history get NaN for the metrics they can't compute.
    """
    prices = prices.sort_values(["symbol", "date"]).copy()
    prices["date"] = pd.to_datetime(prices["date"]).dt.normalize()

    results = []

    for symbol, grp in prices.groupby("symbol", sort=False):
        grp = grp.sort_values("date").reset_index(drop=True)
        dates  = grp["date"]
        closes = grp["close"]
        n = len(closes)

        if n == 0:
            continue

        latest_close = closes.iloc[-1]
        latest_date  = dates.iloc[-1]

        row = {
            "symbol": symbol,
            "date":   latest_date.date(),
            "cmp":    round(float(latest_close), 2),
        }

        # Returns — calendar-day lookback
        for col, cal_days in RETURN_WINDOWS.items():
            past, _ = _past_close(dates, closes, latest_date, cal_days)
            if past is not None and float(past) != 0:
                row[col] = round((float(latest_close) / float(past)) - 1, 6)
            else:
                row[col] = None

        # DMAs (use last 50/200 trading days of data)
        row["dma_50"]  = round(float(closes.tail(50).mean()),  2) if n >= 50  else None
        row["dma_200"] = round(float(closes.tail(200).mean()), 2) if n >= 200 else None

        # Status flags
        if row["dma_50"] is not None:
            row["status_50dma"] = (
                "Above 50DMA" if latest_close >= row["dma_50"] else "Below 50DMA"
            )
        else:
            row["status_50dma"] = None

        if row["dma_200"] is not None:
            row["status_200dma"] = (
                "Above 200DMA" if latest_close >= row["dma_200"] else "Below 200DMA"
            )
        else:
            row["status_200dma"] = None

        results.append(row)

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results)


def compute_sector_performance(snapshots: pd.DataFrame, stocks_meta: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate snapshot metrics by sector.

    snapshots:   DataFrame from compute_snapshots()
    stocks_meta: DataFrame with columns (symbol, sector)

    Returns a DataFrame with sector-level medians and advance/decline counts.
    """
    if snapshots.empty:
        return pd.DataFrame()

    df = snapshots.merge(stocks_meta[["symbol", "sector"]], on="symbol", how="left")
    df = df[df["sector"].notna()]

    if df.empty:
        return pd.DataFrame()

    snap_date = df["date"].max()

    agg = (
        df.groupby("sector")
        .agg(
            num_companies=("symbol", "count"),
            advances=("ret_1d", lambda x: (x > 0).sum()),
            declines=("ret_1d", lambda x: (x < 0).sum()),
            day_change_pct=("ret_1d",   "median"),
            week_chg_pct=("ret_1w",     "median"),
            month_chg_pct=("ret_30d",   "median"),
            qtr_chg_pct=("ret_60d",     "median"),
            half_yr_chg_pct=("ret_180d","median"),
            year_chg_pct=("ret_365d",   "median"),
        )
        .reset_index()
    )

    agg["date"] = snap_date
    return agg
