"""
compute.py — Returns and DMA calculations from a price history DataFrame.

All functions operate on a DataFrame that has columns:
    symbol, date, close

sorted by (symbol, date) ascending.

Return methodology: matches Google Finance exactly.
- 1D, 1W, 30D, 365D : subtract fixed calendar days (TODAY()-N)
- 60D               : subtract exactly 2 months  (EDATE(TODAY(), -2))
- 180D              : subtract exactly 6 months  (EDATE(TODAY(), -6))

For all periods: if the cutoff falls on a weekend/holiday, snap FORWARD to
the next available trading day (matches GOOGLEFINANCE sheet formula behaviour).
"""

import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta


# Return window definitions — "days" uses timedelta, "months" uses relativedelta
RETURN_WINDOWS = {
    "ret_1d":   {"type": "days",   "n": 1},
    "ret_1w":   {"type": "days",   "n": 7},
    "ret_30d":  {"type": "days",   "n": 30},
    "ret_60d":  {"type": "months", "n": 2},   # EDATE(today, -2)
    "ret_180d": {"type": "months", "n": 6},   # EDATE(today, -6)
    "ret_365d": {"type": "days",   "n": 365},
}


def _cutoff_date(latest_date, spec: dict):
    """Compute the target/cutoff date for a given window spec."""
    if spec["type"] == "days":
        return latest_date - timedelta(days=spec["n"])
    else:
        return latest_date - relativedelta(months=spec["n"])


def _past_close(dates: pd.Series, closes: pd.Series, latest_date, spec: dict):
    """
    Find the close price of the first available trading day on or after
    the cutoff date, excluding latest_date itself.
    Matches Google Finance: snaps forward when cutoff is a weekend/holiday.
    Returns (price, date) or (None, None) if not enough history.
    """
    cutoff = _cutoff_date(latest_date, spec)
    mask = (dates >= cutoff) & (dates < latest_date)
    if not mask.any():
        return None, None
    idx = mask[mask].index[0]
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

        # Returns — Google Finance methodology
        for col, spec in RETURN_WINDOWS.items():
            # 1D special case: always use the immediately previous trading day
            # (timedelta(1) breaks on Mondays — lands on Sunday with no data)
            if col == "ret_1d":
                if n >= 2:
                    past = closes.iloc[-2]
                    if pd.notna(past) and float(past) != 0:
                        row["ret_1d"] = round((float(latest_close) / float(past)) - 1, 6)
                    else:
                        row["ret_1d"] = None
                else:
                    row["ret_1d"] = None
                continue

            past, _ = _past_close(dates, closes, latest_date, spec)
            # Guard: past must be a real, non-zero number to avoid div/0 or NaN
            if past is not None and pd.notna(past) and float(past) != 0:
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
            # Only count stocks with a valid (non-NaN) 1D return
            advances=("ret_1d", lambda x: int((x.dropna() > 0).sum())),
            declines=("ret_1d", lambda x: int((x.dropna() < 0).sum())),
            unchanged=("ret_1d", lambda x: int((x.dropna() == 0).sum())),
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
