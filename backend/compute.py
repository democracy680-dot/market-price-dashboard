"""
compute.py — Returns and DMA calculations from a price history DataFrame.

All functions operate on a DataFrame that has columns:
    symbol, date, close

sorted by (symbol, date) ascending.
"""

import pandas as pd


# Trading-day offsets for return windows
RETURN_WINDOWS = {
    "ret_1d":   1,
    "ret_1w":   5,
    "ret_30d":  22,
    "ret_60d":  44,
    "ret_180d": 132,
    "ret_365d": 252,
}


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
    prices["date"] = pd.to_datetime(prices["date"])

    results = []

    for symbol, grp in prices.groupby("symbol", sort=False):
        grp = grp.sort_values("date").reset_index(drop=True)
        closes = grp["close"]
        n = len(closes)

        if n == 0:
            continue

        latest_close = closes.iloc[-1]
        latest_date = grp["date"].iloc[-1].date()

        row = {
            "symbol": symbol,
            "date":   latest_date,
            "cmp":    round(float(latest_close), 2),
        }

        # Returns
        for col, offset in RETURN_WINDOWS.items():
            if n > offset:
                past_close = closes.iloc[-(offset + 1)]
                if past_close and past_close != 0:
                    row[col] = round((latest_close / past_close) - 1, 6)
                else:
                    row[col] = None
            else:
                row[col] = None

        # DMAs
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
