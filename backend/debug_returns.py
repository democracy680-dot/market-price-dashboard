"""
Run this from the backend/ folder to verify return calculations match screener.in.
Usage: python debug_returns.py
"""
import sys
import pandas as pd
import yfinance as yf
from datetime import timedelta

SYMBOL = "ADANIENT.NS"

print(f"\n=== Fetching {SYMBOL} ===\n")

raw = yf.download(
    tickers=SYMBOL,
    period="2y",
    interval="1d",
    auto_adjust=False,
    progress=False,
)

if "Adj Close" in raw.columns:
    raw = raw.drop(columns=["Adj Close"])

closes = raw["Close"].squeeze().dropna()
dates  = pd.Series(pd.to_datetime(raw.index).normalize())
closes = pd.Series(closes.values, index=dates.index)

latest       = float(closes.iloc[-1])
latest_date  = dates.iloc[-1]

print(f"Total trading days fetched: {len(closes)}")
print(f"Date range: {dates.iloc[0].date()} to {dates.iloc[-1].date()}")
print(f"\nLast 10 closes:")
for d, c in zip(dates.iloc[-10:], closes.iloc[-10:]):
    print(f"  {d.date()}  {c:.2f}")

windows = {
    "1D  (1 cal day)":    1,
    "1W  (7 cal days)":   7,
    "30D (30 cal days)":  30,
    "60D (60 cal days)":  60,
    "180D(180 cal days)": 180,
    "365D(365 cal days)": 365,
}

print(f"\nLatest close ({latest_date.date()}): {latest:.2f}")
print(f"\n=== Returns (calendar-day lookback, matches screener.in) ===")
for label, cal_days in windows.items():
    cutoff = latest_date - timedelta(days=cal_days)
    mask = dates <= cutoff
    if mask.any():
        idx = mask[mask].index[-1]
        past_close = float(closes.loc[idx])
        past_date  = dates.loc[idx].date()
        ret = (latest / past_close - 1) * 100
        print(f"  {label}: ref={past_date}  close={past_close:.2f}  =>  {ret:+.2f}%")
    else:
        print(f"  {label}: not enough history")

print("\nCompare these against screener.in for ADANIENT to verify.")
