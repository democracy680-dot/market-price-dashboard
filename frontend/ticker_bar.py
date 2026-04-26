import concurrent.futures
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime
import pytz

TICKER_HEIGHT = 52  # px

TICKER_SYMBOLS = {
    "NIFTY 50":   "^NSEI",
    "SENSEX":     "^BSESN",
    "BANK NIFTY": "^NSEBANK",
    "NIFTY IT":   "^CNXIT",
    "INDIA VIX":  "^INDIAVIX",
    "CRUDE OIL":  "CL=F",
    "GOLD":       "GC=F",
    "SILVER":     "SI=F",
    "USD/INR":    "USDINR=X",
}


def _yf_download_safe(symbols, period, interval, timeout=12):
    def _run():
        return yf.download(
            tickers=symbols, period=period, interval=interval,
            auto_adjust=True, progress=False, threads=False,
        )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            return fut.result(timeout=timeout)
        except Exception:
            return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def fetch_ticker_data():
    results = []
    symbols = list(TICKER_SYMBOLS.values())
    try:
        raw = _yf_download_safe(symbols, period="2d", interval="1d")
        if raw.empty:
            raise ValueError("empty")
        close = (raw["Close"] if isinstance(raw.columns, pd.MultiIndex)
                 else raw[["Close"]].rename(columns={"Close": symbols[0]}))
        for display_name, symbol in TICKER_SYMBOLS.items():
            try:
                prices = close[symbol].dropna()
                if len(prices) >= 2:
                    last_price = float(prices.iloc[-1])
                    prev_close = float(prices.iloc[-2])
                    change_pct = ((last_price - prev_close) / prev_close) * 100
                elif len(prices) == 1:
                    last_price = float(prices.iloc[-1])
                    change_pct = None
                else:
                    last_price, change_pct = None, None
                results.append({"name": display_name, "price": last_price, "change_pct": change_pct})
            except Exception:
                results.append({"name": display_name, "price": None, "change_pct": None})
    except Exception:
        for display_name in TICKER_SYMBOLS:
            results.append({"name": display_name, "price": None, "change_pct": None})
    return results


def is_market_open() -> bool:
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:
        return False
    return (now.replace(hour=9, minute=15, second=0, microsecond=0)
            <= now <=
            now.replace(hour=15, minute=30, second=0, microsecond=0))


def _fmt_price(price: float) -> str:
    if price is None or price != price:
        return "&#8212;"
    if price > 10000:
        return f"{price:,.0f}"
    if price > 100:
        return f"{price:,.2f}"
    return f"{price:.4f}"


def render_ticker_bar():
    data        = fetch_ticker_data()
    market_open = is_market_open()
    all_failed  = all(d["price"] is None for d in data)

    ist      = pytz.timezone("Asia/Kolkata")
    now_ist  = datetime.now(ist)
    time_str = now_ist.strftime("%H:%M IST")

    if all_failed:
        items_html = '<span style="font-size:12px;color:#64748b;padding:0 24px">&#9888; Market data temporarily unavailable</span>'
    else:
        items_html = ""
        for d in data:
            price, change_pct, name = d["price"], d["change_pct"], d["name"]
            p_str = _fmt_price(price)
            if change_pct is None or price is None:
                chg_html = '<span style="color:#475569;font-size:10.5px;font-weight:600">&#8212;</span>'
            elif change_pct >= 0:
                chg_html = f'<span style="color:#22c55e;font-size:10.5px;font-weight:600">&#9650; {abs(change_pct):.2f}%</span>'
            else:
                chg_html = f'<span style="color:#ef4444;font-size:10.5px;font-weight:600">&#9660; {abs(change_pct):.2f}%</span>'

            items_html += (
                f'<span style="display:inline-flex;align-items:center;gap:7px;padding:0 18px;height:100%">'
                f'<span style="font-size:9.5px;font-weight:700;color:#3d4f68;letter-spacing:0.9px;text-transform:uppercase;white-space:nowrap">{name}</span>'
                f'<span style="font-size:13px;font-weight:600;color:#c8d3e0;font-variant-numeric:tabular-nums;white-space:nowrap">{p_str}</span>'
                f'{chg_html}'
                f'</span>'
                f'<span style="width:1px;height:14px;background:#131d2e;flex-shrink:0;display:inline-block"></span>'
            )

    scroll_html = items_html * 2

    if market_open:
        dot_html   = '<span style="width:6px;height:6px;border-radius:50%;background:#22c55e;flex-shrink:0;display:inline-block"></span>'
        badge_html = '<span style="font-size:9.5px;font-weight:700;letter-spacing:1.2px;padding:2px 7px;border-radius:3px;color:#22c55e;background:rgba(34,197,94,.1);border:1px solid rgba(34,197,94,.25);white-space:nowrap">LIVE</span>'
    else:
        dot_html   = '<span style="width:6px;height:6px;border-radius:50%;background:#475569;flex-shrink:0;display:inline-block"></span>'
        badge_html = '<span style="font-size:9.5px;font-weight:700;letter-spacing:1.2px;padding:2px 7px;border-radius:3px;color:#64748b;background:rgba(100,116,139,.1);border:1px solid rgba(100,116,139,.2);white-space:nowrap">CLOSED</span>'

    html = f"""
<style>
#stk-ticker {{
  position: fixed;
  top: 0; left: 0; right: 0;
  z-index: 999999;
  height: {TICKER_HEIGHT}px;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}
#stk-outer {{
  width: 100%; height: {TICKER_HEIGHT}px;
  background: linear-gradient(180deg, #0d1626 0%, #080c14 100%);
  display: flex; align-items: center; overflow: hidden; position: relative;
}}
#stk-outer::after {{
  content: '';
  position: absolute; bottom: 0; left: 0; right: 0; height: 1px;
  background: linear-gradient(90deg, transparent 0%, #1e3a5f 15%, #3b82f6 50%, #1e3a5f 85%, transparent 100%);
  opacity: 0.6;
}}
#stk-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
  mask-image: linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
}}
#stk-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: stk-marquee 90s linear infinite;
  will-change: transform;
}}
#stk-scroll:hover {{ animation-play-state: paused; }}
@keyframes stk-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}
[data-testid="stAppViewContainer"] {{ padding-top: {TICKER_HEIGHT}px !important; }}
header[data-testid="stHeader"]     {{ top: {TICKER_HEIGHT}px !important; }}
</style>

<div id="stk-ticker">
  <div id="stk-outer">
    <div style="flex-shrink:0;display:flex;align-items:center;gap:9px;padding:0 20px;height:100%">
      <div style="width:24px;height:24px;border-radius:6px;background:linear-gradient(135deg,#1d4ed8,#3b82f6);display:flex;align-items:center;justify-content:center;flex-shrink:0;box-shadow:0 0 10px rgba(59,130,246,0.3)">
        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
          <polyline points="16 7 22 7 22 13"/>
        </svg>
      </div>
      <span style="font-size:13.5px;font-weight:700;color:#cbd5e1;letter-spacing:-0.03em;white-space:nowrap">Stock<span style="color:#3b82f6">Stack</span></span>
    </div>
    <div style="width:1px;height:22px;background:linear-gradient(180deg,transparent,#1e2d45,transparent);flex-shrink:0"></div>
    <div style="flex-shrink:0;display:flex;align-items:center;gap:8px;padding:0 16px;height:100%">
      {dot_html}
      {badge_html}
      <span style="font-size:10.5px;font-weight:500;color:#374151;white-space:nowrap">{time_str}</span>
    </div>
    <div style="width:1px;height:22px;background:linear-gradient(180deg,transparent,#1e2d45,transparent);flex-shrink:0"></div>
    <div id="stk-track">
      <div id="stk-scroll">{scroll_html}</div>
    </div>
  </div>
</div>
"""
    st.html(html)
