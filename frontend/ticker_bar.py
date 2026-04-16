import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime
import pytz

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


@st.cache_data(ttl=60)
def fetch_ticker_data():
    results = []
    symbols = list(TICKER_SYMBOLS.values())
    try:
        tickers = yf.Tickers(" ".join(symbols))
        for display_name, symbol in TICKER_SYMBOLS.items():
            try:
                info = tickers.tickers[symbol].fast_info
                last_price = getattr(info, "last_price", None)
                prev_close = getattr(info, "previous_close", None)
                if last_price is not None and prev_close is not None and prev_close != 0:
                    change = last_price - prev_close
                    change_pct = (change / prev_close) * 100
                else:
                    change = change_pct = None
                results.append({"name": display_name, "price": last_price,
                                 "change": change, "change_pct": change_pct})
            except Exception:
                results.append({"name": display_name, "price": None,
                                 "change": None, "change_pct": None})
    except Exception:
        for display_name in TICKER_SYMBOLS:
            results.append({"name": display_name, "price": None,
                             "change": None, "change_pct": None})
    return results


def is_market_open() -> bool:
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


def _fmt_price(price: float) -> str:
    if price > 10000:
        return f"{price:,.0f}"
    if price > 100:
        return f"{price:,.2f}"
    return f"{price:.4f}"


def render_ticker_bar():
    data = fetch_ticker_data()
    market_open = is_market_open()

    all_failed = all(item["price"] is None for item in data)
    if all_failed:
        components.html(
            '<div style="width:100%;height:48px;display:flex;align-items:center;justify-content:center;'
            'background:#0b0f1a;border-bottom:1px solid #1a2236;font-family:Inter,sans-serif;'
            'font-size:12px;color:#64748b;">'
            "⚠️&nbsp; Market data temporarily unavailable &nbsp;•&nbsp; Dashboard data (EOD) is unaffected"
            "</div>",
            height=50,
        )
        return

    # Build individual ticker items
    items_html = ""
    for item in data:
        price = item["price"]
        change_pct = item["change_pct"]
        name = item["name"]

        if price is None:
            price_str = "—"
            change_html = '<span style="color:#475569;">N/A</span>'
        else:
            price_str = _fmt_price(price)
            if change_pct is not None:
                if change_pct >= 0:
                    change_html = (
                        f'<span style="color:#22c55e;">&#9650;&nbsp;{abs(change_pct):.2f}%</span>'
                    )
                else:
                    change_html = (
                        f'<span style="color:#ef4444;">&#9660;&nbsp;{abs(change_pct):.2f}%</span>'
                    )
            else:
                change_html = '<span style="color:#475569;">—</span>'

        items_html += f"""
        <span class="t-item">
            <span class="t-name">{name}</span>
            <span class="t-price">{price_str}</span>
            {change_html}
        </span>
        <span class="t-sep">&#9679;</span>
        """

    # Duplicate for seamless infinite scroll
    scroll_content = items_html * 2

    if market_open:
        dot_html = '<span class="dot-live"></span>'
        status_label = "LIVE"
        badge_color = "#16a34a"
        badge_bg = "rgba(22,163,74,0.15)"
    else:
        dot_html = '<span class="dot-closed"></span>'
        status_label = "CLOSED"
        badge_color = "#475569"
        badge_bg = "rgba(71,85,105,0.15)"

    full_html = f"""<!DOCTYPE html>
<html>
<head>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{ height: 48px; overflow: hidden; background: #080c14; }}

  .ticker-outer {{
    width: 100%;
    height: 48px;
    background: #080c14;
    border-bottom: 1px solid #1a2236;
    display: flex;
    align-items: center;
    overflow: hidden;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  }}

  /* Left badge */
  .ticker-badge {{
    flex-shrink: 0;
    display: flex;
    align-items: center;
    gap: 7px;
    padding: 0 16px;
    height: 100%;
    background: #0b0f1a;
    border-right: 1px solid #1a2236;
    min-width: 100px;
  }}
  .badge-label {{
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 1.5px;
    color: {badge_color};
    background: {badge_bg};
    padding: 2px 7px;
    border-radius: 3px;
    border: 1px solid {badge_color}44;
  }}

  /* Animated dots */
  .dot-live {{
    width: 7px; height: 7px;
    background: #22c55e;
    border-radius: 50%;
    animation: livepulse 1.8s ease-in-out infinite;
    flex-shrink: 0;
  }}
  .dot-closed {{
    width: 7px; height: 7px;
    background: #475569;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  @keyframes livepulse {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(34,197,94,0.5); opacity: 1; }}
    50%        {{ box-shadow: 0 0 0 5px rgba(34,197,94,0);  opacity: 0.75; }}
  }}

  /* Scroll track */
  .ticker-track {{
    flex: 1;
    overflow: hidden;
    height: 100%;
    position: relative;
    /* fade edges */
    -webkit-mask-image: linear-gradient(to right, transparent 0%, black 3%, black 97%, transparent 100%);
    mask-image: linear-gradient(to right, transparent 0%, black 3%, black 97%, transparent 100%);
  }}
  .ticker-scroll {{
    display: inline-flex;
    align-items: center;
    height: 100%;
    white-space: nowrap;
    animation: marquee 55s linear infinite;
    will-change: transform;
  }}
  .ticker-scroll:hover {{ animation-play-state: paused; }}

  @keyframes marquee {{
    0%   {{ transform: translateX(0); }}
    100% {{ transform: translateX(-50%); }}
  }}

  /* Individual item */
  .t-item {{
    display: inline-flex;
    align-items: center;
    gap: 7px;
    padding: 0 18px;
    height: 100%;
    cursor: default;
    transition: background 0.2s;
  }}
  .t-item:hover {{ background: rgba(255,255,255,0.04); }}

  .t-name {{
    font-size: 10px;
    font-weight: 700;
    color: #64748b;
    letter-spacing: 0.8px;
    text-transform: uppercase;
  }}
  .t-price {{
    font-size: 13px;
    font-weight: 700;
    color: #e2e8f0;
    letter-spacing: 0.2px;
  }}
  span[style*="color:#22c55e"],
  span[style*="color:#ef4444"],
  span[style*="color:#475569"] {{
    font-size: 11px;
    font-weight: 600;
  }}

  /* Separator dot */
  .t-sep {{
    color: #1e2d45;
    font-size: 6px;
    flex-shrink: 0;
  }}
</style>
</head>
<body>
<div class="ticker-outer">
  <div class="ticker-badge">
    {dot_html}
    <span class="badge-label">{status_label}</span>
  </div>
  <div class="ticker-track">
    <div class="ticker-scroll">
      {scroll_content}
    </div>
  </div>
</div>
</body>
</html>"""

    components.html(full_html, height=50, scrolling=False)
