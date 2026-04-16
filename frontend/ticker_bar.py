import yfinance as yf
import streamlit as st
from datetime import datetime
import pytz

TICKER_SYMBOLS = {
    "Nifty 50":   "^NSEI",
    "Sensex":     "^BSESN",
    "Bank Nifty": "^NSEBANK",
    "Nifty IT":   "^CNXIT",
    "India VIX":  "^INDIAVIX",
    "Crude Oil":  "CL=F",
    "Gold":       "GC=F",
    "USD/INR":    "USDINR=X",
}


@st.cache_data(ttl=60)
def fetch_ticker_data():
    """
    Fetches current price and day change for all ticker instruments.
    Cached for 60 seconds to avoid hammering yfinance.
    Returns a list of dicts, one per instrument.
    """
    results = []
    symbols = list(TICKER_SYMBOLS.values())

    try:
        tickers = yf.Tickers(" ".join(symbols))

        for display_name, symbol in TICKER_SYMBOLS.items():
            try:
                ticker = tickers.tickers[symbol]
                info = ticker.fast_info

                last_price = getattr(info, "last_price", None)
                prev_close = getattr(info, "previous_close", None)

                if last_price is not None and prev_close is not None and prev_close != 0:
                    change = last_price - prev_close
                    change_pct = (change / prev_close) * 100
                else:
                    change = None
                    change_pct = None

                results.append({
                    "name": display_name,
                    "price": last_price,
                    "change": change,
                    "change_pct": change_pct,
                })

            except Exception:
                results.append({
                    "name": display_name,
                    "price": None,
                    "change": None,
                    "change_pct": None,
                })

    except Exception:
        for display_name in TICKER_SYMBOLS:
            results.append({
                "name": display_name,
                "price": None,
                "change": None,
                "change_pct": None,
            })

    return results


def is_market_open() -> bool:
    """
    Returns True if Indian stock market is likely open right now.
    NSE hours: Mon-Fri, 9:15 AM - 3:30 PM IST.
    Does NOT account for holidays — acceptable for a visual indicator.
    """
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    if now.weekday() >= 5:  # Saturday, Sunday
        return False
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


def render_ticker_bar():
    """
    Renders the horizontal market ticker bar at the top of the page.
    Must be called before any other st.* content in app.py.
    """
    data = fetch_ticker_data()
    market_open = is_market_open()

    # Check for total failure (all prices None)
    all_failed = all(item["price"] is None for item in data)
    if all_failed:
        st.markdown(
            '<div style="'
            "width:100%; padding:10px 16px; margin-bottom:16px;"
            "border-bottom:1px solid rgba(128,128,128,0.2);"
            'font-size:13px; color:#888; text-align:center;">'
            "⚠️ Market data temporarily unavailable • Dashboard data (EOD) is unaffected"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    cards_html = ""
    for item in data:
        name = item["name"]
        price = item["price"]
        change_pct = item["change_pct"]

        if price is None:
            price_str = "—"
            change_str = ""
            color = "#888888"
        else:
            if price > 10000:
                price_str = f"{price:,.0f}"
            elif price > 100:
                price_str = f"{price:,.2f}"
            else:
                price_str = f"{price:.2f}"

            if change_pct is not None:
                if change_pct >= 0:
                    arrow = "▲"
                    color = "#22c55e"
                else:
                    arrow = "▼"
                    color = "#ef4444"
                change_str = f"{arrow} {abs(change_pct):.2f}%"
            else:
                change_str = ""
                color = "#888888"

        cards_html += f"""
        <div style="
            display: inline-flex;
            flex-direction: column;
            align-items: center;
            padding: 6px 18px;
            min-width: 120px;
            border-right: 1px solid rgba(128,128,128,0.2);
        ">
            <span style="font-size: 11px; color: #888; font-weight: 500; letter-spacing: 0.5px;">
                {name}
            </span>
            <span style="font-size: 15px; font-weight: 700; margin: 2px 0; color: #e2e8f0;">
                {price_str}
            </span>
            <span style="font-size: 12px; color: {color}; font-weight: 600;">
                {change_str}
            </span>
        </div>
        """

    if market_open:
        status_dot = (
            '<span style="display:inline-block; width:8px; height:8px; background:#22c55e;'
            ' border-radius:50%; margin-right:6px; animation: pulse 2s infinite;"></span>'
        )
        status_text = "Market Open"
    else:
        status_dot = (
            '<span style="display:inline-block; width:8px; height:8px; background:#888;'
            ' border-radius:50%; margin-right:6px;"></span>'
        )
        status_text = "Market Closed"

    full_html = f"""
    <style>
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.4; }}
        }}
    </style>
    <div style="
        width: 100%;
        overflow-x: auto;
        white-space: nowrap;
        background-color: #0b0f1a;
        border: 1px solid #1a2236;
        border-radius: 8px;
        padding: 8px 0;
        margin-bottom: 16px;
        display: flex;
        align-items: center;
    ">
        <div style="
            display: inline-flex;
            align-items: center;
            padding: 0 16px;
            border-right: 1px solid rgba(128,128,128,0.2);
            min-width: 130px;
        ">
            {status_dot}
            <span style="font-size: 11px; color: #888; font-weight: 500;">{status_text}</span>
        </div>
        {cards_html}
    </div>
    """

    st.markdown(full_html, unsafe_allow_html=True)
