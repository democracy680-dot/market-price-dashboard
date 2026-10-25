"""
market_summary_tab.py — Market Summary tab: one-screen view of today's move
across Indian indexes, global indexes, commodities, currencies, bonds and crypto.

Compact grouped tables (Price · Change · % Change · intraday sparkline), live
via yfinance. Quote + sparkline fetching is shared with the Global Markets tab.
"""

from datetime import datetime

import pytz
import streamlit as st

from global_markets_tab import (
    _fmt_price,
    _palette,
    _svg_spark,
    _YIELD_SYMS,
    fetch_intraday_for,
    fetch_quotes_for,
)

try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False


# ─── GROUPS CONFIG ───────────────────────────────────────────────────────────
# (sym, display name). Left column groups first, then right column.

LEFT_GROUPS = [
    ("India — Broad Market", [
        ("^NSEI",               "Nifty 50"),
        ("^BSESN",              "BSE Sensex"),
        ("^NSMIDCP",            "Nifty Next 50"),
        ("^CNX100",             "Nifty 100"),
        ("^CRSLDX",             "Nifty 500"),
        ("NIFTY_MIDCAP_100.NS", "Nifty Midcap 100"),
        ("NIFTYMIDCAP150.NS",   "Nifty Midcap 150"),
        ("^CNXSC",              "Nifty Smallcap 100"),
        ("NIFTYSMLCAP250.NS",   "Nifty Smallcap 250"),
        ("^INDIAVIX",           "India VIX"),
    ]),
    ("India — Sectoral", [
        ("^NSEBANK",             "Nifty Bank"),
        ("NIFTY_FIN_SERVICE.NS", "Financial Services"),
        ("NIFTY_PVT_BANK.NS",    "Private Bank"),
        ("^CNXPSUBANK",          "PSU Bank"),
        ("NIFTY_CAPITAL_MKT.NS", "Capital Markets"),
        ("^CNXIT",               "IT"),
        ("^CNXAUTO",             "Auto"),
        ("^CNXPHARMA",           "Pharma"),
        ("NIFTY_HEALTHCARE.NS",  "Healthcare"),
        ("^CNXFMCG",             "FMCG"),
        ("NIFTY_CONSR_DURBL.NS", "Consumer Durables"),
        ("^CNXMETAL",            "Metal"),
        ("^CNXREALTY",           "Realty"),
        ("^CNXENERGY",           "Energy"),
        ("NIFTY_OIL_AND_GAS.NS", "Oil & Gas"),
        ("^CNXINFRA",            "Infrastructure"),
        ("^CNXPSE",              "PSE"),
        ("NIFTY_IND_DEFENCE.NS", "Defence"),
        ("NIFTY_CHEMICALS.NS",   "Chemicals"),
        ("^CNXMEDIA",            "Media"),
    ]),
]

RIGHT_GROUPS = [
    ("United States", [
        ("^GSPC",  "S&P 500"),
        ("^DJI",   "Dow Jones"),
        ("^IXIC",  "Nasdaq Composite"),
        ("^RUT",   "Russell 2000"),
        ("^VIX",   "CBOE VIX"),
        ("ES=F",   "S&P 500 Futures"),
    ]),
    ("Europe", [
        ("^FTSE",     "FTSE 100"),
        ("^GDAXI",    "DAX 40"),
        ("^FCHI",     "CAC 40"),
        ("^STOXX50E", "Euro Stoxx 50"),
    ]),
    ("Asia Pacific", [
        ("^N225",     "Nikkei 225"),
        ("^HSI",      "Hang Seng"),
        ("000001.SS", "Shanghai Composite"),
        ("^KS11",     "KOSPI"),
        ("^TWII",     "Taiwan TAIEX"),
        ("^STI",      "Straits Times"),
        ("^AXJO",     "ASX 200"),
    ]),
    ("Commodities", [
        ("GC=F",  "Gold"),
        ("SI=F",  "Silver"),
        ("BZ=F",  "Brent Crude"),
        ("CL=F",  "WTI Crude"),
        ("NG=F",  "Natural Gas"),
        ("HG=F",  "Copper"),
        ("ALI=F", "Aluminium"),
    ]),
    ("Currency", [
        ("USDINR=X", "USD / INR"),
        ("EURINR=X", "EUR / INR"),
        ("GBPINR=X", "GBP / INR"),
        ("JPYINR=X", "JPY / INR"),
        ("DX-Y.NYB", "Dollar Index"),
    ]),
    ("Bonds & Crypto", [
        ("^TNX",      "US 10Y Yield"),
        ("IN10YT=RR", "India 10Y Yield"),
        ("BTC-USD",   "Bitcoin"),
        ("ETH-USD",   "Ethereum"),
    ]),
]

ALL_SYMS = tuple(dict.fromkeys(
    sym for _, items in LEFT_GROUPS + RIGHT_GROUPS for sym, _ in items
))

_UP, _DOWN = "#16a34a", "#dc2626"
_UP_DARK, _DOWN_DARK = "#22c55e", "#ef4444"


# ─── PURE HELPERS ────────────────────────────────────────────────────────────

def build_rows(items: list, quotes: dict) -> list:
    """Turn (sym, name) pairs + quote dict into display rows.

    Missing quotes yield '—' placeholders and direction 0 so the row still renders.
    """
    rows = []
    for sym, name in items:
        q = quotes.get(sym)
        if not q:
            rows.append({"sym": sym, "name": name, "price": "—",
                         "change": "—", "pct": "—", "dir": 0})
            continue
        chg, pct = q["change"], q["pct"]
        rows.append({
            "sym":    sym,
            "name":   name,
            "price":  _fmt_price(q["price"], sym),
            # Sub-1 prices (e.g. JPY/INR) need 4 decimals or the change rounds to 0.00
            "change": f"{chg:+.4f}" if q["price"] < 1 else f"{chg:+,.2f}",
            "pct":    f"{pct:+.2f}%",
            "dir":    (chg > 0) - (chg < 0),
        })
    return rows


def group_breadth(rows: list) -> tuple:
    """(advancing, declining) count for a group's rows."""
    return (sum(r["dir"] > 0 for r in rows), sum(r["dir"] < 0 for r in rows))


# ─── RENDERING ───────────────────────────────────────────────────────────────

def _group_html(title: str, rows: list, intraday: dict, C: dict, dark: bool) -> str:
    up_c, dn_c = (_UP_DARK, _DOWN_DARK) if dark else (_UP, _DOWN)
    adv, dec = group_breadth(rows)
    body = []
    for r in rows:
        color = up_c if r["dir"] > 0 else dn_c if r["dir"] < 0 else C["text_muted"]
        spark = _svg_spark(intraday.get(r["sym"], []), color if r["dir"] else "#64748b", w=90, h=22)
        body.append(
            f'<tr>'
            f'<td class="ms-name">{r["name"]}</td>'
            f'<td class="ms-num">{r["price"]}</td>'
            f'<td class="ms-num" style="color:{color};">{r["change"]}</td>'
            f'<td class="ms-num ms-pct" style="color:{color};">{r["pct"]}</td>'
            f'<td class="ms-spark">{spark}</td>'
            f'</tr>'
        )
    return (
        f'<div class="ms-group">'
        f'<div class="ms-head"><span>{title}</span>'
        f'<span class="ms-breadth"><span style="color:{up_c};">▲ {adv}</span>'
        f'&nbsp;&nbsp;<span style="color:{dn_c};">▼ {dec}</span></span></div>'
        f'<table class="ms-table"><thead><tr>'
        f'<th>Name</th><th class="ms-num">Price</th><th class="ms-num">Chg</th>'
        f'<th class="ms-num">% Chg</th><th class="ms-spark">Today</th>'
        f'</tr></thead><tbody>{"".join(body)}</tbody></table></div>'
    )


def _css(C: dict) -> str:
    return f"""
    <style>
      .ms-group {{
        background: {C['bg_card']}; border: 1px solid {C['bd_card']};
        border-radius: 8px; margin-bottom: 14px; overflow: hidden;
      }}
      .ms-head {{
        display: flex; justify-content: space-between; align-items: center;
        padding: 8px 12px; font-size: 13px; font-weight: 700;
        color: {C['text_primary']}; border-bottom: 1px solid {C['bd_card']};
      }}
      .ms-breadth {{ font-size: 12px; font-weight: 600; }}
      .ms-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
      .ms-table th {{
        text-align: left; font-weight: 600; font-size: 11px; text-transform: uppercase;
        letter-spacing: 0.04em; color: {C['text_label']}; padding: 6px 12px;
        border-bottom: 1px solid {C['bd_card']};
      }}
      .ms-table td {{
        padding: 5px 12px; color: {C['text_num']};
        border-bottom: 1px solid {C['grid']};
      }}
      .ms-table tr:last-child td {{ border-bottom: none; }}
      .ms-name {{ color: {C['text_secondary']} !important; white-space: nowrap; }}
      .ms-num {{ text-align: right !important; font-variant-numeric: tabular-nums; white-space: nowrap; }}
      .ms-pct {{ font-weight: 700; }}
      .ms-spark {{ width: 90px; padding-right: 12px !important; }}
      @media (max-width: 640px) {{
        .ms-spark {{ display: none; }}
        .ms-table td, .ms-table th {{ padding: 5px 8px; }}
      }}
    </style>
    """


def render_market_summary_tab():
    """Entry point called from app.py."""
    dark = st.session_state.get("dark_mode", False)
    C    = _palette(dark)

    if _HAS_AUTOREFRESH:
        st_autorefresh(interval=300_000, key="ms_autorefresh")

    with st.spinner("Loading market data…"):
        quotes, fetched_at = fetch_quotes_for(ALL_SYMS)
        intraday = fetch_intraday_for(ALL_SYMS)

    # fetched_at is naive server-local time (UTC on Streamlit Cloud) — show it in IST
    stamp = datetime.fromtimestamp(fetched_at.timestamp(), pytz.timezone("Asia/Kolkata"))
    info_col, btn_col = st.columns([8, 1])
    with info_col:
        st.caption(
            f"Live via Yahoo Finance · last updated {stamp:%d %b %Y, %I:%M %p} IST · "
            f"auto-refreshes every 5 min. Closed markets show their last session's move."
        )
    with btn_col:
        if st.button("↺ Refresh", key="ms_refresh", use_container_width=True):
            fetch_quotes_for.clear()
            fetch_intraday_for.clear()
            st.rerun()

    st.markdown(_css(C), unsafe_allow_html=True)
    left, right = st.columns(2, gap="medium")
    for col, groups in ((left, LEFT_GROUPS), (right, RIGHT_GROUPS)):
        with col:
            st.markdown(
                "".join(_group_html(t, build_rows(items, quotes), intraday, C, dark)
                        for t, items in groups),
                unsafe_allow_html=True,
            )
