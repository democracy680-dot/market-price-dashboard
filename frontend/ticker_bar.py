import json
import sys
import pandas as pd
import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime
import pytz

_COMPONENTS_HTML_SAFE = sys.version_info < (3, 14)

TICKER_HEIGHT = 52  # px — keep in sync with CSS below

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


@st.cache_data(ttl=60, show_spinner=False)
def fetch_ticker_data():
    results = []
    symbols = list(TICKER_SYMBOLS.values())
    try:
        raw = yf.download(
            tickers=symbols, period='2d', interval='1d',
            auto_adjust=True, progress=False, threads=True,
        )
        close = (raw['Close'] if isinstance(raw.columns, pd.MultiIndex)
                 else raw[['Close']].rename(columns={'Close': symbols[0]}))
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
        return "—"
    if price > 10000:
        return f"{price:,.0f}"
    if price > 100:
        return f"{price:,.2f}"
    return f"{price:.4f}"


def render_ticker_bar():
    data        = fetch_ticker_data()
    market_open = is_market_open()
    all_failed  = all(d["price"] is None for d in data)

    ist     = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist)
    time_str = now_ist.strftime("%H:%M IST")

    # ── Build scrolling items HTML ──────────────────────────────────────────
    if all_failed:
        items_html = '<span class="t-unavail">⚠ Market data temporarily unavailable</span>'
    else:
        items_html = ""
        for d in data:
            price, change_pct, name = d["price"], d["change_pct"], d["name"]
            if price is None:
                p_str   = "—"
                chg_cls = "neutral"
                chg_str = "N/A"
                arrow   = ""
            else:
                p_str = _fmt_price(price)
                if change_pct is None:
                    chg_cls, chg_str, arrow = "neutral", "—", ""
                elif change_pct >= 0:
                    chg_cls = "up"
                    chg_str = f"{abs(change_pct):.2f}%"
                    arrow   = "▲"
                else:
                    chg_cls = "dn"
                    chg_str = f"{abs(change_pct):.2f}%"
                    arrow   = "▼"

            items_html += (
                f'<span class="t-item">'
                f'  <span class="t-name">{name}</span>'
                f'  <span class="t-price">{p_str}</span>'
                f'  <span class="t-chg {chg_cls}">'
                f'    <span class="t-arrow">{arrow}</span>{chg_str}'
                f'  </span>'
                f'</span>'
                f'<span class="t-sep"></span>'
            )

    scroll_html = items_html * 2  # duplicate for seamless loop

    # ── Left-side brand + badge markup ─────────────────────────────────────
    brand_html = """
<div class="stk-brand">
  <div class="stk-logo-icon">
    <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24"
         fill="none" stroke="white" stroke-width="2.8"
         stroke-linecap="round" stroke-linejoin="round">
      <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
      <polyline points="16 7 22 7 22 13"/>
    </svg>
  </div>
  <span class="stk-brand-name">Stock<em>Stack</em></span>
</div>
<div class="stk-divider-v"></div>
"""

    if market_open:
        badge_html = (
            f'<div class="stk-meta">'
            f'  <span class="dot-live"></span>'
            f'  <span class="badge live-badge">LIVE</span>'
            f'  <span class="stk-clock" id="stk-clock">{time_str}</span>'
            f'</div>'
        )
    else:
        badge_html = (
            f'<div class="stk-meta">'
            f'  <span class="dot-off"></span>'
            f'  <span class="badge off-badge">CLOSED</span>'
            f'  <span class="stk-clock" id="stk-clock">{time_str}</span>'
            f'</div>'
        )

    # ── Full ticker markup ──────────────────────────────────────────────────
    ticker_html = (
        f'<div class="stk-outer">'
        f'  {brand_html}'
        f'  {badge_html}'
        f'  <div class="stk-divider-v"></div>'
        f'  <div class="stk-track">'
        f'    <div class="stk-scroll">{scroll_html}</div>'
        f'  </div>'
        f'</div>'
    )

    # ── CSS ─────────────────────────────────────────────────────────────────
    ticker_css = f"""
#stk-ticker {{
  position: fixed;
  top: 0; left: 0; right: 0;
  z-index: 999999;
  height: {TICKER_HEIGHT}px;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}
.stk-outer {{
  width: 100%; height: {TICKER_HEIGHT}px;
  background: linear-gradient(180deg, #0d1626 0%, #080c14 100%);
  position: relative;
  display: flex; align-items: center; overflow: hidden;
}}
.stk-outer::after {{
  content: '';
  position: absolute;
  bottom: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg,
    transparent 0%,
    #1e3a5f 15%,
    #2d5a9e 40%,
    #3b82f6 50%,
    #2d5a9e 60%,
    #1e3a5f 85%,
    transparent 100%);
  opacity: 0.6;
}}

/* ── Brand ── */
.stk-brand {{
  flex-shrink: 0;
  display: flex; align-items: center; gap: 9px;
  padding: 0 20px;
  height: 100%;
}}
.stk-logo-icon {{
  width: 24px; height: 24px; border-radius: 6px;
  background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%);
  display: flex; align-items: center; justify-content: center;
  flex-shrink: 0;
  box-shadow: 0 0 10px rgba(59,130,246,0.3);
}}
.stk-brand-name {{
  font-size: 13.5px; font-weight: 700; color: #cbd5e1;
  letter-spacing: -0.03em; white-space: nowrap;
}}
.stk-brand-name em {{
  color: #3b82f6; font-style: normal;
}}

/* ── Vertical divider ── */
.stk-divider-v {{
  width: 1px; height: 22px;
  background: linear-gradient(180deg, transparent, #1e2d45, transparent);
  flex-shrink: 0;
}}

/* ── Meta (badge + clock) ── */
.stk-meta {{
  flex-shrink: 0;
  display: flex; align-items: center; gap: 8px;
  padding: 0 16px;
  height: 100%;
}}
.badge {{
  font-size: 9.5px; font-weight: 700; letter-spacing: 1.2px;
  padding: 2px 7px; border-radius: 3px; white-space: nowrap;
}}
.live-badge {{
  color: #22c55e;
  background: rgba(34,197,94,.1);
  border: 1px solid rgba(34,197,94,.25);
}}
.off-badge {{
  color: #64748b;
  background: rgba(100,116,139,.1);
  border: 1px solid rgba(100,116,139,.2);
}}
.dot-live {{
  width: 6px; height: 6px; border-radius: 50%;
  background: #22c55e; flex-shrink: 0;
  animation: stk-pulse 2s ease-in-out infinite;
}}
.dot-off {{
  width: 6px; height: 6px; border-radius: 50%;
  background: #475569; flex-shrink: 0;
}}
@keyframes stk-pulse {{
  0%,100% {{ box-shadow: 0 0 0 0 rgba(34,197,94,.5); }}
  50%      {{ box-shadow: 0 0 0 4px rgba(34,197,94,0); }}
}}
.stk-clock {{
  font-size: 10.5px; font-weight: 500; color: #374151;
  font-variant-numeric: tabular-nums;
  letter-spacing: 0.02em;
}}

/* ── Scrolling track ── */
.stk-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
  mask-image:         linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
}}
.stk-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: stk-marquee 70s linear infinite;
  will-change: transform;
}}
.stk-scroll:hover {{ animation-play-state: paused; }}
@keyframes stk-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}

/* ── Ticker items ── */
.t-item {{
  display: inline-flex; align-items: center; gap: 7px;
  padding: 0 18px; height: 100%;
  cursor: default;
  transition: background 0.15s;
}}
.t-item:hover {{ background: rgba(255,255,255,.03); }}
.t-name {{
  font-size: 9.5px; font-weight: 700; color: #3d4f68;
  letter-spacing: 0.9px; text-transform: uppercase;
}}
.t-price {{
  font-size: 13px; font-weight: 600; color: #c8d3e0;
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum";
}}
.t-chg {{
  font-size: 10.5px; font-weight: 600;
  font-variant-numeric: tabular-nums;
  display: inline-flex; align-items: center; gap: 2px;
}}
.t-arrow {{ font-size: 8px; }}
.up      {{ color: #22c55e; }}
.dn      {{ color: #ef4444; }}
.neutral {{ color: #475569; }}
.t-sep {{
  width: 1px; height: 14px;
  background: #131d2e;
  flex-shrink: 0;
}}
.t-unavail {{ font-size: 12px; color: #64748b; padding: 0 24px; }}
"""

    # ── JS: inject fixed bar into parent document ───────────────────────────
    html_json = json.dumps(ticker_html)
    css_json  = json.dumps(ticker_css)

    script = f"""
<script>
(function() {{
  var HTML = {html_json};
  var CSS  = {css_json};
  var H    = {TICKER_HEIGHT};

  function collapseOwnIframe() {{
    var p = window.parent.document;
    var frames = p.querySelectorAll('iframe');
    for (var i = 0; i < frames.length; i++) {{
      try {{
        if (frames[i].contentWindow === window) {{
          var iframe = frames[i];
          var s = 'height:0!important;min-height:0!important;max-height:0!important;' +
                  'margin:0!important;padding:0!important;border:none!important;overflow:hidden!important;';
          iframe.style.cssText = s;
          var parent = iframe.parentElement;
          if (parent && parent !== p.body) parent.style.cssText += s;
          break;
        }}
      }} catch(e) {{}}
    }}
  }}

  function startClock(p) {{
    var el = p.getElementById('stk-clock');
    if (!el) return;
    function tick() {{
      var now = new Date();
      var ist = new Date(now.toLocaleString('en-US', {{timeZone: 'Asia/Kolkata'}}));
      var h = String(ist.getHours()).padStart(2, '0');
      var m = String(ist.getMinutes()).padStart(2, '0');
      if (el) el.textContent = h + ':' + m + ' IST';
    }}
    tick();
    setInterval(tick, 30000);
  }}

  function inject() {{
    var p = window.parent.document;
    if (!p) return;

    var old = p.getElementById('stk-ticker');
    if (old) old.remove();
    var oldCss = p.getElementById('stk-ticker-css');
    if (oldCss) oldCss.remove();

    var style = p.createElement('style');
    style.id = 'stk-ticker-css';
    style.textContent = CSS;
    p.head.appendChild(style);

    var wrap = p.createElement('div');
    wrap.id = 'stk-ticker';
    wrap.innerHTML = HTML;
    p.body.insertBefore(wrap, p.body.firstChild);

    var root = p.querySelector('[data-testid="stAppViewContainer"]')
            || p.querySelector('.stApp')
            || p.querySelector('#root');
    if (root) root.style.paddingTop = H + 'px';

    startClock(p);
    collapseOwnIframe();
  }}

  inject();
  setTimeout(inject, 200);
}})();
</script>
"""

    if _COMPONENTS_HTML_SAFE:
        components.html(script, height=0, scrolling=False)
