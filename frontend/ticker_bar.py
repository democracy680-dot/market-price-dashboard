import json
import yfinance as yf
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime
import pytz

TICKER_HEIGHT = 48  # px — keep in sync with CSS below

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
                    change_pct = ((last_price - prev_close) / prev_close) * 100
                else:
                    change_pct = None
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
    if price > 10000:
        return f"{price:,.0f}"
    if price > 100:
        return f"{price:,.2f}"
    return f"{price:.4f}"


def render_ticker_bar():
    data       = fetch_ticker_data()
    market_open = is_market_open()
    all_failed  = all(d["price"] is None for d in data)

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
            else:
                p_str = _fmt_price(price)
                if change_pct is None:
                    chg_cls, chg_str = "neutral", "—"
                elif change_pct >= 0:
                    chg_cls = "up"
                    chg_str = f"▲ {abs(change_pct):.2f}%"
                else:
                    chg_cls = "dn"
                    chg_str = f"▼ {abs(change_pct):.2f}%"

            items_html += (
                f'<span class="t-item">'
                f'<span class="t-name">{name}</span>'
                f'<span class="t-price">{p_str}</span>'
                f'<span class="t-chg {chg_cls}">{chg_str}</span>'
                f'</span>'
                f'<span class="t-dot">●</span>'
            )

    scroll_html = items_html * 2  # duplicate for seamless loop

    # ── Badge ───────────────────────────────────────────────────────────────
    if market_open:
        badge_html  = '<span class="dot-live"></span><span class="badge live-badge">LIVE</span>'
    else:
        badge_html  = '<span class="dot-off"></span><span class="badge off-badge">CLOSED</span>'

    # ── Full ticker markup (will be injected into parent DOM) ───────────────
    ticker_html = (
        f'<div class="stk-outer">'
        f'  <div class="stk-badge">{badge_html}</div>'
        f'  <div class="stk-track">'
        f'    <div class="stk-scroll">{scroll_html}</div>'
        f'  </div>'
        f'</div>'
    )

    # ── CSS injected into parent <head> ─────────────────────────────────────
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
  background: #080c14;
  border-bottom: 1px solid #1a2236;
  display: flex; align-items: center; overflow: hidden;
}}
.stk-badge {{
  flex-shrink: 0; display: flex; align-items: center; gap: 7px;
  padding: 0 16px; height: 100%;
  background: #0b0f1a; border-right: 1px solid #1a2236;
  min-width: 96px;
}}
.badge {{
  font-size: 10px; font-weight: 700; letter-spacing: 1.4px;
  padding: 2px 7px; border-radius: 3px;
}}
.live-badge {{ color: #22c55e; background: rgba(34,197,94,.12); border: 1px solid rgba(34,197,94,.3); }}
.off-badge  {{ color: #64748b; background: rgba(100,116,139,.12); border: 1px solid rgba(100,116,139,.3); }}
.dot-live {{
  width: 7px; height: 7px; border-radius: 50%; background: #22c55e; flex-shrink: 0;
  animation: stk-pulse 1.8s ease-in-out infinite;
}}
.dot-off  {{ width: 7px; height: 7px; border-radius: 50%; background: #475569; flex-shrink: 0; }}
@keyframes stk-pulse {{
  0%,100% {{ box-shadow: 0 0 0 0 rgba(34,197,94,.5); }}
  50%      {{ box-shadow: 0 0 0 5px rgba(34,197,94,0); }}
}}
.stk-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right,transparent 0%,#000 2%,#000 98%,transparent 100%);
  mask-image:         linear-gradient(to right,transparent 0%,#000 2%,#000 98%,transparent 100%);
}}
.stk-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: stk-marquee 60s linear infinite; will-change: transform;
}}
.stk-scroll:hover {{ animation-play-state: paused; }}
@keyframes stk-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}
.t-item  {{ display: inline-flex; align-items: center; gap: 6px; padding: 0 16px; height: 100%; cursor: default; }}
.t-item:hover {{ background: rgba(255,255,255,.04); }}
.t-name  {{ font-size: 10px; font-weight: 700; color: #64748b; letter-spacing: .8px; }}
.t-price {{ font-size: 13px; font-weight: 700; color: #e2e8f0; }}
.t-chg   {{ font-size: 11px; font-weight: 600; }}
.up      {{ color: #22c55e; }}
.dn      {{ color: #ef4444; }}
.neutral {{ color: #475569; }}
.t-dot   {{ color: #1e2d45; font-size: 5px; flex-shrink: 0; }}
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
    // Find this script's iframe in the parent and zero its height + all wrappers
    var p = window.parent.document;
    var frames = p.querySelectorAll('iframe');
    for (var i = 0; i < frames.length; i++) {{
      try {{
        if (frames[i].contentWindow === window) {{
          var el = frames[i];
          var s = 'height:0!important;min-height:0!important;max-height:0!important;' +
                  'margin:0!important;padding:0!important;border:none!important;display:block!important;overflow:hidden!important;';
          el.style.cssText = s;
          // Walk up 4 wrapper divs Streamlit adds and collapse them too
          for (var j = 0; j < 4; j++) {{
            el = el.parentElement;
            if (!el || el === p.body) break;
            el.style.cssText += s;
          }}
          break;
        }}
      }} catch(e) {{}}
    }}
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

    // Push root down by exactly the ticker height
    var root = p.querySelector('[data-testid="stAppViewContainer"]')
            || p.querySelector('.stApp')
            || p.querySelector('#root');
    if (root) root.style.paddingTop = H + 'px';

    collapseOwnIframe();
  }}

  inject();
  setTimeout(inject, 200);
}})();
</script>
"""

    components.html(script, height=0, scrolling=False)
