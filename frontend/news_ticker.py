"""
news_ticker.py — Scrolling news headline ticker bar.

Renders a second fixed bar below the stock price ticker (ticker_bar.py).
Reads the 30 most recent article titles from Supabase and scrolls them
as clickable links. Falls back silently if the news_articles table doesn't
exist yet or the DB is unreachable.

Integrated into app.py:
    from news_ticker import render_news_ticker
    render_news_ticker(engine)
"""

import json
import sys

import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import text

_COMPONENTS_HTML_SAFE = sys.version_info < (3, 14)

NEWS_TICKER_HEIGHT  = 32   # px — slim bar below stock ticker
STOCK_TICKER_HEIGHT = 52   # must match ticker_bar.TICKER_HEIGHT
TOTAL_HEADER_HEIGHT = STOCK_TICKER_HEIGHT + NEWS_TICKER_HEIGHT  # 84px


@st.cache_data(ttl=600, show_spinner=False)
def _load_headlines(_engine, limit: int = 30) -> list[dict]:
    try:
        with _engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT a.title, a.url, ns.display_name AS source_name
                    FROM news_articles a
                    JOIN news_sources ns ON ns.source_id = a.source_id
                    WHERE a.published_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY a.published_at DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            ).fetchall()
        return [{"title": r[0], "url": r[1], "source_name": r[2]} for r in rows]
    except Exception:
        return []


def render_news_ticker(engine):
    headlines = _load_headlines(engine)
    if not headlines:
        # No news yet — still inject the padding update so layout doesn't break
        # when news arrives later (do nothing, stock ticker padding stays at 52px)
        return

    # ── Build scrolling items HTML ──────────────────────────────────────────
    items_html = ""
    for h in headlines:
        src   = h["source_name"]
        title = h["title"].replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")
        url   = h["url"].replace('"', "%22")
        items_html += (
            f'<span class="nt-item">'
            f'  <span class="nt-src">{src}</span>'
            f'  <a class="nt-link" href="{url}" target="_blank" rel="noopener noreferrer">{title}</a>'
            f'</span>'
            f'<span class="nt-sep">·</span>'
        )

    scroll_html = items_html * 2  # duplicate for seamless loop

    ticker_html = (
        f'<div class="nt-outer">'
        f'  <div class="nt-label">NEWS</div>'
        f'  <div class="nt-divider-v"></div>'
        f'  <div class="nt-track">'
        f'    <div class="nt-scroll">{scroll_html}</div>'
        f'  </div>'
        f'</div>'
    )

    # ── CSS ─────────────────────────────────────────────────────────────────
    ticker_css = f"""
#news-ticker {{
  position: fixed;
  top: {STOCK_TICKER_HEIGHT}px;
  left: 0; right: 0;
  z-index: 999998;
  height: {NEWS_TICKER_HEIGHT}px;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}
.nt-outer {{
  width: 100%; height: {NEWS_TICKER_HEIGHT}px;
  background: #0a1628;
  position: relative;
  display: flex; align-items: center; overflow: hidden;
}}
.nt-outer::after {{
  content: '';
  position: absolute;
  bottom: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg,
    transparent 0%,
    rgba(245,158,11,0.2) 15%,
    rgba(245,158,11,0.5) 50%,
    rgba(245,158,11,0.2) 85%,
    transparent 100%);
}}
.nt-label {{
  flex-shrink: 0;
  font-size: 9px; font-weight: 800; letter-spacing: 1.4px;
  color: #f59e0b;
  padding: 0 14px;
  white-space: nowrap;
}}
.nt-divider-v {{
  width: 1px; height: 16px;
  background: rgba(245,158,11,0.25);
  flex-shrink: 0;
}}
.nt-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
  mask-image:         linear-gradient(to right, transparent 0%, #000 3%, #000 97%, transparent 100%);
}}
.nt-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: nt-marquee 90s linear infinite;
  will-change: transform;
}}
.nt-scroll:hover {{ animation-play-state: paused; }}
@keyframes nt-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}
.nt-item {{
  display: inline-flex; align-items: center; gap: 7px;
  padding: 0 12px; height: 100%;
}}
.nt-src {{
  font-size: 8.5px; font-weight: 700; letter-spacing: 0.8px;
  color: #f59e0b; text-transform: uppercase; white-space: nowrap;
  flex-shrink: 0;
}}
.nt-link {{
  font-size: 11px; font-weight: 500; color: #94a3b8;
  text-decoration: none; white-space: nowrap;
  transition: color 0.15s;
}}
.nt-link:hover {{ color: #e2e8f0; text-decoration: underline; }}
.nt-sep {{
  font-size: 10px; color: rgba(245,158,11,0.3);
  padding: 0 4px; flex-shrink: 0;
}}
"""

    # ── JS: inject below stock ticker into parent document ───────────────────
    html_json = json.dumps(ticker_html)
    css_json  = json.dumps(ticker_css)

    script = f"""
<script>
(function() {{
  var HTML  = {html_json};
  var CSS   = {css_json};
  var TOTAL = {TOTAL_HEADER_HEIGHT};

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

  function inject() {{
    var p = window.parent.document;
    if (!p) return;

    var old = p.getElementById('news-ticker');
    if (old) old.remove();
    var oldCss = p.getElementById('news-ticker-css');
    if (oldCss) oldCss.remove();

    var style = p.createElement('style');
    style.id = 'news-ticker-css';
    style.textContent = CSS;
    p.head.appendChild(style);

    var wrap = p.createElement('div');
    wrap.id = 'news-ticker';
    wrap.innerHTML = HTML;

    var stkTicker = p.getElementById('stk-ticker');
    if (stkTicker) {{
      stkTicker.insertAdjacentElement('afterend', wrap);
    }} else {{
      p.body.insertBefore(wrap, p.body.firstChild);
    }}

    // Update total padding — overrides the 52px set by ticker_bar.py
    var root = p.querySelector('[data-testid="stAppViewContainer"]')
            || p.querySelector('.stApp')
            || p.querySelector('#root');
    if (root) root.style.paddingTop = TOTAL + 'px';

    collapseOwnIframe();
  }}

  inject();
  setTimeout(inject, 300);
}})();
</script>
"""

    if _COMPONENTS_HTML_SAFE:
        components.html(script, height=0, scrolling=False)
