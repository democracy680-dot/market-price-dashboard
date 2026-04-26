"""
news_ticker.py — Scrolling news headline ticker bar.

Renders a fixed bar below the stock price ticker using st.markdown
(no iframe injection). Falls back silently if news_articles is empty.
"""

import streamlit as st
from sqlalchemy import text

NEWS_TICKER_HEIGHT  = 28   # px
STOCK_TICKER_HEIGHT = 52   # must match ticker_bar.TICKER_HEIGHT
TOTAL_HEADER_HEIGHT = STOCK_TICKER_HEIGHT + NEWS_TICKER_HEIGHT  # 80px


@st.cache_data(ttl=600, show_spinner=False)
def _load_headlines(_engine, limit: int = 40) -> list[dict]:
    try:
        with _engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT a.title, a.url, ns.display_name AS source_name
                    FROM news_articles a
                    JOIN news_sources ns ON ns.source_id = a.source_id
                    WHERE a.published_at >= NOW() - INTERVAL '48 hours'
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
        return

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

    st.markdown(f"""
<style>
#news-ticker {{
  position: fixed;
  top: {STOCK_TICKER_HEIGHT}px;
  left: 0; right: 0;
  z-index: 999998;
  height: {NEWS_TICKER_HEIGHT}px;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
  pointer-events: auto;
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
    transparent 0%, rgba(245,158,11,0.2) 15%,
    rgba(245,158,11,0.5) 50%, rgba(245,158,11,0.2) 85%, transparent 100%);
}}
.nt-label {{
  flex-shrink: 0;
  font-size: 8px; font-weight: 800; letter-spacing: 1.4px;
  color: #f59e0b; padding: 0 12px; white-space: nowrap;
}}
.nt-divider-v {{
  width: 1px; height: 14px;
  background: rgba(245,158,11,0.25); flex-shrink: 0;
}}
.nt-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right, transparent 0%, #000 2%, #000 98%, transparent 100%);
  mask-image: linear-gradient(to right, transparent 0%, #000 2%, #000 98%, transparent 100%);
}}
.nt-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: nt-marquee 120s linear infinite;
  will-change: transform;
}}
.nt-scroll:hover {{ animation-play-state: paused; }}
@keyframes nt-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}
.nt-item {{
  display: inline-flex; align-items: center; gap: 6px;
  padding: 0 10px; height: 100%;
}}
.nt-src {{
  font-size: 7.5px; font-weight: 700; letter-spacing: 0.8px;
  color: #f59e0b; text-transform: uppercase; white-space: nowrap; flex-shrink: 0;
}}
.nt-link {{
  font-size: 10.5px; font-weight: 500; color: #94a3b8;
  text-decoration: none; white-space: nowrap; transition: color 0.15s;
}}
.nt-link:hover {{ color: #e2e8f0; text-decoration: underline; }}
.nt-sep {{
  font-size: 9px; color: rgba(245,158,11,0.3); padding: 0 4px; flex-shrink: 0;
}}

/* ── Push Streamlit content below both bars ── */
[data-testid="stAppViewContainer"] {{
  padding-top: {TOTAL_HEADER_HEIGHT}px !important;
}}
header[data-testid="stHeader"] {{
  top: {TOTAL_HEADER_HEIGHT}px !important;
}}
</style>

<div id="news-ticker">
  <div class="nt-outer">
    <div class="nt-label">NEWS</div>
    <div class="nt-divider-v"></div>
    <div class="nt-track">
      <div class="nt-scroll">{scroll_html}</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)
