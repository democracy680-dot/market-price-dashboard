"""
news_ticker.py — Scrolling news headline ticker bar (below stock ticker).
Uses st.html() for direct HTML injection without markdown processing.
"""

import streamlit as st
from sqlalchemy import text

NEWS_TICKER_HEIGHT  = 28
STOCK_TICKER_HEIGHT = 52
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
        title = h["title"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        url   = h["url"].replace('"', "%22")
        items_html += (
            f'<span style="display:inline-flex;align-items:center;gap:6px;padding:0 10px;height:100%">'
            f'<span style="font-size:7.5px;font-weight:700;letter-spacing:0.8px;color:#f59e0b;text-transform:uppercase;white-space:nowrap;flex-shrink:0">{src}</span>'
            f'<a href="{url}" target="_blank" rel="noopener noreferrer" style="font-size:10.5px;font-weight:500;color:#94a3b8;text-decoration:none;white-space:nowrap">{title}</a>'
            f'</span>'
            f'<span style="font-size:9px;color:rgba(245,158,11,0.35);padding:0 4px;flex-shrink:0">&#183;</span>'
        )

    scroll_html = items_html * 2

    html = f"""
<style>
#news-ticker {{
  position: fixed;
  top: {STOCK_TICKER_HEIGHT}px;
  left: 0; right: 0;
  z-index: 999998;
  height: {NEWS_TICKER_HEIGHT}px;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}}
#nt-outer {{
  width: 100%; height: {NEWS_TICKER_HEIGHT}px;
  background: #0a1628;
  display: flex; align-items: center; overflow: hidden; position: relative;
}}
#nt-outer::after {{
  content: '';
  position: absolute; bottom: 0; left: 0; right: 0; height: 1px;
  background: linear-gradient(90deg, transparent 0%, rgba(245,158,11,0.5) 50%, transparent 100%);
}}
#nt-track {{
  flex: 1; overflow: hidden; height: 100%; position: relative;
  -webkit-mask-image: linear-gradient(to right, transparent 0%, #000 2%, #000 98%, transparent 100%);
  mask-image: linear-gradient(to right, transparent 0%, #000 2%, #000 98%, transparent 100%);
}}
#nt-scroll {{
  display: inline-flex; align-items: center; height: 100%; white-space: nowrap;
  animation: nt-marquee 180s linear infinite;
  will-change: transform;
}}
#nt-scroll:hover {{ animation-play-state: paused; }}
@keyframes nt-marquee {{
  0%   {{ transform: translateX(0); }}
  100% {{ transform: translateX(-50%); }}
}}
#nt-scroll a:hover {{ color: #e2e8f0 !important; text-decoration: underline !important; }}
[data-testid="stAppViewContainer"] {{ padding-top: {TOTAL_HEADER_HEIGHT}px !important; }}
header[data-testid="stHeader"]     {{ top: {TOTAL_HEADER_HEIGHT}px !important; }}
</style>

<div id="news-ticker">
  <div id="nt-outer">
    <div style="flex-shrink:0;font-size:8px;font-weight:800;letter-spacing:1.4px;color:#f59e0b;padding:0 12px;white-space:nowrap">NEWS</div>
    <div style="width:1px;height:14px;background:rgba(245,158,11,0.25);flex-shrink:0"></div>
    <div id="nt-track">
      <div id="nt-scroll">{scroll_html}</div>
    </div>
  </div>
</div>
"""
    st.html(html)
