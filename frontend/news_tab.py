"""
news_tab.py — News tab: one card per story, theme / sector filter, cards or headline list.

Data: news_articles + news_article_symbols (backend/news_fetcher.py, every 30 min in
market hours). Duplicate coverage of the same story by several outlets is merged in
the browser-facing layer (cluster_articles) so each story shows once with its sources.

Pure helpers (tested in frontend/tests/test_news_tab.py):
    title_words, cluster_articles, fmt_age, cards_html, list_html
"""
import html
import re
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

CACHE_TTL = 300
DAYS = 7
MAX_ARTICLES = 400

_STOP = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "at", "by", "with", "from", "as", "is",
    "are", "be", "its", "it", "this", "that", "after", "over", "up", "down", "amid", "into", "vs", "how",
    "what", "why", "who", "will", "may", "can", "new", "top", "check", "today", "live", "updates", "update",
    "here", "says", "said", "now", "out", "more", "than", "all", "about", "news", "latest", "set", "per",
}


# ── Pure helpers ─────────────────────────────────────────────────────────────

def title_words(title: str) -> set:
    """Content words of a headline (lower-case, no stop words, len >= 3 or a number)."""
    words = re.findall(r"[a-z0-9&]+", str(title).lower())
    return {w for w in words if w not in _STOP and (len(w) >= 3 or w.isdigit())}


def cluster_articles(df: pd.DataFrame, threshold: float = 0.5, min_shared: int = 3,
                     window_hours: float = 36) -> list[dict]:
    """Group articles that cover the same story (headline word overlap, Jaccard >=
    threshold with >= min_shared common words, published within window_hours).
    Input newest-first. Returns clusters newest-first:
    {"main": row dict, "others": [row dicts], "symbols": [..]} — main = newest."""
    clusters: list[dict] = []
    if df is None or df.empty:
        return clusters
    rows = df.sort_values("published_at", ascending=False).to_dict("records")
    for r in rows:
        w = title_words(r.get("title", ""))
        t = pd.Timestamp(r.get("published_at"))
        placed = False
        for c in clusters:
            if abs((c["_t"] - t).total_seconds()) > window_hours * 3600:
                continue
            if r.get("source_name") in c["_src"]:     # duplicates come from different outlets
                continue
            inter = len(w & c["_w"])
            union = len(w | c["_w"]) or 1
            if inter >= min_shared and inter / union >= threshold:
                c["others"].append(r)
                c["_syms"].update(_syms(r))
                c["_src"].add(r.get("source_name"))
                placed = True
                break
        if not placed:
            clusters.append({"main": r, "others": [], "_w": w, "_t": t, "_syms": set(_syms(r)),
                             "_src": {r.get("source_name")}})
    for c in clusters:
        c["symbols"] = sorted(c.pop("_syms"))
        c.pop("_w"); c.pop("_t"); c.pop("_src")
    return clusters


def _syms(r) -> list:
    v = r.get("symbols")
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return []
    return [s.strip() for s in str(v).split(",") if s.strip()]


def fmt_age(published_at, now=None) -> str:
    if published_at is None or (isinstance(published_at, float) and published_at != published_at):
        return ""
    ts = pd.Timestamp(published_at)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    now = pd.Timestamp(now) if now is not None else pd.Timestamp(datetime.now(timezone.utc))
    secs = (now - ts).total_seconds()
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    if secs < 172800:
        return "Yesterday"
    ist = ts.tz_convert("Asia/Kolkata")
    return f"{ist:%b} {ist.day}"


def _pills(symbols, T, n=5) -> str:
    return "".join(
        f"<span style='background:{T['bg_tag']};color:{T['text_accent']};border:1px solid {T['bd_tag']};"
        f"border-radius:4px;font-size:9.5px;font-weight:700;padding:1px 6px;margin:0 4px 2px 0;"
        f"display:inline-block;letter-spacing:.4px'>{html.escape(s)}</span>" for s in symbols[:n])


def _also(c, T) -> str:
    if not c["others"]:
        return ""
    seen, links = {c["main"].get("source_name")}, []
    for o in c["others"]:
        src = o.get("source_name")
        if src in seen:
            continue
        seen.add(src)
        links.append(f"<a href='{html.escape(str(o.get('url', '#')))}' target='_blank' rel='noopener noreferrer' "
                     f"style='color:{T['text_accent']};text-decoration:none'>{html.escape(str(src))}</a>")
    if not links:
        return ""
    return (f"<div style='font-size:10.5px;color:{T['text_muted']};margin-top:6px'>Also covered by "
            + " · ".join(links) + "</div>")


def cards_html(clusters: list[dict], T: dict, dark: bool) -> str:
    out = []
    bg = "#0f1729" if dark else "#ffffff"
    for c in clusters:
        r = c["main"]
        n_src = 1 + len({o.get("source_name") for o in c["others"]} - {r.get("source_name")})
        badge = (f"<span style='margin-left:8px;font-size:9.5px;font-weight:700;color:{T['text_accent']};"
                 f"background:{T['bg_tag']};border-radius:10px;padding:1px 7px'>{n_src} sources</span>"
                 if n_src > 1 else "")
        out.append(f"""
<div style='background:{bg};border:1px solid {T["bd_card"]};border-radius:10px;padding:13px 15px 11px;margin-bottom:10px'>
  <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:5px'>
    <span style='font-size:9.5px;font-weight:800;letter-spacing:1.1px;color:#f59e0b;text-transform:uppercase'>{html.escape(str(r.get("source_name", "")))}{badge}</span>
    <span style='font-size:10px;color:{T["text_muted"]}'>{fmt_age(r.get("published_at"))}</span>
  </div>
  <a href='{html.escape(str(r.get("url", "#")))}' target='_blank' rel='noopener noreferrer'
     style='font-size:13.5px;font-weight:600;color:{T["card_title"]};text-decoration:none;line-height:1.45'>{html.escape(str(r.get("title", "")))}</a>
  <div style='font-size:11.5px;color:{T["text_muted"]};line-height:1.5;margin:5px 0 7px;display:-webkit-box;
       -webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden'>{html.escape(str(r.get("summary") or "")[:300])}</div>
  <div>{_pills(c["symbols"], T)}</div>{_also(c, T)}
</div>""")
    return "".join(out)


def list_html(clusters: list[dict], T: dict) -> str:
    rows = []
    for c in clusters:
        r = c["main"]
        more = len({o.get("source_name") for o in c["others"]} - {r.get("source_name")})
        more_html = (f"<span style='font-size:10px;color:{T['text_accent']};margin-left:6px'>+{more} more</span>"
                     if more else "")
        rows.append(
            f"<tr><td style='white-space:nowrap;color:{T['text_muted']};font-size:11px;padding:7px 10px 7px 0'>"
            f"{fmt_age(r.get('published_at'))}</td>"
            f"<td style='white-space:nowrap;font-size:9.5px;font-weight:800;letter-spacing:.8px;color:#f59e0b;"
            f"text-transform:uppercase;padding:7px 12px 7px 0'>{html.escape(str(r.get('source_name', '')))}</td>"
            f"<td style='padding:7px 0;font-size:13px;line-height:1.4'><a href='{html.escape(str(r.get('url', '#')))}' "
            f"target='_blank' rel='noopener noreferrer' style='color:{T['card_title']};text-decoration:none;font-weight:500'>"
            f"{html.escape(str(r.get('title', '')))}</a>{more_html}</td>"
            f"<td style='padding:7px 0 7px 12px;text-align:right'>{_pills(c['symbols'], T, 3)}</td></tr>")
    cell = f"<td style='border:none;border-bottom:1px solid {T['bd_card']};"
    return ("<table style='width:100%;border-collapse:collapse;border:none'>"
            + "".join(rows).replace("<td style='", cell) + "</table>")


# ── Loaders ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_news(_engine, query: str = "", sources: tuple = (), symbols: tuple = ()) -> pd.DataFrame:
    cond = [f"a.published_at >= NOW() - INTERVAL '{DAYS} days'"]
    params: dict = {}
    if query.strip():
        cond.append("a.ts_vector @@ plainto_tsquery('english', :q)")
        params["q"] = query.strip()
    if sources:
        cond.append("ns.display_name = ANY(:srcs)")
        params["srcs"] = list(sources)
    if symbols:
        cond.append("EXISTS (SELECT 1 FROM news_article_symbols x WHERE x.article_id = a.article_id "
                    "AND x.symbol = ANY(:syms))")
        params["syms"] = list(symbols)
    sql = text(f"""
        SELECT a.article_id, a.title, a.url, ns.display_name AS source_name, a.published_at, a.summary,
               STRING_AGG(nas.symbol, ',' ORDER BY nas.symbol) AS symbols
        FROM news_articles a
        JOIN news_sources ns ON ns.source_id = a.source_id
        LEFT JOIN news_article_symbols nas ON nas.article_id = a.article_id
        WHERE {' AND '.join(cond)}
        GROUP BY a.article_id, a.title, a.url, ns.display_name, a.published_at, a.summary
        ORDER BY a.published_at DESC
        LIMIT {MAX_ARTICLES}
    """)
    try:
        with _engine.connect() as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception:  # noqa: BLE001 — the tab must render even if news is unavailable
        return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def load_filters(_engine, sector_tabs: tuple):
    """(sources list, groups {label: symbols tuple}, companies DataFrame) in one connection."""
    with _engine.connect() as conn:
        sources = pd.read_sql(text("SELECT DISTINCT display_name FROM news_sources WHERE is_active "
                                   "ORDER BY display_name"), conn)["display_name"].tolist()
        th = pd.read_sql(text("SELECT t.theme_name, t.theme_order, tm.symbol FROM theme_membership tm "
                              "JOIN themes t USING (theme_slug)"), conn)
        keys = [k for k, _ in sector_tabs]
        sec = pd.read_sql(text("SELECT index_name, symbol FROM index_membership WHERE index_name = ANY(:k)"),
                          conn, params={"k": keys}) if keys else pd.DataFrame(columns=["index_name", "symbol"])
        comp = pd.read_sql(text("SELECT symbol, name FROM stocks WHERE is_active AND symbol NOT LIKE '^%' "
                                "ORDER BY symbol"), conn)
    groups: dict[str, tuple] = {}
    label_of = dict(sector_tabs)
    for k in keys:
        syms = tuple(sorted(sec.loc[sec["index_name"] == k, "symbol"]))
        if syms:
            groups[f"Sector · {label_of[k]}"] = syms
    for name, g in th.sort_values("theme_order").groupby("theme_name", sort=False):
        groups[f"Theme · {str(name).removesuffix(' Index')}"] = tuple(sorted(g["symbol"]))
    return sources, groups, comp


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_source_stats(_engine) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text("""
                SELECT ns.display_name AS source_name, COUNT(*) AS article_count
                FROM news_articles a JOIN news_sources ns ON ns.source_id = a.source_id
                WHERE a.published_at >= NOW() - INTERVAL '24 hours'
                GROUP BY ns.display_name ORDER BY article_count DESC"""), conn)
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


# ── Renderer ─────────────────────────────────────────────────────────────────

def _render_stories(df: pd.DataFrame, view: str, T: dict, dark: bool, empty_msg: str) -> None:
    clusters = cluster_articles(df)
    if not clusters:
        st.info(empty_msg)
        return
    oldest = fmt_age(df["published_at"].min())
    span = f"since {oldest}" if oldest else f"last {DAYS} days"
    st.caption(f"{len(clusters)} stories from {len(df)} articles ({span}) · "
               "news updates every 30 min during market hours")
    if view == "Headlines":
        st.markdown(list_html(clusters, T), unsafe_allow_html=True)
        return
    left, right = st.columns(2)
    left.markdown(cards_html(clusters[0::2], T, dark), unsafe_allow_html=True)
    right.markdown(cards_html(clusters[1::2], T, dark), unsafe_allow_html=True)


def render_news_tab(engine, T: dict, dark: bool, sector_tabs) -> None:
    sources, groups, comp = load_filters(engine, tuple(sector_tabs))
    label = {r.symbol: f"{r.symbol} — {r.name}" for r in comp.itertuples(index=False)}
    sym_opts = [""] + comp["symbol"].tolist()
    sub_all, sub_company, sub_source = st.tabs(["All News", "By Company", "By Source"])

    with sub_all:
        c1, c2, c3, c4, c5 = st.columns([2.6, 1.8, 2.2, 2.0, 1.4], vertical_alignment="bottom")
        query = c1.text_input("Search", placeholder="RBI, results, order win, SEBI…",
                              label_visibility="collapsed", key="news_search")
        sel_sources = c2.multiselect("Sources", sources, placeholder="All sources", key="news_sources",
                                     label_visibility="collapsed")
        group = c3.selectbox("Theme / Sector", [""] + list(groups), key="news_group",
                             format_func=lambda x: x or "All themes & sectors", label_visibility="collapsed")
        sym = c4.selectbox("Company", sym_opts, key="news_sym", label_visibility="collapsed",
                           format_func=lambda x: label.get(x, x) if x else "All companies")
        with c5:
            view = st.segmented_control("View", ["Cards", "Headlines"], default="Cards", key="news_view",
                                        label_visibility="collapsed") or "Cards"
        symbols = (sym,) if sym else (groups.get(group, ()) if group else ())
        df = load_news(engine, query, tuple(sel_sources), tuple(symbols))
        what = sym or (group.split(" · ", 1)[-1] if group else "")
        _render_stories(df, view, T, dark,
                        f"No recent news for {what}." if what else "No articles found — check back soon.")

    with sub_company:
        cc1, cc2 = st.columns([4, 1.4], vertical_alignment="bottom")
        sym2 = cc1.selectbox("Select company", sym_opts, key="news_company_sym",
                             format_func=lambda x: label.get(x, x) if x else "Choose a company…")
        with cc2:
            view2 = st.segmented_control("View ", ["Cards", "Headlines"], default="Cards", key="news_view2",
                                         label_visibility="collapsed") or "Cards"
        if sym2:
            _render_stories(load_news(engine, "", (), (sym2,)), view2, T, dark,
                            f"No recent articles mentioning {sym2}.")
        else:
            st.info("Select a company to see its news.")

    with sub_source:
        stats = load_source_stats(engine)
        if not stats.empty:
            fig = px.bar(stats, x="article_count", y="source_name", orientation="h", text="article_count",
                         color_discrete_sequence=["#f59e0b"],
                         labels={"article_count": "Articles (last 24h)", "source_name": ""})
            fig.update_layout(height=60 + 30 * len(stats), margin=dict(l=0, r=20, t=10, b=10),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font=dict(color=T["text_muted"], size=11),
                              xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                              yaxis=dict(showgrid=False))
            fig.update_traces(textposition="outside", textfont_size=11)
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
        sc1, sc2 = st.columns([4, 1.4], vertical_alignment="bottom")
        src = sc1.selectbox("Filter by source", ["All"] + sources, key="news_src_only")
        with sc2:
            view3 = st.segmented_control("View  ", ["Cards", "Headlines"], default="Headlines",
                                         key="news_view3", label_visibility="collapsed") or "Headlines"
        _render_stories(load_news(engine, "", () if src == "All" else (src,), ()), view3, T, dark,
                        "No articles from this source yet.")
