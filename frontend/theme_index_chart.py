"""
theme_index_chart.py — Themes tab: theme list (left) + theme index chart (right).

The index levels are pre-computed by backend/compute_theme_index.py into
theme_index_daily (equal-weight, base 100 on the theme's first trading day).
This module only reads them.

Pure helpers (unit-tested in frontend/tests/test_theme_index_chart.py):
    window_start, period_return, returns_table, chart_window, rangebreaks,
    build_index_figure
Loaders (cached):  load_theme_index, load_member_closes
Renderers:         render_theme_list, render_theme_chart
"""
import html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from market_mood_tab import CHART_CONFIG, _base_layout, _palette, finalize_chart

LIST_PERIODS = ["1W", "1M", "3M", "6M", "1Y"]
CHART_PERIODS = ["1M", "3M", "6M", "YTD", "1Y", "Max"]
PERIOD_DAYS = {"1W": 7, "1M": 30, "3M": 91, "6M": 182, "1Y": 365}


# ── Pure helpers ─────────────────────────────────────────────────────────────

def window_start(last_date, period: str):
    """Date whose close is the base for `period` ending on `last_date`.
    None for 'Max' (use the first value). YTD uses the last close of the prior year."""
    last = pd.Timestamp(last_date)
    if period == "Max":
        return None
    if period == "YTD":
        return pd.Timestamp(last.year - 1, 12, 31)
    return last - pd.Timedelta(days=PERIOD_DAYS[period])


def period_return(s: pd.Series, period: str):
    """% return of a price/index series (DatetimeIndex, ascending) over `period`.
    '1D' = last vs previous value. Other periods use the last value on or before
    the window start. None if there isn't enough history."""
    s = s.dropna()
    if len(s) < 2:
        return None
    last = float(s.iloc[-1])
    if period == "1D":
        base = float(s.iloc[-2])
    else:
        start = window_start(s.index[-1], period)
        if start is None:
            base = float(s.iloc[0])
        else:
            before = s[s.index <= start]
            if before.empty:
                return None
            base = float(before.iloc[-1])
    return None if base == 0 else (last / base - 1.0) * 100.0


def returns_table(long: pd.DataFrame, key: str, value: str, periods) -> pd.DataFrame:
    """One row per `key` with a column per period (e.g. '1D', '1W') of % returns."""
    rows = []
    for k, g in long.groupby(key, sort=False):
        s = pd.Series(g[value].astype(float).values, index=pd.to_datetime(g["date"])).sort_index()
        rows.append({key: k, **{p: period_return(s, p) for p in periods}})
    return pd.DataFrame(rows, columns=[key, *periods])


def chart_window(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Rows of one theme's series to plot for `period`: from the base point
    (last row on or before the window start) to the latest row."""
    df = df.sort_values("date")
    start = window_start(df["date"].iloc[-1], period)
    if start is None:
        return df
    before = df[df["date"] <= start]
    first = before["date"].iloc[-1] if not before.empty else df["date"].iloc[0]
    return df[df["date"] >= first]


def rangebreaks(dates) -> list:
    """Plotly rangebreaks hiding weekends and weekday market holidays."""
    d = pd.DatetimeIndex(pd.to_datetime(dates)).normalize()
    if len(d) == 0:
        return []
    weekdays = pd.bdate_range(d.min(), d.max())
    missing = weekdays.difference(d)
    out = [dict(bounds=["sat", "mon"])]
    if len(missing):
        out.append(dict(values=[x.strftime("%Y-%m-%d") for x in missing]))
    return out


def build_index_figure(win: pd.DataFrame, dark: bool, height: int = 440) -> go.Figure:
    """Area line of the theme index, coloured by the window's return, with
    turnover bars along the bottom (secondary axis)."""
    C = _palette(dark)
    win = win.sort_values("date")
    x = pd.to_datetime(win["date"])
    y = win["index_ew"].astype(float)
    up = y.iloc[-1] >= y.iloc[0]
    line = C["green"] if up else C["red"]
    area = C["grn_fill"] if up else C["red_fill"]
    bar = ("rgba(34,197,94,0.40)" if dark else "rgba(22,163,74,0.35)") if up else \
          ("rgba(239,68,68,0.40)" if dark else "rgba(220,38,38,0.35)")

    lo, hi = float(y.min()), float(y.max())
    span = (hi - lo) or max(abs(hi) * 0.02, 1.0)
    turnover = pd.to_numeric(win.get("turnover_cr"), errors="coerce") if "turnover_cr" in win else None

    fig = go.Figure()
    if turnover is not None and turnover.notna().any():
        fig.add_trace(go.Bar(
            x=x, y=turnover, yaxis="y2", marker_color=bar, marker_line_width=0,
            name="Turnover", hovertemplate="Turnover ₹%{y:,.0f} cr<extra></extra>",
        ))
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="lines", line=dict(color=line, width=2),
        fill="tozeroy", fillcolor=area, name="Index",
        hovertemplate="Index %{y:,.2f}<extra></extra>",
    ))
    vmax = float(turnover.max()) if turnover is not None and turnover.notna().any() else 1.0
    fig.update_layout(**_base_layout(
        C, height=height, showlegend=False, bargap=0.25,
        margin=dict(l=8, r=60, t=12, b=32),
        xaxis=dict(showgrid=True, gridcolor=C["grid"], tickfont=dict(size=11),
                   rangebreaks=rangebreaks(x), hoverformat="%d %b %Y"),
        yaxis=dict(side="right", gridcolor=C["grid"], tickformat=",.2f", zeroline=False,
                   range=[lo - span * 0.35, hi + span * 0.08], tickfont=dict(size=11)),
        yaxis2=dict(overlaying="y", range=[0, vmax * 4.5], visible=False, fixedrange=True),
    ))
    return finalize_chart(fig, C)


# ── Loaders ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_theme_index(_engine) -> pd.DataFrame:
    sql = text("""SELECT theme_slug, date, index_ew, turnover_cr, n_members
                  FROM theme_index_daily ORDER BY theme_slug, date""")
    with _engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    df["date"] = pd.to_datetime(df["date"])
    for c in ("index_ew", "turnover_cr"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_member_closes(_engine, theme_slug: str) -> pd.DataFrame:
    sql = text("""SELECT s.symbol, s.name, p.date, p.close
                  FROM theme_membership tm
                  JOIN stocks s ON s.symbol = tm.symbol
                  JOIN prices_daily p ON p.symbol = tm.symbol
                  WHERE tm.theme_slug = :slug AND p.date >= CURRENT_DATE - 400
                  ORDER BY s.symbol, p.date""")
    with _engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"slug": theme_slug})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


# ── Renderers ────────────────────────────────────────────────────────────────

def display_name(name: str) -> str:
    """'Cements Index' -> 'Cements' (the list and chart already say it's an index)."""
    name = str(name).strip()
    return name[:-6].rstrip() if name.lower().endswith(" index") else name


def _pct_html(v, C) -> str:
    if v is None or pd.isna(v):
        return f"<span style='color:{C['muted']}'>—</span>"
    col = C["green"] if v >= 0 else C["red"]
    return f"<span style='color:{col};font-variant-numeric:tabular-nums'>{v:+.2f}%</span>"


def _css(C, dark: bool) -> str:
    sel_bg = "#16213a" if dark else "#eef2ff"
    sel_txt = "#60a5fa" if dark else "#1d4ed8"
    return f"""<style>
.st-key-theme_list_box [data-testid="stVerticalBlock"] {{ gap: 0 !important; }}
.st-key-theme_list_box [data-testid="stHorizontalBlock"] {{ gap: 0.4rem !important; }}
.st-key-theme_list_box .stButton button, [class*="st-key-tl_hdr"] .stButton button {{
  background: transparent !important; border: none !important; box-shadow: none !important;
  min-height: 0 !important; padding: 9px 4px !important; justify-content: flex-start !important;
  color: {C['text']} !important; }}
.st-key-theme_list_box .stButton button div, [class*="st-key-tl_hdr"] .stButton button div {{
  justify-content: flex-start !important; }}
.st-key-theme_list_box .stButton button p {{ font-size: 14px; text-align: left; }}
[class*="st-key-tl_hdr"] .stButton button p {{ font-size: 12px; color: {C['muted']}; }}
[class*="st-key-tl_row_"] {{ border-bottom: 1px solid {C['row_bd']}; }}
[class*="st-key-tl_row_sel_"] {{ background: {sel_bg}; border-radius: 6px; }}
[class*="st-key-tl_row_sel_"] .stButton button p {{ color: {sel_txt} !important; font-weight: 600; }}
.st-key-theme_list_box [data-testid="stMarkdownContainer"],
.st-key-theme_list_box [data-testid="stMarkdownContainer"] div {{ margin: 0 !important; }}
.st-key-theme_list_box [data-testid="stElementContainer"] {{ margin: 0 !important; }}
.tl-num {{ text-align: right; font-size: 13.5px; padding: 0 2px; line-height: 1.4; }}
.tl-cons {{ width: 100%; border-collapse: collapse; margin: 2px 0 6px 0; border: none !important; }}
.tl-cons td {{ padding: 7px 4px 7px 14px; font-size: 13px; color: {C['text']};
  border: none !important; border-bottom: 1px solid {C['row_bd']} !important; }}
.tl-cons td.n {{ text-align: right; padding-right: 2px; font-variant-numeric: tabular-nums; }}
</style>"""


def _set_sort(col: str):
    cur_col, cur_asc = st.session_state.get("theme_list_sort", ("name", True))
    st.session_state["theme_list_sort"] = (col, not cur_asc) if cur_col == col else (col, col == "name")


def _select_theme(slug: str):
    st.session_state["selected_theme_slug"] = slug


def render_theme_list(engine, themes: pd.DataFrame, selected_slug: str, dark: bool) -> None:
    """themes: theme_slug, theme_name, actual_stock_count. Selection is stored in
    st.session_state['selected_theme_slug'] via on_click callbacks."""
    C = _palette(dark)
    st.markdown(_css(C, dark), unsafe_allow_html=True)

    period = st.segmented_control(
        "Returns period", LIST_PERIODS, default="1W", key="theme_list_period",
        label_visibility="collapsed",
    ) or "1W"

    idx = load_theme_index(engine)
    rets = returns_table(idx, "theme_slug", "index_ew", ["1D", period])
    df = themes.merge(rets, on="theme_slug", how="left")

    sort_col, asc = st.session_state.get("theme_list_sort", ("name", True))
    key = {"name": "theme_name", "1d": "1D", "period": period}[sort_col]
    df["theme_name"] = df["theme_name"].map(display_name)
    df = df.sort_values(key, ascending=asc, na_position="last",
                        key=(lambda s: s.str.lower()) if key == "theme_name" else None)

    arrow = lambda c: (" ↑" if asc else " ↓") if sort_col == c else " ⇅"
    with st.container(key="tl_hdr"):
        h1, h2, h3 = st.columns([5, 2, 2], vertical_alignment="center")
        h1.button(f"Name{arrow('name')}", key="tl_sort_name", on_click=_set_sort, args=("name",))
        h2.button(f"1D{arrow('1d')}", key="tl_sort_1d", on_click=_set_sort, args=("1d",))
        h3.button(f"{period}{arrow('period')}", key="tl_sort_period", on_click=_set_sort, args=("period",))

    with st.container(height=620, key="theme_list_box", border=False):
        for row in df.to_dict("records"):
            slug = row["theme_slug"]
            sel = slug == selected_slug
            with st.container(key=f"tl_row_{'sel_' if sel else ''}{slug}"):
                c1, c2, c3 = st.columns([5, 2, 2], vertical_alignment="center")
                c1.button(f"{display_name(row['theme_name'])}{'  ⌄' if sel else ''}", key=f"tl_btn_{slug}",
                          on_click=_select_theme, args=(slug,), use_container_width=True)
                c2.markdown(f"<div class='tl-num'>{_pct_html(row.get('1D'), C)}</div>",
                            unsafe_allow_html=True)
                c3.markdown(f"<div class='tl-num'>{_pct_html(row.get(period), C)}</div>",
                            unsafe_allow_html=True)
            if sel:
                _render_constituents(engine, slug, period, C)


def _render_constituents(engine, slug: str, period: str, C: dict) -> None:
    closes = load_member_closes(engine, slug)
    if closes.empty:
        st.caption("No price data for this theme's stocks yet.")
        return
    names = closes.groupby("symbol")["name"].first()
    r = returns_table(closes, "symbol", "close", ["1D", period]).sort_values("symbol")
    rows = "".join(
        f"<tr><td title='{html.escape(str(names.get(t.symbol, '')))}'>{html.escape(t.symbol)}</td>"
        f"<td class='n'>{_pct_html(t[1], C)}</td><td class='n'>{_pct_html(t[2], C)}</td></tr>"
        for t in r.itertuples(index=False)
    )
    st.markdown(f"<table class='tl-cons'><colgroup><col style='width:56%'><col style='width:22%'>"
                f"<col style='width:22%'></colgroup>{rows}</table>", unsafe_allow_html=True)


def render_theme_chart(engine, slug: str, theme_name: str, n_stocks: int, dark: bool) -> None:
    C = _palette(dark)
    idx = load_theme_index(engine)
    s = idx[idx["theme_slug"] == slug]
    st.markdown(
        f"<div style='font-size:20px;font-weight:600;color:{C['text']};margin:2px 0 0 0'>"
        f"{html.escape(display_name(theme_name))}</div>", unsafe_allow_html=True)
    if s.empty:
        st.info("The index for this theme will appear after the next daily refresh.")
        return

    last = s["date"].max()
    c1, c2 = st.columns([3, 2], vertical_alignment="center")
    with c1:
        period = st.segmented_control(
            "Chart period", CHART_PERIODS, default="6M", key="theme_chart_period",
            label_visibility="collapsed",
        ) or "6M"
    win = chart_window(s, period)
    ret = period_return(pd.Series(s["index_ew"].values, index=s["date"]), period)
    with c2:
        st.markdown(
            f"<div style='text-align:right;font-size:14px;color:{C['muted']}'>Returns "
            f"<b>{_pct_html(ret, C)}</b></div>", unsafe_allow_html=True)

    st.plotly_chart(build_index_figure(win, dark), width="stretch", config=CHART_CONFIG,
                    theme=None, key=f"tix_{slug}_{period}")
    n_priced = int(s["n_members"].iloc[-1]) if pd.notna(s["n_members"].iloc[-1]) else 0
    st.caption(
        f"Equal-weight index of {n_stocks} stocks, starting at 100 on "
        f"{s['date'].min():%d %b %Y} · bars = ₹ crore traded · "
        f"{n_priced} of {n_stocks} stocks priced on {last:%d %b %Y}. "
        "Past values use today's stocks.")
