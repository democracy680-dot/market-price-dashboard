"""
index_valuation_tab.py — Sectors ▸ Valuation: historical P/E, P/B and dividend
yield of NSE indices with average and ±1σ/±2σ bands.

Data: index_valuation_daily (backend/fetch_index_valuation.py, from niftyindices.com).
One cached query loads all indices (daily for the last ~13 months, weekly before),
so switching index / metric / period never touches the database.

Pure helpers (tested in frontend/tests/test_index_valuation_tab.py):
    window, weekly, band_stats, summary_table, build_valuation_figure
"""
import html

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from market_mood_tab import CHART_CONFIG, _base_layout, _palette, finalize_chart

METRICS = {"P/E": "pe", "P/B": "pb", "Div. Yield": "div_yield"}
PERIODS = {"1Y": 1, "3Y": 3, "5Y": 5, "10Y": 10, "Max": None}
GROUPS = ["All", "Broad", "Sectoral", "Thematic"]
CACHE_TTL = 1800


# ── Pure helpers ─────────────────────────────────────────────────────────────

def window(df: pd.DataFrame, years) -> pd.DataFrame:
    """Rows within the last `years` years of df's latest date (None = all)."""
    if df.empty or years is None:
        return df
    last = pd.Timestamp(df["date"].max())
    return df[pd.to_datetime(df["date"]) >= last - pd.DateOffset(years=years)]


def weekly(s: pd.Series) -> pd.Series:
    """Last value per calendar week — evens out daily (recent) vs weekly (older) rows."""
    s = s.dropna()
    if s.empty:
        return s
    s.index = pd.to_datetime(s.index)
    return s.resample("W").last().dropna()


def band_stats(s: pd.Series) -> dict | None:
    """Mean / std / min / max / percentile of the latest value, on weekly points."""
    w = weekly(s)
    if len(w) < 4:
        return None
    cur = float(s.dropna().iloc[-1])
    mean, std = float(w.mean()), float(w.std(ddof=0))
    return {"current": cur, "mean": mean, "std": std, "min": float(w.min()), "max": float(w.max()),
            "z": (cur - mean) / std if std > 0 else 0.0,
            "dev_pct": (cur / mean - 1.0) * 100.0 if mean else None,
            "pctile": float((w <= cur).mean() * 100.0), "n_weeks": int(len(w))}


def zone(z: float, metric: str) -> str:
    """Cheap / Fair / Expensive vs own history. For dividend yield a HIGH value is cheap."""
    zz = -z if metric == "div_yield" else z
    if zz >= 1:
        return "Expensive"
    if zz <= -1:
        return "Cheap"
    return "Fair"


def summary_table(df: pd.DataFrame, metric: str, years) -> pd.DataFrame:
    """One row per index for `metric` over the last `years` years."""
    rows = []
    for (api, name, grp, order), g in df.groupby(["api_name", "display_name", "grp", "sort_order"], sort=False):
        g = window(g.sort_values("date"), years)
        st_ = band_stats(pd.Series(g[metric].values, index=g["date"]))
        if not st_:
            continue
        rows.append({"api_name": api, "Index": name, "Group": grp, "order": order,
                     "Current": st_["current"], "Average": st_["mean"], "Min": st_["min"], "Max": st_["max"],
                     "vs Avg %": st_["dev_pct"], "Percentile": st_["pctile"], "Z": st_["z"],
                     "Zone": zone(st_["z"], metric),
                     "As of": pd.Timestamp(g["date"].max()).date()})
    out = pd.DataFrame(rows)
    return out.sort_values("order").drop(columns="order").reset_index(drop=True) if not out.empty else out


def build_valuation_figure(g: pd.DataFrame, metric: str, label: str, stats: dict, dark: bool,
                           height: int = 460) -> go.Figure:
    C = _palette(dark)
    x = pd.to_datetime(g["date"])
    y = g[metric].astype(float)
    m, sd = stats["mean"], stats["std"]
    fig = go.Figure()
    # ±1σ band
    fig.add_trace(go.Scatter(x=[x.min(), x.max()], y=[m + sd, m + sd], mode="lines", line=dict(width=0),
                             hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=[x.min(), x.max()], y=[m - sd, m - sd], mode="lines", line=dict(width=0),
                             fill="tonexty", fillcolor=C["amb_fill"], hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=x, y=y, mode="lines", line=dict(color=C["line"], width=2), name=label,
                              hovertemplate=f"{label} %{{y:.2f}}<extra></extra>"))
    lines = [(m, "Avg", C["nifty"], "solid"), (m + sd, "+1σ", C["amber"], "dash"), (m - sd, "−1σ", C["amber"], "dash"),
             (m + 2 * sd, "+2σ", C["red"], "dot"), (m - 2 * sd, "−2σ", C["green"], "dot")]
    if metric == "div_yield":   # high yield = cheap: swap the ±2σ colours
        lines[3], lines[4] = (m + 2 * sd, "+2σ", C["green"], "dot"), (m - 2 * sd, "−2σ", C["red"], "dot")
    for val, name, col, dash in lines:
        if val <= 0:
            continue
        fig.add_hline(y=val, line=dict(color=col, width=1.2, dash=dash),
                      annotation_text=f"{name} {val:.1f}", annotation_position="right",
                      annotation_font=dict(size=10, color=col))
    lo = min(float(y.min()), m - 2 * sd)
    hi = max(float(y.max()), m + 2 * sd)
    pad = (hi - lo) * 0.05
    fig.update_layout(**_base_layout(
        C, height=height, showlegend=False, margin=dict(l=52, r=92, t=12, b=32),
        xaxis=dict(showgrid=True, gridcolor=C["grid"], hoverformat="%d %b %Y"),
        yaxis=dict(side="left", gridcolor=C["grid"], zeroline=False, range=[max(lo - pad, 0), hi + pad]),
    ))
    return finalize_chart(fig, C)


# ── Loader ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def load_valuation(_engine) -> pd.DataFrame:
    sql = text("""
        WITH v AS (
            SELECT api_name, date, pe, pb, div_yield FROM index_valuation_daily
            WHERE date >= CURRENT_DATE - 400
            UNION ALL
            SELECT * FROM (
                SELECT DISTINCT ON (api_name, date_trunc('week', date))
                       api_name, date, pe, pb, div_yield
                FROM index_valuation_daily
                WHERE date < CURRENT_DATE - 400
                ORDER BY api_name, date_trunc('week', date), date DESC
            ) w
        )
        SELECT api_name, date, pe::float4 AS pe, pb::float4 AS pb, div_yield::float4 AS div_yield
        FROM v ORDER BY api_name, date
    """)
    meta_sql = text("SELECT api_name, display_name, grp, sort_order FROM index_valuation_indices")
    with _engine.connect() as conn:          # one connection: connecting costs ~1-2s
        meta = pd.read_sql(meta_sql, conn)
        df = pd.read_sql(sql, conn)
    df = df.merge(meta, on="api_name").sort_values(["sort_order", "date"]).reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"])
    for c in ("pe", "pb", "div_yield"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def _cached_summary(_engine, metric: str, period: str) -> pd.DataFrame:
    return summary_table(load_valuation(_engine), metric, PERIODS[period])


# ── Renderer ─────────────────────────────────────────────────────────────────

_ZONE_COL = {"Cheap": "#16a34a", "Fair": "#64748b", "Expensive": "#dc2626"}


def render_valuation_tab(engine, dark: bool) -> None:
    C = _palette(dark)
    df = load_valuation(engine)
    if df.empty:
        st.info("Index valuation data will appear after the next daily refresh.")
        return
    names = (df[["api_name", "display_name", "grp", "sort_order"]].drop_duplicates("api_name")
             .sort_values("sort_order"))

    c1, c2, c3 = st.columns([2.4, 1.3, 1.6], vertical_alignment="bottom")
    with c1:
        grp = st.segmented_control("Group", GROUPS, default="All", key="val_group") or "All"
    with c2:
        metric_label = st.segmented_control("Metric", list(METRICS), default="P/E", key="val_metric") or "P/E"
    with c3:
        period = st.segmented_control("Period", list(PERIODS), default="10Y", key="val_period") or "10Y"
    metric = METRICS[metric_label]

    opts = names if grp == "All" else names[names["grp"] == grp]
    api_list = opts["api_name"].tolist()
    label_of = dict(zip(names["api_name"], names["display_name"]))
    pending = st.session_state.pop("_val_pick", None)      # row clicked in the table last run
    if pending in api_list:
        st.session_state["val_index"] = pending
    if st.session_state.get("val_index") not in api_list:
        st.session_state["val_index"] = "NIFTY 50" if "NIFTY 50" in api_list else api_list[0]
    api = st.selectbox("Index", api_list, key="val_index", format_func=lambda a: label_of.get(a, a))

    g = window(df[df["api_name"] == api].sort_values("date"), PERIODS[period])
    g = g.dropna(subset=[metric])
    stats = band_stats(pd.Series(g[metric].values, index=g["date"])) if not g.empty else None
    if not stats:
        st.info(f"Not enough {metric_label} history for {label_of.get(api, api)} in this period.")
    else:
        z = zone(stats["z"], metric)
        zc = _ZONE_COL[z]
        st.markdown(
            f"<div style='display:flex;gap:28px;align-items:baseline;flex-wrap:wrap;margin:6px 0 2px 0'>"
            f"<div style='font-size:20px;font-weight:600;color:{C['text']}'>{html.escape(label_of.get(api, api))}</div>"
            f"<div style='color:{C['muted']}'>{metric_label} <b style='color:{C['text']};font-size:18px'>"
            f"{stats['current']:.2f}</b></div>"
            f"<div style='color:{C['muted']}'>{period} avg <b style='color:{C['text']}'>{stats['mean']:.2f}</b></div>"
            f"<div style='color:{C['muted']}'>vs avg <b style='color:{zc}'>{stats['dev_pct']:+.1f}%</b></div>"
            f"<div style='color:{C['muted']}'>percentile <b style='color:{C['text']}'>{stats['pctile']:.0f}</b></div>"
            f"<div style='background:{zc}1f;color:{zc};border:1px solid {zc}55;border-radius:12px;"
            f"padding:1px 10px;font-size:13px;font-weight:600'>{z}</div></div>",
            unsafe_allow_html=True)
        st.plotly_chart(build_valuation_figure(g, metric, metric_label, stats, dark), width="stretch",
                        config=CHART_CONFIG, theme=None, key=f"val_{api}_{metric}_{period}")
        st.caption(f"Average and ±1σ / ±2σ bands are over the selected period ({period}), on weekly values. "
                   f"Cheap / Expensive = more than 1σ below / above the average"
                   f"{' (reversed for dividend yield: a high yield is cheap)' if metric == 'div_yield' else ''}. "
                   f"Source: NSE Indices · as of {pd.Timestamp(g['date'].max()):%d %b %Y}.")

    st.markdown(f"##### All indices — {metric_label} vs their {period} history")
    tbl = _cached_summary(engine, metric, period)
    if grp != "All" and not tbl.empty:
        tbl = tbl[tbl["Group"] == grp]
    if tbl.empty:
        return
    show = tbl.drop(columns=["api_name", "Z"])
    styled = (show.style
              .format({"Current": "{:.2f}", "Average": "{:.2f}", "Min": "{:.2f}", "Max": "{:.2f}",
                       "vs Avg %": "{:+.1f}%", "Percentile": "{:.0f}"}, na_rep="—")
              .map(lambda v: f"color: {_ZONE_COL.get(v, '')}; font-weight: 600" if v in _ZONE_COL else "",
                   subset=["Zone"]))
    ev = st.dataframe(styled, hide_index=True, width="stretch", height=min(38 + 35 * len(show), 720),
                      on_select="rerun", selection_mode="single-row", key=f"val_tbl_{metric}_{period}_{grp}")
    sel = getattr(getattr(ev, "selection", None), "rows", None)
    tkey = f"{metric}_{period}_{grp}"
    if sel and st.session_state.get("_val_last_sel") != (tkey, sel[0]):
        st.session_state["_val_last_sel"] = (tkey, sel[0])     # act once per new click
        picked = tbl.iloc[sel[0]]["api_name"]
        if picked != st.session_state.get("val_index"):
            st.session_state["_val_pick"] = picked
            st.rerun(scope="fragment")
    st.caption("Tip: click a row to chart that index.")
