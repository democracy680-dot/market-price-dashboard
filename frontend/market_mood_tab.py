"""
market_mood_tab.py — Market Mood tab (universe = all dashboard stocks).

Reads pre-computed rows from market_breadth_daily / market_turnover_monthly
(written by backend/compute_market_breadth.py). No indicator math here.

Sections:
  1. Mood gauges   — "The Broader Market" composite score + "Market Momentum" (% above 50 DMA)
  2. Stat tiles    — breadth inputs with day-over-day deltas
  3. Breadth table — ±3/5/10/20% movers per session
  4. Charts        — % below 200 DMA, % below 50 DMA, overbought vs oversold, monthly turnover
"""
from __future__ import annotations

import math
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

LOOKBACKS = {"6M": 182, "1Y": 365, "2Y": 730, "All": None}
TABLE_ROWS = 15
TURNOVER_MA_MONTHS = 12

RED, AMBER, GREEN, BLUE = "#ef4444", "#f59e0b", "#22c55e", "#3b82f6"


# ── Palette ──────────────────────────────────────────────────────────────────

def _palette(dark: bool) -> dict:
    return {
        "card_bg":   "#0f1729" if dark else "#ffffff",
        "card_bd":   "#1e2d45" if dark else "#e2e8f0",
        "text":      "#f1f5f9" if dark else "#0f172a",
        "muted":     "#8b97a8" if dark else "#64748b",
        "section":   "#475569" if dark else "#64748b",
        "grid":      "#1f2937" if dark else "#cbd5e1",
        "plot_bg":   "#080c14" if dark else "#ffffff",
        "tick":      "#94a3b8" if dark else "#475569",
        # chart inks — saturated enough to read on both backgrounds
        "line":      "#60a5fa" if dark else "#1d4ed8",
        "line_fill": "rgba(96,165,250,0.32)" if dark else "rgba(29,78,216,0.26)",
        "zone_red":  "rgba(239,68,68,0.30)" if dark else "rgba(220,38,38,0.20)",
        "zone_grn":  "rgba(34,197,94,0.30)" if dark else "rgba(22,163,74,0.20)",
        "red":       "#ef4444" if dark else "#dc2626",
        "green":     "#22c55e" if dark else "#16a34a",
        "amber":     "#f59e0b" if dark else "#d97706",
        "red_fill":  "rgba(239,68,68,0.24)" if dark else "rgba(220,38,38,0.20)",
        "grn_fill":  "rgba(34,197,94,0.24)" if dark else "rgba(22,163,74,0.20)",
        "needle":    "#60a5fa" if dark else "#1d4ed8",
        "hub":       "#0f1729" if dark else "#ffffff",
        "table_hdr": "#0b1a33" if dark else "#eef2f8",
        "row_bd":    "#1a2236" if dark else "#f1f5f9",
        "hover_bg":  "#1e2535" if dark else "#f1f5f9",
        "hover_txt": "#e2e8f0" if dark else "#0f172a",
    }


# ── Data loaders ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_breadth_history(_engine) -> pd.DataFrame:
    sql = text("""
        SELECT date, total_stocks, advances, declines, unchanged,
               up_3, down_3, up_5, down_5, up_10, down_10, up_20, down_20,
               pct_above_50dma, pct_below_50dma, pct_above_200dma, pct_below_200dma,
               overbought, oversold, pct_rsi_above_50, adv_ratio_20d,
               mood_score, mood_label, momentum_pct, turnover_cr
        FROM market_breadth_daily
        ORDER BY date
    """)
    with _engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    num_cols = [c for c in df.columns if c not in ("date", "mood_label")]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def load_turnover_monthly(_engine) -> pd.DataFrame:
    sql = text("SELECT month, turnover_cr, trading_days, is_partial FROM market_turnover_monthly ORDER BY month")
    with _engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    if not df.empty:
        df["month"] = pd.to_datetime(df["month"])
        df["turnover_cr"] = pd.to_numeric(df["turnover_cr"], errors="coerce")
    return df


# ── Pure helpers ─────────────────────────────────────────────────────────────

def _fmt(v, suffix="", nd=1):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{v:.{nd}f}{suffix}"


def _delta_chip(cur, prev, suffix="", invert=False) -> str:
    if cur is None or prev is None or pd.isna(cur) or pd.isna(prev):
        return ""
    d = float(cur) - float(prev)
    good = d <= 0 if invert else d >= 0
    color = GREEN if good else RED
    arrow = "▲" if d >= 0 else "▼"
    return (f"<span style='font-size:11px;font-weight:600;color:{color};margin-left:6px;'>"
            f"{arrow} {abs(d):.1f}{suffix}</span>")


def _polar(cx, cy, r, deg):
    """Gauge angle: 180° = left (score 0), 0° = right (score 100)."""
    rad = math.radians(deg)
    return cx + r * math.cos(rad), cy - r * math.sin(rad)


def _arc_path(cx, cy, r_outer, r_inner, start_deg, end_deg) -> str:
    """Annular sector from start_deg → end_deg (degrees decrease left→right)."""
    x1, y1 = _polar(cx, cy, r_outer, start_deg)
    x2, y2 = _polar(cx, cy, r_outer, end_deg)
    x3, y3 = _polar(cx, cy, r_inner, end_deg)
    x4, y4 = _polar(cx, cy, r_inner, start_deg)
    large = 1 if abs(start_deg - end_deg) > 180 else 0
    return (f"M {x1:.2f} {y1:.2f} A {r_outer} {r_outer} 0 {large} 1 {x2:.2f} {y2:.2f} "
            f"L {x3:.2f} {y3:.2f} A {r_inner} {r_inner} 0 {large} 0 {x4:.2f} {y4:.2f} Z")


def gauge_svg(score, segments, C: dict, width=420, labels=True, value_text=None) -> str:
    """
    Semicircular SVG gauge with a needle.
    segments: list of (start_pct, end_pct, color, label)
    """
    w = width
    h = int(w * 0.58)
    cx, cy = w / 2, h * 0.86
    r_out = w * 0.42
    r_in = r_out * 0.72

    parts = [f"<svg viewBox='0 0 {w} {h}' width='100%' style='max-width:{w}px;display:block;margin:0 auto;'>"]
    for s, e, color, label in segments:
        a0 = 180 - s * 1.8
        a1 = 180 - e * 1.8
        parts.append(f"<path d='{_arc_path(cx, cy, r_out, r_in, a0, a1)}' fill='{color}' opacity='0.92'/>")
        if labels and label:
            mid = (a0 + a1) / 2
            lx, ly = _polar(cx, cy, r_out + w * 0.055, mid)
            anchor = "middle" if 60 < mid < 120 else ("end" if mid >= 120 else "start")
            parts.append(f"<text x='{lx:.1f}' y='{ly:.1f}' text-anchor='{anchor}' dominant-baseline='middle' "
                         f"font-size='{w*0.034:.0f}' font-weight='700' fill='{color}'>{label}</text>")
    # segment separators
    for s, e, _, _ in segments[1:]:
        a = 180 - s * 1.8
        x1, y1 = _polar(cx, cy, r_out + 2, a)
        x2, y2 = _polar(cx, cy, r_in - 2, a)
        parts.append(f"<line x1='{x1:.1f}' y1='{y1:.1f}' x2='{x2:.1f}' y2='{y2:.1f}' stroke='{C['card_bg']}' stroke-width='3'/>")

    if score is not None and not pd.isna(score):
        a = 180 - max(0.0, min(100.0, float(score))) * 1.8
        nx, ny = _polar(cx, cy, r_in * 1.02, a)
        parts.append(f"<line x1='{cx:.1f}' y1='{cy:.1f}' x2='{nx:.1f}' y2='{ny:.1f}' "
                     f"stroke='{C['needle']}' stroke-width='{w*0.014:.1f}' stroke-linecap='round'/>")
        parts.append(f"<circle cx='{cx:.1f}' cy='{cy:.1f}' r='{w*0.03:.1f}' fill='{C['hub']}' "
                     f"stroke='{C['needle']}' stroke-width='{w*0.012:.1f}'/>")
    if value_text:
        parts.append(f"<text x='{cx:.1f}' y='{cy + w*0.085:.1f}' text-anchor='middle' font-size='{w*0.07:.0f}' "
                     f"font-weight='700' fill='{C['muted']}'>{value_text}</text>")
    parts.append("</svg>")
    return "".join(parts)


def _card(inner_html: str, C: dict, title: str = "", subtitle: str = "", pad=18) -> str:
    t = (f"<div style='font-size:15px;font-weight:700;color:{C['text']};text-align:center;margin-top:6px;'>{title}</div>"
         if title else "")
    s = (f"<div style='font-size:11px;color:{C['muted']};text-align:center;margin-top:2px;'>{subtitle}</div>"
         if subtitle else "")
    return (f"<div style='background:{C['card_bg']};border:1px solid {C['card_bd']};border-radius:12px;"
            f"padding:{pad}px;height:100%;'>{inner_html}{t}{s}</div>")


# ── Section 1: gauges ────────────────────────────────────────────────────────

def _render_gauges(last: pd.Series, prev: pd.Series | None, C: dict):
    score = last.get("mood_score")
    label = last.get("mood_label") or "—"
    mom = last.get("momentum_pct")

    big = gauge_svg(score, [(0, 35, RED, "Bear"), (35, 65, AMBER, "Dull"), (65, 100, GREEN, "Bull")], C, width=460)
    score_line = (f"<div style='text-align:center;margin-top:4px;'>"
                  f"<span style='font-size:26px;font-weight:800;color:{C['text']};'>{_fmt(score, '', 0)}</span>"
                  f"<span style='font-size:13px;color:{C['muted']};margin-left:6px;'>/ 100 · {label}</span>"
                  f"{_delta_chip(score, prev.get('mood_score') if prev is not None else None)}</div>")
    small = gauge_svg(mom, [(0, 35, RED, ""), (35, 65, AMBER, ""), (65, 100, GREEN, "")], C, width=260,
                      labels=False, value_text=_fmt(mom, "%", 0))
    mom_line = (f"<div style='text-align:center;'>{_delta_chip(mom, prev.get('momentum_pct') if prev is not None else None, '%')}</div>")

    col_a, col_b = st.columns([2.2, 1])
    with col_a:
        st.markdown(_card(big + score_line, C, "The Broader Market",
                          "Composite of % above 200 DMA · % above 50 DMA · 20-day advance ratio · % RSI > 50"),
                    unsafe_allow_html=True)
    with col_b:
        st.markdown(_card(small + mom_line, C, "The Market Momentum", "% of stocks above 50 DMA"),
                    unsafe_allow_html=True)


# ── Section 2: stat tiles ────────────────────────────────────────────────────

def _render_tiles(last: pd.Series, prev: pd.Series | None, C: dict):
    p = (lambda k: prev.get(k)) if prev is not None else (lambda k: None)
    tiles = [
        ("Above 200 DMA", _fmt(last["pct_above_200dma"], "%"), _delta_chip(last["pct_above_200dma"], p("pct_above_200dma"), "%")),
        ("Above 50 DMA", _fmt(last["pct_above_50dma"], "%"), _delta_chip(last["pct_above_50dma"], p("pct_above_50dma"), "%")),
        ("20D Advance Ratio", _fmt(last["adv_ratio_20d"], "%"), _delta_chip(last["adv_ratio_20d"], p("adv_ratio_20d"), "%")),
        ("RSI > 50", _fmt(last["pct_rsi_above_50"], "%"), _delta_chip(last["pct_rsi_above_50"], p("pct_rsi_above_50"), "%")),
        ("Overbought (RSI>70)", _fmt(last["overbought"], "", 0), _delta_chip(last["overbought"], p("overbought"), "", invert=True)),
        ("Oversold (RSI<30)", _fmt(last["oversold"], "", 0), _delta_chip(last["oversold"], p("oversold"), "")),
        ("Adv / Dec today", f"<span style='color:{GREEN}'>{int(last['advances'])}</span> / "
                            f"<span style='color:{RED}'>{int(last['declines'])}</span>", ""),
        ("Turnover (₹ cr)", f"{last['turnover_cr']:,.0f}" if pd.notna(last["turnover_cr"]) else "—", ""),
    ]
    cols = st.columns(len(tiles))
    for col, (title, value, chip) in zip(cols, tiles):
        with col:
            st.markdown(
                f"<div style='background:{C['card_bg']};border:1px solid {C['card_bd']};border-radius:10px;"
                f"padding:12px 14px;min-height:78px;'>"
                f"<div style='font-size:10px;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;"
                f"color:{C['section']};'>{title}</div>"
                f"<div style='font-size:20px;font-weight:800;color:{C['text']};margin-top:4px;'>{value}{chip}</div></div>",
                unsafe_allow_html=True,
            )


# ── Section 3: breadth table ─────────────────────────────────────────────────

def _render_breadth_table(df: pd.DataFrame, C: dict):
    rows = df.sort_values("date", ascending=False).head(TABLE_ROWS)
    cols = [("up_3", "Up 3%+", GREEN), ("down_3", "Down 3%+", RED),
            ("up_5", "Up 5%+", GREEN), ("down_5", "Down 5%+", RED),
            ("up_10", "Up 10%+", GREEN), ("down_10", "Down 10%+", RED),
            ("up_20", "Up 20%+", GREEN), ("down_20", "Down 20%+", RED)]
    th = "".join(f"<th style='text-align:left;padding:10px 12px;font-size:11px;font-weight:700;color:{C['muted']};'>{h}</th>"
                 for _, h, _ in cols)
    body = []
    for _, r in rows.iterrows():
        tds = "".join(
            f"<td style='padding:9px 12px;font-size:13px;font-weight:600;color:{c};'>{int(r[k]) if pd.notna(r[k]) else '—'}</td>"
            for k, _, c in cols)
        body.append(f"<tr style='border-bottom:1px solid {C['row_bd']};'>"
                    f"<td style='padding:9px 12px;font-size:13px;color:{C['text']};white-space:nowrap;'>"
                    f"{r['date'].strftime('%d %b %Y')}</td>{tds}</tr>")
    html = (f"<div style='font-size:15px;font-weight:700;color:{C['text']};'>Market Breadth</div>"
            f"<div style='font-size:11px;color:{C['muted']};margin-bottom:10px;'>"
            f"Stock count distribution across daily price movements · universe {int(rows.iloc[0]['total_stocks'])} stocks</div>"
            f"<div style='overflow-x:auto;'><table style='width:100%;border-collapse:collapse;'>"
            f"<thead><tr style='background:{C['table_hdr']};'>"
            f"<th style='text-align:left;padding:10px 12px;font-size:11px;font-weight:700;color:{C['muted']};'>Date</th>{th}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table></div>")
    st.markdown(_card(html, C), unsafe_allow_html=True)


# ── Section 4: charts ────────────────────────────────────────────────────────

def _base_layout(C: dict, height=300, **kw) -> dict:
    layout = dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor=C["plot_bg"],
        font=dict(color=C["tick"], size=11),
        margin=dict(l=48, r=56, t=36, b=36),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=C["hover_bg"], font=dict(color=C["hover_txt"], size=12)),
        xaxis=dict(gridcolor=C["grid"], showgrid=False, tickfont=dict(size=10)),
        yaxis=dict(gridcolor=C["grid"], gridwidth=1, tickfont=dict(size=10), zeroline=False),
        legend=dict(orientation="h", y=1.02, yanchor="bottom", x=1, xanchor="right",
                    font=dict(size=11, color=C["tick"]), bgcolor="rgba(0,0,0,0)"),
    )
    layout.update(kw)
    return layout


def _pct_below_chart(df: pd.DataFrame, col: str, title: str, C: dict) -> go.Figure:
    d = df.dropna(subset=[col])
    fig = go.Figure()
    lo, hi = float(d[col].min()), float(d[col].max())
    # Symmetric range around 50 so the 50% line always sits in the vertical centre
    half = min(50.0, max(hi - 50.0, 50.0 - lo, 10.0) * 1.08)
    y0, y1 = 50.0 - half, 50.0 + half
    # shaded zones: above 50% = weak (red), below 50% = healthy (green)
    fig.add_shape(type="rect", xref="paper", x0=0, x1=1, y0=50, y1=y1, fillcolor=C["zone_red"], line_width=0, layer="below")
    fig.add_shape(type="rect", xref="paper", x0=0, x1=1, y0=y0, y1=50, fillcolor=C["zone_grn"], line_width=0, layer="below")
    # invisible 50% baseline so the area fills between the series and the midline
    fig.add_trace(go.Scatter(x=d["date"], y=[50.0] * len(d), mode="lines", showlegend=False,
                             line=dict(width=0), hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=d["date"], y=d[col], mode="lines", name=title,
        line=dict(color=C["line"], width=2.2),
        fill="tonexty", fillcolor=C["line_fill"],
        hovertemplate="%{y:.1f}%<extra></extra>",
    ))
    fig.add_hline(y=50, line=dict(color=C["amber"], dash="dash", width=1.5),
                  annotation_text="50%", annotation_position="right",
                  annotation_font=dict(color=C["amber"], size=11))
    last = d.iloc[-1]
    fig.update_layout(**_base_layout(
        C, title=dict(text=f"{title}  <span style='color:{C['muted']};font-size:12px'>latest {last[col]:.1f}%</span>",
                      x=0, font=dict(size=14, color=C["text"])),
        showlegend=False, yaxis=dict(range=[y0, y1], ticksuffix="%", gridcolor=C["grid"], tickfont=dict(size=10)),
    ))
    return fig


def _ob_os_chart(df: pd.DataFrame, C: dict) -> go.Figure:
    d = df.dropna(subset=["overbought", "oversold"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["date"], y=d["oversold"], mode="lines", name="Oversold (RSI < 30)",
                             line=dict(color=C["green"], width=2.2), fill="tozeroy", fillcolor=C["grn_fill"],
                             hovertemplate="%{y:.0f}<extra>Oversold</extra>"))
    fig.add_trace(go.Scatter(x=d["date"], y=d["overbought"], mode="lines", name="Overbought (RSI > 70)",
                             line=dict(color=C["red"], width=2.2), fill="tozeroy", fillcolor=C["red_fill"],
                             hovertemplate="%{y:.0f}<extra>Overbought</extra>"))
    last = d.iloc[-1]
    fig.update_layout(**_base_layout(
        C, title=dict(text=f"Overbought vs Oversold  <span style='color:{C['muted']};font-size:12px'>"
                           f"latest {int(last['overbought'])} / {int(last['oversold'])}</span>",
                      x=0, font=dict(size=14, color=C["text"])),
        yaxis=dict(title="stocks", gridcolor=C["grid"], tickfont=dict(size=10), rangemode="tozero"),
    ))
    return fig


def _turnover_chart(m: pd.DataFrame, C: dict) -> go.Figure:
    d = m.copy()
    d["k_cr"] = d["turnover_cr"] / 1e5   # rupees lakh crore
    d["ma"] = d["k_cr"].rolling(TURNOVER_MA_MONTHS, min_periods=3).mean()
    colors = [C["amber"] if p else C["green"] for p in d["is_partial"]]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=d["month"], y=d["k_cr"], name="Monthly traded value (₹ lakh cr)",
                         marker=dict(color=colors, opacity=0.9),
                         customdata=d["trading_days"],
                         hovertemplate="₹%{y:.2f} lakh cr · %{customdata} sessions<extra></extra>"))
    fig.add_trace(go.Scatter(x=d["month"], y=d["ma"], mode="lines", name=f"{TURNOVER_MA_MONTHS}-month avg",
                             line=dict(color=C["red"], width=2.4), hovertemplate="₹%{y:.2f} lakh cr<extra>avg</extra>"))
    fig.update_layout(**_base_layout(
        C, title=dict(text="Total Monthly Traded Value — all dashboard stocks (₹ lakh crore)"
                           f"  <span style='color:{C['muted']};font-size:12px'>amber = month in progress</span>",
                      x=0, font=dict(size=14, color=C["text"])),
        bargap=0.25, xaxis=dict(showgrid=False, tickformat="%b %Y", tickfont=dict(size=10)),
        yaxis=dict(gridcolor=C["grid"], tickfont=dict(size=10), rangemode="tozero"),
    ))
    return fig


# ── Entry point ──────────────────────────────────────────────────────────────

def render_market_mood_tab(engine):
    dark = st.session_state.get("dark_mode", False)
    C = _palette(dark)

    df = load_breadth_history(engine)
    if df.empty:
        st.info("Market breadth has not been computed yet. Run `python backend/compute_market_breadth.py --full`.")
        return

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None

    st.markdown(
        f"<div style='font-size:11px;color:{C['section']};margin-bottom:14px;'>"
        f"As of <b>{last['date'].strftime('%d %b %Y')}</b> · universe of {int(last['total_stocks'])} stocks · "
        "score 0–35 Bear, 35–65 Dull, 65–100 Bull</div>",
        unsafe_allow_html=True,
    )

    _render_gauges(last, prev, C)
    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    _render_tiles(last, prev, C)
    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
    _render_breadth_table(df, C)
    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

    # ── Charts ──
    hdr, sel = st.columns([5, 2])
    with hdr:
        st.markdown(f"<div style='font-size:15px;font-weight:700;color:{C['text']};padding-top:6px;"
                    f"border-left:3px solid {BLUE};padding-left:10px;'>Market Breadth Charts</div>",
                    unsafe_allow_html=True)
    with sel:
        lb = st.radio("Lookback", list(LOOKBACKS), index=1, horizontal=True,
                      key="mood_lookback", label_visibility="collapsed")
    days = LOOKBACKS[lb]
    view = df if days is None else df[df["date"] >= df["date"].max() - pd.Timedelta(days=days)]

    cfg = {"displayModeBar": False}
    st.plotly_chart(_pct_below_chart(view, "pct_below_200dma", "% Below 200 DMA", C), width="stretch", config=cfg)
    st.plotly_chart(_pct_below_chart(view, "pct_below_50dma", "% Below 50 DMA", C), width="stretch", config=cfg)
    st.plotly_chart(_ob_os_chart(view, C), width="stretch", config=cfg)

    monthly = load_turnover_monthly(engine)
    if not monthly.empty:
        st.plotly_chart(_turnover_chart(monthly, C), width="stretch", config=cfg)
