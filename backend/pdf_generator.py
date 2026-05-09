"""
pdf_generator.py — Generates a professional StockStack Daily Digest PDF.

Uses reportlab for layout + matplotlib for embedded charts.
Called from email_digest.py after all data is fetched.
Returns the path of the saved PDF file (in .tmp/).
"""

import io
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend — safe for headless / CI use
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    Image,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    KeepTogether,
)

# ── Palette ────────────────────────────────────────────────────────────────────
_NAVY       = colors.HexColor("#0f172a")
_NAVY2      = colors.HexColor("#1e293b")
_BLUE       = colors.HexColor("#1d4ed8")
_BLUE_L     = colors.HexColor("#3b82f6")
_BLUE_XL    = colors.HexColor("#93c5fd")
_GREEN      = colors.HexColor("#16a34a")
_GREEN_L    = colors.HexColor("#bbf7d0")
_GREEN_XL   = colors.HexColor("#f0fdf4")
_RED        = colors.HexColor("#dc2626")
_RED_L      = colors.HexColor("#fecaca")
_RED_XL     = colors.HexColor("#fef2f2")
_AMBER      = colors.HexColor("#d97706")
_MUTED      = colors.HexColor("#475569")   # darkened from #64748b for legibility
_BORDER     = colors.HexColor("#e2e8f0")
_BG_LIGHT   = colors.HexColor("#f8fafc")
_BG_ALT     = colors.HexColor("#f1f5f9")
_WHITE      = colors.white

# Matplotlib hex strings
M_NAVY    = "#0f172a"
M_BLUE    = "#3b82f6"
M_GREEN   = "#16a34a"
M_RED     = "#dc2626"
M_AMBER   = "#d97706"
M_MUTED   = "#475569"   # darkened
M_BG      = "#f8fafc"
M_BORDER  = "#e2e8f0"
# Fixed series colors for index breadth chart (legend must always match bars)
M_200DMA  = "#f59e0b"   # amber — 200 DMA series
M_50DMA   = "#3b82f6"   # blue  — 50 DMA series

# ── Text styles ────────────────────────────────────────────────────────────────
S_H1   = ParagraphStyle("h1",  fontSize=11, fontName="Helvetica-Bold",
                          textColor=_NAVY, spaceBefore=10, spaceAfter=3)
S_H1W  = ParagraphStyle("h1w", fontSize=13, fontName="Helvetica-Bold",
                          textColor=_WHITE, spaceAfter=2)
S_SUB  = ParagraphStyle("sub", fontSize=8,  fontName="Helvetica",
                          textColor=colors.HexColor("#475569"), spaceAfter=5)
S_SUBW = ParagraphStyle("subw",fontSize=8,  fontName="Helvetica",
                          textColor=_BLUE_XL, spaceAfter=0)
S_CELL = ParagraphStyle("c",   fontSize=8.5,fontName="Helvetica",
                          textColor=colors.HexColor("#1e293b"), leading=11)
S_CELB = ParagraphStyle("cb",  fontSize=8.5,fontName="Helvetica-Bold",
                          textColor=colors.HexColor("#0f172a"), leading=11)
S_CELM = ParagraphStyle("cm",  fontSize=7.5,fontName="Helvetica",
                          textColor=colors.HexColor("#475569"), leading=10)
S_TH     = ParagraphStyle("th",  fontSize=8.5, fontName="Helvetica-Bold",
                            textColor=colors.white)
S_FOOT   = ParagraphStyle("ft",  fontSize=7,  fontName="Helvetica",
                            textColor=colors.HexColor("#475569"), alignment=TA_CENTER)
S_LINK   = ParagraphStyle("lnk", fontSize=6.5, fontName="Helvetica",
                            textColor=colors.HexColor("#3b82f6"), leading=9)
S_CARD_L = ParagraphStyle("cl", fontSize=7,   fontName="Helvetica",
                            textColor=colors.HexColor("#64748b"),
                            alignment=TA_CENTER, spaceAfter=2)
S_CARD_V = ParagraphStyle("cv", fontSize=14,  fontName="Helvetica-Bold",
                            textColor=colors.HexColor("#0f172a"),
                            alignment=TA_CENTER, leading=16)

PAGE_W, PAGE_H = A4
MARGIN = 14 * mm
USABLE_W = PAGE_W - 2 * MARGIN

# ── Page template with header + footer ────────────────────────────────────────

def _draw_page(canvas, doc):
    canvas.saveState()
    # Top header bar
    canvas.setFillColor(_NAVY)
    canvas.rect(0, PAGE_H - 20*mm, PAGE_W, 20*mm, fill=1, stroke=0)
    # Blue accent line under header
    canvas.setFillColor(_BLUE_L)
    canvas.rect(0, PAGE_H - 20.8*mm, PAGE_W, 0.8*mm, fill=1, stroke=0)
    # Logo box
    canvas.setFillColor(_BLUE_L)
    canvas.roundRect(MARGIN, PAGE_H - 15.5*mm, 10*mm, 10*mm, 2*mm, fill=1, stroke=0)
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 11)
    canvas.drawCentredString(MARGIN + 5*mm, PAGE_H - 11.5*mm, "S")
    # Title
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 12)
    canvas.drawString(MARGIN + 12*mm, PAGE_H - 10*mm, "StockStack")
    canvas.setFillColor(colors.HexColor("#93c5fd"))
    canvas.setFont("Helvetica", 7.5)
    canvas.drawString(MARGIN + 12*mm, PAGE_H - 14*mm, "DAILY MARKET DIGEST")
    # Date (right side)
    edition_num = getattr(doc, "edition_num", None)
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 10)
    date_str = doc.digest_date.strftime("%d %b %Y")
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 10*mm, date_str)
    canvas.setFillColor(colors.HexColor("#93c5fd"))
    canvas.setFont("Helvetica", 7.5)
    weekday = doc.digest_date.strftime("%A  ·  NSE India")
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 14.5*mm, weekday)
    if edition_num:
        canvas.setFillColor(colors.HexColor("#bfdbfe"))
        canvas.setFont("Helvetica", 6.5)
        canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 18.5*mm, f"Issue #{edition_num}")
    # Footer line
    canvas.setStrokeColor(_BORDER)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, 10*mm, PAGE_W - MARGIN, 10*mm)
    canvas.setFillColor(_MUTED)
    canvas.setFont("Helvetica", 6.5)
    canvas.drawString(MARGIN, 7*mm, "StockStack  ·  Data via NSE / yfinance  ·  Personal use only")
    canvas.drawRightString(PAGE_W - MARGIN, 7*mm, f"Page {doc.page}")
    canvas.restoreState()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pct(val) -> str:
    try:
        v = float(val)
        return f"{'+'if v>0 else ''}{v:.2f}%"
    except (TypeError, ValueError):
        return "—"

def _vol(val) -> str:
    try:
        v = int(val)
        if v >= 10_000_000: return f"{v/10_000_000:.1f} Cr"
        if v >= 100_000:    return f"{v/100_000:.1f} L"
        return f"{v:,}"
    except (TypeError, ValueError):
        return "—"

def _mcap(val) -> str:
    try:
        v = float(val)
        if v >= 1_00_000: return f"Rs.{v/1_00_000:.1f}L Cr"
        return f"Rs.{v:,.0f} Cr"
    except (TypeError, ValueError):
        return "—"

def _gc(val) -> str:
    try:
        return "#16a34a" if float(val) > 0 else "#dc2626"
    except (TypeError, ValueError):
        return "#64748b"

def _th(*labels):
    return [Paragraph(l, S_TH) for l in labels]


def _stock_cell_pdf(symbol: str, name: str, tv_url=None, sc_url=None) -> Paragraph:
    """Stock cell with symbol, name, and dashed TradingView / Screener links."""
    links = []
    if tv_url and str(tv_url).startswith("http"):
        links.append(f'<link href="{tv_url}"><u>TradingView</u></link>')
    if sc_url and str(sc_url).startswith("http"):
        links.append(f'<link href="{sc_url}"><u>Screener</u></link>')
    links_html = (
        f'  <font color="#3b82f6" size="6">' + '  ·  '.join(links) + '</font>'
    ) if links else ""
    return Paragraph(
        f'<b><font color="#0f172a">{symbol}</font></b>'
        f'  <font color="#475569" size="7.5">{name}</font>'
        + (f'<br/>{links_html}' if links_html else ""),
        S_CELL,
    )

def _section(title: str, subtitle: str, story: list):
    story.append(Spacer(1, 4*mm))
    bar = Table([[""]], colWidths=[3*mm], rowHeights=[6*mm])
    bar.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),_BLUE_L),
                              ("TOPPADDING",(0,0),(-1,-1),0),
                              ("BOTTOMPADDING",(0,0),(-1,-1),0)]))
    title_p  = Paragraph(title,    S_H1)
    sub_p    = Paragraph(subtitle, S_SUB)
    inner = Table([[bar, [title_p, sub_p]]],
                  colWidths=[5*mm, USABLE_W - 5*mm])
    inner.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(-1,-1),0),
        ("RIGHTPADDING",(0,0),(-1,-1),0),
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),2),
    ]))
    story.append(inner)


def _table_style(n_rows: int, header_bg=None, alt_bg=None):
    hbg  = header_bg or colors.HexColor("#1e3a5f")
    abg  = alt_bg    or _WHITE
    abg2 = _BG_LIGHT
    ts = [
        ("BACKGROUND",    (0, 0), (-1, 0),   hbg),
        ("TEXTCOLOR",     (0, 0), (-1, 0),   colors.white),
        ("FONTSIZE",      (0, 0), (-1, 0),   8.5),
        ("GRID",          (0, 0), (-1, -1),  0.35, _BORDER),
        ("VALIGN",        (0, 0), (-1, -1),  "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1),  4),
        ("BOTTOMPADDING", (0, 0), (-1, -1),  4),
        ("LEFTPADDING",   (0, 0), (-1, -1),  6),
        ("RIGHTPADDING",  (0, 0), (-1, -1),  6),
    ]
    for i in range(1, n_rows):
        ts.append(("BACKGROUND", (0,i),(-1,i), abg if i%2==1 else abg2))
    return TableStyle(ts)


# ── Chart helpers ──────────────────────────────────────────────────────────────

def _fig_to_image(fig, width_mm: float) -> Image:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=200, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    img = Image(buf)
    aspect = img.imageHeight / img.imageWidth
    w = width_mm * mm
    img.drawWidth  = w
    img.drawHeight = w * aspect
    return img


def _setup_ax(ax, title: str = ""):
    ax.set_facecolor(M_BG)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(M_BORDER)
    ax.spines["bottom"].set_color(M_BORDER)
    ax.tick_params(colors=M_MUTED, labelsize=7)
    ax.xaxis.label.set_color(M_MUTED)
    ax.yaxis.label.set_color(M_MUTED)
    if title:
        ax.set_title(title, fontsize=8, color=M_NAVY, fontweight="bold", pad=6)


# ── Chart builders ─────────────────────────────────────────────────────────────

def _chart_breadth_donut(b: dict) -> Image:
    adv   = b["advances"]
    dec   = b["declines"]
    unch  = b["unchanged"]
    total = adv + dec + unch

    adv_pct = round(100 * adv / total, 1) if total else 0
    dec_pct = round(100 * dec / total, 1) if total else 0

    fig, ax = plt.subplots(figsize=(3.4, 3.4), facecolor="white")
    sizes  = [adv, dec, unch] if unch > 0 else [adv, dec]
    clrs   = [M_GREEN, M_RED, "#e2e8f0"][:len(sizes)]
    wedges, _ = ax.pie(
        sizes, colors=clrs, startangle=90,
        wedgeprops=dict(width=0.56, edgecolor="white", linewidth=2.5),
    )

    # Center: A/D ratio label
    ax.text(0,  0.22, f"{adv:,}", ha="center", va="center",
            fontsize=17, fontweight="bold", color=M_GREEN, family="sans-serif")
    ax.text(0, -0.04, "▲ Advances", ha="center", va="center",
            fontsize=6.5, color=M_GREEN)
    ax.text(0, -0.26, f"{dec:,}", ha="center", va="center",
            fontsize=17, fontweight="bold", color=M_RED, family="sans-serif")
    ax.text(0, -0.48, "▼ Declines", ha="center", va="center",
            fontsize=6.5, color=M_RED)

    # Legend with percentages
    labels_pct = [f"Advances  {adv_pct:.1f}%", f"Declines  {dec_pct:.1f}%"]
    if unch > 0:
        labels_pct.append(f"Unchanged  {round(100*unch/total,1):.1f}%")
    leg = ax.legend(wedges, labels_pct[:len(wedges)],
                    loc="lower center", bbox_to_anchor=(0.5, -0.14),
                    ncol=len(wedges), fontsize=6.5, frameon=False,
                    handlelength=1.2, handletextpad=0.4)
    for t in leg.get_texts():
        t.set_color(M_MUTED)

    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")
    ax.set_title("Advance / Decline Ratio", fontsize=9, fontweight="bold",
                 color=M_NAVY, pad=6)
    fig.tight_layout(pad=0.4)
    return _fig_to_image(fig, 68)


def _chart_index_breadth(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    labels = df["index"].tolist()
    pct200 = df["pct_200"].tolist()
    pct50  = df["pct_50"].tolist()
    y      = np.arange(len(labels))
    h      = 0.32

    fig, ax = plt.subplots(figsize=(6.8, 2.4), facecolor="white")
    ax.set_facecolor("white")

    # Fixed colors per series so legend always matches bars
    bars200 = ax.barh(y + h/2, pct200, h, color=M_200DMA, alpha=0.88, label="Above 200 DMA")
    bars50  = ax.barh(y - h/2, pct50,  h, color=M_50DMA,  alpha=0.88, label="Above 50 DMA")

    # Value labels
    for bar, v in zip(bars200, pct200):
        ax.text(min(v + 1, 97), bar.get_y() + bar.get_height()/2,
                f"{v:.0f}%", va="center", fontsize=6.5, color=M_NAVY, fontweight="bold")
    for bar, v in zip(bars50, pct50):
        ax.text(min(v + 1, 97), bar.get_y() + bar.get_height()/2,
                f"{v:.0f}%", va="center", fontsize=6.5, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.5, color=M_NAVY)
    ax.set_xlim(0, 108)
    ax.axvline(50, color=M_BORDER, linewidth=0.8, linestyle="--")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    _setup_ax(ax)
    ax.set_title("Index Breadth — % Stocks Above Key Moving Averages",
                 fontsize=8.5, fontweight="bold", color=M_NAVY, pad=6)

    patch200 = mpatches.Patch(color=M_200DMA, label="Above 200 DMA")
    patch50  = mpatches.Patch(color=M_50DMA,  label="Above 50 DMA")
    leg = ax.legend(handles=[patch200, patch50], fontsize=6.5,
                    loc="lower right", frameon=True,
                    facecolor="white", edgecolor=M_BORDER)
    for t in leg.get_texts():
        t.set_color(M_MUTED)
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_sector_breadth(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    labels = df["index"].tolist()
    pct200 = df["pct_200"].tolist()
    pct50  = df["pct_50"].tolist()
    y      = np.arange(len(labels))
    h      = 0.32

    fig, ax = plt.subplots(figsize=(6.8, max(3.6, len(labels) * 0.38)), facecolor="white")
    ax.set_facecolor("white")

    bars200 = ax.barh(y + h/2, pct200, h, color=M_200DMA, alpha=0.88, label="Above 200 DMA")
    bars50  = ax.barh(y - h/2, pct50,  h, color=M_50DMA,  alpha=0.88, label="Above 50 DMA")

    for bar, v in zip(bars200, pct200):
        ax.text(min(v + 1, 97), bar.get_y() + bar.get_height()/2,
                f"{v:.0f}%", va="center", fontsize=6, color=M_NAVY, fontweight="bold")
    for bar, v in zip(bars50, pct50):
        ax.text(min(v + 1, 97), bar.get_y() + bar.get_height()/2,
                f"{v:.0f}%", va="center", fontsize=6, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7.5, color=M_NAVY)
    ax.set_xlim(0, 108)
    ax.axvline(50, color=M_BORDER, linewidth=0.8, linestyle="--")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    _setup_ax(ax)
    ax.set_title("Sector Breadth — % Stocks Above Key Moving Averages",
                 fontsize=8.5, fontweight="bold", color=M_NAVY, pad=6)

    patch200 = mpatches.Patch(color=M_200DMA, label="Above 200 DMA")
    patch50  = mpatches.Patch(color=M_50DMA,  label="Above 50 DMA")
    leg = ax.legend(handles=[patch200, patch50], fontsize=6.5,
                    loc="lower right", frameon=True,
                    facecolor="white", edgecolor=M_BORDER)
    for t in leg.get_texts():
        t.set_color(M_MUTED)
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_themes(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    names = [n if len(n) <= 28 else n[:26] + "…" for n in df["theme_name"].tolist()]
    vals  = df["avg_ret_1w"].tolist()
    clrs  = [M_GREEN if v >= 0 else M_RED for v in vals]

    fig, ax = plt.subplots(figsize=(6.8, 2.0), facecolor="white")
    ax.set_facecolor("white")
    y = np.arange(len(names))
    bars = ax.barh(y, vals, 0.55, color=clrs, alpha=0.85)

    for bar, v in zip(bars, vals):
        xpos = v + 0.05 if v >= 0 else v - 0.05
        ha = "left" if v >= 0 else "right"
        ax.text(xpos, bar.get_y() + bar.get_height()/2,
                f"{'+'if v>0 else ''}{v:.2f}%",
                va="center", ha=ha, fontsize=6.5, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=7.5, color=M_NAVY)
    ax.axvline(0, color=M_BORDER, linewidth=0.8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))
    _setup_ax(ax, "Top 5 Themes — Weekly Average Return")
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_volume_surge(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    syms  = df["symbol"].tolist()
    surges = df["surge_ratio"].tolist()

    fig, ax = plt.subplots(figsize=(6.8, 2.0), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, surges, 0.55, color=M_BLUE, alpha=0.82)

    for bar, v in zip(bars, surges):
        ax.text(v + 0.1, bar.get_y() + bar.get_height()/2,
                f"{v:.1f}x", va="center", fontsize=7, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(syms, fontsize=8, color=M_NAVY, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}x"))
    _setup_ax(ax, "Volume Surge vs 20-Day Average")
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_movers(df: pd.DataFrame, title: str, is_gainers: bool) -> Image:
    if df.empty:
        return None
    syms = df["symbol"].tolist()
    vals = df["ret_1d_pct"].tolist()
    clr  = M_GREEN if is_gainers else M_RED

    fig, ax = plt.subplots(figsize=(6.8, 3.2), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, vals, 0.58, color=clr, alpha=0.82)

    for bar, v in zip(bars, vals):
        xpos = v + 0.05 if v >= 0 else v - 0.05
        ha = "left" if v >= 0 else "right"
        ax.text(xpos, bar.get_y() + bar.get_height()/2,
                f"{'+'if v>0 else ''}{v:.2f}%",
                va="center", ha=ha, fontsize=6.5, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(syms, fontsize=7.5, color=M_NAVY, fontweight="bold")
    ax.axvline(0, color=M_BORDER, linewidth=0.8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))
    _setup_ax(ax, title)
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


# ── Section builders ──────────────────────────────────────────────────────────

def _add_executive_summary(story: list, breadth: dict, themes_df: pd.DataFrame,
                            vol_df: pd.DataFrame, gainers_df: pd.DataFrame,
                            losers_df: pd.DataFrame, earnings_df: pd.DataFrame):
    adv   = breadth["advances"]
    dec   = breadth["declines"]
    p200  = breadth["pct_above_200"]
    ratio = adv / (adv + dec) if (adv + dec) > 0 else 0.5
    tone  = "Bullish" if ratio >= 0.55 else ("Bearish" if ratio <= 0.45 else "Neutral")
    tone_color = "#16a34a" if tone == "Bullish" else ("#dc2626" if tone == "Bearish" else "#d97706")

    S_ES = ParagraphStyle("es", fontSize=8.5, fontName="Helvetica",
                           textColor=colors.HexColor("#1e293b"), leading=13)
    S_ES_HEAD = ParagraphStyle("esh", fontSize=9, fontName="Helvetica-Bold",
                                textColor=colors.HexColor("#1e3a5f"), spaceAfter=3)

    bullets = []
    bullets.append(
        f'<font color="{tone_color}"><b>Market:</b></font> Closed <b>{tone}</b> — '
        f'{adv:,} advances vs {dec:,} declines · '
        f'<font color="#1d4ed8"><b>{p200:.1f}%</b></font> stocks above 200 DMA'
    )
    if not themes_df.empty:
        t = themes_df.iloc[0]
        tc = "#16a34a" if float(t["avg_ret_1w"]) > 0 else "#dc2626"
        sign = "+" if float(t["avg_ret_1w"]) > 0 else ""
        bullets.append(
            f'<b>Top Theme:</b> {t["theme_name"]} — avg '
            f'<font color="{tc}"><b>{sign}{float(t["avg_ret_1w"]):.2f}%</b></font> (1W)'
        )
    if not vol_df.empty:
        vs = vol_df.iloc[0]
        bullets.append(
            f'<b>Volume Surge:</b> <b>{vs["symbol"]}</b> traded at '
            f'<font color="#1d4ed8"><b>{float(vs["surge_ratio"]):.1f}x</b></font> normal volume'
        )
    if not gainers_df.empty and not losers_df.empty:
        g = gainers_df.iloc[0]; l = losers_df.iloc[0]
        bullets.append(
            f'<b>Top Movers:</b> '
            f'<font color="#16a34a"><b>{g["symbol"]} +{float(g["ret_1d_pct"]):.2f}%</b></font>'
            f'  ·  '
            f'<font color="#dc2626"><b>{l["symbol"]} {float(l["ret_1d_pct"]):.2f}%</b></font>'
        )
    if not earnings_df.empty:
        e = earnings_df.iloc[0]
        ec = "#16a34a" if float(e["ret_1d_pct"]) > 0 else "#dc2626"
        sign = "+" if float(e["ret_1d_pct"]) > 0 else ""
        bullets.append(
            f'<b>Results Mover:</b> <b>{e["symbol"]}</b> moved '
            f'<font color="{ec}"><b>{sign}{float(e["ret_1d_pct"]):.2f}%</b></font> post-results'
        )

    story.append(Spacer(1, 2*mm))
    story.append(Paragraph("Today's Highlights", S_ES_HEAD))
    story.append(Spacer(1, 1*mm))
    rows = [[Paragraph(f"• {b}", S_ES)] for b in bullets]
    t = Table(rows, colWidths=[USABLE_W])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#f0f9ff")),
        ("BOX",           (0, 0), (-1, -1), 0.75, colors.HexColor("#1d4ed8")),
        ("LEFTPADDING",   (0, 0), (-1, -1), 12),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _chart_rs_leaders(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    syms = df["symbol"].tolist()
    vals = df["rs_1m"].tolist()

    fig, ax = plt.subplots(figsize=(6.8, 3.2), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, vals, 0.58, color=M_BLUE, alpha=0.82)

    for bar, v in zip(bars, vals):
        xpos = v + 0.1 if v >= 0 else v - 0.1
        ha   = "left" if v >= 0 else "right"
        ax.text(xpos, bar.get_y() + bar.get_height() / 2,
                f"{'+'if v>0 else ''}{v:.1f}%",
                va="center", ha=ha, fontsize=6.5, color=M_NAVY, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(syms, fontsize=7.5, color=M_NAVY, fontweight="bold")
    ax.axvline(0, color=M_BORDER, linewidth=0.8)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:+.1f}%"))
    _setup_ax(ax, "RS Leaders — 1-Month Excess Return vs Nifty 50")
    fig.tight_layout(pad=0.6)
    return _fig_to_image(fig, USABLE_W / mm)


def _add_rs_leaders(story: list, df: pd.DataFrame):
    _section("RS Leaders", "Top 10 stocks outperforming Nifty 50 on a 1-month basis", story)
    chart = _chart_rs_leaders(df)
    if chart:
        story.append(chart)

    if df.empty:
        story.append(Paragraph("No relative strength data.", S_CELL))
        story.append(Spacer(1, 4*mm))
        return

    header = _th("#", "Stock", "CMP", "1D Return", "1M RS vs Nifty", "RS Bucket")
    rows   = [header]
    for rank, (_, r) in enumerate(df.iterrows(), 1):
        rs_val  = float(r["rs_1m"])
        rs_col  = "#16a34a" if rs_val > 0 else "#dc2626"
        rs_sign = "+" if rs_val > 0 else ""
        rows.append([
            Paragraph(f"<font color='#1d4ed8'><b>{rank}</b></font>", S_CELB),
            _stock_cell_pdf(r["symbol"], r["name"],
                            r.get("tradingview_url"), r.get("screener_url")),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(
                f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>",
                S_CELB),
            Paragraph(
                f"<font color='{rs_col}'><b>{rs_sign}{rs_val:.1f}%</b></font>",
                S_CELB),
            Paragraph(str(r.get("rs_1m_bucket") or "—"), S_CELL),
        ])
    t = Table(rows, colWidths=[10*mm, 76*mm, 22*mm, 22*mm, 28*mm, 20*mm])
    t.setStyle(_table_style(len(rows)))
    story.append(Spacer(1, 3*mm))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _add_breadth(story: list, b: dict):
    _section("Market Breadth", "Advance/decline split, index returns, and 200 DMA strength", story)
    donut = _chart_breadth_donut(b)

    n50  = b.get("nifty50_ret")
    bank = b.get("bank_ret")
    p200 = b["pct_above_200"]
    adv  = b["advances"]
    dec  = b["declines"]
    total = b["total"]

    # ── helper: one metric card cell ──────────────────────────────────────────
    def _card(label: str, value: str, val_color: str,
              bg: str, border: str) -> list:
        lbl = Paragraph(label, S_CARD_L)
        val = Paragraph(
            f'<font color="{val_color}"><b>{value}</b></font>',
            S_CARD_V,
        )
        inner = Table([[lbl], [val]], colWidths=[None])
        inner.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor(bg)),
            ("BOX",           (0, 0), (-1, -1), 0.6, colors.HexColor(border)),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ]))
        return inner

    def _ret_triplet(val):
        if val is None:
            return "—", "#64748b", "#f8fafc", "#e2e8f0"
        color  = "#16a34a" if val > 0 else "#dc2626"
        bg     = "#f0fdf4" if val > 0 else "#fef2f2"
        border = "#bbf7d0" if val > 0 else "#fecaca"
        sign   = "+" if val > 0 else ""
        return f"{sign}{val:.2f}%", color, bg, border

    n50_val,  n50_col,  n50_bg,  n50_brd  = _ret_triplet(n50)
    bnk_val,  bnk_col,  bnk_bg,  bnk_brd  = _ret_triplet(bank)

    dma_pct = f"{p200:.1f}%"
    dma_col = "#1d4ed8" if p200 >= 60 else ("#d97706" if p200 >= 40 else "#dc2626")

    cw = (USABLE_W - 72*mm - 3*mm) / 2

    cards = Table([
        [_card("Nifty 50",       n50_val,       n50_col,  n50_bg,  n50_brd),
         _card("Nifty Bank",     bnk_val,       bnk_col,  bnk_bg,  bnk_brd)],
        [_card("Above 200 DMA",  dma_pct,       dma_col,  "#eff6ff", "#bfdbfe"),
         _card("Total Tracked",  f"{total:,}",  "#334155", "#f8fafc", "#e2e8f0")],
        [_card("▲ Advances",     f"{adv:,}",    "#16a34a", "#f0fdf4", "#bbf7d0"),
         _card("▼ Declines",     f"{dec:,}",    "#dc2626", "#fef2f2", "#fecaca")],
    ], colWidths=[cw, cw], rowHeights=[18*mm, 18*mm, 18*mm])
    cards.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 2),
        ("TOPPADDING",    (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    row = Table([[donut, cards]], colWidths=[72*mm, USABLE_W - 72*mm])
    row.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(row)

    # ── A/D progress bar ──────────────────────────────────────────────────────
    story.append(Spacer(1, 3*mm))
    adv_w = int(USABLE_W * adv / total) if total else 0
    bar = Table(
        [["", ""]],
        colWidths=[adv_w, USABLE_W - adv_w],
        rowHeights=[3.5*mm],
    )
    bar.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (0, 0), colors.HexColor("#16a34a")),
        ("BACKGROUND",    (1, 0), (1, 0), colors.HexColor("#dc2626")),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(bar)
    story.append(Spacer(1, 4*mm))


def _add_index_breadth(story: list, df: pd.DataFrame):
    _section("Index Breadth", "% of stocks above 50 DMA and 200 DMA across 4 major indexes", story)
    chart = _chart_index_breadth(df)
    if chart:
        story.append(chart)

    if df.empty:
        story.append(Paragraph("No data.", S_CELL))
        return

    # Summary table below chart
    header = _th("Index", "Total", "▲ 200 DMA", "▼ 200 DMA", "% 200", "▲ 50 DMA", "▼ 50 DMA", "% 50")
    rows = [header]
    for _, r in df.iterrows():
        p200 = float(r["pct_200"]); p50 = float(r["pct_50"])
        c200 = "#16a34a" if p200>=60 else ("#dc2626" if p200<40 else "#d97706")
        c50  = "#16a34a" if p50>=60  else ("#dc2626" if p50<40  else "#d97706")
        rows.append([
            Paragraph(f"<b>{r['index']}</b>",  S_CELB),
            Paragraph(str(int(r["total"])),     S_CELL),
            Paragraph(f"<font color='#16a34a'><b>{int(r['above_200'])}</b></font>", S_CELB),
            Paragraph(f"<font color='#dc2626'>{int(r['below_200'])}</font>",         S_CELL),
            Paragraph(f"<font color='{c200}'><b>{p200:.0f}%</b></font>",             S_CELB),
            Paragraph(f"<font color='#16a34a'><b>{int(r['above_50'])}</b></font>",   S_CELB),
            Paragraph(f"<font color='#dc2626'>{int(r['below_50'])}</font>",           S_CELL),
            Paragraph(f"<font color='{c50}'><b>{p50:.0f}%</b></font>",               S_CELB),
        ])
    cw = [36*mm, 16*mm, 24*mm, 24*mm, 16*mm, 24*mm, 24*mm, 14*mm]
    t  = Table(rows, colWidths=cw)
    t.setStyle(_table_style(len(rows)))
    story.append(Spacer(1, 3*mm))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _add_themes(story: list, df: pd.DataFrame):
    _section("Top 5 Themes — Weekly", "Best performing thematic baskets (avg 1-week return of member stocks)", story)
    chart = _chart_themes(df)
    if chart:
        story.append(chart)

    if df.empty:
        story.append(Paragraph("No theme data.", S_CELL))
        return

    header = _th("#", "Theme", "Stocks", "1W Avg Return", "1D Avg Return")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        rows.append([
            Paragraph(str(i), S_CELL),
            Paragraph(f"<b>{r['theme_name']}</b>", S_CELB),
            Paragraph(str(int(r["stock_count"])), S_CELL),
            Paragraph(f"<font color='{_gc(r['avg_ret_1w'])}'><b>{_pct(r['avg_ret_1w'])}</b></font>", S_CELB),
            Paragraph(f"<font color='{_gc(r['avg_ret_1d'])}'>{_pct(r['avg_ret_1d'])}</font>", S_CELL),
        ])
    t = Table(rows, colWidths=[10*mm, 90*mm, 18*mm, 38*mm, 22*mm])
    t.setStyle(_table_style(len(rows)))
    story.append(Spacer(1, 3*mm))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _add_volume_surge(story: list, df: pd.DataFrame):
    _section("Top 5 Volume Surge", "Highest volume vs 20-day average", story)
    chart = _chart_volume_surge(df)
    if chart:
        story.append(chart)

    if df.empty:
        story.append(Paragraph("No volume surge data.", S_CELL))
        return

    header = _th("#", "Stock", "CMP", "1D Return", "Today Vol", "20D Avg", "Surge")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        rows.append([
            Paragraph(str(i), S_CELL),
            _stock_cell_pdf(r["symbol"], r["name"],
                            r.get("tradingview_url"), r.get("screener_url")),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELB),
            Paragraph(_vol(r["today_vol"]),   S_CELL),
            Paragraph(_vol(r["avg_vol_20d"]), S_CELL),
            Paragraph(f"<font color='#1d4ed8'><b>{float(r['surge_ratio']):.1f}x</b></font>", S_CELB),
        ])
    t = Table(rows, colWidths=[8*mm, 72*mm, 22*mm, 22*mm, 20*mm, 20*mm, 14*mm])
    t.setStyle(_table_style(len(rows)))
    story.append(Spacer(1, 3*mm))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _add_earnings(story: list, df: pd.DataFrame):
    _section("Quarterly Results — Big Movers", "Announced results today with > 5% move", story)
    if df.empty:
        story.append(Paragraph("No quarterly result stocks moved more than 5% today.", S_CELL))
        story.append(Spacer(1, 4*mm))
        return

    header = _th("#", "Stock", "CMP", "1D Return", "Market Cap")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        rows.append([
            Paragraph(str(i), S_CELL),
            _stock_cell_pdf(r["symbol"], r["name"],
                            r.get("tradingview_url"), r.get("screener_url")),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELB),
            Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
        ])
    t = Table(rows, colWidths=[8*mm, 90*mm, 24*mm, 26*mm, 26*mm])
    t.setStyle(_table_style(len(rows)))
    story.append(t)
    story.append(Spacer(1, 4*mm))


def _add_movers(story: list, df: pd.DataFrame, title: str, is_gainers: bool):
    subtitle = "Best performing stocks today" if is_gainers else "Worst performing stocks today"
    _section(title, subtitle, story)
    chart = _chart_movers(df, title, is_gainers)
    if chart:
        story.append(chart)

    if df.empty:
        story.append(Paragraph("No data.", S_CELL))
        return

    header = _th("#", "Stock", "CMP", "1D Return", "Market Cap")
    rows = [header]
    accent = "#16a34a" if is_gainers else "#dc2626"
    for rank, (_, r) in enumerate(df.iterrows(), 1):
        rows.append([
            Paragraph(f"<font color='{accent}'><b>{rank}</b></font>", S_CELB),
            _stock_cell_pdf(r["symbol"], r["name"],
                            r.get("tradingview_url"), r.get("screener_url")),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{accent}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELB),
            Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
        ])
    row_bg = _GREEN_XL if is_gainers else _RED_XL
    t = Table(rows, colWidths=[10*mm, 92*mm, 22*mm, 26*mm, 18*mm])
    ts = _table_style(len(rows), alt_bg=row_bg)
    t.setStyle(ts)
    story.append(Spacer(1, 3*mm))
    story.append(t)
    story.append(Spacer(1, 4*mm))


# ── Main entry ────────────────────────────────────────────────────────────────

def generate_pdf(
    as_of: date,
    breadth:       dict,
    idx_breadth:   pd.DataFrame,
    themes_df:     pd.DataFrame,
    vol_df:        pd.DataFrame,
    earnings_df:   pd.DataFrame,
    gainers_df:    pd.DataFrame,
    losers_df:     pd.DataFrame,
    rs_leaders_df: pd.DataFrame | None = None,
    edition_num:   int | None = None,
) -> str:
    tmp_dir = Path(__file__).parent.parent / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    path = str(tmp_dir / f"stockstack_digest_{as_of.isoformat()}.pdf")

    doc = BaseDocTemplate(
        path,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=22*mm,   bottomMargin=14*mm,
        title=f"StockStack Daily Digest — {as_of.strftime('%d %b %Y')}",
        author="StockStack",
    )
    doc.digest_date = as_of   # picked up by _draw_page
    doc.edition_num = edition_num

    frame = Frame(MARGIN, 14*mm, USABLE_W, PAGE_H - 22*mm - 14*mm, id="body")
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame], onPage=_draw_page)])

    story = []
    _add_executive_summary(story, breadth, themes_df, vol_df, gainers_df, losers_df, earnings_df)
    _add_breadth(story, breadth)
    _add_index_breadth(story, idx_breadth)
    if rs_leaders_df is not None and not rs_leaders_df.empty:
        _add_rs_leaders(story, rs_leaders_df)
    _add_themes(story, themes_df)
    _add_volume_surge(story, vol_df)
    _add_earnings(story, earnings_df)
    _add_movers(story, gainers_df, "Top 10 Gainers", is_gainers=True)
    _add_movers(story, losers_df,  "Top 10 Losers",  is_gainers=False)

    doc.build(story)
    return path
