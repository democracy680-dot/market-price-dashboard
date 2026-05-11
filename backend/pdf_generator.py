"""
pdf_generator.py — StockStack Daily Digest PDF.

Professional newsletter layout for serious investors.
Every section is wrapped in KeepTogether so headings never orphan
from their charts or tables. CondPageBreak before heavy sections
prevents thin slivers of orphaned white space on prior pages.
"""

import io
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
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
    CondPageBreak,
    Frame,
    HRFlowable,
    Image,
    KeepTogether,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

# ── Palette ──────────────────────────────────────────────────────────────────────
_INK       = colors.HexColor("#0c1527")   # near-black body text
_NAVY      = colors.HexColor("#0f172a")   # header bar, heavy text
_NAVY2     = colors.HexColor("#132240")   # table header background
_STEEL     = colors.HexColor("#1e293b")   # paragraph text
_SLATE     = colors.HexColor("#475569")   # muted subtitles
_RULE      = colors.HexColor("#cbd5e1")   # thin rules & borders
_RULE_H    = colors.HexColor("#334155")   # heavy section rule
_ACCENT    = colors.HexColor("#1d4ed8")   # blue links / key numbers
_ACCENT_L  = colors.HexColor("#3b82f6")   # light blue
_GOLD      = colors.HexColor("#b45309")   # amber — section label accent
_GREEN     = colors.HexColor("#15803d")
_GREEN_L   = colors.HexColor("#bbf7d0")
_GREEN_XL  = colors.HexColor("#f0fdf4")
_RED       = colors.HexColor("#b91c1c")
_RED_L     = colors.HexColor("#fecaca")
_RED_XL    = colors.HexColor("#fef2f2")
_BG_STRIPE = colors.HexColor("#f8fafc")
_WHITE     = colors.white

# Matplotlib equivalents
M_NAVY   = "#0f172a"
M_BLUE   = "#3b82f6"
M_GREEN  = "#15803d"
M_RED    = "#b91c1c"
M_AMBER  = "#d97706"
M_MUTED  = "#475569"
M_BG     = "#ffffff"
M_BORDER = "#cbd5e1"
M_200DMA = "#d97706"
M_50DMA  = "#3b82f6"

# ── Layout ───────────────────────────────────────────────────────────────────────
PAGE_W, PAGE_H = A4
MARGIN   = 15 * mm
USABLE_W = PAGE_W - 2 * MARGIN
TOP_H    = 22 * mm    # header bar
BOT_H    = 14 * mm    # footer area
FRAME_H  = PAGE_H - TOP_H - BOT_H   # ≈ 261 mm — usable vertical space per page

# ── Type styles ──────────────────────────────────────────────────────────────────
S_SEC = ParagraphStyle(
    "sec", fontSize=9.5, fontName="Helvetica-Bold",
    textColor=_NAVY, spaceBefore=0, spaceAfter=1, leading=12,
)
S_SEC_SUB = ParagraphStyle(
    "secSub", fontSize=7.5, fontName="Helvetica",
    textColor=_SLATE, spaceAfter=0, leading=10,
)
S_CELL = ParagraphStyle(
    "c", fontSize=8.5, fontName="Helvetica",
    textColor=_STEEL, leading=11,
)
S_CELB = ParagraphStyle(
    "cb", fontSize=8.5, fontName="Helvetica-Bold",
    textColor=_INK, leading=11,
)
S_CELM = ParagraphStyle(
    "cm", fontSize=7.5, fontName="Helvetica",
    textColor=_SLATE, leading=10,
)
S_TH = ParagraphStyle(
    "th", fontSize=8, fontName="Helvetica-Bold",
    textColor=colors.white, leading=10,
)
S_FOOT = ParagraphStyle(
    "ft", fontSize=7, fontName="Helvetica",
    textColor=_SLATE, alignment=TA_CENTER,
)
S_LINK = ParagraphStyle(
    "lnk", fontSize=6.5, fontName="Helvetica",
    textColor=_ACCENT_L, leading=9,
)
S_CARD_L = ParagraphStyle(
    "cl", fontSize=6.5, fontName="Helvetica",
    textColor=_SLATE, alignment=TA_CENTER, spaceAfter=1,
)
S_CARD_V = ParagraphStyle(
    "cv", fontSize=13, fontName="Helvetica-Bold",
    textColor=_INK, alignment=TA_CENTER, leading=15,
)
S_BRIEF_HDR = ParagraphStyle(
    "bh", fontSize=7, fontName="Helvetica-Bold",
    textColor=_GOLD, leading=9, spaceAfter=3,
)
S_BRIEF_BODY = ParagraphStyle(
    "bb", fontSize=8.5, fontName="Helvetica",
    textColor=_STEEL, leading=13,
)


# ── Page template: header + footer ───────────────────────────────────────────────

def _draw_page(canvas, doc):
    canvas.saveState()

    # ── Header bar ──────────────────────────────────────────────────────────────
    canvas.setFillColor(_NAVY)
    canvas.rect(0, PAGE_H - TOP_H, PAGE_W, TOP_H, fill=1, stroke=0)

    # Gold accent stripe (editorial touch)
    canvas.setFillColor(colors.HexColor("#b45309"))
    canvas.rect(0, PAGE_H - TOP_H - 1.2*mm, PAGE_W, 1.2*mm, fill=1, stroke=0)

    # Logo badge
    canvas.setFillColor(_ACCENT_L)
    canvas.roundRect(MARGIN, PAGE_H - 15.5*mm, 10*mm, 10*mm, 2*mm, fill=1, stroke=0)
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 11)
    canvas.drawCentredString(MARGIN + 5*mm, PAGE_H - 11.5*mm, "S")

    # Masthead
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 12)
    canvas.drawString(MARGIN + 12*mm, PAGE_H - 9.5*mm, "StockStack")
    canvas.setFillColor(colors.HexColor("#94a3b8"))
    canvas.setFont("Helvetica", 7)
    canvas.drawString(MARGIN + 12*mm, PAGE_H - 14*mm, "DAILY MARKET DIGEST  ·  NSE INDIA")

    # Date block (right-aligned)
    canvas.setFillColor(_WHITE)
    canvas.setFont("Helvetica-Bold", 10)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 9.5*mm,
                           doc.digest_date.strftime("%d %B %Y"))
    canvas.setFillColor(colors.HexColor("#94a3b8"))
    canvas.setFont("Helvetica", 7)
    canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 14*mm,
                           doc.digest_date.strftime("%A").upper())

    edition_num = getattr(doc, "edition_num", None)
    if edition_num:
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.setFont("Helvetica", 6.5)
        canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 18.5*mm,
                               f"ISSUE #{edition_num}")

    # ── Footer ──────────────────────────────────────────────────────────────────
    canvas.setStrokeColor(_RULE)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, BOT_H - 4*mm, PAGE_W - MARGIN, BOT_H - 4*mm)
    canvas.setFillColor(_SLATE)
    canvas.setFont("Helvetica", 6.5)
    canvas.drawString(MARGIN, BOT_H - 7*mm,
                      "StockStack  ·  Data: NSE / yfinance  ·  Personal use only. Not investment advice.")
    canvas.drawRightString(PAGE_W - MARGIN, BOT_H - 7*mm,
                           f"Page {doc.page}")
    canvas.restoreState()


# ── Helpers ───────────────────────────────────────────────────────────────────────

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
        if v >= 1_00_000: return f"₹{v/1_00_000:.1f}L Cr"
        return f"₹{v:,.0f} Cr"
    except (TypeError, ValueError):
        return "—"

def _gc(val) -> str:
    try:
        return "#15803d" if float(val) > 0 else "#b91c1c"
    except (TypeError, ValueError):
        return "#64748b"

def _th(*labels):
    return [Paragraph(l, S_TH) for l in labels]


def _stock_cell_pdf(symbol: str, name: str, tv_url=None, sc_url=None) -> Paragraph:
    links = []
    if tv_url and str(tv_url).startswith("http"):
        links.append(f'<link href="{tv_url}"><u>TV</u></link>')
    if sc_url and str(sc_url).startswith("http"):
        links.append(f'<link href="{sc_url}"><u>Screener</u></link>')
    links_html = (
        f'  <font color="#3b82f6" size="6">' + "  ·  ".join(links) + "</font>"
    ) if links else ""
    return Paragraph(
        f'<b><font color="#0c1527">{symbol}</font></b>'
        f'  <font color="#475569" size="7.5">{name}</font>'
        + (f"<br/>{links_html}" if links_html else ""),
        S_CELL,
    )


def _section_flowables(title: str, subtitle: str = "") -> list:
    """
    Editorial-style section header: heavy rule, UPPERCASE title, subtitle, thin rule.
    Returns a list of flowables to be prepended to the section's KeepTogether group.
    """
    items = [
        Spacer(1, 5*mm),
        HRFlowable(width=USABLE_W, thickness=1.5, color=_RULE_H,
                   spaceAfter=2*mm, spaceBefore=0),
        Paragraph(title.upper(), S_SEC),
    ]
    if subtitle:
        items.append(Paragraph(subtitle, S_SEC_SUB))
    items.append(HRFlowable(width=USABLE_W, thickness=0.4, color=_RULE,
                             spaceAfter=4*mm, spaceBefore=2*mm))
    return items


def _table_style(n_rows: int, header_bg=None, alt_bg=None):
    hbg = header_bg or _NAVY2
    ts = [
        ("BACKGROUND",    (0, 0), (-1, 0),   hbg),
        ("TEXTCOLOR",     (0, 0), (-1, 0),   colors.white),
        ("FONTNAME",      (0, 0), (-1, 0),   "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, 0),   8),
        ("GRID",          (0, 0), (-1, -1),  0.35, _RULE),
        ("VALIGN",        (0, 0), (-1, -1),  "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1),  3),
        ("BOTTOMPADDING", (0, 0), (-1, -1),  3),
        ("LEFTPADDING",   (0, 0), (-1, -1),  6),
        ("RIGHTPADDING",  (0, 0), (-1, -1),  6),
    ]
    row_alt = alt_bg or _BG_STRIPE
    for i in range(1, n_rows):
        bg = _WHITE if i % 2 == 1 else row_alt
        ts.append(("BACKGROUND", (0, i), (-1, i), bg))
    return TableStyle(ts)


# ── Chart helpers ─────────────────────────────────────────────────────────────────

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


# ── Chart builders ────────────────────────────────────────────────────────────────

def _chart_breadth_donut(b: dict) -> Image:
    adv   = b["advances"]
    dec   = b["declines"]
    unch  = b["unchanged"]
    total = adv + dec + unch
    adv_pct = round(100 * adv / total, 1) if total else 0
    dec_pct = round(100 * dec / total, 1) if total else 0

    fig, ax = plt.subplots(figsize=(3.2, 3.2), facecolor="white")
    sizes  = [adv, dec, unch] if unch > 0 else [adv, dec]
    clrs   = [M_GREEN, M_RED, "#e2e8f0"][:len(sizes)]
    wedges, _ = ax.pie(
        sizes, colors=clrs, startangle=90,
        wedgeprops=dict(width=0.54, edgecolor="white", linewidth=2.5),
    )
    ax.text(0,  0.22, f"{adv:,}", ha="center", va="center",
            fontsize=16, fontweight="bold", color=M_GREEN)
    ax.text(0, -0.02, "Advances", ha="center", va="center",
            fontsize=6.5, color=M_GREEN)
    ax.text(0, -0.28, f"{dec:,}", ha="center", va="center",
            fontsize=16, fontweight="bold", color=M_RED)
    ax.text(0, -0.50, "Declines", ha="center", va="center",
            fontsize=6.5, color=M_RED)
    labels_pct = [f"Adv  {adv_pct:.1f}%", f"Dec  {dec_pct:.1f}%"]
    if unch > 0:
        labels_pct.append(f"Unch  {round(100*unch/total,1):.1f}%")
    leg = ax.legend(wedges, labels_pct[:len(wedges)],
                    loc="lower center", bbox_to_anchor=(0.5, -0.12),
                    ncol=len(wedges), fontsize=6.5, frameon=False,
                    handlelength=1.2, handletextpad=0.4)
    for t in leg.get_texts():
        t.set_color(M_MUTED)
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")
    ax.set_title("Advance / Decline", fontsize=8.5, fontweight="bold",
                 color=M_NAVY, pad=4)
    fig.tight_layout(pad=0.3)
    return _fig_to_image(fig, 65)


def _chart_index_breadth(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    labels = df["index"].tolist()
    pct200 = df["pct_200"].tolist()
    pct50  = df["pct_50"].tolist()
    y, h   = np.arange(len(labels)), 0.30

    fig, ax = plt.subplots(figsize=(6.8, 2.2), facecolor="white")
    ax.set_facecolor("white")
    bars200 = ax.barh(y + h/2, pct200, h, color=M_200DMA, alpha=0.88, label="Above 200 DMA")
    bars50  = ax.barh(y - h/2, pct50,  h, color=M_50DMA,  alpha=0.88, label="Above 50 DMA")
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
    _setup_ax(ax, "Index Breadth — % Stocks Above Key Moving Averages")
    p200 = mpatches.Patch(color=M_200DMA, label="Above 200 DMA")
    p50  = mpatches.Patch(color=M_50DMA,  label="Above 50 DMA")
    leg  = ax.legend(handles=[p200, p50], fontsize=6.5, loc="lower right",
                     frameon=True, facecolor="white", edgecolor=M_BORDER)
    for t in leg.get_texts(): t.set_color(M_MUTED)
    fig.tight_layout(pad=0.5)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_sector_breadth(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    labels = df["index"].tolist()
    pct200 = df["pct_200"].tolist()
    pct50  = df["pct_50"].tolist()
    y, h   = np.arange(len(labels)), 0.30

    # Cap chart height so heading + chart + table fits within one page frame
    chart_h = max(3.0, len(labels) * 0.33)
    fig, ax = plt.subplots(figsize=(6.8, chart_h), facecolor="white")
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
    _setup_ax(ax, "Sector Breadth — % Stocks Above Key Moving Averages")
    p200 = mpatches.Patch(color=M_200DMA, label="Above 200 DMA")
    p50  = mpatches.Patch(color=M_50DMA,  label="Above 50 DMA")
    leg  = ax.legend(handles=[p200, p50], fontsize=6.5, loc="lower right",
                     frameon=True, facecolor="white", edgecolor=M_BORDER)
    for t in leg.get_texts(): t.set_color(M_MUTED)
    fig.tight_layout(pad=0.5)
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
    bars = ax.barh(y, vals, 0.52, color=clrs, alpha=0.85)
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
    fig.tight_layout(pad=0.5)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_volume_surge(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    syms   = df["symbol"].tolist()
    surges = df["surge_ratio"].tolist()

    fig, ax = plt.subplots(figsize=(6.8, 2.0), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, surges, 0.52, color=M_BLUE, alpha=0.82)
    for bar, v in zip(bars, surges):
        ax.text(v + 0.1, bar.get_y() + bar.get_height()/2,
                f"{v:.1f}x", va="center", fontsize=7, color=M_NAVY, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels(syms, fontsize=8, color=M_NAVY, fontweight="bold")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}x"))
    _setup_ax(ax, "Volume Surge vs 20-Day Average")
    fig.tight_layout(pad=0.5)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_movers(df: pd.DataFrame, title: str, is_gainers: bool) -> Image:
    if df.empty:
        return None
    syms = df["symbol"].tolist()
    vals = df["ret_1d_pct"].tolist()
    clr  = M_GREEN if is_gainers else M_RED

    fig, ax = plt.subplots(figsize=(6.8, 3.0), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, vals, 0.55, color=clr, alpha=0.82)
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
    fig.tight_layout(pad=0.5)
    return _fig_to_image(fig, USABLE_W / mm)


def _chart_rs_leaders(df: pd.DataFrame) -> Image:
    if df.empty:
        return None
    syms = df["symbol"].tolist()
    vals = df["rs_1m"].tolist()

    fig, ax = plt.subplots(figsize=(6.8, 3.0), facecolor="white")
    ax.set_facecolor("white")
    y    = np.arange(len(syms))
    bars = ax.barh(y, vals, 0.55, color=M_BLUE, alpha=0.82)
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
    fig.tight_layout(pad=0.5)
    return _fig_to_image(fig, USABLE_W / mm)


# ── Section builders ──────────────────────────────────────────────────────────────
# Each builder appends exactly ONE KeepTogether to story. CondPageBreak before
# large sections ensures the prior page never ends with a thin orphaned sliver.

def _add_executive_summary(story: list, breadth: dict, themes_df: pd.DataFrame,
                            vol_df: pd.DataFrame, gainers_df: pd.DataFrame,
                            losers_df: pd.DataFrame, earnings_df: pd.DataFrame):
    adv   = breadth["advances"]
    dec   = breadth["declines"]
    p200  = breadth["pct_above_200"]
    ratio = adv / (adv + dec) if (adv + dec) > 0 else 0.5
    tone  = "Bullish" if ratio >= 0.55 else ("Bearish" if ratio <= 0.45 else "Neutral")
    tone_color = "#15803d" if tone == "Bullish" else ("#b91c1c" if tone == "Bearish" else "#d97706")

    bullets = []
    bullets.append(
        f'<font color="{tone_color}"><b>Market:</b></font> Closed <b>{tone}</b> — '
        f'{adv:,} advances vs {dec:,} declines &nbsp;·&nbsp; '
        f'<font color="#1d4ed8"><b>{p200:.1f}%</b></font> stocks above 200 DMA'
    )
    if not themes_df.empty:
        t = themes_df.iloc[0]
        tc   = "#15803d" if float(t["avg_ret_1w"]) > 0 else "#b91c1c"
        sign = "+" if float(t["avg_ret_1w"]) > 0 else ""
        bullets.append(
            f'<b>Top Theme:</b> {t["theme_name"]} — avg '
            f'<font color="{tc}"><b>{sign}{float(t["avg_ret_1w"]):.2f}%</b></font> (1-week)'
        )
    if not vol_df.empty:
        vs = vol_df.iloc[0]
        bullets.append(
            f'<b>Volume Surge:</b> <b>{vs["symbol"]}</b> at '
            f'<font color="#1d4ed8"><b>{float(vs["surge_ratio"]):.1f}x</b></font> normal volume'
        )
    if not gainers_df.empty and not losers_df.empty:
        g = gainers_df.iloc[0]
        lo = losers_df.iloc[0]
        bullets.append(
            f'<b>Top Movers:</b> '
            f'<font color="#15803d"><b>{g["symbol"]} +{float(g["ret_1d_pct"]):.2f}%</b></font>'
            f'  &nbsp;·&nbsp;  '
            f'<font color="#b91c1c"><b>{lo["symbol"]} {float(lo["ret_1d_pct"]):.2f}%</b></font>'
        )
    if not earnings_df.empty:
        e = earnings_df.iloc[0]
        ec   = "#15803d" if float(e["ret_1d_pct"]) > 0 else "#b91c1c"
        sign = "+" if float(e["ret_1d_pct"]) > 0 else ""
        bullets.append(
            f'<b>Results Mover:</b> <b>{e["symbol"]}</b> moved '
            f'<font color="{ec}"><b>{sign}{float(e["ret_1d_pct"]):.2f}%</b></font> post-earnings'
        )

    # Tone badge alongside section label
    tone_badge_color = "#15803d" if tone == "Bullish" else ("#b91c1c" if tone == "Bearish" else "#d97706")

    items = [
        Spacer(1, 3*mm),
        # Section label row: label on left, tone badge on right
        Table(
            [[
                Paragraph("TODAY'S BRIEFING", S_BRIEF_HDR),
                Paragraph(
                    f'<font color="{tone_badge_color}"><b>◆ {tone.upper()}</b></font>',
                    ParagraphStyle("tone", fontSize=7, fontName="Helvetica-Bold",
                                   textColor=colors.HexColor(tone_badge_color),
                                   alignment=TA_RIGHT, leading=9),
                ),
            ]],
            colWidths=[USABLE_W * 0.6, USABLE_W * 0.4],
        ),
    ]
    items[-1].setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    rows = [[Paragraph(f"• &nbsp; {b}", S_BRIEF_BODY)] for b in bullets]
    briefing_table = Table(rows, colWidths=[USABLE_W])
    briefing_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor("#f0f9ff")),
        ("BOX",           (0, 0), (-1, -1), 0.8, colors.HexColor("#1d4ed8")),
        ("LINEBELOW",     (0, 0), (-1, -2), 0.35, colors.HexColor("#dde8f8")),
        ("LEFTPADDING",   (0, 0), (-1, -1), 12),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    items.append(briefing_table)
    items.append(Spacer(1, 5*mm))
    story.append(KeepTogether(items))


def _add_breadth(story: list, b: dict):
    items = _section_flowables(
        "Market Breadth",
        "Advance/decline split, index returns, and DMA strength",
    )
    donut = _chart_breadth_donut(b)

    n50  = b.get("nifty50_ret")
    bank = b.get("bank_ret")
    p200 = b["pct_above_200"]
    adv  = b["advances"]
    dec  = b["declines"]
    total = b["total"]

    def _card(label, value, val_color, bg, border):
        lbl = Paragraph(label, S_CARD_L)
        val = Paragraph(f'<font color="{val_color}"><b>{value}</b></font>', S_CARD_V)
        inner = Table([[lbl], [val]], colWidths=[None])
        inner.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), colors.HexColor(bg)),
            ("BOX",           (0, 0), (-1, -1), 0.6, colors.HexColor(border)),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("ALIGN",         (0, 0), (-1, -1), "CENTER"),
        ]))
        return inner

    def _ret_triplet(val):
        if val is None:
            return "—", "#64748b", "#f8fafc", "#e2e8f0"
        color  = "#15803d" if val > 0 else "#b91c1c"
        bg     = "#f0fdf4" if val > 0 else "#fef2f2"
        border = "#bbf7d0" if val > 0 else "#fecaca"
        return f"{'+'if val>0 else ''}{val:.2f}%", color, bg, border

    n50_val, n50_col, n50_bg, n50_brd   = _ret_triplet(n50)
    bnk_val, bnk_col, bnk_bg, bnk_brd   = _ret_triplet(bank)
    dma_pct = f"{p200:.1f}%"
    dma_col = "#1d4ed8" if p200 >= 60 else ("#d97706" if p200 >= 40 else "#b91c1c")

    cw = (USABLE_W - 68*mm - 3*mm) / 2
    cards = Table([
        [_card("Nifty 50",      n50_val,      n50_col,  n50_bg,  n50_brd),
         _card("Nifty Bank",    bnk_val,      bnk_col,  bnk_bg,  bnk_brd)],
        [_card("Above 200 DMA", dma_pct,      dma_col,  "#eff6ff", "#bfdbfe"),
         _card("Tracked",       f"{total:,}", "#334155", "#f8fafc", "#e2e8f0")],
        [_card("Advances",      f"{adv:,}",   "#15803d", "#f0fdf4", "#bbf7d0"),
         _card("Declines",      f"{dec:,}",   "#b91c1c", "#fef2f2", "#fecaca")],
    ], colWidths=[cw, cw], rowHeights=[17*mm, 17*mm, 17*mm])
    cards.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 2),
        ("TOPPADDING",    (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))

    row = Table([[donut, cards]], colWidths=[68*mm, USABLE_W - 68*mm])
    row.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    items.append(row)

    # A/D progress bar
    items.append(Spacer(1, 3*mm))
    adv_w = int(USABLE_W * adv / total) if total else 0
    bar = Table([["", ""]], colWidths=[adv_w, USABLE_W - adv_w], rowHeights=[3*mm])
    bar.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (0, 0), colors.HexColor("#15803d")),
        ("BACKGROUND",    (1, 0), (1, 0), colors.HexColor("#b91c1c")),
        ("LEFTPADDING",   (0, 0), (-1, -1), 0),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    items.append(bar)
    items.append(Spacer(1, 4*mm))

    story.append(KeepTogether(items))


def _add_index_breadth(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "Index Breadth",
        "% of stocks above 50 DMA and 200 DMA across major indexes",
    )
    chart = _chart_index_breadth(df)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No data.", S_CELL))
    else:
        header = _th("Index", "Total", "Above 200 DMA", "Below 200", "% 200",
                     "Above 50 DMA", "Below 50", "% 50")
        rows = [header]
        for _, r in df.iterrows():
            p200 = float(r["pct_200"]); p50 = float(r["pct_50"])
            c200 = "#15803d" if p200>=60 else ("#b91c1c" if p200<40 else "#d97706")
            c50  = "#15803d" if p50>=60  else ("#b91c1c" if p50<40  else "#d97706")
            rows.append([
                Paragraph(f"<b>{r['index']}</b>",  S_CELB),
                Paragraph(str(int(r["total"])),     S_CELL),
                Paragraph(f"<font color='#15803d'><b>{int(r['above_200'])}</b></font>", S_CELB),
                Paragraph(f"<font color='#b91c1c'>{int(r['below_200'])}</font>",         S_CELL),
                Paragraph(f"<font color='{c200}'><b>{p200:.0f}%</b></font>",             S_CELB),
                Paragraph(f"<font color='#15803d'><b>{int(r['above_50'])}</b></font>",   S_CELB),
                Paragraph(f"<font color='#b91c1c'>{int(r['below_50'])}</font>",           S_CELL),
                Paragraph(f"<font color='{c50}'><b>{p50:.0f}%</b></font>",               S_CELB),
            ])
        cw = [36*mm, 16*mm, 26*mm, 18*mm, 14*mm, 26*mm, 18*mm, 14*mm]
        t  = Table(rows, colWidths=cw)
        t.setStyle(_table_style(len(rows)))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    # CondPageBreak: push to next page if less than 90 mm remains
    story.append(CondPageBreak(90*mm))
    story.append(KeepTogether(items))


def _add_sector_breadth(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "Sector Breadth",
        "% of stocks above 50 DMA and 200 DMA across all sectors",
    )
    chart = _chart_sector_breadth(df)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No sector breadth data.", S_CELL))
    else:
        header = _th("Sector", "Total", "Above 200 DMA", "Below 200", "% 200",
                     "Above 50 DMA", "Below 50", "% 50")
        rows = [header]
        for _, r in df.iterrows():
            p200 = float(r["pct_200"]); p50 = float(r["pct_50"])
            c200 = "#15803d" if p200>=60 else ("#b91c1c" if p200<40 else "#d97706")
            c50  = "#15803d" if p50>=60  else ("#b91c1c" if p50<40  else "#d97706")
            rows.append([
                Paragraph(f"<b>{r['index']}</b>",  S_CELB),
                Paragraph(str(int(r["total"])),     S_CELL),
                Paragraph(f"<font color='#15803d'><b>{int(r['above_200'])}</b></font>", S_CELB),
                Paragraph(f"<font color='#b91c1c'>{int(r['below_200'])}</font>",         S_CELL),
                Paragraph(f"<font color='{c200}'><b>{p200:.0f}%</b></font>",             S_CELB),
                Paragraph(f"<font color='#15803d'><b>{int(r['above_50'])}</b></font>",   S_CELB),
                Paragraph(f"<font color='#b91c1c'>{int(r['below_50'])}</font>",           S_CELL),
                Paragraph(f"<font color='{c50}'><b>{p50:.0f}%</b></font>",               S_CELB),
            ])
        cw = [36*mm, 16*mm, 26*mm, 18*mm, 14*mm, 26*mm, 18*mm, 14*mm]
        t  = Table(rows, colWidths=cw)
        t.setStyle(_table_style(len(rows)))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(120*mm))
    story.append(KeepTogether(items))


def _add_rs_leaders(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "RS Leaders",
        "Top 10 stocks outperforming Nifty 50 on a 1-month basis",
    )
    chart = _chart_rs_leaders(df)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No relative strength data.", S_CELL))
    else:
        header = _th("#", "Stock", "CMP", "1D Return", "1M RS vs Nifty", "RS Bucket")
        rows   = [header]
        for rank, (_, r) in enumerate(df.iterrows(), 1):
            rs_val  = float(r["rs_1m"])
            rs_col  = "#15803d" if rs_val > 0 else "#b91c1c"
            rs_sign = "+" if rs_val > 0 else ""
            rows.append([
                Paragraph(f"<font color='#1d4ed8'><b>{rank}</b></font>", S_CELB),
                _stock_cell_pdf(r["symbol"], r["name"],
                                r.get("tradingview_url"), r.get("screener_url")),
                Paragraph(f"₹{float(r['cmp']):.2f}", S_CELL),
                Paragraph(
                    f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>",
                    S_CELB),
                Paragraph(
                    f"<font color='{rs_col}'><b>{rs_sign}{rs_val:.1f}%</b></font>",
                    S_CELB),
                Paragraph(str(r.get("rs_1m_bucket") or "—"), S_CELL),
            ])
        t = Table(rows, colWidths=[10*mm, 76*mm, 24*mm, 24*mm, 28*mm, 16*mm])
        t.setStyle(_table_style(len(rows)))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(100*mm))
    story.append(KeepTogether(items))


def _add_themes(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "Top 5 Themes — Weekly",
        "Best performing thematic baskets by average 1-week member return",
    )
    chart = _chart_themes(df)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No theme data.", S_CELL))
    else:
        header = _th("#", "Theme", "Stocks", "1W Avg Return", "1D Avg Return")
        rows = [header]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rows.append([
                Paragraph(str(i), S_CELL),
                Paragraph(f"<b>{r['theme_name']}</b>", S_CELB),
                Paragraph(str(int(r["stock_count"])), S_CELL),
                Paragraph(
                    f"<font color='{_gc(r['avg_ret_1w'])}'><b>{_pct(r['avg_ret_1w'])}</b></font>",
                    S_CELB),
                Paragraph(
                    f"<font color='{_gc(r['avg_ret_1d'])}'>{_pct(r['avg_ret_1d'])}</font>",
                    S_CELL),
            ])
        t = Table(rows, colWidths=[10*mm, 92*mm, 18*mm, 38*mm, 20*mm])
        t.setStyle(_table_style(len(rows)))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(80*mm))
    story.append(KeepTogether(items))


def _add_volume_surge(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "Top 5 Volume Surge",
        "Highest volume vs 20-day average — unusual institutional activity",
    )
    chart = _chart_volume_surge(df)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No volume surge data.", S_CELL))
    else:
        header = _th("#", "Stock", "CMP", "1D Return", "Today Vol", "20D Avg", "Surge")
        rows = [header]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rows.append([
                Paragraph(str(i), S_CELL),
                _stock_cell_pdf(r["symbol"], r["name"],
                                r.get("tradingview_url"), r.get("screener_url")),
                Paragraph(f"₹{float(r['cmp']):.2f}", S_CELL),
                Paragraph(
                    f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>",
                    S_CELB),
                Paragraph(_vol(r["today_vol"]),   S_CELL),
                Paragraph(_vol(r["avg_vol_20d"]), S_CELL),
                Paragraph(
                    f"<font color='#1d4ed8'><b>{float(r['surge_ratio']):.1f}x</b></font>",
                    S_CELB),
            ])
        t = Table(rows, colWidths=[8*mm, 72*mm, 22*mm, 22*mm, 22*mm, 22*mm, 10*mm])
        t.setStyle(_table_style(len(rows)))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(80*mm))
    story.append(KeepTogether(items))


def _add_earnings(story: list, df: pd.DataFrame):
    items = _section_flowables(
        "Quarterly Results — Notable Movers",
        "Stocks that announced results today with move > 5%",
    )
    if df.empty:
        items.append(Paragraph(
            "No quarterly result stocks moved more than 5% today.", S_CELL))
    else:
        header = _th("#", "Stock", "CMP", "1D Return", "Market Cap")
        rows = [header]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rows.append([
                Paragraph(str(i), S_CELL),
                _stock_cell_pdf(r["symbol"], r["name"],
                                r.get("tradingview_url"), r.get("screener_url")),
                Paragraph(f"₹{float(r['cmp']):.2f}", S_CELL),
                Paragraph(
                    f"<font color='{_gc(r['ret_1d_pct'])}'><b>{_pct(r['ret_1d_pct'])}</b></font>",
                    S_CELB),
                Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
            ])
        t = Table(rows, colWidths=[8*mm, 90*mm, 24*mm, 26*mm, 30*mm])
        t.setStyle(_table_style(len(rows)))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(60*mm))
    story.append(KeepTogether(items))


def _add_movers(story: list, df: pd.DataFrame, title: str, is_gainers: bool):
    subtitle = "Best performing stocks today (1-day return)" if is_gainers \
               else "Worst performing stocks today (1-day return)"
    items = _section_flowables(title, subtitle)
    chart = _chart_movers(df, title, is_gainers)
    if chart:
        items.append(chart)

    if df.empty:
        items.append(Paragraph("No data.", S_CELL))
    else:
        header = _th("#", "Stock", "CMP", "1D Return", "Market Cap")
        rows = [header]
        accent = "#15803d" if is_gainers else "#b91c1c"
        for rank, (_, r) in enumerate(df.iterrows(), 1):
            rows.append([
                Paragraph(f"<font color='{accent}'><b>{rank}</b></font>", S_CELB),
                _stock_cell_pdf(r["symbol"], r["name"],
                                r.get("tradingview_url"), r.get("screener_url")),
                Paragraph(f"₹{float(r['cmp']):.2f}", S_CELL),
                Paragraph(
                    f"<font color='{accent}'><b>{_pct(r['ret_1d_pct'])}</b></font>",
                    S_CELB),
                Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
            ])
        row_bg = _GREEN_XL if is_gainers else _RED_XL
        t = Table(rows, colWidths=[10*mm, 92*mm, 24*mm, 26*mm, 26*mm])
        t.setStyle(_table_style(len(rows), alt_bg=row_bg))
        items.append(Spacer(1, 3*mm))
        items.append(t)

    items.append(Spacer(1, 4*mm))
    story.append(CondPageBreak(100*mm))
    story.append(KeepTogether(items))


# ── Main entry ────────────────────────────────────────────────────────────────────

def generate_pdf(
    as_of: date,
    breadth:       dict,
    idx_breadth:   pd.DataFrame,
    themes_df:     pd.DataFrame,
    vol_df:        pd.DataFrame,
    earnings_df:   pd.DataFrame,
    gainers_df:    pd.DataFrame,
    losers_df:     pd.DataFrame,
    rs_leaders_df:      pd.DataFrame | None = None,
    edition_num:        int | None = None,
    sector_breadth_df:  pd.DataFrame | None = None,
) -> str:
    tmp_dir = Path(__file__).parent.parent / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    path = str(tmp_dir / f"stockstack_digest_{as_of.isoformat()}.pdf")

    doc = BaseDocTemplate(
        path,
        pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=TOP_H,   bottomMargin=BOT_H,
        title=f"StockStack Daily Digest — {as_of.strftime('%d %b %Y')}",
        author="StockStack",
    )
    doc.digest_date = as_of
    doc.edition_num = edition_num

    frame = Frame(
        MARGIN, BOT_H, USABLE_W, FRAME_H,
        leftPadding=0, rightPadding=0,
        topPadding=0, bottomPadding=0,
        id="body",
    )
    doc.addPageTemplates([
        PageTemplate(id="main", frames=[frame], onPage=_draw_page)
    ])

    story = []
    _add_executive_summary(story, breadth, themes_df, vol_df,
                           gainers_df, losers_df, earnings_df)
    _add_breadth(story, breadth)
    _add_index_breadth(story, idx_breadth)
    if sector_breadth_df is not None and not sector_breadth_df.empty:
        _add_sector_breadth(story, sector_breadth_df)
    if rs_leaders_df is not None and not rs_leaders_df.empty:
        _add_rs_leaders(story, rs_leaders_df)
    _add_themes(story, themes_df)
    _add_volume_surge(story, vol_df)
    _add_earnings(story, earnings_df)
    _add_movers(story, gainers_df, "Top 10 Gainers", is_gainers=True)
    _add_movers(story, losers_df,  "Top 10 Losers",  is_gainers=False)

    doc.build(story)
    return path
