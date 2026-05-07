"""
pdf_generator.py — Generates a StockStack Daily Digest PDF using reportlab.

Called from email_digest.py after all data is fetched.
Returns the path of the saved PDF file (in .tmp/).
"""

import os
from datetime import date
from pathlib import Path

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# ── Colour palette ─────────────────────────────────────────────────────────────
_NAVY       = colors.HexColor("#0f172a")
_BLUE       = colors.HexColor("#1d4ed8")
_BLUE_LIGHT = colors.HexColor("#3b82f6")
_GREEN      = colors.HexColor("#16a34a")
_RED        = colors.HexColor("#dc2626")
_AMBER      = colors.HexColor("#d97706")
_MUTED      = colors.HexColor("#64748b")
_BORDER     = colors.HexColor("#e2e8f0")
_BG_LIGHT   = colors.HexColor("#f8fafc")
_BG_GREEN   = colors.HexColor("#f0fdf4")
_BG_RED     = colors.HexColor("#fef2f2")
_WHITE      = colors.white


# ── Styles ─────────────────────────────────────────────────────────────────────
_base = getSampleStyleSheet()

S_TITLE    = ParagraphStyle("title",    fontSize=18, fontName="Helvetica-Bold",
                             textColor=_WHITE,   spaceAfter=2)
S_SUBTITLE = ParagraphStyle("subtitle", fontSize=9,  fontName="Helvetica",
                             textColor=colors.HexColor("#93c5fd"), spaceAfter=0)
S_SECTION  = ParagraphStyle("section",  fontSize=11, fontName="Helvetica-Bold",
                             textColor=_NAVY,    spaceBefore=14, spaceAfter=4)
S_SUB      = ParagraphStyle("sub",      fontSize=8,  fontName="Helvetica",
                             textColor=_MUTED,   spaceAfter=6)
S_CELL     = ParagraphStyle("cell",     fontSize=9,  fontName="Helvetica",     leading=12)
S_CELL_B   = ParagraphStyle("cell_b",   fontSize=9,  fontName="Helvetica-Bold", leading=12)
S_CELL_SM  = ParagraphStyle("cell_sm",  fontSize=7,  fontName="Helvetica",
                             textColor=_MUTED,   leading=10)
S_FOOTER   = ParagraphStyle("footer",   fontSize=7,  fontName="Helvetica",
                             textColor=_MUTED,   alignment=TA_CENTER)

TH_STYLE = ParagraphStyle("th", fontSize=7, fontName="Helvetica-Bold",
                           textColor=_MUTED, spaceAfter=0,
                           spaceBefore=0)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _pct(val, prefix: bool = True) -> str:
    try:
        v = float(val)
        sign = "+" if v > 0 else ""
        p = f"{sign}{v:.2f}%"
        return p
    except (TypeError, ValueError):
        return "—"


def _vol(val) -> str:
    try:
        v = int(val)
        if v >= 10_000_000:
            return f"{v/10_000_000:.1f} Cr"
        if v >= 100_000:
            return f"{v/100_000:.1f} L"
        return f"{v:,}"
    except (TypeError, ValueError):
        return "—"


def _mcap(val) -> str:
    try:
        v = float(val)
        if v >= 1_00_000:
            return f"Rs.{v/1_00_000:.1f}L Cr"
        return f"Rs.{v:,.0f} Cr"
    except (TypeError, ValueError):
        return "—"


def _pct_color(val) -> colors.Color:
    try:
        return _GREEN if float(val) > 0 else _RED
    except (TypeError, ValueError):
        return _MUTED


def _th(*labels) -> list:
    return [Paragraph(l, TH_STYLE) for l in labels]


def _section_header(title: str, subtitle: str, story: list):
    story.append(HRFlowable(width="100%", thickness=0.5, color=_BORDER, spaceAfter=6))
    story.append(Paragraph(title, S_SECTION))
    if subtitle:
        story.append(Paragraph(subtitle, S_SUB))


# ── Section builders ──────────────────────────────────────────────────────────

def _add_breadth(story: list, b: dict):
    _section_header("Market Breadth", "Overall advance/decline and index returns", story)

    total    = b["total"]
    advances = b["advances"]
    declines = b["declines"]
    pct_200  = b["pct_above_200"]
    n50      = b["nifty50_ret"]
    bank     = b["bank_ret"]

    def _idx(label, ret):
        if ret is None:
            return f"{label}: —"
        arrow = "▲" if ret > 0 else "▼"
        return f"{label}: {arrow} {_pct(ret)}"

    data = [
        [Paragraph("Advances", TH_STYLE),
         Paragraph("Declines", TH_STYLE),
         Paragraph("% Above 200 DMA", TH_STYLE),
         Paragraph("Nifty 50", TH_STYLE),
         Paragraph("Nifty Bank", TH_STYLE)],
        [Paragraph(f"<font color='#16a34a'><b>{advances:,}</b></font>", S_CELL_B),
         Paragraph(f"<font color='#dc2626'><b>{declines:,}</b></font>", S_CELL_B),
         Paragraph(f"<b>{pct_200:.1f}%</b>", S_CELL_B),
         Paragraph(f"<font color='{'#16a34a' if n50 and n50 > 0 else '#dc2626'}'><b>{_pct(n50)}</b></font>", S_CELL_B) if n50 is not None else Paragraph("—", S_CELL),
         Paragraph(f"<font color='{'#16a34a' if bank and bank > 0 else '#dc2626'}'><b>{_pct(bank)}</b></font>", S_CELL_B) if bank is not None else Paragraph("—", S_CELL)],
    ]
    t = Table(data, colWidths=[32*mm, 32*mm, 38*mm, 38*mm, 38*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",       (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [_WHITE]),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


def _add_index_breadth(story: list, df: pd.DataFrame):
    _section_header("Index Breadth", "Stocks above / below 50 DMA and 200 DMA across major indexes", story)
    if df.empty:
        story.append(Paragraph("No data available.", S_CELL))
        return

    header = _th("Index", "Total", "Above 200 DMA", "Below 200 DMA", "% 200", "Above 50 DMA", "Below 50 DMA", "% 50")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows()):
        p200 = float(r["pct_200"])
        p50  = float(r["pct_50"])
        c200 = "#16a34a" if p200 >= 60 else ("#dc2626" if p200 < 40 else "#d97706")
        c50  = "#16a34a" if p50  >= 60 else ("#dc2626" if p50  < 40 else "#d97706")
        rows.append([
            Paragraph(f"<b>{r['index']}</b>", S_CELL_B),
            Paragraph(str(int(r["total"])),    S_CELL),
            Paragraph(f"<font color='#16a34a'><b>{int(r['above_200'])}</b></font>", S_CELL_B),
            Paragraph(f"<font color='#dc2626'>{int(r['below_200'])}</font>", S_CELL),
            Paragraph(f"<font color='{c200}'><b>{p200:.0f}%</b></font>", S_CELL_B),
            Paragraph(f"<font color='#16a34a'><b>{int(r['above_50'])}</b></font>", S_CELL_B),
            Paragraph(f"<font color='#dc2626'>{int(r['below_50'])}</font>", S_CELL),
            Paragraph(f"<font color='{c50}'><b>{p50:.0f}%</b></font>", S_CELL_B),
        ])

    col_w = [38*mm, 18*mm, 26*mm, 26*mm, 18*mm, 26*mm, 26*mm, 18*mm]
    t = Table(rows, colWidths=col_w)
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",          (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, _BG_LIGHT]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


def _add_themes(story: list, df: pd.DataFrame):
    _section_header("Top 5 Themes", "Best performing thematic baskets today", story)
    if df.empty:
        story.append(Paragraph("No theme data available.", S_CELL))
        return

    header = _th("#", "Theme", "Stocks", "1D Avg Return", "30D Avg Return")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        c1d = "#16a34a" if float(r["avg_ret_1d"]) > 0 else "#dc2626"
        rows.append([
            Paragraph(str(i), S_CELL),
            Paragraph(f"<b>{r['theme_name']}</b>", S_CELL_B),
            Paragraph(str(int(r["stock_count"])), S_CELL),
            Paragraph(f"<font color='{c1d}'><b>{_pct(r['avg_ret_1d'])}</b></font>", S_CELL_B),
            Paragraph(f"{_pct(r['avg_ret_30d'])}", S_CELL),
        ])

    t = Table(rows, colWidths=[10*mm, 70*mm, 20*mm, 40*mm, 38*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",          (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, _BG_LIGHT]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


def _add_volume_surge(story: list, df: pd.DataFrame):
    _section_header("Top 5 Volume Surge", "Highest volume vs 20-day average", story)
    if df.empty:
        story.append(Paragraph("No volume surge data available.", S_CELL))
        return

    header = _th("#", "Symbol", "Company", "CMP", "1D Return", "Today Vol", "20D Avg", "Surge")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        c = "#16a34a" if float(r["ret_1d_pct"]) > 0 else "#dc2626"
        rows.append([
            Paragraph(str(i), S_CELL),
            Paragraph(f"<b>{r['symbol']}</b>", S_CELL_B),
            Paragraph(str(r["name"]), S_CELL),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{c}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELL_B),
            Paragraph(_vol(r["today_vol"]), S_CELL),
            Paragraph(_vol(r["avg_vol_20d"]), S_CELL),
            Paragraph(f"<font color='#1d4ed8'><b>{float(r['surge_ratio']):.1f}x</b></font>", S_CELL_B),
        ])

    t = Table(rows, colWidths=[8*mm, 22*mm, 48*mm, 22*mm, 22*mm, 22*mm, 22*mm, 16*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",          (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, _BG_LIGHT]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


def _add_earnings(story: list, df: pd.DataFrame):
    _section_header("Quarterly Results — Big Movers", "Announced results today and moved > 5%", story)
    if df.empty:
        story.append(Paragraph("No quarterly result stocks moved more than 5% today.", S_CELL))
        story.append(Spacer(1, 4*mm))
        return

    header = _th("#", "Symbol", "Company", "CMP", "1D Return", "Market Cap")
    rows = [header]
    for i, (_, r) in enumerate(df.iterrows(), 1):
        c = "#16a34a" if float(r["ret_1d_pct"]) > 0 else "#dc2626"
        rows.append([
            Paragraph(str(i), S_CELL),
            Paragraph(f"<b>{r['symbol']}</b>", S_CELL_B),
            Paragraph(str(r["name"]), S_CELL),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{c}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELL_B),
            Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
        ])

    t = Table(rows, colWidths=[8*mm, 22*mm, 60*mm, 24*mm, 28*mm, 36*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",          (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, _BG_LIGHT]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


def _add_movers(story: list, df: pd.DataFrame, title: str, is_gainers: bool):
    subtitle = "Best performing stocks today" if is_gainers else "Worst performing stocks today"
    _section_header(title, subtitle, story)
    if df.empty:
        story.append(Paragraph("No data available.", S_CELL))
        return

    header = _th("#", "Symbol", "Company", "CMP", "1D Return", "Market Cap")
    rows = [header]
    accent = "#16a34a" if is_gainers else "#dc2626"
    for rank, (_, r) in enumerate(df.iterrows(), 1):
        rows.append([
            Paragraph(f"<font color='{accent}'><b>{rank}</b></font>", S_CELL_B),
            Paragraph(f"<b>{r['symbol']}</b>", S_CELL_B),
            Paragraph(str(r["name"]), S_CELL),
            Paragraph(f"Rs.{float(r['cmp']):.2f}", S_CELL),
            Paragraph(f"<font color='{accent}'><b>{_pct(r['ret_1d_pct'])}</b></font>", S_CELL_B),
            Paragraph(_mcap(r["market_cap_cr"]), S_CELL),
        ])

    row_bg = _BG_GREEN if is_gainers else _BG_RED
    t = Table(rows, colWidths=[10*mm, 24*mm, 72*mm, 24*mm, 28*mm, 42*mm])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0), _BG_LIGHT),
        ("GRID",          (0, 0), (-1, -1), 0.4, _BORDER),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, row_bg]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))


# ── Main entry point ──────────────────────────────────────────────────────────

def generate_pdf(
    as_of: date,
    breadth:     dict,
    idx_breadth: pd.DataFrame,
    themes_df:   pd.DataFrame,
    vol_df:      pd.DataFrame,
    earnings_df: pd.DataFrame,
    gainers_df:  pd.DataFrame,
    losers_df:   pd.DataFrame,
) -> str:
    """
    Build the digest PDF and save to .tmp/.
    Returns the absolute path of the generated file.
    """
    tmp_dir = Path(__file__).parent.parent / ".tmp"
    tmp_dir.mkdir(exist_ok=True)
    filename = f"stockstack_digest_{as_of.isoformat()}.pdf"
    path = str(tmp_dir / filename)

    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=12*mm,  bottomMargin=12*mm,
        title=f"StockStack Daily Digest — {as_of.strftime('%d %b %Y')}",
        author="StockStack",
    )

    story = []
    date_str    = as_of.strftime("%d %b %Y")
    weekday_str = as_of.strftime("%A")

    # ── Header band ──────────────────────────────────────────────────────────
    header_data = [[
        Paragraph(f"<b>StockStack</b>", S_TITLE),
        Paragraph(f"<b>{date_str}</b>  ·  {weekday_str}  ·  NSE India", S_SUBTITLE),
    ]]
    hdr_table = Table(header_data, colWidths=[80*mm, 100*mm])
    hdr_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _NAVY),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING",   (0, 0), (-1, -1), 12),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 12),
        ("LINEBELOW",     (0, 0), (-1, -1), 3, _BLUE_LIGHT),
    ]))
    story.append(hdr_table)
    story.append(Spacer(1, 6*mm))

    # ── Sections ─────────────────────────────────────────────────────────────
    _add_breadth(story, breadth)
    _add_index_breadth(story, idx_breadth)
    _add_themes(story, themes_df)
    _add_volume_surge(story, vol_df)
    _add_earnings(story, earnings_df)
    _add_movers(story, gainers_df, "Top 10 Gainers", is_gainers=True)
    _add_movers(story, losers_df,  "Top 10 Losers",  is_gainers=False)

    # ── Footer ────────────────────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=_BORDER, spaceBefore=6))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(
        "Generated by StockStack after daily market refresh  ·  Data via NSE / yfinance  ·  Personal use only",
        S_FOOTER,
    ))

    doc.build(story)
    return path
