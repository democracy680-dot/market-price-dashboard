"""
email_digest.py — Daily market summary email sent after daily_refresh completes.

Sends to DIGEST_EMAIL_TO (default: democracy680@gmail.com) via Gmail SMTP.
Requires GMAIL_APP_PASSWORD in .env (16-char Google App Password, not your login password).
Get one at: https://myaccount.google.com/apppasswords

Sections in the email:
  0. Market Breadth Snapshot (advances/declines, % above 200 DMA, index returns)
  1. Sector Leaderboard (top 3 + bottom 3 sectors)
  2. Top 5 Themes by 1D return
  3. Top 5 Volume Surge stocks (today volume vs 20-day avg)
  4. Quarterly Results stocks with >5% return today
  5. Top 10 Gainers of the day
  6. Top 10 Losers of the day

Usage (standalone):
    python backend/email_digest.py
    python backend/email_digest.py 2026-05-07   # specific date
"""

import logging
import os
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from db import get_engine

load_dotenv()
logger = logging.getLogger(__name__)

GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
DIGEST_EMAIL_FROM  = os.getenv("DIGEST_EMAIL_FROM",  "democracy680@gmail.com")
DIGEST_EMAIL_TO    = os.getenv("DIGEST_EMAIL_TO",    "democracy680@gmail.com")


# ── Queries ───────────────────────────────────────────────────────────────────

def _get_volume_surge(engine, as_of: date, top_n: int = 5) -> pd.DataFrame:
    sql = text("""
        WITH today AS (
            SELECT p.symbol, p.volume AS today_vol
            FROM prices_daily p
            WHERE p.date = :as_of AND p.volume > 0
        ),
        avg20 AS (
            SELECT symbol,
                   ROUND(AVG(volume)::NUMERIC, 0) AS avg_vol_20d
            FROM prices_daily
            WHERE date < :as_of
              AND date >= :as_of - INTERVAL '30 days'
              AND volume > 0
            GROUP BY symbol
            HAVING COUNT(*) >= 10
        )
        SELECT
            t.symbol,
            s.name,
            s.tradingview_url,
            s.screener_url,
            sd.cmp,
            ROUND(sd.ret_1d * 100, 2)                              AS ret_1d_pct,
            t.today_vol,
            a.avg_vol_20d,
            ROUND((t.today_vol::NUMERIC / a.avg_vol_20d), 2)       AS surge_ratio
        FROM today t
        JOIN avg20 a         ON a.symbol  = t.symbol
        JOIN stocks s        ON s.symbol  = t.symbol
        JOIN snapshots_daily sd ON sd.symbol = t.symbol AND sd.date = :as_of
        ORDER BY surge_ratio DESC
        LIMIT :top_n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of, "top_n": top_n}).fetchall()
    df = pd.DataFrame(rows, columns=[
        "symbol", "name", "tradingview_url", "screener_url",
        "cmp", "ret_1d_pct", "today_vol", "avg_vol_20d", "surge_ratio",
    ])
    for col in ("cmp", "ret_1d_pct", "surge_ratio"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _get_earnings_movers(engine, as_of: date, min_return_pct: float = 5.0) -> pd.DataFrame:
    sql = text("""
        SELECT
            ec.symbol,
            s.name,
            s.tradingview_url,
            s.screener_url,
            sd.cmp,
            ROUND(sd.ret_1d * 100, 2) AS ret_1d_pct,
            sd.market_cap_cr
        FROM earnings_calendar ec
        JOIN stocks s        ON s.symbol  = ec.symbol
        JOIN snapshots_daily sd ON sd.symbol = ec.symbol AND sd.date = :as_of
        WHERE ec.result_date = :as_of
          AND sd.ret_1d >= :min_ret
        ORDER BY sd.ret_1d DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of, "min_ret": min_return_pct / 100.0}).fetchall()
    df = pd.DataFrame(rows, columns=[
        "symbol", "name", "tradingview_url", "screener_url",
        "cmp", "ret_1d_pct", "market_cap_cr",
    ])
    for col in ("cmp", "ret_1d_pct", "market_cap_cr"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _get_top_movers(engine, as_of: date, top_n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    sql = text("""
        SELECT
            sd.symbol,
            s.name,
            s.tradingview_url,
            s.screener_url,
            sd.cmp,
            ROUND(sd.ret_1d * 100, 2) AS ret_1d_pct,
            sd.market_cap_cr
        FROM snapshots_daily sd
        JOIN stocks s ON s.symbol = sd.symbol
        WHERE sd.date = :as_of
          AND sd.ret_1d IS NOT NULL
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of}).fetchall()
    df = pd.DataFrame(rows, columns=[
        "symbol", "name", "tradingview_url", "screener_url",
        "cmp", "ret_1d_pct", "market_cap_cr",
    ])
    for col in ("cmp", "ret_1d_pct", "market_cap_cr"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    gainers = df.nlargest(top_n, "ret_1d_pct").reset_index(drop=True)
    losers  = df.nsmallest(top_n, "ret_1d_pct").reset_index(drop=True)
    return gainers, losers


def _get_breadth(engine, as_of: date) -> dict:
    """Market-wide advance/decline, % above 200 DMA, and index-level 1D returns."""
    sql = text("""
        SELECT
            COUNT(*)                                                        AS total,
            COUNT(*) FILTER (WHERE ret_1d > 0)                             AS advances,
            COUNT(*) FILTER (WHERE ret_1d < 0)                             AS declines,
            COUNT(*) FILTER (WHERE ret_1d = 0 OR ret_1d IS NULL)           AS unchanged,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE status_200dma = 'Above 200DMA')
                / NULLIF(COUNT(*), 0), 1
            )                                                               AS pct_above_200dma
        FROM snapshots_daily
        WHERE date = :as_of
    """)
    # Nifty 50 proxy: average ret_1d of Nifty 50 members
    nifty_sql = text("""
        SELECT
            ROUND(AVG(sd.ret_1d) * 100, 2) AS nifty50_ret,
            ROUND(AVG(sd.cmp), 0)           AS nifty50_cmp
        FROM snapshots_daily sd
        JOIN index_membership im ON im.symbol = sd.symbol AND im.index_name = 'NIFTY_50'
        WHERE sd.date = :as_of
    """)
    bank_sql = text("""
        SELECT ROUND(AVG(sd.ret_1d) * 100, 2) AS bank_ret
        FROM snapshots_daily sd
        JOIN index_membership im ON im.symbol = sd.symbol AND im.index_name = 'NIFTY_BANK'
        WHERE sd.date = :as_of
    """)
    with engine.connect() as conn:
        b    = conn.execute(sql,      {"as_of": as_of}).fetchone()
        n50  = conn.execute(nifty_sql,{"as_of": as_of}).fetchone()
        bank = conn.execute(bank_sql, {"as_of": as_of}).fetchone()

    return {
        "total":           int(b.total)           if b else 0,
        "advances":        int(b.advances)         if b else 0,
        "declines":        int(b.declines)         if b else 0,
        "unchanged":       int(b.unchanged)        if b else 0,
        "pct_above_200":   float(b.pct_above_200dma) if b and b.pct_above_200dma else 0.0,
        "nifty50_ret":     float(n50.nifty50_ret)  if n50 and n50.nifty50_ret  else None,
        "bank_ret":        float(bank.bank_ret)    if bank and bank.bank_ret    else None,
    }


def _get_sector_leaderboard(engine, as_of: date, n: int = 3) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Top N and bottom N sectors by 1D return."""
    sql = text("""
        SELECT sector, day_change_pct, num_companies, advances, declines
        FROM sector_performance_daily
        WHERE date = :as_of
          AND day_change_pct IS NOT NULL
          AND sector NOT IN ('Unknown', '')
        ORDER BY day_change_pct DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of}).fetchall()
    df = pd.DataFrame(rows, columns=["sector", "day_change_pct", "num_companies", "advances", "declines"])
    df["day_change_pct"] = pd.to_numeric(df["day_change_pct"], errors="coerce")
    return df.head(n).reset_index(drop=True), df.tail(n).iloc[::-1].reset_index(drop=True)


def _get_top_themes(engine, as_of: date, top_n: int = 5) -> pd.DataFrame:
    """Top themes by average 1D return of member stocks."""
    sql = text("""
        SELECT
            t.theme_name,
            COUNT(tm.symbol)                            AS stock_count,
            ROUND(AVG(sd.ret_1d)  * 100, 2)            AS avg_ret_1d,
            ROUND(AVG(sd.ret_30d) * 100, 2)            AS avg_ret_30d
        FROM themes t
        JOIN theme_membership tm ON tm.theme_slug = t.theme_slug
        JOIN snapshots_daily sd
            ON sd.symbol = tm.symbol AND sd.date = :as_of
        WHERE sd.ret_1d IS NOT NULL
        GROUP BY t.theme_name, t.theme_order
        HAVING COUNT(tm.symbol) >= 3
        ORDER BY AVG(sd.ret_1d) DESC
        LIMIT :top_n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of, "top_n": top_n}).fetchall()
    df = pd.DataFrame(rows, columns=["theme_name", "stock_count", "avg_ret_1d", "avg_ret_30d"])
    for col in ("avg_ret_1d", "avg_ret_30d"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_pct(val) -> str:
    try:
        v = float(val)
        sign = "+" if v > 0 else ""
        return f"{sign}{v:.2f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_vol(val) -> str:
    try:
        v = int(val)
        if v >= 10_000_000:
            return f"{v/10_000_000:.1f} Cr"
        if v >= 100_000:
            return f"{v/100_000:.1f} L"
        return f"{v:,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_mcap(val) -> str:
    try:
        v = float(val)
        if v >= 1_00_000:
            return f"₹{v/1_00_000:.1f}L Cr"
        return f"₹{v:,.0f} Cr"
    except (TypeError, ValueError):
        return "—"


def _pct_color(val) -> str:
    try:
        return "#16a34a" if float(val) > 0 else "#dc2626"
    except (TypeError, ValueError):
        return "#64748b"


def _pct_bg(val) -> str:
    try:
        return "#f0fdf4" if float(val) > 0 else "#fef2f2"
    except (TypeError, ValueError):
        return "#f8fafc"


def _pct_border(val) -> str:
    try:
        return "#bbf7d0" if float(val) > 0 else "#fecaca"
    except (TypeError, ValueError):
        return "#e2e8f0"


def _stock_cell(symbol: str, name: str, tv_url, sc_url) -> str:
    """Renders symbol + company name with TradingView and Screener links."""
    links = []
    if tv_url and str(tv_url).startswith("http"):
        links.append(
            f'<a href="{tv_url}" style="color:#3b82f6;font-size:10px;font-weight:500;'
            f'text-decoration:underline;text-decoration-style:dashed;'
            f'text-underline-offset:2px;letter-spacing:.02em;">TradingView</a>'
        )
    if sc_url and str(sc_url).startswith("http"):
        links.append(
            f'<a href="{sc_url}" style="color:#8b5cf6;font-size:10px;font-weight:500;'
            f'text-decoration:underline;text-decoration-style:dashed;'
            f'text-underline-offset:2px;letter-spacing:.02em;">Screener</a>'
        )
    links_html = (
        f'<div style="margin-top:3px;display:flex;gap:8px;">'
        + ' <span style="color:#cbd5e1;font-size:10px;">·</span> '.join(links)
        + '</div>'
    ) if links else ""
    return (
        f'<div>'
        f'<span style="font-weight:700;font-size:13px;color:#0f172a;'
        f'letter-spacing:.01em;">{symbol}</span>'
        f'<div style="font-size:11px;color:#64748b;margin-top:1px;line-height:1.3;">{name}</div>'
        f'{links_html}'
        f'</div>'
    )


def _pct_pill(val) -> str:
    color  = _pct_color(val)
    bg     = _pct_bg(val)
    border = _pct_border(val)
    arrow  = "▲" if float(val) > 0 else "▼"
    return (
        f'<span style="display:inline-block;padding:3px 8px;border-radius:20px;'
        f'background:{bg};border:1px solid {border};color:{color};'
        f'font-weight:700;font-size:12px;letter-spacing:.01em;white-space:nowrap;">'
        f'{arrow} {_fmt_pct(val)}</span>'
    )


# ── Breadth & sector builders ─────────────────────────────────────────────────

def _build_breadth_card(b: dict) -> str:
    """Single full-width summary card shown at the top of the email."""
    def _idx_pill(label: str, ret) -> str:
        if ret is None:
            return ""
        color  = _pct_color(ret)
        bg     = _pct_bg(ret)
        border = _pct_border(ret)
        arrow  = "▲" if ret > 0 else "▼"
        return f"""
        <td style="padding:0 6px;text-align:center;">
          <div style="font-size:10px;font-weight:600;color:#64748b;
            text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;">{label}</div>
          <span style="display:inline-block;padding:4px 10px;border-radius:20px;
            background:{bg};border:1px solid {border};color:{color};
            font-weight:700;font-size:13px;white-space:nowrap;">{arrow} {_fmt_pct(ret)}</span>
        </td>"""

    total    = b["total"]
    advances = b["advances"]
    declines = b["declines"]
    pct_200  = b["pct_above_200"]
    adv_pct  = round(100 * advances / total, 1) if total else 0
    dec_pct  = round(100 * declines / total, 1) if total else 0

    bar_adv_w = int(adv_pct)
    bar_dec_w = 100 - bar_adv_w

    return f"""
    <div style="background:linear-gradient(135deg,#f0f9ff 0%,#f8fafc 100%);
      border:1px solid #e2e8f0;border-radius:10px;padding:20px 24px;margin-bottom:28px;">

      <!-- Index returns row -->
      <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:18px;">
        <tr>
          <td style="vertical-align:middle;">
            <div style="font-size:13px;font-weight:700;color:#0f172a;">Market Breadth</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:1px;">{total:,} stocks tracked</div>
          </td>
          <td style="text-align:right;">
            <table cellpadding="0" cellspacing="0" style="margin-left:auto;">
              <tr>
                {_idx_pill("Nifty 50", b["nifty50_ret"])}
                {_idx_pill("Nifty Bank", b["bank_ret"])}
              </tr>
            </table>
          </td>
        </tr>
      </table>

      <!-- A/D progress bar -->
      <div style="margin-bottom:10px;">
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:6px;">
          <tr>
            <td style="font-size:12px;font-weight:600;color:#16a34a;">▲ {advances:,} Advances ({adv_pct}%)</td>
            <td style="text-align:right;font-size:12px;font-weight:600;color:#dc2626;">▼ {declines:,} Declines ({dec_pct}%)</td>
          </tr>
        </table>
        <div style="height:8px;border-radius:4px;background:#fee2e2;overflow:hidden;">
          <div style="height:8px;width:{bar_adv_w}%;background:#16a34a;border-radius:4px 0 0 4px;"></div>
        </div>
      </div>

      <!-- % above 200 DMA -->
      <div style="font-size:12px;color:#64748b;margin-top:8px;">
        <span style="color:#1d4ed8;font-weight:700;">{pct_200:.1f}%</span> of stocks trading above their 200-day moving average
      </div>

    </div>"""


def _build_sector_leaderboard(top_df: pd.DataFrame, bot_df: pd.DataFrame) -> str:
    def _sector_row(r, rank: int, is_top: bool) -> str:
        accent = "#16a34a" if is_top else "#dc2626"
        bg     = "#f0fdf4" if is_top else "#fef2f2"
        border = "#bbf7d0" if is_top else "#fecaca"
        arrow  = "▲" if is_top else "▼"
        pct    = float(r["day_change_pct"])
        ad_str = f'{int(r["advances"])}↑ / {int(r["declines"])}↓'
        return f"""
        <tr>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;width:28px;">
            <span style="display:inline-flex;align-items:center;justify-content:center;
              width:22px;height:22px;border-radius:50%;background:{accent}18;
              color:{accent};font-weight:800;font-size:10px;">{rank}</span>
          </td>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;">
            <div style="font-size:13px;font-weight:600;color:#0f172a;">{r["sector"]}</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:1px;">{int(r["num_companies"])} stocks &nbsp;·&nbsp; {ad_str}</div>
          </td>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;text-align:right;">
            <span style="display:inline-block;padding:3px 10px;border-radius:20px;
              background:{bg};border:1px solid {border};color:{accent};
              font-weight:700;font-size:12px;">{arrow} {abs(pct):.2f}%</span>
          </td>
        </tr>"""

    if top_df.empty and bot_df.empty:
        return _empty("No sector data available for today.")

    top_rows = "".join(_sector_row(r, i+1, True)  for i, (_, r) in enumerate(top_df.iterrows()))
    bot_rows = "".join(_sector_row(r, i+1, False) for i, (_, r) in enumerate(bot_df.iterrows()))

    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead>
        <tr><th colspan="3" style="padding:10px 14px;text-align:left;font-size:10px;
          font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#16a34a;
          background:#f0fdf4;border-bottom:1px solid #bbf7d0;">🏆 Top Performers</th></tr>
      </thead>
      <tbody>{top_rows}</tbody>
      <thead>
        <tr><th colspan="3" style="padding:10px 14px;text-align:left;font-size:10px;
          font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#dc2626;
          background:#fef2f2;border-bottom:1px solid #fecaca;border-top:2px solid #e2e8f0;">
          ⚠️ Underperformers</th></tr>
      </thead>
      <tbody>{bot_rows}</tbody>
    </table>"""


def _build_themes_table(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty("No theme performance data available for today.")
    rows = ""
    for i, (_, r) in enumerate(df.iterrows()):
        bg = "#fafbff" if i % 2 == 0 else "#ffffff"
        rows += f"""
        <tr style="background:{bg};">
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;width:26px;">
            <span style="display:inline-flex;align-items:center;justify-content:center;
              width:22px;height:22px;border-radius:50%;background:#6366f118;
              color:#6366f1;font-weight:800;font-size:10px;">{i+1}</span>
          </td>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;">
            <div style="font-size:13px;font-weight:600;color:#0f172a;">{r["theme_name"]}</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:1px;">{int(r["stock_count"])} stocks</div>
          </td>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;text-align:center;">
            {_pct_pill(r["avg_ret_1d"])}
          </td>
          <td style="padding:10px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;
            font-family:'SF Mono','Fira Code',monospace;font-size:12px;color:#64748b;text-align:center;">
            {_fmt_pct(r["avg_ret_30d"])} <span style="font-size:10px;color:#94a3b8;">30D</span>
          </td>
        </tr>"""
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead><tr>
        <th {_TH_STYLE}>#</th>
        <th {_TH_STYLE}>Theme</th>
        <th {_TH_STYLE} style="text-align:center;">1D Avg Return</th>
        <th {_TH_STYLE} style="text-align:center;">30D Avg</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── Table builders ────────────────────────────────────────────────────────────

_TH_STYLE = (
    'style="padding:10px 14px;text-align:left;font-size:10px;font-weight:700;'
    'text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;'
    'background:#f8fafc;border-bottom:2px solid #e2e8f0;"'
)
_TD_STYLE = (
    'style="padding:12px 14px;vertical-align:middle;'
    'border-bottom:1px solid #f1f5f9;"'
)
_TD_MONO = (
    'style="padding:12px 14px;vertical-align:middle;'
    'font-family:\'SF Mono\',\'Fira Code\',monospace;font-size:12px;color:#374151;'
    'border-bottom:1px solid #f1f5f9;"'
)


def _section(icon: str, title: str, subtitle: str, content: str) -> str:
    return f"""
    <div style="margin-bottom:36px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="padding:0 0 12px 0;">
            <div style="display:flex;align-items:center;gap:10px;border-left:3px solid #3b82f6;padding-left:12px;">
              <span style="font-size:18px;">{icon}</span>
              <div>
                <div style="font-size:15px;font-weight:700;color:#0f172a;letter-spacing:-0.01em;">{title}</div>
                <div style="font-size:11px;color:#94a3b8;margin-top:1px;">{subtitle}</div>
              </div>
            </div>
          </td>
        </tr>
      </table>
      <div style="border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;background:#ffffff;">
        {content}
      </div>
    </div>"""


def _empty(msg: str) -> str:
    return (
        f'<div style="padding:24px;text-align:center;color:#94a3b8;'
        f'font-size:13px;font-style:italic;">{msg}</div>'
    )


def _build_volume_table(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty("No significant volume surges detected today.")
    rows = ""
    for i, r in df.iterrows():
        bg = "#fafbff" if i % 2 == 0 else "#ffffff"
        rows += f"""
        <tr style="background:{bg};">
          <td {_TD_STYLE}>{_stock_cell(r["symbol"], r["name"], r["tradingview_url"], r["screener_url"])}</td>
          <td {_TD_MONO}>₹{float(r["cmp"]):.2f}</td>
          <td {_TD_STYLE}>{_pct_pill(r["ret_1d_pct"])}</td>
          <td {_TD_MONO}>{_fmt_vol(r["today_vol"])}</td>
          <td {_TD_MONO}>{_fmt_vol(r["avg_vol_20d"])}</td>
          <td style="padding:12px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;">
            <span style="font-weight:800;font-size:14px;color:#1d4ed8;letter-spacing:-.01em;">{float(r["surge_ratio"]):.1f}x</span>
          </td>
        </tr>"""
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead><tr>
        <th {_TH_STYLE}>Stock</th>
        <th {_TH_STYLE}>CMP</th>
        <th {_TH_STYLE}>1D Return</th>
        <th {_TH_STYLE}>Today Vol</th>
        <th {_TH_STYLE}>20D Avg Vol</th>
        <th {_TH_STYLE}>Surge</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def _build_earnings_table(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty("No quarterly result stocks moved more than 5% today.")
    rows = ""
    for i, r in df.iterrows():
        bg = "#fafbff" if i % 2 == 0 else "#ffffff"
        rows += f"""
        <tr style="background:{bg};">
          <td {_TD_STYLE}>{_stock_cell(r["symbol"], r["name"], r["tradingview_url"], r["screener_url"])}</td>
          <td {_TD_MONO}>₹{float(r["cmp"]):.2f}</td>
          <td {_TD_STYLE}>{_pct_pill(r["ret_1d_pct"])}</td>
          <td {_TD_MONO}>{_fmt_mcap(r["market_cap_cr"])}</td>
        </tr>"""
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead><tr>
        <th {_TH_STYLE}>Stock</th>
        <th {_TH_STYLE}>CMP</th>
        <th {_TH_STYLE}>1D Return</th>
        <th {_TH_STYLE}>Market Cap</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def _build_movers_table(df: pd.DataFrame, is_gainers: bool) -> str:
    if df.empty:
        return _empty("No data available.")
    accent = "#16a34a" if is_gainers else "#dc2626"
    rows = ""
    for rank, (_, r) in enumerate(df.iterrows(), 1):
        bg = "#fafbff" if rank % 2 == 0 else "#ffffff"
        rows += f"""
        <tr style="background:{bg};">
          <td style="padding:12px 14px;vertical-align:middle;border-bottom:1px solid #f1f5f9;width:32px;">
            <span style="display:inline-flex;align-items:center;justify-content:center;
              width:24px;height:24px;border-radius:50%;background:{accent}14;
              color:{accent};font-weight:800;font-size:11px;">{rank}</span>
          </td>
          <td {_TD_STYLE}>{_stock_cell(r["symbol"], r["name"], r["tradingview_url"], r["screener_url"])}</td>
          <td {_TD_MONO}>₹{float(r["cmp"]):.2f}</td>
          <td {_TD_STYLE}>{_pct_pill(r["ret_1d_pct"])}</td>
          <td {_TD_MONO}>{_fmt_mcap(r["market_cap_cr"])}</td>
        </tr>"""
    return f"""
    <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
      <thead><tr>
        <th {_TH_STYLE}>#</th>
        <th {_TH_STYLE}>Stock</th>
        <th {_TH_STYLE}>CMP</th>
        <th {_TH_STYLE}>1D Return</th>
        <th {_TH_STYLE}>Market Cap</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


# ── Full email ────────────────────────────────────────────────────────────────

def build_html_email(
    as_of: date,
    breadth:     dict,
    top_sectors: pd.DataFrame,
    bot_sectors: pd.DataFrame,
    themes_df:   pd.DataFrame,
    vol_df:      pd.DataFrame,
    earnings_df: pd.DataFrame,
    gainers_df:  pd.DataFrame,
    losers_df:   pd.DataFrame,
) -> str:
    date_str     = as_of.strftime("%d %b %Y")
    weekday_str  = as_of.strftime("%A")
    total_stocks = breadth["total"]

    breadth_card     = _build_breadth_card(breadth)
    sector_html      = _build_sector_leaderboard(top_sectors, bot_sectors)
    themes_html      = _build_themes_table(themes_df)
    vol_html         = _build_volume_table(vol_df)
    earnings_html    = _build_earnings_table(earnings_df)
    gainers_html     = _build_movers_table(gainers_df, is_gainers=True)
    losers_html      = _build_movers_table(losers_df,  is_gainers=False)

    sector_section   = _section("🏭", "Sector Leaderboard",
                                 "Top 3 and bottom 3 sectors by 1-day return",
                                 sector_html)
    themes_section   = _section("🎯", "Top 5 Themes",
                                 "Best performing thematic baskets today (avg return of member stocks)",
                                 themes_html)
    vol_section      = _section("🔥", "Top 5 Volume Surge",
                                 "Stocks with the highest volume vs their 20-day average",
                                 vol_html)
    earnings_section = _section("📅", "Quarterly Results — Big Movers",
                                 "Stocks that announced results today and moved more than 5%",
                                 earnings_html)
    gainers_section  = _section("📈", "Top 10 Gainers",
                                 f"Best performing stocks across {total_stocks:,} tracked NSE stocks",
                                 gainers_html)
    losers_section   = _section("📉", "Top 10 Losers",
                                 f"Worst performing stocks across {total_stocks:,} tracked NSE stocks",
                                 losers_html)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>StockStack Daily Digest — {date_str}</title>
</head>
<body style="margin:0;padding:0;background:#eef2f7;font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;-webkit-font-smoothing:antialiased;">

<div style="max-width:680px;margin:32px auto;padding:0 16px;">

  <!-- Header card -->
  <div style="background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);border-radius:14px 14px 0 0;padding:28px 32px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td>
          <table cellpadding="0" cellspacing="0">
            <tr>
              <td style="padding-right:14px;vertical-align:middle;">
                <table cellpadding="0" cellspacing="0" style="width:42px;height:42px;background:#3b82f6;border-radius:10px;">
                  <tr><td style="text-align:center;vertical-align:middle;font-size:22px;font-weight:800;color:#ffffff;font-family:Arial,sans-serif;width:42px;height:42px;">S</td></tr>
                </table>
              </td>
              <td style="vertical-align:middle;">
                <div style="color:#f1f5f9;font-size:20px;font-weight:700;letter-spacing:-0.02em;">StockStack</div>
                <div style="color:#93c5fd;font-size:11px;margin-top:2px;letter-spacing:.06em;text-transform:uppercase;">Daily Market Digest</div>
              </td>
            </tr>
          </table>
        </td>
        <td style="text-align:right;vertical-align:middle;">
          <div style="color:#f1f5f9;font-size:15px;font-weight:600;">{date_str}</div>
          <div style="color:#93c5fd;font-size:11px;margin-top:2px;">{weekday_str} · NSE India</div>
        </td>
      </tr>
    </table>
  </div>

  <!-- Divider band -->
  <div style="background:#3b82f6;height:3px;"></div>

  <!-- Body -->
  <div style="background:#f8fafc;padding:28px 28px 8px;border-radius:0 0 14px 14px;
    border:1px solid #e2e8f0;border-top:none;">

    {breadth_card}
    {sector_section}
    {themes_section}
    {vol_section}
    {earnings_section}
    {gainers_section}
    {losers_section}

    <!-- Footer -->
    <div style="border-top:1px solid #e2e8f0;padding:20px 0 8px;margin-top:8px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <div style="font-size:10px;color:#94a3b8;line-height:1.6;">
              Generated automatically by <strong style="color:#64748b;">StockStack</strong>
              after daily market data refresh &nbsp;·&nbsp;
              Data sourced from NSE via yfinance &nbsp;·&nbsp;
              For personal use only
            </div>
          </td>
        </tr>
      </table>
    </div>

  </div>
</div>

</body>
</html>"""


# ── Send ──────────────────────────────────────────────────────────────────────

def send_digest(as_of: date | None = None) -> bool:
    if not GMAIL_APP_PASSWORD:
        logger.warning("GMAIL_APP_PASSWORD not set — skipping email digest")
        return False

    if as_of is None:
        as_of = date.today()

    engine = get_engine()
    logger.info(f"Building daily digest for {as_of}...")

    try:
        breadth              = _get_breadth(engine, as_of)
        top_sec, bot_sec     = _get_sector_leaderboard(engine, as_of)
        themes_df            = _get_top_themes(engine, as_of)
        vol_df               = _get_volume_surge(engine, as_of)
        earnings_df          = _get_earnings_movers(engine, as_of)
        gainers_df, losers_df = _get_top_movers(engine, as_of)
        logger.info(
            f"  breadth={breadth['advances']}A/{breadth['declines']}D, "
            f"sectors={len(top_sec)+len(bot_sec)}, themes={len(themes_df)}, "
            f"vol_surge={len(vol_df)}, earnings_movers={len(earnings_df)}, "
            f"gainers={len(gainers_df)}, losers={len(losers_df)}"
        )
    except Exception as e:
        logger.error(f"Email digest query failed: {e}", exc_info=True)
        return False

    html_body = build_html_email(
        as_of, breadth, top_sec, bot_sec, themes_df,
        vol_df, earnings_df, gainers_df, losers_df,
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"StockStack Daily Digest — {as_of.strftime('%d %b %Y')}"
    msg["From"]    = DIGEST_EMAIL_FROM
    msg["To"]      = DIGEST_EMAIL_TO
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(DIGEST_EMAIL_FROM, GMAIL_APP_PASSWORD)
            server.sendmail(DIGEST_EMAIL_FROM, DIGEST_EMAIL_TO, msg.as_string())
        logger.info(f"Daily digest sent to {DIGEST_EMAIL_TO}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "Gmail auth failed. Ensure GMAIL_APP_PASSWORD is a valid 16-char App Password. "
            "Generate one at https://myaccount.google.com/apppasswords"
        )
        return False
    except Exception as e:
        logger.error(f"Failed to send daily digest: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
    target_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    ok = send_digest(target_date)
    sys.exit(0 if ok else 1)
