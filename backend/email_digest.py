"""
email_digest.py — Daily market summary email sent after daily_refresh completes.

Sends to DIGEST_EMAIL_TO (default: democracy680@gmail.com) via Gmail SMTP.
Requires GMAIL_APP_PASSWORD in .env (16-char Google App Password, not your login password).
Get one at: https://myaccount.google.com/apppasswords

Sections in the email:
  1. Top 5 Volume Surge stocks (today volume vs 20-day avg)
  2. Quarterly Results stocks with >5% return today
  3. Top 10 Gainers of the day
  4. Top 10 Losers of the day

Usage (standalone):
    python backend/email_digest.py

Called automatically from daily_refresh.py after all computations finish.
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

# ── Config ────────────────────────────────────────────────────────────────────
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
DIGEST_EMAIL_FROM  = os.getenv("DIGEST_EMAIL_FROM",  "democracy680@gmail.com")
DIGEST_EMAIL_TO    = os.getenv("DIGEST_EMAIL_TO",    "democracy680@gmail.com")


# ── Queries ───────────────────────────────────────────────────────────────────

def _get_volume_surge(engine, as_of: date, top_n: int = 5) -> pd.DataFrame:
    """Top N stocks by volume surge ratio (today vol / 20-day avg vol)."""
    sql = text("""
        WITH today AS (
            SELECT p.symbol, s.name, p.volume AS today_vol
            FROM prices_daily p
            JOIN stocks s ON s.symbol = p.symbol
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
            t.name,
            sd.cmp,
            ROUND(sd.ret_1d * 100, 2)           AS ret_1d_pct,
            t.today_vol,
            a.avg_vol_20d,
            ROUND((t.today_vol::NUMERIC / a.avg_vol_20d), 2) AS surge_ratio
        FROM today t
        JOIN avg20 a ON a.symbol = t.symbol
        JOIN snapshots_daily sd ON sd.symbol = t.symbol AND sd.date = :as_of
        ORDER BY surge_ratio DESC
        LIMIT :top_n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"as_of": as_of, "top_n": top_n}).fetchall()
    return pd.DataFrame(rows, columns=[
        "symbol", "name", "cmp", "ret_1d_pct",
        "today_vol", "avg_vol_20d", "surge_ratio",
    ])


def _get_earnings_movers(engine, as_of: date, min_return_pct: float = 5.0) -> pd.DataFrame:
    """Stocks that announced results today and moved > min_return_pct%."""
    sql = text("""
        SELECT
            ec.symbol,
            s.name,
            sd.cmp,
            ROUND(sd.ret_1d * 100, 2) AS ret_1d_pct,
            sd.market_cap_cr
        FROM earnings_calendar ec
        JOIN stocks s ON s.symbol = ec.symbol
        JOIN snapshots_daily sd ON sd.symbol = ec.symbol AND sd.date = :as_of
        WHERE ec.result_date = :as_of
          AND sd.ret_1d >= :min_ret
        ORDER BY sd.ret_1d DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {
            "as_of":   as_of,
            "min_ret": min_return_pct / 100.0,
        }).fetchall()
    return pd.DataFrame(rows, columns=["symbol", "name", "cmp", "ret_1d_pct", "market_cap_cr"])


def _get_top_movers(engine, as_of: date, top_n: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Top N gainers and top N losers across all tracked stocks for today."""
    sql = text("""
        SELECT
            sd.symbol,
            s.name,
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

    df = pd.DataFrame(rows, columns=["symbol", "name", "cmp", "ret_1d_pct", "market_cap_cr"])
    gainers = df.nlargest(top_n, "ret_1d_pct").reset_index(drop=True)
    losers  = df.nsmallest(top_n, "ret_1d_pct").reset_index(drop=True)
    return gainers, losers


# ── HTML builders ─────────────────────────────────────────────────────────────

_GREEN  = "#16a34a"
_RED    = "#dc2626"
_BLUE   = "#1d4ed8"
_HEADER_BG = "#0f172a"
_CARD_BG   = "#ffffff"
_BORDER    = "#e2e8f0"
_MUTED     = "#64748b"


def _color(val) -> str:
    """Return green/red/black depending on sign of a numeric value."""
    try:
        return _GREEN if float(val) > 0 else (_RED if float(val) < 0 else "#374151")
    except (TypeError, ValueError):
        return "#374151"


def _fmt_pct(val) -> str:
    try:
        return f"{float(val):+.2f}%"
    except (TypeError, ValueError):
        return "—"


def _fmt_vol(val) -> str:
    try:
        v = int(val)
        if v >= 10_000_000:
            return f"{v/10_000_000:.1f}Cr"
        if v >= 100_000:
            return f"{v/100_000:.1f}L"
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


def _table_header(*cols: str) -> str:
    ths = "".join(
        f'<th style="padding:8px 12px;text-align:left;font-size:11px;'
        f'font-weight:700;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{_MUTED};border-bottom:2px solid {_BORDER};">{c}</th>'
        for c in cols
    )
    return f"<thead><tr>{ths}</tr></thead>"


def _td(content: str, color: str = "#1e293b", bold: bool = False) -> str:
    weight = "700" if bold else "400"
    return (
        f'<td style="padding:8px 12px;font-size:13px;color:{color};'
        f'font-weight:{weight};border-bottom:1px solid {_BORDER};">{content}</td>'
    )


def _section_title(title: str, subtitle: str = "") -> str:
    sub = f'<p style="margin:4px 0 0;font-size:12px;color:{_MUTED};">{subtitle}</p>' if subtitle else ""
    return (
        f'<div style="margin:32px 0 12px;">'
        f'<h2 style="margin:0;font-size:16px;font-weight:700;color:#0f172a;">{title}</h2>'
        f'{sub}'
        f'</div>'
    )


def _empty_note(msg: str) -> str:
    return f'<p style="color:{_MUTED};font-size:13px;font-style:italic;">{msg}</p>'


def _build_volume_surge_table(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty_note("No volume surge data available for today.")
    rows_html = ""
    for _, r in df.iterrows():
        ret_color = _color(r["ret_1d_pct"])
        rows_html += (
            "<tr>"
            + _td(f'<strong>{r["symbol"]}</strong>', "#0f172a", bold=True)
            + _td(str(r["name"]))
            + _td(f'₹{float(r["cmp"]):.2f}')
            + _td(_fmt_pct(r["ret_1d_pct"]), color=ret_color, bold=True)
            + _td(_fmt_vol(r["today_vol"]))
            + _td(_fmt_vol(r["avg_vol_20d"]))
            + _td(f'{float(r["surge_ratio"]):.1f}x', color=_BLUE, bold=True)
            + "</tr>"
        )
    return (
        '<table style="width:100%;border-collapse:collapse;">'
        + _table_header("Symbol", "Company", "CMP", "1D Return", "Today Vol", "20D Avg Vol", "Surge")
        + f"<tbody>{rows_html}</tbody>"
        + "</table>"
    )


def _build_earnings_table(df: pd.DataFrame) -> str:
    if df.empty:
        return _empty_note("No quarterly result stocks moved >5% today.")
    rows_html = ""
    for _, r in df.iterrows():
        ret_color = _color(r["ret_1d_pct"])
        rows_html += (
            "<tr>"
            + _td(f'<strong>{r["symbol"]}</strong>', "#0f172a", bold=True)
            + _td(str(r["name"]))
            + _td(f'₹{float(r["cmp"]):.2f}')
            + _td(_fmt_pct(r["ret_1d_pct"]), color=ret_color, bold=True)
            + _td(_fmt_mcap(r["market_cap_cr"]))
            + "</tr>"
        )
    return (
        '<table style="width:100%;border-collapse:collapse;">'
        + _table_header("Symbol", "Company", "CMP", "1D Return", "Market Cap")
        + f"<tbody>{rows_html}</tbody>"
        + "</table>"
    )


def _build_movers_table(df: pd.DataFrame, is_gainers: bool) -> str:
    if df.empty:
        return _empty_note("No data available.")
    rows_html = ""
    for rank, (_, r) in enumerate(df.iterrows(), 1):
        ret_color = _GREEN if is_gainers else _RED
        rows_html += (
            "<tr>"
            + _td(str(rank), _MUTED)
            + _td(f'<strong>{r["symbol"]}</strong>', "#0f172a", bold=True)
            + _td(str(r["name"]))
            + _td(f'₹{float(r["cmp"]):.2f}')
            + _td(_fmt_pct(r["ret_1d_pct"]), color=ret_color, bold=True)
            + _td(_fmt_mcap(r["market_cap_cr"]))
            + "</tr>"
        )
    return (
        '<table style="width:100%;border-collapse:collapse;">'
        + _table_header("#", "Symbol", "Company", "CMP", "1D Return", "Market Cap")
        + f"<tbody>{rows_html}</tbody>"
        + "</table>"
    )


def build_html_email(
    as_of: date,
    vol_df:      pd.DataFrame,
    earnings_df: pd.DataFrame,
    gainers_df:  pd.DataFrame,
    losers_df:   pd.DataFrame,
) -> str:
    date_str = as_of.strftime("%d %b %Y")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>StockStack Daily Digest — {date_str}</title>
</head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;">
  <div style="max-width:700px;margin:32px auto;background:{_CARD_BG};border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

    <!-- Header -->
    <div style="background:{_HEADER_BG};padding:24px 32px;">
      <div style="display:flex;align-items:center;gap:12px;">
        <div style="background:#3b82f6;width:36px;height:36px;border-radius:8px;display:inline-flex;align-items:center;justify-content:center;">
          <span style="color:#fff;font-size:18px;font-weight:700;">S</span>
        </div>
        <div>
          <div style="color:#f1f5f9;font-size:18px;font-weight:700;letter-spacing:-0.02em;">StockStack Daily Digest</div>
          <div style="color:#64748b;font-size:12px;margin-top:2px;">{date_str} &nbsp;·&nbsp; NSE India</div>
        </div>
      </div>
    </div>

    <!-- Body -->
    <div style="padding:24px 32px 40px;">

      <!-- Volume Surge -->
      {_section_title("🔥 Top 5 Volume Surge", "Stocks with the highest volume vs their 20-day average")}
      {_build_volume_surge_table(vol_df)}

      <!-- Quarterly Results Movers -->
      {_section_title("📅 Quarterly Results — Big Movers", "Stocks that announced results today and moved >5%")}
      {_build_earnings_table(earnings_df)}

      <!-- Top Gainers -->
      {_section_title("📈 Top 10 Gainers", "Best performing stocks across all tracked NSE stocks today")}
      {_build_movers_table(gainers_df, is_gainers=True)}

      <!-- Top Losers -->
      {_section_title("📉 Top 10 Losers", "Worst performing stocks across all tracked NSE stocks today")}
      {_build_movers_table(losers_df, is_gainers=False)}

    </div>

    <!-- Footer -->
    <div style="background:#f8fafc;padding:16px 32px;border-top:1px solid {_BORDER};">
      <p style="margin:0;font-size:11px;color:{_MUTED};">
        Generated by StockStack after daily refresh &nbsp;·&nbsp; Data sourced from NSE via yfinance &nbsp;·&nbsp; For personal use only
      </p>
    </div>

  </div>
</body>
</html>"""


# ── Send ──────────────────────────────────────────────────────────────────────

def send_digest(as_of: date | None = None) -> bool:
    """
    Fetch data, build email, and send via Gmail SMTP.
    Returns True on success, False on failure (so daily_refresh stays non-fatal).
    """
    if not GMAIL_APP_PASSWORD:
        logger.warning("GMAIL_APP_PASSWORD not set — skipping email digest")
        return False

    if as_of is None:
        as_of = date.today()

    engine = get_engine()
    logger.info(f"Building daily digest for {as_of}...")

    try:
        vol_df      = _get_volume_surge(engine, as_of)
        earnings_df = _get_earnings_movers(engine, as_of)
        gainers_df, losers_df = _get_top_movers(engine, as_of)

        logger.info(
            f"  vol_surge={len(vol_df)}, earnings_movers={len(earnings_df)}, "
            f"gainers={len(gainers_df)}, losers={len(losers_df)}"
        )
    except Exception as e:
        logger.error(f"Email digest query failed: {e}", exc_info=True)
        return False

    html_body = build_html_email(as_of, vol_df, earnings_df, gainers_df, losers_df)

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
