"""
app.py — Indian Equity Dashboard (Streamlit)

Reads primarily from Supabase. yfinance is used for live benchmark index returns.
All heavy stock computation happens in the daily refresh job.
"""

import os
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yfinance as yf

load_dotenv()

# Global Markets tab (live data via yfinance — imported from sibling module)
try:
    from global_markets_tab import render_global_markets_tab as _render_global_markets
    _GM_AVAILABLE = True
except Exception as _gm_err:  # noqa: BLE001
    _GM_AVAILABLE = False
    _GM_ERROR = str(_gm_err)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="StockStack",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* ── Global ── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Main background */
    .stApp {
        background-color: #080c14;
    }
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 100%;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background-color: #0b0f1a;
        border-right: 1px solid #1a2236;
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stCaption {
        color: #4a5568;
        font-size: 11px;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: transparent;
        border-bottom: 1px solid #1e2d45;
        padding: 0 0 8px 0;
        align-items: flex-end;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 7px 16px;
        font-size: 11.5px;
        font-weight: 600;
        color: #475569;
        border-radius: 4px;
        background: transparent;
        border: none;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        transition: color 0.15s, background 0.15s;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #cbd5e1;
        background: transparent;
    }
    .stTabs [aria-selected="true"] {
        color: #e2e8f0 !important;
        background: #1e3a5f !important;
        border: 1px solid #2d5a9e !important;
        border-radius: 4px !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1.25rem;
    }

    /* ── Metric cards ── */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, #0f1729 0%, #111827 100%);
        border: 1px solid #1a2236;
        border-radius: 12px;
        padding: 16px 20px;
        transition: border-color 0.2s;
    }
    [data-testid="metric-container"]:hover {
        border-color: #2a3a5c;
    }
    [data-testid="stMetricLabel"] {
        font-size: 11px !important;
        font-weight: 600 !important;
        color: #4a5568 !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    [data-testid="stMetricValue"] {
        font-size: 22px !important;
        font-weight: 700 !important;
        color: #f1f5f9 !important;
        letter-spacing: -0.02em;
    }
    [data-testid="stMetricDelta"] {
        font-size: 12px !important;
        font-weight: 500 !important;
    }

    /* ── Buttons ── */
    .stButton button {
        border-radius: 6px;
        font-size: 12px;
        font-weight: 500;
        transition: all 0.15s;
        letter-spacing: 0.01em;
    }
    .stButton button[kind="secondary"] {
        background: transparent;
        border: 1px solid #1a2236;
        color: #64748b;
    }
    .stButton button[kind="secondary"]:hover {
        background: #0f1729;
        border-color: #2a3a5c;
        color: #94a3b8;
    }
    .stButton button[kind="primary"] {
        background: #1d3461;
        border: 1px solid #2d4f8e;
        color: #e2e8f0;
    }

    /* ── Theme picker buttons (left panel) ── */
    [data-testid="stVerticalBlock"] .stButton button {
        text-align: left;
        padding: 8px 12px;
        font-size: 12.5px;
        border-radius: 8px;
        white-space: normal;
        height: auto;
        line-height: 1.4;
    }

    /* ── Dataframe ── */
    [data-testid="stDataFrame"] {
        border: 1px solid #1a2236;
        border-radius: 10px;
        overflow: hidden;
    }
    [data-testid="stDataFrame"] th {
        background-color: #0b0f1a !important;
        color: #4a5568 !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    [data-testid="stDataFrame"] td {
        font-size: 13px;
    }

    /* ── Input / Select ── */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] {
        background-color: #0b0f1a;
        border: 1px solid #1a2236;
        border-radius: 8px;
        color: #e2e8f0;
        font-size: 13px;
    }
    .stTextInput input:focus {
        border-color: #3b82f6;
        box-shadow: 0 0 0 2px rgba(59,130,246,0.15);
    }
    /* pointer cursor on all selectbox triggers */
    .stSelectbox div[data-baseweb="select"],
    .stSelectbox div[data-baseweb="select"] * {
        cursor: pointer !important;
    }
    .stMultiSelect div[data-baseweb="select"],
    .stMultiSelect div[data-baseweb="select"] * {
        cursor: pointer !important;
    }

    /* ── Radio toggle ── */
    .stRadio > div {
        gap: 6px;
        flex-direction: row;
    }
    .stRadio label {
        background: #0b0f1a;
        border: 1px solid #1a2236;
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 12px;
        font-weight: 500;
        color: #4a5568;
        cursor: pointer;
        transition: all 0.15s;
    }
    .stRadio label:has(input:checked) {
        background: #1d3461;
        border-color: #3b82f6;
        color: #e2e8f0;
    }

    /* ── Divider ── */
    hr {
        border: none;
        border-top: 1px solid #1a2236;
        margin: 12px 0;
    }

    /* ── Captions & helpers ── */
    .stCaption, .stCaption p {
        color: #374151;
        font-size: 11.5px;
    }

    /* ── Alerts ── */
    .stAlert {
        border-radius: 8px;
        font-size: 13px;
    }

    /* ── Scrollbar (webkit) ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #080c14; }
    ::-webkit-scrollbar-thumb { background: #1a2236; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #2a3a5c; }

    /* ── Suppress Streamlit's content-dim during rerun ── */
    [data-stale="true"] {
        opacity: 1 !important;
        transition: opacity 0s !important;
        pointer-events: none;
    }
</style>
""", unsafe_allow_html=True)

# Inject centered loading overlay — runs once per session in the parent window
components.html("""
<script>
(function() {
    var pdoc = window.parent.document;
    if (pdoc.getElementById('eq-loader')) return;  // already injected

    /* Keyframes + overlay styles */
    var style = pdoc.createElement('style');
    style.textContent = [
        '@keyframes eq-spin { to { transform: rotate(360deg); } }',
        '#eq-loader {',
        '  display: none; position: fixed; inset: 0;',
        '  background: rgba(8,12,20,0.55);',
        '  z-index: 99999;',
        '  align-items: center; justify-content: center;',
        '  backdrop-filter: blur(3px);',
        '  -webkit-backdrop-filter: blur(3px);',
        '}',
        '#eq-loader.show { display: flex; }',
        '#eq-spinner {',
        '  width: 44px; height: 44px;',
        '  border: 3px solid rgba(59,130,246,0.18);',
        '  border-top-color: #3b82f6;',
        '  border-radius: 50%;',
        '  animation: eq-spin 0.72s linear infinite;',
        '}',
    ].join('');
    pdoc.head.appendChild(style);

    /* Overlay element */
    var overlay = pdoc.createElement('div');
    overlay.id = 'eq-loader';
    overlay.innerHTML = '<div id="eq-spinner"></div>';
    pdoc.body.appendChild(overlay);

    /* Watch data-stale attribute — Streamlit sets this on content during rerun */
    var hideTimer;
    var observer = new MutationObserver(function() {
        var stale = pdoc.querySelector('[data-stale="true"]');
        if (stale) {
            clearTimeout(hideTimer);
            overlay.classList.add('show');
        } else {
            hideTimer = setTimeout(function() {
                overlay.classList.remove('show');
            }, 120);
        }
    });

    observer.observe(pdoc.body, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ['data-stale']
    });
})();
</script>
""", height=0)

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def _check_password():
    try:
        correct = st.secrets["DASHBOARD_PASSWORD"]
    except Exception:
        correct = os.environ.get("DASHBOARD_PASSWORD", "")
    if not correct:
        st.error("DASHBOARD_PASSWORD not configured.")
        st.stop()
    if st.session_state.get("authenticated"):
        return

    # ── Login page ───────────────────────────────────────────────────────────
    st.markdown("""
    <style>
        [data-testid="stSidebar"] { display: none !important; }

        /* hide "Press Enter to apply" hint on password field */
        .stTextInput div[data-baseweb="input"] ~ div small,
        .stTextInput [class*="InputInstructions"],
        .stTextInput ~ div > small { display: none !important; }

        .lp-page {
            display: flex; align-items: center; justify-content: center;
            min-height: 90vh;
        }
        .lp-card {
            background: #0c1220;
            border: 1px solid #1e2d45;
            border-radius: 24px;
            padding: 52px 56px 48px;
            width: 100%; max-width: 420px;
            box-shadow: 0 32px 80px rgba(0,0,0,0.6);
            text-align: center;
        }
        .lp-logo-icon {
            width: 56px; height: 56px; border-radius: 14px;
            background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%);
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 26px; margin-bottom: 18px;
        }
        .lp-name {
            font-size: 32px; font-weight: 800; color: #f1f5f9;
            letter-spacing: -0.05em; margin: 0 0 6px;
        }
        .lp-name span { color: #3b82f6; }
        .lp-tagline {
            font-size: 12px; color: #334155; letter-spacing: 0.08em;
            text-transform: uppercase; font-weight: 500; margin-bottom: 36px;
        }
        .lp-divider { border: none; border-top: 1px solid #1a2740; margin: 0 0 28px; }
        .lp-footer {
            text-align: center; font-size: 11px; color: #2d3f57;
            margin-top: 18px; letter-spacing: 0.02em;
        }
    </style>
    <div class="lp-page">
      <div class="lp-card">
        <div class="lp-logo-icon">📈</div>
        <div class="lp-name">Stock<span>Stack</span></div>
        <div class="lp-tagline">Indian Equity Intelligence</div>
        <hr class="lp-divider">
      </div>
    </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        st.markdown("<div style='margin-top:-148px'>", unsafe_allow_html=True)
        pw = st.text_input("", type="password", placeholder="Enter password…",
                           label_visibility="collapsed")
        if st.button("Sign In →", use_container_width=True, type="primary"):
            if pw == correct:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown(
            "<div class='lp-footer'>Restricted access · Authorised users only</div>",
            unsafe_allow_html=True,
        )
    st.stop()


_check_password()

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@st.cache_resource
def _get_engine():
    try:
        url = st.secrets["SUPABASE_DB_URL"]
    except Exception:
        url = os.environ.get("SUPABASE_DB_URL", "")
    if not url:
        st.error("SUPABASE_DB_URL not configured.")
        st.stop()
    return create_engine(url, pool_pre_ping=True)


engine = _get_engine()

# ---------------------------------------------------------------------------
# Universe definitions
# ---------------------------------------------------------------------------
INDEX_TABS   = [("NIFTY_50", "Nifty 50"), ("NIFTY_500", "Nifty 500"),
                ("NIFTY_BANK", "Nifty Bank"), ("FNO", "F&O")]
SECTOR_TABS  = [("BANKS", "Banks"), ("NBFCS", "NBFCs"),
                ("PHARMA", "Pharma"), ("DEFENCE", "Defence")]
ALL_UNIVERSES = {k: v for k, v in INDEX_TABS + SECTOR_TABS}

# yfinance ticker symbol for each universe (None = no benchmark index)
INDEX_YF_SYMBOL = {
    "NIFTY_50":   "^NSEI",
    "NIFTY_500":  "^CRSLDX",
    "NIFTY_BANK": "^NSEBANK",
    "FNO":        None,
    "BANKS":      "^NSEBANK",
    "NBFCS":      None,
    "PHARMA":     "NIFTYPHARMA.NS",
    "DEFENCE":    None,
}

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_available_dates() -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT date FROM snapshots_daily ORDER BY date DESC LIMIT 90")
        ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def fetch_index_returns(yf_symbol: str) -> dict:
    """Fetch 1D, 1M, 1Y returns for a benchmark index via yfinance."""
    try:
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period="2y")
        if hist.empty or len(hist) < 2:
            return {}
        closes = hist["Close"].dropna()
        last   = closes.iloc[-1]
        prev   = closes.iloc[-2]
        ret_1d = (last / prev - 1) if prev else None
        # ~21 trading days ≈ 1 month
        idx_1m = max(0, len(closes) - 22)
        close_1m = closes.iloc[idx_1m]
        ret_1m = (last / close_1m - 1) if close_1m else None
        # ~252 trading days ≈ 1 year
        idx_1y = max(0, len(closes) - 253)
        close_1y = closes.iloc[idx_1y]
        ret_1y = (last / close_1y - 1) if close_1y else None
        return {"1D": ret_1d, "1M": ret_1m, "1Y": ret_1y}
    except Exception:
        return {}


@st.cache_data(ttl=300)
def load_snapshot(snap_date, index_name: str | None = None) -> pd.DataFrame:
    params: dict = {"date": str(snap_date)}
    index_join = ""
    if index_name:
        index_join = """
            JOIN index_membership im
              ON s.symbol = im.symbol
             AND im.index_name = :index_name
        """
        params["index_name"] = index_name

    sql = text(f"""
        SELECT
            sd.symbol, s.name, s.sector,
            sd.cmp,
            sd.ret_1d, sd.ret_1w, sd.ret_30d, sd.ret_60d, sd.ret_180d, sd.ret_365d,
            sd.dma_50, sd.dma_200, sd.status_50dma, sd.status_200dma,
            sd.pe_ratio, sd.market_cap_cr,
            s.screener_url, s.tradingview_url
        FROM snapshots_daily sd
        JOIN stocks s ON sd.symbol = s.symbol
        {index_join}
        WHERE sd.date = :date AND s.is_active = TRUE
        ORDER BY sd.symbol
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_sector_performance(snap_date) -> pd.DataFrame:
    """Aggregate all sectors live from snapshots_daily so every sector is included."""
    sql = text("""
        SELECT
            s.sector,
            COUNT(*)                                                         AS num_companies,
            SUM(CASE WHEN sd.ret_1d > 0 THEN 1 ELSE 0 END)                 AS advances,
            SUM(CASE WHEN sd.ret_1d < 0 THEN 1 ELSE 0 END)                 AS declines,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_1d)          AS day_change_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_1w)          AS week_chg_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_30d)         AS month_chg_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_60d)         AS qtr_chg_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_180d)        AS half_yr_chg_pct,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.ret_365d)        AS year_chg_pct
        FROM snapshots_daily sd
        JOIN stocks s ON s.symbol = sd.symbol
        WHERE sd.date = :date
          AND s.sector IS NOT NULL
          AND s.is_active = TRUE
        GROUP BY s.sector
        ORDER BY month_chg_pct DESC NULLS LAST
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"date": str(snap_date)})
    return df


@st.cache_data(ttl=300)
def load_all_symbols() -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT symbol, name, sector FROM stocks WHERE is_active = TRUE"),
            conn,
        )
    return df


@st.cache_data(ttl=300)
def load_ohlcv(symbol: str, days: int = 365) -> pd.DataFrame:
    sql = text("""
        SELECT date, open, high, low, close, volume
        FROM prices_daily
        WHERE symbol = :symbol
          AND date >= CURRENT_DATE - CAST(:days AS INT) * INTERVAL '1 day'
        ORDER BY date ASC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"symbol": symbol, "days": days})
    df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=300)
def load_themes() -> pd.DataFrame:
    sql = text("""
        SELECT theme_slug, theme_name, theme_order, actual_stock_count
        FROM themes_with_counts
        ORDER BY theme_order
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=300)
def load_theme_averages() -> pd.DataFrame:
    sql = text("""
        SELECT
            tm.theme_slug,
            AVG(snap.ret_1w)    AS avg_ret_1w,
            AVG(snap.ret_30d)   AS avg_ret_30d,
            AVG(snap.ret_365d)  AS avg_ret_365d
        FROM theme_membership tm
        JOIN snapshots_daily snap
            ON snap.symbol = tm.symbol
            AND snap.date = (SELECT MAX(date) FROM snapshots_daily)
        GROUP BY tm.theme_slug
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=300)
def load_theme_stocks(theme_slug: str) -> pd.DataFrame:
    sql = text("""
        SELECT
            s.symbol,
            s.name,
            s.screener_url,
            s.tradingview_url,
            snap.cmp,
            snap.ret_1w,
            snap.ret_30d,
            snap.ret_60d,
            snap.ret_180d,
            snap.ret_365d,
            snap.market_cap_cr,
            COALESCE(lf.pe_ttm, snap.pe_ratio) AS pe_ratio
        FROM theme_membership tm
        JOIN stocks s ON s.symbol = tm.symbol
        LEFT JOIN snapshots_daily snap
            ON snap.symbol = s.symbol
            AND snap.date = (SELECT MAX(date) FROM snapshots_daily)
        LEFT JOIN latest_financials lf ON lf.symbol = s.symbol
        WHERE tm.theme_slug = :theme_slug
        ORDER BY snap.market_cap_cr DESC NULLS LAST
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"theme_slug": theme_slug})


@st.cache_data(ttl=300)
def load_refresh_status() -> dict | None:
    sql = text("""
        SELECT started_at, finished_at, stocks_total, stocks_success, stocks_failed, status
        FROM refresh_log ORDER BY started_at DESC LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    return dict(row._mapping) if row else None

# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------
PCT_COLS = ["ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d"]

DISPLAY_COLS = {
    "symbol":        "Symbol",
    "name":          "Name",
    "sector":        "Sector",
    "cmp":           "CMP",
    "ret_1d":        "1D%",
    "ret_1w":        "1W%",
    "ret_30d":       "30D%",
    "ret_60d":       "60D%",
    "ret_180d":      "180D%",
    "ret_365d":      "365D%",
    "market_cap_cr": "MCap (Cr)",
    "pe_ratio":      "P/E",
    "status_50dma":  "50DMA",
    "status_200dma": "200DMA",
}


def _color_return(val):
    if pd.isna(val) or val == "—":
        return "color: #4a5568"
    try:
        n = float(str(val).replace("%", "").replace("+", ""))
        return "color: #22c55e; font-weight:600" if n >= 0 else "color: #ef4444; font-weight:600"
    except (ValueError, TypeError):
        return ""


def _fmt_pct(val):
    if pd.isna(val): return "—"
    return f"{val * 100:+.2f}%"


def _fmt_mcap(val):
    if pd.isna(val): return "—"
    return f"₹{val:,.2f} Cr"


def prepare_display(df: pd.DataFrame) -> pd.DataFrame:
    d = df[list(DISPLAY_COLS.keys())].copy()
    d = d.rename(columns=DISPLAY_COLS)
    for raw, pretty in DISPLAY_COLS.items():
        if raw in PCT_COLS:
            d[pretty] = df[raw].map(_fmt_pct)
    d["CMP"] = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    d["MCap (Cr)"] = df["market_cap_cr"].map(_fmt_mcap)
    d["P/E"] = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    return d

# ---------------------------------------------------------------------------
# Stock chart
# ---------------------------------------------------------------------------
CHART_DURATIONS = {
    "1D":  1,
    "1W":  7,
    "1M":  30,
    "3M":  90,
    "6M":  180,
    "1Y":  365,
}


def _render_chart_body(symbol: str, name: str):
    ohlcv = load_ohlcv(symbol, days=365)

    if ohlcv.empty:
        st.warning(f"No price history found for **{symbol}** in the database.")
        return

    # ── Header row: name + live price + day change ───────────────────────────
    last  = ohlcv.iloc[-1]
    prev  = ohlcv.iloc[-2] if len(ohlcv) > 1 else last
    day_chg_pct = (last["close"] - prev["close"]) / prev["close"] * 100 if prev["close"] else 0
    chg_color   = "#22c55e" if day_chg_pct >= 0 else "#ef4444"
    arrow       = "▲" if day_chg_pct >= 0 else "▼"

    st.markdown(
        f"<div style='display:flex;align-items:baseline;gap:14px;margin-bottom:10px'>"
        f"<span style='font-size:20px;font-weight:700;color:#e2e8f0'>{symbol}</span>"
        f"<span style='color:#8b97a8;font-size:13px'>{name}</span>"
        f"<span style='font-size:26px;font-weight:700;color:#e2e8f0'>₹{last['close']:,.2f}</span>"
        f"<span style='font-size:14px;font-weight:600;color:{chg_color}'>"
        f"{arrow} {abs(day_chg_pct):.2f}%</span>"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Duration selector ────────────────────────────────────────────────────
    dur_key = f"chart_dur_{symbol}"
    if dur_key not in st.session_state:
        st.session_state[dur_key] = "3M"

    dur_cols = st.columns(len(CHART_DURATIONS))
    for i, label in enumerate(CHART_DURATIONS):
        with dur_cols[i]:
            if st.button(
                label,
                key=f"dur_{symbol}_{label}",
                type="primary" if st.session_state[dur_key] == label else "secondary",
                use_container_width=True,
            ):
                st.session_state[dur_key] = label
                st.rerun()

    sel_label = st.session_state[dur_key]
    cutoff    = pd.Timestamp.now() - pd.Timedelta(days=CHART_DURATIONS[sel_label])
    df        = ohlcv[ohlcv["date"] >= cutoff].copy()

    if df.empty:
        st.info("No data available for this time range.")
        return

    # ── 1D: show summary cards instead of chart ──────────────────────────────
    if sel_label == "1D":
        row = df.iloc[-1]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Open",   f"₹{row['open']:,.2f}"  if pd.notna(row["open"])   else "—")
        c2.metric("High",   f"₹{row['high']:,.2f}"  if pd.notna(row["high"])   else "—")
        c3.metric("Low",    f"₹{row['low']:,.2f}"   if pd.notna(row["low"])    else "—")
        c4.metric("Close",  f"₹{row['close']:,.2f}" if pd.notna(row["close"])  else "—")
        c5.metric("Volume", f"{int(row['volume']):,}" if pd.notna(row["volume"]) else "—")
        return

    # ── Candlestick + Volume chart ───────────────────────────────────────────
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"], high=df["high"],
            low=df["low"],   close=df["close"],
            name=symbol,
            increasing=dict(line=dict(color="#22c55e", width=1), fillcolor="#22c55e"),
            decreasing=dict(line=dict(color="#ef4444", width=1), fillcolor="#ef4444"),
            whiskerwidth=0.4,
        ),
        row=1, col=1,
    )

    bar_colors = [
        "#22c55e" if c >= o else "#ef4444"
        for c, o in zip(df["close"], df["open"])
    ]
    fig.add_trace(
        go.Bar(
            x=df["date"],
            y=df["volume"],
            name="Volume",
            marker_color=bar_colors,
            marker_line_width=0,
            opacity=0.55,
        ),
        row=2, col=1,
    )

    grid = dict(color="#1e2535", width=1)
    fig.update_layout(
        height=500,
        plot_bgcolor="#0f1117",
        paper_bgcolor="#0f1117",
        font=dict(color="#cbd5e0", size=12),
        xaxis_rangeslider_visible=False,
        showlegend=False,
        margin=dict(l=0, r=10, t=10, b=10),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#1e2535", bordercolor="#2d3748", font_color="#e2e8f0"),
        xaxis=dict(gridcolor=grid["color"], showgrid=True, zeroline=False,
                   showspikes=True, spikethickness=1, spikecolor="#4a5568",
                   spikedash="solid"),
        yaxis=dict(gridcolor=grid["color"], showgrid=True, zeroline=False,
                   tickprefix="₹", side="right",
                   showspikes=True, spikethickness=1, spikecolor="#4a5568",
                   spikedash="solid"),
        xaxis2=dict(gridcolor=grid["color"], showgrid=True, zeroline=False),
        yaxis2=dict(gridcolor=grid["color"], showgrid=True, zeroline=False,
                    tickformat=".2s", side="right"),
    )
    fig.update_traces(
        selector=dict(type="candlestick"),
        hoverlabel=dict(namelength=0),
    )

    st.plotly_chart(fig, use_container_width=True)

    # ── OHLCV stats strip ────────────────────────────────────────────────────
    h52 = ohlcv["high"].max()
    l52 = ohlcv["low"].min()
    avg_vol = ohlcv["volume"].tail(30).mean()

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("52W High",   f"₹{h52:,.2f}")
    s2.metric("52W Low",    f"₹{l52:,.2f}")
    s3.metric("Today O/H/L", f"₹{last['open']:,.0f} / {last['high']:,.0f} / {last['low']:,.0f}")
    s4.metric("Volume",     f"{int(last['volume']):,}" if pd.notna(last["volume"]) else "—")
    s5.metric("Avg Vol 30D", f"{int(avg_vol):,}" if pd.notna(avg_vol) else "—")


@st.dialog("Stock Chart", width="large")
def _show_chart_dialog(symbol: str, name: str):
    _render_chart_body(symbol, name)


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
def render_summary_cards(df: pd.DataFrame, index_name: str | None = None):
    adv       = int((df["ret_1d"] > 0).sum())
    dec       = int((df["ret_1d"] < 0).sum())
    above_200 = int((df["status_200dma"] == "Above 200DMA").sum())
    total     = len(df)

    # Fetch index-level returns if a benchmark symbol is mapped
    idx_rets: dict = {}
    yf_sym = INDEX_YF_SYMBOL.get(index_name) if index_name else None
    if yf_sym:
        idx_rets = fetch_index_returns(yf_sym)

    def _idx_val(key):
        v = idx_rets.get(key)
        return _fmt_pct(v) if v is not None else "—"

    label_prefix = ALL_UNIVERSES.get(index_name, "Index") if index_name else "Index"

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric(f"{label_prefix} 1D",  _idx_val("1D"),  delta=None)
    with c2: st.metric(f"{label_prefix} 1M",  _idx_val("1M"),  delta=None)
    with c3: st.metric(f"{label_prefix} 1Y",  _idx_val("1Y"),  delta=None)
    with c4: st.metric("Adv / Dec",            f"{adv} / {dec}")
    with c5: st.metric("Above 200 DMA",        f"{above_200} / {total}")


def render_table(df: pd.DataFrame, key: str = "default", page_size: int = 500):
    total = len(df)
    pages = max(1, (total + page_size - 1) // page_size)

    hc1, hc2 = st.columns([4, 1])
    with hc1:
        st.caption(f"{total} stocks")
    with hc2:
        if pages > 1:
            page = st.number_input(
                "Page", min_value=1, max_value=pages, value=1, step=1,
                label_visibility="collapsed", key=f"page_{key}",
            )
        else:
            page = 1

    start = (page - 1) * page_size
    chunk = df.iloc[start : start + page_size].reset_index(drop=True)
    display = prepare_display(chunk)

    # Add link columns directly into the display df
    display["Screener"] = chunk["screener_url"].where(chunk["screener_url"].notna(), other=None)
    display["TradingView"] = chunk["tradingview_url"].where(chunk["tradingview_url"].notna(), other=None)

    styled = display.style
    for raw, pretty in DISPLAY_COLS.items():
        if raw in PCT_COLS:
            styled = styled.map(_color_return, subset=[pretty])

    st.caption("💡 Click any row to open its candlestick chart")

    event = st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=700,
        selection_mode="single-row",
        on_select="rerun",
        column_config={
            "Screener":     st.column_config.LinkColumn("Screener",     display_text="Screener ↗"),
            "TradingView":  st.column_config.LinkColumn("TradingView",  display_text="TV ↗"),
        },
    )

    if event.selection and event.selection.rows:
        selected_idx = event.selection.rows[0]
        selected_row = chunk.iloc[selected_idx]
        _show_chart_dialog(
            symbol=selected_row["symbol"],
            name=selected_row["name"] if "name" in selected_row.index else selected_row["symbol"],
        )

    csv_bytes = df[list(DISPLAY_COLS.keys())].to_csv(index=False).encode()
    st.download_button("⬇ Download CSV", csv_bytes, "stocks.csv", "text/csv",
                       key=f"dl_{key}")


# ---------------------------------------------------------------------------
# Sort buttons — keyed per universe so each tab has independent sort state
# ---------------------------------------------------------------------------
SORT_BUTTONS = [
    ("1D%",    "ret_1d",        True),
    ("1W%",    "ret_1w",        True),
    ("30D%",   "ret_30d",       True),
    ("60D%",   "ret_60d",       True),
    ("180D%",  "ret_180d",      True),
    ("365D%",  "ret_365d",      True),
    ("MCap",   "market_cap_cr", True),
    ("P/E",    "pe_ratio",      True),
    ("Symbol", "symbol",        False),
]


def render_sort_and_table(df: pd.DataFrame, key: str):
    sc = f"sc_{key}"
    sd = f"sd_{key}"
    if sc not in st.session_state:
        st.session_state[sc] = "market_cap_cr"
        st.session_state[sd] = True

    btn_cols = st.columns(len(SORT_BUTTONS))
    for i, (label, col, default_desc) in enumerate(SORT_BUTTONS):
        active = st.session_state[sc] == col
        arrow  = (" ↓" if st.session_state[sd] else " ↑") if active else ""
        with btn_cols[i]:
            if st.button(
                f"{label}{arrow}",
                key=f"sb_{key}_{col}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                if st.session_state[sc] == col:
                    st.session_state[sd] = not st.session_state[sd]
                else:
                    st.session_state[sc] = col
                    st.session_state[sd] = default_desc
                st.rerun()

    col  = st.session_state[sc]
    desc = st.session_state[sd]
    if col in df.columns:
        df = df.sort_values(col, ascending=not desc, na_position="last")

    render_table(df, key=key)


# ---------------------------------------------------------------------------
# Themes view — left sidebar picker + right stock table
# ---------------------------------------------------------------------------
THEME_PCT_COLS = ["ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d"]
THEME_DISPLAY_COLS = {
    "symbol":        "Symbol",
    "name":          "Name",
    "cmp":           "CMP",
    "ret_1w":        "1W %",
    "ret_30d":       "1M %",
    "ret_60d":       "3M %",
    "ret_180d":      "6M %",
    "ret_365d":      "1Y %",
    "market_cap_cr": "Market Cap (₹ Cr)",
    "pe_ratio":      "P/E",
}


def _prepare_theme_display(df: pd.DataFrame) -> pd.DataFrame:
    d = df[list(THEME_DISPLAY_COLS.keys())].copy()
    d = d.rename(columns=THEME_DISPLAY_COLS)
    for raw, pretty in THEME_DISPLAY_COLS.items():
        if raw in THEME_PCT_COLS:
            d[pretty] = df[raw].map(_fmt_pct)
    d["CMP"] = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    d["Market Cap (₹ Cr)"] = df["market_cap_cr"].map(
        lambda v: f"₹{v:,.2f} Cr" if pd.notna(v) else "—"
    )
    d["P/E"] = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "N/A")
    return d


_THEME_SORT_OPTIONS = {
    "1W":  ("avg_ret_1w",   "1W Avg"),
    "1M":  ("avg_ret_30d",  "1M Avg"),
    "1Y":  ("avg_ret_365d", "1Y Avg"),
}


def render_themes_view():
    # Anchor at the very top — JS scrolls here after theme selection
    st.markdown('<div id="themes-top"></div>', unsafe_allow_html=True)
    if st.session_state.pop("_theme_scroll_top", False):
        components.html("""
<script>
(function() {
    var anchor = window.parent.document.getElementById('themes-top');
    if (anchor) {
        anchor.scrollIntoView({behavior: 'instant', block: 'start'});
        return;
    }
    // fallback: scroll any known Streamlit container
    var selectors = [
        '[data-testid="stAppViewContainer"]',
        '[data-testid="stMain"]',
        'section.main',
        '.main'
    ];
    for (var i = 0; i < selectors.length; i++) {
        var el = window.parent.document.querySelector(selectors[i]);
        if (el) { el.scrollTop = 0; break; }
    }
})();
</script>
""", height=0)

    themes_df = load_themes()
    if themes_df.empty:
        st.info("No themes found. Run `python backend/seed_themes.py` to populate.")
        return

    # Load and merge average returns
    avgs_df = load_theme_averages()
    themes_df = themes_df.merge(avgs_df, on="theme_slug", how="left")

    # Initialise selected theme
    if "selected_theme_slug" not in st.session_state:
        st.session_state["selected_theme_slug"] = themes_df.iloc[0]["theme_slug"]

    left_col, right_col = st.columns([1, 3])

    # ── Left: theme picker ───────────────────────────────────────────────────
    with left_col:
        # Duration pill selector
        if "theme_sort_period" not in st.session_state:
            st.session_state["theme_sort_period"] = "1M"

        st.markdown(
            "<div style='font-size:10px;font-weight:700;letter-spacing:0.12em;"
            "text-transform:uppercase;color:#374151;margin-bottom:6px;'>"
            "Return Period</div>",
            unsafe_allow_html=True,
        )
        dur_cols = st.columns(len(_THEME_SORT_OPTIONS))
        for i, dur_label in enumerate(_THEME_SORT_OPTIONS):
            with dur_cols[i]:
                if st.button(
                    dur_label,
                    key=f"theme_dur_{dur_label}",
                    type="primary" if st.session_state["theme_sort_period"] == dur_label else "secondary",
                    use_container_width=True,
                ):
                    st.session_state["theme_sort_period"] = dur_label
                    st.rerun()

        sort_col, sort_label = _THEME_SORT_OPTIONS[st.session_state["theme_sort_period"]]

        search = st.text_input("Search themes", placeholder="Type to filter…", label_visibility="collapsed")
        filtered = themes_df.sort_values(sort_col, ascending=False, na_position="last")
        if search.strip():
            filtered = filtered[
                filtered["theme_name"].str.contains(search.strip(), case=False, na=False)
            ]

        for _, row in filtered.iterrows():
            avg_val = row.get(sort_col)
            avg_str = f"{avg_val * 100:+.1f}%" if pd.notna(avg_val) else "—"
            label = f"{row['theme_name']} ({int(row['actual_stock_count'])})  {avg_str}"
            is_selected = st.session_state["selected_theme_slug"] == row["theme_slug"]
            if st.button(
                label,
                key=f"theme_btn_{row['theme_slug']}",
                type="primary" if is_selected else "secondary",
                use_container_width=True,
            ):
                st.session_state["selected_theme_slug"] = row["theme_slug"]
                st.session_state["_theme_scroll_top"] = True
                st.rerun()

    # ── Right: stock table for selected theme ────────────────────────────────
    with right_col:
        selected_slug = st.session_state["selected_theme_slug"]
        theme_row = themes_df[themes_df["theme_slug"] == selected_slug]
        if theme_row.empty:
            # Fallback if selection no longer exists after a search
            selected_slug = themes_df.iloc[0]["theme_slug"]
            theme_row = themes_df.iloc[[0]]

        theme_name  = theme_row.iloc[0]["theme_name"]
        stock_count = int(theme_row.iloc[0]["actual_stock_count"])

        stocks_df = load_theme_stocks(selected_slug)

        # Latest snapshot date (reuse already-loaded dates list)
        dates = load_available_dates()
        latest_date = pd.Timestamp(dates[0]).strftime("%d %b %Y") if dates else "—"

        st.subheader(theme_name)
        st.caption(f"{stock_count} companies • Data as of {latest_date}")

        # Warn if >10% of stocks have no price data yet
        null_count = stocks_df["cmp"].isna().sum()
        if len(stocks_df) > 0 and null_count / len(stocks_df) > 0.1:
            st.warning(
                "⚠️ Some stocks in this theme were recently added and will be "
                "populated after the next daily refresh (4:30 PM IST)."
            )

        if stocks_df.empty:
            st.info("No stocks found for this theme.")
            return

        display = _prepare_theme_display(stocks_df)
        display["Screener"]     = stocks_df["screener_url"].where(stocks_df["screener_url"].notna(), other=None)
        display["TradingView"]  = stocks_df["tradingview_url"].where(stocks_df["tradingview_url"].notna(), other=None)

        styled = display.style
        for raw, pretty in THEME_DISPLAY_COLS.items():
            if raw in THEME_PCT_COLS:
                styled = styled.map(_color_return, subset=[pretty])

        event = st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=650,
            selection_mode="single-row",
            on_select="rerun",
            column_config={
                "Screener":    st.column_config.LinkColumn("Screener",    display_text="Screener ↗"),
                "TradingView": st.column_config.LinkColumn("TradingView", display_text="TV ↗"),
            },
        )

        if event.selection and event.selection.rows:
            selected_idx = event.selection.rows[0]
            selected_row = stocks_df.iloc[selected_idx]
            _show_chart_dialog(
                symbol=selected_row["symbol"],
                name=selected_row["name"],
            )

        csv_bytes = stocks_df[list(THEME_DISPLAY_COLS.keys())].to_csv(index=False).encode()
        st.download_button(
            "⬇ Download CSV", csv_bytes,
            f"{selected_slug}.csv", "text/csv",
            key=f"dl_theme_{selected_slug}",
        )


# ---------------------------------------------------------------------------
# Analysis view — Top N / Bottom N per universe with timeframe toggle
# ---------------------------------------------------------------------------
ANALYSIS_TOP_N = {
    "NIFTY_50":   5,
    "NIFTY_500":  20,
    "NIFTY_BANK": 5,
    "FNO":        10,
    "BANKS":      5,
    "NBFCS":      5,
    "PHARMA":     5,
    "DEFENCE":    5,
}

RETURN_COLS = {
    "1D":   ("ret_1d",   "1-Day Return (%)"),
    "1W":   ("ret_1w",   "1-Week Return (%)"),
    "30D":  ("ret_30d",  "30-Day Return (%)"),
    "60D":  ("ret_60d",  "60-Day Return (%)"),
    "180D": ("ret_180d", "6-Month Return (%)"),
    "365D": ("ret_365d", "1-Year Return (%)"),
}


_CHART_FONT = "Inter, -apple-system, BlinkMacSystemFont, sans-serif"
_BG         = "rgba(0,0,0,0)"
_PLOT_BG    = "#080c14"
_GRID       = "#111827"
_ZERO_LINE  = "#1e293b"


def _build_ranked_chart(
    rows: pd.DataFrame,   # ['symbol','name','pct'] already sorted for display
    color: str,
    ret_label: str,
) -> go.Figure:
    """
    Build a single clean horizontal bar chart.
    rows must already be sorted in the direction you want rendered top→bottom.
    """
    n = len(rows)
    # Tight per-bar pixel height + fixed header/footer padding
    chart_h = n * 26 + 52

    # Pad the x-axis range so outside text never clips
    abs_max = rows["pct"].abs().max() if not rows.empty else 1
    x_pad   = abs_max * 0.30          # 30% extra room on the value side

    # Determine text side: top chart values positive → pad right; bottom → pad left
    positive_dom = rows["pct"].median() >= 0
    x_range = (
        [-(abs_max * 0.05), abs_max + x_pad] if positive_dom
        else [-(abs_max + x_pad), abs_max * 0.05]
    )

    fig = go.Figure(go.Bar(
        x=rows["pct"].tolist(),
        y=rows["symbol"].tolist(),
        orientation="h",
        width=0.45,                     # thin bars
        text=[f"{v:+.2f}%" for v in rows["pct"]],
        textposition="outside",
        cliponaxis=False,               # prevent value labels from being clipped
        marker=dict(
            color=color,
            opacity=0.88,
            line=dict(width=0),
        ),
        customdata=rows["name"].tolist(),
        hovertemplate=(
            "<b style='font-size:13px'>%{y}</b><br>"
            "<span style='color:#8b97a8;font-size:11px'>%{customdata}</span><br>"
            f"<b>{ret_label}:</b> %{{x:+.2f}}%<extra></extra>"
        ),
    ))

    fig.update_layout(
        height=chart_h,
        plot_bgcolor=_PLOT_BG,
        paper_bgcolor=_BG,
        font=dict(family=_CHART_FONT, color="#64748b", size=11),
        margin=dict(l=90, r=72, t=6, b=6),
        bargap=0,
        xaxis=dict(
            range=x_range,
            gridcolor=_GRID,
            gridwidth=1,
            tickformat="+.1f",
            ticksuffix="%",
            tickfont=dict(size=9, color="#94a3b8"),
            zeroline=True,
            zerolinecolor=_ZERO_LINE,
            zerolinewidth=1,
            showline=False,
        ),
        yaxis=dict(
            autorange="reversed",
            gridcolor=_GRID,
            tickfont=dict(
                size=11,
                color="#cbd5e0",
                family=_CHART_FONT,
            ),
            showline=False,
        ),
        showlegend=False,
        hoverlabel=dict(
            bgcolor="#1e2535",
            bordercolor="#334155",
            font=dict(family=_CHART_FONT, color="#e2e8f0", size=12),
        ),
    )
    fig.update_traces(
        textfont=dict(
            family=_CHART_FONT,
            color="#e2e8f0",
            size=10,
        ),
    )
    return fig


def render_breadth_tab(snap_date, universes: list, section_key: str):
    """
    Dedicated Market Breadth tab — 50/200 DMA breadth donuts per universe.
    Renders a 2-column card grid; each card pairs both donuts in one figure.
    """
    st.markdown(
        "<div style='font-size:11px;color:#475569;margin-bottom:20px;'>"
        "Market breadth tracks how many stocks are trading above their key moving averages. "
        "High breadth (≥65%) signals broad participation. "
        "Low breadth (&lt;35%) warns of a narrow or deteriorating rally."
        "</div>",
        unsafe_allow_html=True,
    )

    def _stats(status_col: str, above_val: str, below_val: str):
        valid = df[status_col].dropna()
        above = int((valid == above_val).sum())
        below = int((valid == below_val).sum())
        total = above + below
        pct   = round(above / total * 100, 1) if total else 0.0
        return above, below, total, pct

    def _mood(pct: float):
        if pct >= 65:   return "#22c55e", "Bullish"
        elif pct >= 50: return "#4ade80", "Leaning Bullish"
        elif pct >= 35: return "#f59e0b", "Neutral"
        else:           return "#ef4444", "Bearish"

    def _progress_bar(pct: float, color: str) -> str:
        """Thin HTML progress bar."""
        return (
            f"<div style='background:#111827;border-radius:3px;height:5px;"
            f"overflow:hidden;margin:6px 0 2px;'>"
            f"<div style='background:{color};width:{pct:.1f}%;height:100%;"
            f"border-radius:3px;transition:width 0.3s;'></div></div>"
        )

    # ── 2-column card grid ───────────────────────────────────────────────────
    for i in range(0, len(universes), 2):
        pair = universes[i:i+2]
        col_left, col_right = st.columns(2, gap="large")

        for col_widget, (key, label) in zip([col_left, col_right], pair):
            df = load_snapshot(snap_date, index_name=key)

            with col_widget:
                with st.container(border=True):
                    if df.empty:
                        st.caption(f"No data for {label}.")
                        continue

                    a50, b50, t50, pct50   = _stats("status_50dma",  "Above 50DMA",  "Below 50DMA")
                    a200, b200, t200, pct200 = _stats("status_200dma", "Above 200DMA", "Below 200DMA")
                    c50,  mood50  = _mood(pct50)
                    c200, mood200 = _mood(pct200)

                    # ── Card header ──────────────────────────────────────────────
                    # Pick accent color from the dominant breadth signal
                    dominant_color = c50 if pct50 >= pct200 else c200
                    st.markdown(
                        f"<div style='display:flex;align-items:center;gap:12px;"
                        f"margin-bottom:4px;padding-bottom:12px;"
                        f"border-bottom:1px solid #1e2d45;'>"
                        f"<div style='width:4px;height:34px;background:{dominant_color};"
                        f"border-radius:2px;flex-shrink:0;opacity:0.85;'></div>"
                        f"<div>"
                        f"<div style='font-size:17px;font-weight:700;color:#f1f5f9;"
                        f"letter-spacing:-0.02em;line-height:1.2;'>{label}</div>"
                        f"<div style='font-size:11px;color:#64748b;font-weight:500;"
                        f"letter-spacing:0.07em;text-transform:uppercase;margin-top:2px;'>"
                        f"{len(df)} stocks</div>"
                        f"</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                    # ── Combined 2-donut subplot ─────────────────────────────
                    fig = make_subplots(
                        rows=1, cols=2,
                        specs=[[{"type": "pie"}, {"type": "pie"}]],
                        horizontal_spacing=0.06,
                        subplot_titles=["50 DMA", "200 DMA"],
                    )

                    for col_idx, (above, below) in enumerate(
                        [(a50, b50), (a200, b200)], start=1
                    ):
                        total = above + below
                        fig.add_trace(go.Pie(
                            values=[above, below] if total else [1],
                            labels=["Above", "Below"] if total else ["No data"],
                            hole=0.74,
                            marker=dict(
                                colors=["#22c55e", "#ef4444"] if total else ["#1e293b"],
                                line=dict(color="#080c14", width=3),
                            ),
                            textinfo="none",
                            hovertemplate=(
                                "%{label}: <b>%{value} stocks</b> (%{percent})<extra></extra>"
                                if total else ""
                            ),
                            direction="clockwise",
                            sort=False,
                            rotation=90,
                            showlegend=False,
                        ), row=1, col=col_idx)

                    # Center annotations — positions tuned for horizontal_spacing=0.06
                    base_anns = list(fig.layout.annotations)   # subplot titles
                    for ann in base_anns:                       # style subplot titles
                        ann.font.size   = 11
                        ann.font.color  = "#94a3b8"
                        ann.font.family = _CHART_FONT

                    for xc, pct, color in zip(
                        [0.235, 0.765],
                        [pct50, pct200],
                        [c50,   c200],
                    ):
                        base_anns += [
                            dict(
                                text=f"<b>{pct:.0f}%</b>",
                                x=xc, y=0.54,
                                font=dict(size=26, color=color, family=_CHART_FONT),
                                showarrow=False, xanchor="center", yanchor="middle",
                            ),
                            dict(
                                text="ABOVE",
                                x=xc, y=0.38,
                                font=dict(size=9, color="#94a3b8", family=_CHART_FONT),
                                showarrow=False, xanchor="center", yanchor="middle",
                            ),
                        ]

                    fig.update_layout(
                        height=230,
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family=_CHART_FONT, color="#64748b", size=11),
                        margin=dict(l=10, r=10, t=34, b=4),
                        showlegend=False,
                        annotations=base_anns,
                        hoverlabel=dict(
                            bgcolor="#1e2535", bordercolor="#334155",
                            font=dict(family=_CHART_FONT, color="#e2e8f0", size=12),
                        ),
                    )
                    st.plotly_chart(fig, use_container_width=True,
                                    config={"displayModeBar": False})

                    # ── Stats strip ──────────────────────────────────────────
                    sc1, sc2 = st.columns(2)
                    for sc, above, below, total, pct, color, mood, dma_label in [
                        (sc1, a50,  b50,  t50,  pct50,  c50,  mood50,  "50 DMA"),
                        (sc2, a200, b200, t200, pct200, c200, mood200, "200 DMA"),
                    ]:
                        with sc:
                            st.markdown(
                                f"<div style='text-align:center;padding:4px 0 8px;'>"
                                f"<div style='font-size:12px;font-weight:700;color:#94a3b8;"
                                f"text-transform:uppercase;letter-spacing:0.1em;"
                                f"margin-bottom:6px;'>{dma_label}</div>"
                                + _progress_bar(pct, color) +
                                f"<div style='font-size:13px;margin-top:6px;'>"
                                f"<span style='color:#22c55e;font-weight:600;'>{above}↑</span>"
                                f"<span style='color:#475569;'> / </span>"
                                f"<span style='color:#ef4444;font-weight:600;'>{below}↓</span>"
                                f"</div>"
                                f"<div style='font-size:13px;font-weight:700;"
                                f"color:{color};margin-top:3px;letter-spacing:0.02em;'>"
                                f"{mood}</div>"
                                f"</div>",
                                unsafe_allow_html=True,
                            )

        if i + 2 < len(universes):
            st.markdown(
                "<div style='height:8px'></div>",
                unsafe_allow_html=True,
            )


def _render_topbottom_chart(df: pd.DataFrame, ret_col: str, n: int,
                             universe_label: str, ret_label: str):
    """Render Top-N (green) and Bottom-N (red) charts side by side."""
    df_valid = df[df[ret_col].notna()].copy()
    df_valid["pct"] = df_valid[ret_col] * 100

    # Sort: top descending → reversed in chart so rank 1 is at top visually
    top_n    = df_valid.nlargest(n, ret_col)[["symbol", "name", "pct"]].reset_index(drop=True)
    bottom_n = df_valid.nsmallest(n, ret_col)[["symbol", "name", "pct"]].reset_index(drop=True)

    col_top, col_bot = st.columns(2)

    # ── Top N ────────────────────────────────────────────────────────────────
    with col_top:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;letter-spacing:0.08em;"
            f"text-transform:uppercase;color:#22c55e;margin-bottom:4px;'>"
            f"Top {n} &nbsp;·&nbsp; {universe_label}</div>",
            unsafe_allow_html=True,
        )
        if top_n.empty:
            st.caption("No data.")
        else:
            st.plotly_chart(
                _build_ranked_chart(top_n, "#22c55e", ret_label),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    # ── Bottom N ─────────────────────────────────────────────────────────────
    with col_bot:
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;letter-spacing:0.08em;"
            f"text-transform:uppercase;color:#ef4444;margin-bottom:4px;'>"
            f"Bottom {n} &nbsp;·&nbsp; {universe_label}</div>",
            unsafe_allow_html=True,
        )
        if bottom_n.empty:
            st.caption("No data.")
        else:
            st.plotly_chart(
                _build_ranked_chart(bottom_n, "#ef4444", ret_label),
                use_container_width=True,
                config={"displayModeBar": False},
            )


def render_analysis_tab(snap_date, universes: list, section_key: str):
    """
    Renders the Analysis sub-tab for a group of universes.
    universes: list of (index_key, display_label) tuples
    """
    tf_key = f"analysis_tf_{section_key}"
    if tf_key not in st.session_state:
        st.session_state[tf_key] = "30D"

    # ── Timeframe pill selector ───────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:10px;font-weight:700;letter-spacing:0.12em;"
        "text-transform:uppercase;color:#374151;margin-bottom:8px;'>"
        "Timeframe</div>",
        unsafe_allow_html=True,
    )
    tf_cols = st.columns(len(RETURN_COLS))
    for i, tf_label in enumerate(RETURN_COLS):
        with tf_cols[i]:
            if st.button(
                tf_label,
                key=f"tf_{section_key}_{tf_label}",
                type="primary" if st.session_state[tf_key] == tf_label else "secondary",
                use_container_width=True,
            ):
                st.session_state[tf_key] = tf_label
                st.rerun()

    selected_tf = st.session_state[tf_key]
    ret_col, ret_label = RETURN_COLS[selected_tf]

    st.divider()

    # ── One block per universe ────────────────────────────────────────────────
    for key, label in universes:
        n = ANALYSIS_TOP_N.get(key, 5)
        df = load_snapshot(snap_date, index_name=key)
        if df.empty:
            st.caption(f"No snapshot data for {label} on this date.")
            continue

        valid_count = df[ret_col].notna().sum()
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:10px;"
            f"margin-bottom:6px;'>"
            f"<span style='font-size:14px;font-weight:700;color:#e2e8f0;"
            f"letter-spacing:-0.01em;'>{label}</span>"
            f"<span style='font-size:10px;color:#374151;font-weight:500;"
            f"letter-spacing:0.04em;'>{valid_count} stocks · {selected_tf}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        _render_topbottom_chart(df, ret_col, n, label, ret_label)
        st.divider()


# ---------------------------------------------------------------------------
# Universe view — inline filters + cards + sort + table
# ---------------------------------------------------------------------------
def render_universe_view(index_name: str, snap_date):
    df = load_snapshot(snap_date, index_name=index_name)
    if df.empty:
        st.info("No snapshot data for this date.")
        return

    # Inline filters
    fc1, fc2, fc3 = st.columns([1, 1, 1])
    with fc1:
        sectors = sorted(df["sector"].dropna().unique().tolist())
        sel_sectors = st.multiselect(
            "Sector", sectors, default=[],
            key=f"sf_{index_name}", placeholder="All sectors",
        )
    with fc2:
        sel_200dma = st.selectbox(
            "200 DMA",
            options=["All", "Above 200DMA", "Below 200DMA"],
            index=0,
            key=f"dma200_{index_name}",
        )
    with fc3:
        sel_50dma = st.selectbox(
            "50 DMA",
            options=["All", "Above 50DMA", "Below 50DMA"],
            index=0,
            key=f"dma50_{index_name}",
        )

    # Apply filters
    if sel_sectors:
        df = df[df["sector"].isin(sel_sectors)]
    if sel_200dma != "All":
        df = df[df["status_200dma"] == sel_200dma]
    if sel_50dma != "All":
        df = df[df["status_50dma"] == sel_50dma]

    if df.empty:
        st.warning("No stocks match the current filters.")
        return

    st.divider()
    render_summary_cards(df, index_name=index_name)
    st.divider()
    render_sort_and_table(df, key=index_name)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
        <div style="padding: 8px 0 16px 0; display:flex; align-items:center; gap:10px;">
            <div style="width:34px;height:34px;border-radius:8px;
                        background:linear-gradient(135deg,#1d4ed8 0%,#3b82f6 100%);
                        display:flex;align-items:center;justify-content:center;
                        font-size:16px;flex-shrink:0;">📈</div>
            <div style="font-size:20px;font-weight:800;color:#f1f5f9;letter-spacing:-0.04em;">
                Stock<span style="color:#3b82f6;">Stack</span>
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    status = load_refresh_status()
    if status:
        last_run = status.get("finished_at") or status.get("started_at")
        s   = status.get("status", "")
        ok  = status.get("stocks_success", 0)
        tot = status.get("stocks_total", 0)
        if last_run:
            ts = pd.Timestamp(last_run).strftime("%d %b %Y · %H:%M")
        dot_color = "#22c55e" if s == "success" else "#f59e0b"
        status_text = f"{ok}/{tot} stocks" if s == "success" else s.title()
        st.markdown(
            f"<div style='font-size:11.5px;color:#4a5568;display:flex;align-items:center;gap:6px;'>"
            f"<span style='width:6px;height:6px;border-radius:50%;background:{dot_color};"
            f"display:inline-block;flex-shrink:0;'></span>"
            f"<span>{status_text} · {ts}</span></div>",
            unsafe_allow_html=True,
        )

    st.divider()

    dates = load_available_dates()
    if not dates:
        st.error("No snapshot data found in Supabase.")
        st.stop()

    selected_date = st.selectbox(
        "As-of date",
        options=dates,
        format_func=lambda d: pd.Timestamp(d).strftime("%d %b %Y"),
    )

    st.divider()
    st.caption("Click any row in a table to open its price chart.")

# ---------------------------------------------------------------------------
# Main — 5 top-level tabs
# ---------------------------------------------------------------------------
tab_gm, tab_idx, tab_sec, tab_analysis, tab_themes, tab_upload = st.tabs([
    "Global Markets",
    "Indexes",
    "Sectors",
    "Sector Performance",
    "Themes",
    "Custom Upload",
])

def _page_header(title: str, date=None):
    date_str = f" <span style='color:#2d4f8e;font-size:13px;font-weight:500;margin-left:10px;'>{pd.Timestamp(date).strftime('%d %b %Y')}</span>" if date else ""
    st.markdown(
        f"<div style='display:flex;align-items:baseline;gap:0;margin-bottom:1rem;'>"
        f"<span style='font-size:18px;font-weight:700;color:#e2e8f0;letter-spacing:-0.02em;'>{title}</span>"
        f"{date_str}</div>",
        unsafe_allow_html=True,
    )


# ── Tab 1: Indexes ──────────────────────────────────────────────────────────
with tab_idx:
    _page_header("Broad Market Indexes", selected_date)
    sub_tabs = st.tabs([label for _, label in INDEX_TABS] + ["Analysis", "Breadth"])
    for tab, (key, _) in zip(sub_tabs[:len(INDEX_TABS)], INDEX_TABS):
        with tab:
            render_universe_view(key, selected_date)
    with sub_tabs[-2]:
        render_analysis_tab(selected_date, INDEX_TABS, "indexes")
    with sub_tabs[-1]:
        render_breadth_tab(selected_date, INDEX_TABS, "indexes")

# ── Tab 2: Sectors ──────────────────────────────────────────────────────────
with tab_sec:
    _page_header("Sector Views", selected_date)
    sub_tabs2 = st.tabs([label for _, label in SECTOR_TABS] + ["Analysis", "Breadth"])
    for tab, (key, _) in zip(sub_tabs2[:len(SECTOR_TABS)], SECTOR_TABS):
        with tab:
            render_universe_view(key, selected_date)
    with sub_tabs2[-2]:
        render_analysis_tab(selected_date, SECTOR_TABS, "sectors")
    with sub_tabs2[-1]:
        render_breadth_tab(selected_date, SECTOR_TABS, "sectors")

# ── Tab 3: Sector Performance ────────────────────────────────────────────────
with tab_analysis:
    _page_header("Sector Performance", selected_date)

    sector_df = load_sector_performance(selected_date)

    if sector_df.empty:
        st.info("No sector data for this date. Run `daily_refresh.py` to populate.")
    else:
        # Summary table
        disp = sector_df[[
            "sector", "num_companies", "advances", "declines",
            "day_change_pct", "week_chg_pct", "month_chg_pct",
            "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct",
        ]].copy()
        for c in ["day_change_pct", "week_chg_pct", "month_chg_pct",
                  "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct"]:
            disp[c] = disp[c].map(_fmt_pct)
        disp = disp.rename(columns={
            "sector": "Sector", "num_companies": "# Stocks",
            "advances": "Adv", "declines": "Dec",
            "day_change_pct": "1D%", "week_chg_pct": "1W%",
            "month_chg_pct": "30D%", "qtr_chg_pct": "60D%",
            "half_yr_chg_pct": "180D%", "year_chg_pct": "365D%",
        })
        st.dataframe(disp, use_container_width=True, hide_index=True)

        st.divider()

        # Bar chart
        chart_df = sector_df.copy()
        chart_df = chart_df.sort_values("month_chg_pct", ascending=False)
        chart_df["pct"] = chart_df["month_chg_pct"] * 100

        fig = px.bar(
            chart_df,
            x="pct", y="sector", orientation="h",
            color="pct",
            color_continuous_scale=["#ef4444", "#1e2535", "#22c55e"],
            color_continuous_midpoint=0,
            labels={"pct": "Median 30D Return (%)", "sector": ""},
            title="Median 30-Day Return by Sector",
        )
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
            height=max(320, len(chart_df) * 32),
            plot_bgcolor="#0f1117",
            paper_bgcolor="#0f1117",
            font_color="#cbd5e0",
            title_font_size=15,
            margin=dict(l=10, r=20, t=40, b=10),
        )
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: Themes ────────────────────────────────────────────────────────────
with tab_themes:
    render_themes_view()

# ── Tab 5: Custom Upload ─────────────────────────────────────────────────────
with tab_upload:
    _page_header("Custom Stock List")
    st.caption("Upload a CSV with a `symbol` column (NSE symbols, no `.NS` suffix).")

    uploaded = st.file_uploader("Upload CSV", type=["csv"])
    if uploaded:
        try:
            user_df = pd.read_csv(uploaded)
            if "symbol" not in user_df.columns:
                st.error("CSV must have a column named `symbol`.")
            else:
                user_symbols = user_df["symbol"].str.upper().str.strip().unique().tolist()
                all_symbols_df = load_all_symbols()
                valid = set(all_symbols_df["symbol"].tolist())
                known   = [s for s in user_symbols if s in valid]
                unknown = [s for s in user_symbols if s not in valid]

                if unknown:
                    st.warning(f"Unknown symbols (not in master): {', '.join(unknown)}")
                if known:
                    st.success(f"{len(known)} symbols matched.")
                    custom_df = load_snapshot(selected_date, index_name=None)
                    custom_df = custom_df[custom_df["symbol"].isin(known)]
                    if custom_df.empty:
                        st.info("No snapshot data for these symbols on the selected date.")
                    else:
                        st.divider()
                        render_summary_cards(custom_df)
                        st.divider()
                        render_sort_and_table(custom_df, key="custom")
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

# ── Tab 7: Global Markets ─────────────────────────────────────────────────────
with tab_gm:
    if _GM_AVAILABLE:
        _render_global_markets()
    else:
        st.error(f"Global Markets module failed to load: {_GM_ERROR}")
        st.info("Make sure `frontend/global_markets_tab.py` exists and all dependencies are installed.")
