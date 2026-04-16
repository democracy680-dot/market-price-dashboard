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
from PIL import Image, ImageDraw

load_dotenv()


def _make_favicon() -> Image.Image:
    """Generate a favicon matching the login screen logo: blue rounded square + white trend arrow."""
    size = 64
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([0, 0, size - 1, size - 1], radius=14, fill=(59, 130, 246, 255))
    # Scale SVG 24×24 coords to 64×64
    s = size / 24
    # Trend line: "22 7 13.5 15.5 8.5 10.5 2 17"
    draw.line(
        [(22 * s, 7 * s), (13.5 * s, 15.5 * s), (8.5 * s, 10.5 * s), (2 * s, 17 * s)],
        fill=(255, 255, 255, 255), width=3,
    )
    # Arrow head: "16 7 22 7 22 13"
    draw.line(
        [(16 * s, 7 * s), (22 * s, 7 * s), (22 * s, 13 * s)],
        fill=(255, 255, 255, 255), width=3,
    )
    return img

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
    page_icon=_make_favicon(),
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

    /* ── Sidebar section labels ── */
    .sidebar-section-label {
        font-size: 9.5px;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: #1e3050;
        margin: 2px 0 8px 0;
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

        /* Style the login card (st.container with border=True) */
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: #0c1220 !important;
            border: 1px solid #1e2d45 !important;
            border-radius: 24px !important;
            box-shadow: 0 32px 80px rgba(0,0,0,0.6) !important;
        }
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 12px 32px 24px !important;
        }
        .lp-logo-icon {
            width: 56px; height: 56px; border-radius: 14px;
            background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%);
            display: inline-flex; align-items: center; justify-content: center;
            margin: 8px auto 18px;
        }
        .lp-name {
            font-size: 32px; font-weight: 800; color: #f1f5f9;
            letter-spacing: -0.05em; margin: 0 0 6px;
        }
        .lp-name span { color: #3b82f6; }
        .lp-tagline {
            font-size: 12px; color: #334155; letter-spacing: 0.08em;
            text-transform: uppercase; font-weight: 500; margin-bottom: 24px;
        }
        .lp-divider { border: none; border-top: 1px solid #1a2740; margin: 0 0 20px; }
        .lp-footer {
            text-align: center; font-size: 11px; color: #2d3f57;
            margin-top: 12px; letter-spacing: 0.02em;
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<div style='min-height:12vh'></div>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        with st.container(border=True):
            st.markdown("""
            <div style="text-align:center; padding-top:12px;">
                <div class="lp-logo-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="28" height="28" viewBox="0 0 24 24"
                         fill="none" stroke="white" stroke-width="2.5"
                         stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
                        <polyline points="16 7 22 7 22 13"/>
                    </svg>
                </div>
                <div class="lp-name">Stock<span>Stack</span></div>
                <div class="lp-tagline">Indian Equity Intelligence</div>
                <hr class="lp-divider">
            </div>
            """, unsafe_allow_html=True)
            pw = st.text_input("", type="password", placeholder="Enter password…",
                               label_visibility="collapsed")
            if st.button("Sign In →", use_container_width=True, type="primary"):
                if pw == correct:
                    st.session_state["authenticated"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")
            st.markdown(
                "<div class='lp-footer'>Restricted access · Authorised users only</div>",
                unsafe_allow_html=True,
            )
    st.stop()


_check_password()

# ---------------------------------------------------------------------------
# Live ticker bar — auto-refresh + render (above all tabs)
# ---------------------------------------------------------------------------
try:
    from ticker_bar import is_market_open, render_ticker_bar
    try:
        from streamlit_autorefresh import st_autorefresh
        _refresh_interval = 60_000 if is_market_open() else 300_000  # ms
        st_autorefresh(interval=_refresh_interval, key="market_ticker_refresh")
    except Exception:
        pass  # autorefresh optional — ticker bar still renders
    render_ticker_bar()
except Exception:
    pass

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
SECTOR_TABS  = [
    ("BANKS",                   "Banks"),
    ("NBFCS",                   "NBFCs"),
    ("PHARMA",                  "Pharma"),
    ("DEFENCE",                 "Defence"),
    ("NIFTY_AUTO",              "Auto"),
    ("NIFTY_CHEMICAL",          "Chemicals"),
    ("NIFTY_CONSUMER_DURABLES", "Consumer Durables"),
    ("NIFTY_FMCG",              "FMCG"),
    ("NIFTY_HEALTHCARE",        "Healthcare"),
    ("NIFTY_IT",                "IT"),
    ("NIFTY_MEDIA",             "Media"),
    ("NIFTY_METAL",             "Metal"),
]
ALL_UNIVERSES = {k: v for k, v in INDEX_TABS + SECTOR_TABS}

# yfinance ticker symbol for each universe (None = fall back to constituent median)
INDEX_YF_SYMBOL = {
    "NIFTY_50":                 "^NSEI",
    "NIFTY_500":                "^CRSLDX",
    "NIFTY_BANK":               "^NSEBANK",
    "FNO":                      None,
    "BANKS":                    "^NSEBANK",
    "NBFCS":                    "^CNXFIN",       # Nifty Financial Services
    "PHARMA":                   "NIFTYPHARMA.NS",
    "DEFENCE":                  None,            # No reliable yf symbol; use constituent median
    "NIFTY_AUTO":               "^CNXAUTO",
    "NIFTY_CHEMICAL":           None,            # No reliable yf symbol; use constituent median
    "NIFTY_CONSUMER_DURABLES":  None,            # No reliable yf symbol; use constituent median
    "NIFTY_FMCG":               "^CNXFMCG",
    "NIFTY_HEALTHCARE":         "^CNXPHARMA",   # Nifty Healthcare / Pharma index
    "NIFTY_IT":                 "^CNXIT",
    "NIFTY_MEDIA":              "^CNXMEDIA",    # Nifty Media index
    "NIFTY_METAL":              "^CNXMETAL",
}

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1800)
def load_available_dates() -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT date FROM snapshots_daily ORDER BY date DESC LIMIT 90")
        ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=3600)
def fetch_index_returns(yf_symbol: str) -> dict:
    """Fetch 1D, 1M, 1Y returns for a benchmark index via yfinance.
    Returns an empty dict (and sets a flag in session_state) on failure."""
    try:
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period="2y")
        if hist.empty or len(hist) < 2:
            return {"_error": f"No data returned for {yf_symbol}"}
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
    except Exception as e:
        return {"_error": str(e)}


@st.cache_data(ttl=300)
def _load_all_snapshots(snap_date) -> pd.DataFrame:
    """Single bulk query — loads ALL stocks for a date. Shared across all tabs."""
    sql = text("""
        SELECT
            sd.symbol, s.name, s.sector,
            sd.cmp,
            sd.ret_1d, sd.ret_1w, sd.ret_30d, sd.ret_60d, sd.ret_180d, sd.ret_365d,
            sd.dma_50, sd.dma_200, sd.status_50dma, sd.status_200dma,
            sd.pe_ratio, sd.market_cap_cr,
            s.screener_url, s.tradingview_url,
            CASE
                WHEN h52.high_52w IS NOT NULL AND h52.high_52w > 0
                THEN (sd.cmp - h52.high_52w) / h52.high_52w
                ELSE NULL
            END AS pct_from_52wh,
            CASE
                WHEN avg_vol.avg_vol_30d > 0 AND td_vol.today_vol IS NOT NULL
                THEN ROUND((td_vol.today_vol::float / avg_vol.avg_vol_30d)::numeric, 1)
                ELSE NULL
            END AS vol_spike
        FROM snapshots_daily sd
        JOIN stocks s ON sd.symbol = s.symbol
        LEFT JOIN (
            SELECT symbol, MAX(high) AS high_52w
            FROM prices_daily
            WHERE date >= CAST(:date AS date) - INTERVAL '365 days'
              AND date <= CAST(:date AS date)
            GROUP BY symbol
        ) h52 ON h52.symbol = sd.symbol
        LEFT JOIN (
            SELECT symbol, AVG(volume) AS avg_vol_30d
            FROM prices_daily
            WHERE date >= CAST(:date AS date) - INTERVAL '30 days'
              AND date < CAST(:date AS date)
            GROUP BY symbol
        ) avg_vol ON avg_vol.symbol = sd.symbol
        LEFT JOIN (
            SELECT symbol, volume AS today_vol
            FROM prices_daily
            WHERE date = CAST(:date AS date)
        ) td_vol ON td_vol.symbol = sd.symbol
        WHERE sd.date = :date AND s.is_active = TRUE
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"date": str(snap_date)})

    # PostgreSQL NUMERIC/DECIMAL columns come back as Python Decimal objects
    # (object dtype), which pandas sorts lexicographically instead of numerically.
    # Force all sortable numeric columns to float64 so sort_values works correctly.
    _numeric_cols = [
        "cmp", "ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d",
        "pct_from_52wh", "vol_spike", "market_cap_cr", "pe_ratio", "dma_50", "dma_200",
    ]
    for c in _numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=3600)
def _load_index_membership() -> pd.DataFrame:
    """Load all memberships once — changes only when seeds are re-run."""
    with engine.connect() as conn:
        return pd.read_sql(
            text("SELECT symbol, index_name FROM index_membership"),
            conn,
        )


def load_snapshot(snap_date, index_name: str | None = None) -> pd.DataFrame:
    """In-memory filter over the bulk-cached snapshot — zero extra DB round trips."""
    df = _load_all_snapshots(snap_date)
    if not index_name:
        return df.copy()
    membership = _load_index_membership()
    symbols = set(membership.loc[membership["index_name"] == index_name, "symbol"])
    return df[df["symbol"].isin(symbols)].copy()


@st.cache_data(ttl=1800)
def load_sector_performance(snap_date) -> pd.DataFrame:
    """Aggregate all sectors live from snapshots_daily so every sector is included."""
    sql = text("""
        SELECT
            s.sector,
            COUNT(*)                                                                   AS num_companies,
            SUM(CASE WHEN sd.ret_1d IS NOT NULL AND sd.ret_1d > 0 THEN 1 ELSE 0 END) AS advances,
            SUM(CASE WHEN sd.ret_1d IS NOT NULL AND sd.ret_1d < 0 THEN 1 ELSE 0 END) AS declines,
            SUM(CASE WHEN sd.ret_1d IS NOT NULL AND sd.ret_1d = 0 THEN 1 ELSE 0 END) AS unchanged,
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


@st.cache_data(ttl=3600)
def load_all_symbols() -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT symbol, name, sector FROM stocks WHERE is_active = TRUE"),
            conn,
        )
    return df


@st.cache_data(ttl=1800)
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
def load_latest_technicals() -> pd.DataFrame:
    """Load the most recent technical indicators for all active stocks."""
    sql_v2 = text("""
        SELECT
            s.symbol,
            s.name,
            t.cmp,
            t.rsi_14,
            t.macd_line,
            t.macd_signal,
            t.macd_histogram,
            t.adx_14,
            t.sma_50,
            t.sma_200,
            t.volume,
            s.tradingview_url,
            t.technical_status,
            t.signal_score,
            t.sma_200_slope,
            t.volume_ratio,
            t.technical_status_v1,
            t.signal_score_v2,
            t.date AS indicator_date
        FROM stocks s
        JOIN latest_technicals t ON t.symbol = s.symbol
        WHERE s.is_active = true
        ORDER BY s.symbol
    """)
    sql_v1 = text("""
        SELECT
            s.symbol,
            s.name,
            t.cmp,
            t.rsi_14,
            t.macd_line,
            t.macd_signal,
            t.macd_histogram,
            t.adx_14,
            t.sma_50,
            t.sma_200,
            t.volume,
            s.tradingview_url,
            t.technical_status,
            t.signal_score,
            t.date AS indicator_date
        FROM stocks s
        JOIN latest_technicals t ON t.symbol = s.symbol
        WHERE s.is_active = true
        ORDER BY s.symbol
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql_v2, conn)
    except Exception:
        # v2 columns not yet migrated — open a fresh connection for fallback
        with engine.connect() as conn:
            df = pd.read_sql(sql_v1, conn)
    # Cast NUMERIC → float (PostgreSQL returns Decimal objects)
    for c in ["cmp", "rsi_14", "macd_line", "macd_signal", "macd_histogram",
              "adx_14", "sma_50", "sma_200", "signal_score",
              "sma_200_slope", "volume_ratio", "signal_score_v2"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=1800)
def load_themes() -> pd.DataFrame:
    sql = text("""
        SELECT theme_slug, theme_name, theme_order, actual_stock_count
        FROM themes_with_counts
        ORDER BY theme_order
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=1800)
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


@st.cache_data(ttl=1800)
def load_theme_stocks(theme_slug: str) -> pd.DataFrame:
    sql = text("""
        SELECT
            s.symbol,
            s.name,
            s.screener_url,
            s.tradingview_url,
            snap.cmp,
            snap.ret_1d,
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


@st.cache_data(ttl=1800)
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
PCT_COLS = ["ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d", "pct_from_52wh"]

# Schedule string — used in user-facing messages so it stays in sync with daily_refresh.py
DAILY_REFRESH_TIME_IST = "4:00 PM IST"

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
    "pct_from_52wh": "52W High%",
    "vol_spike":     "Vol Spike",
}


def _color_return(val):
    if pd.isna(val) or val == "—":
        return "color: #4a5568"
    try:
        n = float(str(val).replace("%", "").replace("+", ""))
        return "color: #22c55e; font-weight:600" if n >= 0 else "color: #ef4444; font-weight:600"
    except (ValueError, TypeError):
        return ""


def _color_dma(val):
    v = str(val)
    if "▲" in v:
        return "color: #22c55e; font-weight: 600"
    elif "▼" in v:
        return "color: #ef4444; font-weight: 600"
    return "color: #4a5568"


def _color_vol_spike(val):
    if val == "—" or pd.isna(val):
        return "color: #4a5568"
    try:
        n = float(str(val).replace("×", ""))
        if n >= 3.0:
            return "color: #f59e0b; font-weight: 700"
        elif n >= 2.0:
            return "color: #fbbf24; font-weight: 600"
        return "color: #64748b"
    except (ValueError, TypeError):
        return ""


def _fmt_pct(val):
    if pd.isna(val): return "—"
    return f"{val * 100:+.2f}%"


def _fmt_mcap(val):
    if pd.isna(val): return "—"
    return f"₹{val:,.2f} Cr"


def prepare_display(df: pd.DataFrame) -> pd.DataFrame:
    # Only include columns that actually exist in df (vol_spike may be absent on cached data)
    available = {k: v for k, v in DISPLAY_COLS.items() if k in df.columns}
    d = df[list(available.keys())].copy()
    d = d.rename(columns=available)
    for raw, pretty in available.items():
        if raw in PCT_COLS:
            d[pretty] = df[raw].map(_fmt_pct)
    d["CMP"] = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    d["MCap (Cr)"] = df["market_cap_cr"].map(_fmt_mcap)
    d["P/E"] = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    # DMA colored badges
    if "50DMA" in d.columns:
        d["50DMA"] = df["status_50dma"].map(
            lambda v: "▲ Above" if v == "Above 50DMA" else ("▼ Below" if v == "Below 50DMA" else "—")
        )
    if "200DMA" in d.columns:
        d["200DMA"] = df["status_200dma"].map(
            lambda v: "▲ Above" if v == "Above 200DMA" else ("▼ Below" if v == "Below 200DMA" else "—")
        )
    # Volume spike ratio
    if "Vol Spike" in d.columns:
        d["Vol Spike"] = df["vol_spike"].map(lambda v: f"{v:.1f}×" if pd.notna(v) else "—")
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

    # ── Header row: name + sector tag + live price + day change ─────────────
    last  = ohlcv.iloc[-1]
    prev  = ohlcv.iloc[-2] if len(ohlcv) > 1 else last
    day_chg_pct = (last["close"] - prev["close"]) / prev["close"] * 100 if prev["close"] else 0
    chg_color   = "#22c55e" if day_chg_pct >= 0 else "#ef4444"
    arrow       = "▲" if day_chg_pct >= 0 else "▼"

    _sym_info = load_all_symbols()
    _sym_row  = _sym_info[_sym_info["symbol"] == symbol]
    _sector   = _sym_row.iloc[0]["sector"] if not _sym_row.empty and pd.notna(_sym_row.iloc[0]["sector"]) else None
    _sector_tag = (
        f"<span style='font-size:11px;font-weight:600;color:#60a5fa;"
        f"background:#0f1f3d;padding:2px 8px;border-radius:4px;"
        f"border:1px solid #1e3a5f;white-space:nowrap;'>{_sector}</span>"
        if _sector else ""
    )

    st.markdown(
        f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap;'>"
        f"<span style='font-size:20px;font-weight:700;color:#e2e8f0'>{symbol}</span>"
        f"<span style='color:#8b97a8;font-size:13px'>{name}</span>"
        f"{_sector_tag}"
        f"<span style='font-size:26px;font-weight:700;color:#e2e8f0;margin-left:4px;'>₹{last['close']:,.2f}</span>"
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
    with st.spinner("Loading chart…"):
        _render_chart_body(symbol, name)


# ---------------------------------------------------------------------------
# Components
# ---------------------------------------------------------------------------
def render_summary_cards(df: pd.DataFrame, index_name: str | None = None, snap_date=None):
    valid_ret = df["ret_1d"].dropna()
    adv       = int((valid_ret > 0).sum())
    dec       = int((valid_ret < 0).sum())
    unch      = int((valid_ret == 0).sum())
    above_200 = int((df["status_200dma"] == "Above 200DMA").sum())
    total     = len(df)

    # Fetch index-level returns — prefer a benchmark yfinance symbol, else use
    # the median return of constituent stocks already in df.
    idx_rets: dict = {}
    _yf_fetch_error: str | None = None
    yf_sym = INDEX_YF_SYMBOL.get(index_name) if index_name else None
    if yf_sym:
        _raw = fetch_index_returns(yf_sym)
        if "_error" in _raw:
            _yf_fetch_error = _raw["_error"]
        else:
            idx_rets = _raw

    # Fallback: compute median from constituent stocks when no symbol exists
    # (also used when the yfinance fetch failed)
    if not idx_rets and not df.empty:
        col_map = {"1D": "ret_1d", "1M": "ret_30d", "1Y": "ret_365d"}
        for key, col in col_map.items():
            if col in df.columns:
                med = df[col].dropna().median()
                if pd.notna(med):
                    idx_rets[key] = float(med)  # already stored as ratio (0.05 = 5%)

    def _idx_val(key):
        v = idx_rets.get(key)
        return _fmt_pct(v) if v is not None else "—"

    label_prefix = ALL_UNIVERSES.get(index_name, "Index") if index_name else "Index"

    def _delta_pct(key):
        v = idx_rets.get(key)
        if v is None: return None
        return f"{v * 100:+.2f}%"

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric(f"{label_prefix} 1D",  _idx_val("1D"),  delta=_delta_pct("1D"))
    with c2: st.metric(f"{label_prefix} 1M",  _idx_val("1M"),  delta=_delta_pct("1M"))
    with c3: st.metric(f"{label_prefix} 1Y",  _idx_val("1Y"),  delta=_delta_pct("1Y"))
    with c4: st.metric("Adv / Dec",     f"{adv} / {dec}", delta=f"{adv - dec:+d} ({unch} flat)" if unch else f"{adv - dec:+d}")
    with c5: st.metric("Above 200 DMA", f"{above_200} / {total}")

    if snap_date:
        st.markdown(
            f"<div style='font-size:10.5px;color:#2d4f6e;margin-top:2px;"
            f"letter-spacing:0.04em;'>As of market close · "
            f"{pd.Timestamp(snap_date).strftime('%d %b %Y')}</div>",
            unsafe_allow_html=True,
        )

    if _yf_fetch_error and yf_sym:
        st.warning(
            f"Live index data unavailable for `{yf_sym}` — showing constituent median instead. "
            f"_{_yf_fetch_error}_",
            icon="⚠️",
        )


def render_table(df: pd.DataFrame, key: str = "default", page_size: int = 500):
    total = len(df)
    pages = max(1, (total + page_size - 1) // page_size)

    # Reset to page 1 whenever the result set size changes (e.g. after a filter).
    # This prevents landing on a non-existent page when the table shrinks.
    total_state_key = f"total_{key}"
    if st.session_state.get(total_state_key) != total:
        st.session_state[total_state_key] = total
        st.session_state[f"page_{key}"] = 1

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

    # ── Column visibility toggle ─────────────────────────────────────────────
    _all_data_cols = list(display.columns)
    _vis_key = f"vis_cols_{key}"
    if _vis_key not in st.session_state:
        st.session_state[_vis_key] = _all_data_cols
    with st.expander("⚙ Columns", expanded=False):
        _visible = st.multiselect(
            "Visible columns", _all_data_cols,
            default=[c for c in st.session_state[_vis_key] if c in _all_data_cols],
            key=f"mc_{key}", label_visibility="collapsed",
        )
        st.session_state[_vis_key] = _visible
    display = display[_visible] if _visible else display

    # Add link columns (always shown regardless of visibility toggle)
    display["Screener"] = chunk["screener_url"].where(chunk["screener_url"].notna(), other=None)
    display["Chart"] = chunk["tradingview_url"].where(chunk["tradingview_url"].notna(), other=None)

    styled = display.style
    for raw, pretty in DISPLAY_COLS.items():
        if raw in PCT_COLS and pretty in display.columns:
            styled = styled.map(_color_return, subset=[pretty])
    if "50DMA" in display.columns:
        styled = styled.map(_color_dma, subset=["50DMA"])
    if "200DMA" in display.columns:
        styled = styled.map(_color_dma, subset=["200DMA"])
    if "Vol Spike" in display.columns:
        styled = styled.map(_color_vol_spike, subset=["Vol Spike"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=700,
        column_config={
            "Screener": st.column_config.LinkColumn("Screener", display_text="Screener ↗"),
            "Chart":    st.column_config.LinkColumn("Chart",    display_text="📈"),
        },
    )

    csv_cols = [k for k in DISPLAY_COLS.keys() if k in df.columns]
    csv_bytes = df[csv_cols].to_csv(index=False).encode()
    _dl_sp, _dl_col = st.columns([5, 1])
    with _dl_col:
        st.download_button("⬇ CSV", csv_bytes, "stocks.csv", "text/csv",
                           key=f"dl_{key}", use_container_width=True)


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
    ("52WH%",  "pct_from_52wh", True),   # descending = closest to 52W high first
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
THEME_PCT_COLS = ["ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d"]
THEME_DISPLAY_COLS = {
    "symbol":        "Symbol",
    "name":          "Name",
    "cmp":           "CMP",
    "ret_1d":        "1D %",
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
    d["P/E"] = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
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
            # Fallback if selection no longer exists (e.g. after search filters it out).
            # Sync session state so the sidebar buttons reflect the actual displayed theme.
            selected_slug = themes_df.iloc[0]["theme_slug"]
            theme_row = themes_df.iloc[[0]]
            st.session_state["selected_theme_slug"] = selected_slug

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
                f"populated after the next daily refresh ({DAILY_REFRESH_TIME_IST})."
            )

        if stocks_df.empty:
            st.info("No stocks found for this theme.")
            return

        display = _prepare_theme_display(stocks_df)
        display["Screener"] = stocks_df["screener_url"].where(stocks_df["screener_url"].notna(), other=None)
        display["Chart"]    = stocks_df["tradingview_url"].where(stocks_df["tradingview_url"].notna(), other=None)

        styled = display.style
        for raw, pretty in THEME_DISPLAY_COLS.items():
            if raw in THEME_PCT_COLS:
                styled = styled.map(_color_return, subset=[pretty])

        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=650,
            column_config={
                "Screener": st.column_config.LinkColumn("Screener", display_text="Screener ↗"),
                "Chart":    st.column_config.LinkColumn("Chart",    display_text="📈"),
            },
        )

        csv_bytes = stocks_df[list(THEME_DISPLAY_COLS.keys())].to_csv(index=False).encode()
        _dl_sp2, _dl_col2 = st.columns([5, 1])
        with _dl_col2:
            st.download_button(
                "⬇ CSV", csv_bytes,
                f"{selected_slug}.csv", "text/csv",
                key=f"dl_theme_{selected_slug}",
                use_container_width=True,
            )


# ---------------------------------------------------------------------------
# Analysis view — Top N / Bottom N per universe with timeframe toggle
# ---------------------------------------------------------------------------
ANALYSIS_TOP_N = {
    "NIFTY_50":                 5,
    "NIFTY_500":                20,
    "NIFTY_BANK":               5,
    "FNO":                      10,
    "BANKS":                    5,
    "NBFCS":                    5,
    "PHARMA":                   5,
    "DEFENCE":                  5,
    "NIFTY_AUTO":               5,
    "NIFTY_CHEMICAL":           5,
    "NIFTY_CONSUMER_DURABLES":  5,
    "NIFTY_FMCG":               5,
    "NIFTY_HEALTHCARE":         5,
    "NIFTY_IT":                 5,
    "NIFTY_MEDIA":              5,
    "NIFTY_METAL":              5,
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

    # Previous session date — for delta chips
    _all_dates = load_available_dates()
    _prev_date = _all_dates[1] if len(_all_dates) > 1 else None

    def _breadth_pct(target_df, status_col, above_val):
        valid = target_df[status_col].dropna()
        a = int((valid == above_val).sum())
        t = len(valid)
        return round(a / t * 100, 1) if t else 0.0

    def _delta_chip(current_pct: float, prev_pct: float | None) -> str:
        if prev_pct is None:
            return ""
        delta = current_pct - prev_pct
        color  = "#22c55e" if delta >= 0 else "#ef4444"
        arrow  = "↑" if delta >= 0 else "↓"
        return (
            f"<span style='font-size:11px;font-weight:600;color:{color};"
            f"margin-left:4px;'>{arrow} {abs(delta):.1f}pp</span>"
        )

    def _stats(status_col: str, above_val: str, below_val: str):
        # Drop stocks that lack enough history to compute the DMA (NaN status).
        # Percentages are computed over this valid subset — not total universe size —
        # so the numbers are accurate rather than deflated by new/illiquid listings.
        valid = df[status_col].dropna()
        above = int((valid == above_val).sum())
        below = int((valid == below_val).sum())
        total_valid = above + below          # stocks with sufficient history
        total_all   = len(df)               # full universe (including no-history stocks)
        pct   = round(above / total_valid * 100, 1) if total_valid else 0.0
        return above, below, total_valid, pct, total_all

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

            # Previous session breadth for delta chips
            _prev50, _prev200 = None, None
            if _prev_date:
                _prev_df = load_snapshot(_prev_date, index_name=key)
                if not _prev_df.empty:
                    _prev50  = _breadth_pct(_prev_df, "status_50dma",  "Above 50DMA")
                    _prev200 = _breadth_pct(_prev_df, "status_200dma", "Above 200DMA")

            with col_widget:
                with st.container(border=True):
                    if df.empty:
                        st.caption(f"No data for {label}.")
                        continue

                    a50,  b50,  t50,  pct50,  all50  = _stats("status_50dma",  "Above 50DMA",  "Below 50DMA")
                    a200, b200, t200, pct200, all200 = _stats("status_200dma", "Above 200DMA", "Below 200DMA")
                    c50,  mood50  = _mood(pct50)
                    c200, mood200 = _mood(pct200)

                    # Subtitle: show total stocks and how many have valid DMA history
                    dma_note = (
                        f"{len(df)} stocks"
                        if t50 == len(df)
                        else f"{len(df)} stocks · {t50} with 50DMA history"
                    )

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
                        f"{dma_note}</div>"
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
                    for sc, above, below, total, pct, color, mood, dma_label, prev_pct in [
                        (sc1, a50,  b50,  t50,  pct50,  c50,  mood50,  "50 DMA",  _prev50),
                        (sc2, a200, b200, t200, pct200, c200, mood200, "200 DMA", _prev200),
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
                                f"{mood}"
                                + _delta_chip(pct, prev_pct) +
                                f"</div>"
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
    # Ensure numeric dtype — SQL returns object when all values are NULL
    df_valid[ret_col] = pd.to_numeric(df_valid[ret_col], errors="coerce")
    df_valid = df_valid[df_valid[ret_col].notna()]
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

    # Reset pagination whenever filter values change (not just when total count changes)
    _fstate_key  = f"fstate_{index_name}"
    _filter_hash = (tuple(sorted(sel_sectors)), sel_200dma, sel_50dma)
    if st.session_state.get(_fstate_key) != _filter_hash:
        st.session_state[_fstate_key] = _filter_hash
        st.session_state[f"page_{index_name}"] = 1

    if df.empty:
        st.warning("No stocks match the current filters.")
        return

    st.divider()
    render_summary_cards(df, index_name=index_name, snap_date=snap_date)
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
                        flex-shrink:0;">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"
                     fill="none" stroke="white" stroke-width="2.5"
                     stroke-linecap="round" stroke-linejoin="round">
                    <polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/>
                    <polyline points="16 7 22 7 22 13"/>
                </svg>
            </div>
            <div style="font-size:20px;font-weight:800;color:#f1f5f9;letter-spacing:-0.04em;">
                Stock<span style="color:#3b82f6;">Stack</span>
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    st.markdown("<div class='sidebar-section-label'>Last Refresh</div>", unsafe_allow_html=True)
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
    else:
        st.markdown("<div style='font-size:11.5px;color:#2d3f57;'>No refresh data yet</div>", unsafe_allow_html=True)

    st.divider()

    st.markdown("<div class='sidebar-section-label'>Data</div>", unsafe_allow_html=True)
    dates = load_available_dates()
    if not dates:
        st.error("No snapshot data found in Supabase.")
        st.stop()

    selected_date = st.selectbox(
        "As-of date",
        options=dates,
        format_func=lambda d: pd.Timestamp(d).strftime("%d %b %Y"),
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown("<div class='sidebar-section-label'>Tips</div>", unsafe_allow_html=True)
    st.caption("Use the 📈 column in any table to open a chart on TradingView.")

# ---------------------------------------------------------------------------
# Fragment wrappers — isolate each view so button clicks only rerun their
# own fragment instead of the entire app.
# ---------------------------------------------------------------------------
@st.fragment
def _frag_universe_view(index_name: str, snap_date):
    render_universe_view(index_name, snap_date)


@st.fragment
def _frag_analysis_tab(snap_date, universes, section_key):
    render_analysis_tab(snap_date, universes, section_key)


@st.fragment
def _frag_breadth_tab(snap_date, universes, section_key):
    render_breadth_tab(snap_date, universes, section_key)


@st.fragment
def _frag_themes():
    render_themes_view()


@st.fragment
def _frag_volspike(snap_date):
    render_volspike_view(snap_date)


@st.fragment
def _frag_technical_analysis():
    render_technical_analysis_view()


@st.fragment
def _frag_sector_performance(snap_date):
    sector_df = load_sector_performance(snap_date)
    if sector_df.empty:
        st.warning(
            f"No sector data found for **{pd.Timestamp(snap_date).strftime('%d %b %Y')}**. "
            "Run `daily_refresh.py` to populate, or choose a different date."
        )
        return

    # Confirm the date that was actually queried so the user is never in doubt
    date_label = pd.Timestamp(snap_date).strftime("%d %b %Y")
    st.caption(f"Showing sector aggregates for **{date_label}** — advances/declines exclude stocks with no 1D data.")

    keep_cols = ["sector", "num_companies", "advances", "declines"]
    if "unchanged" in sector_df.columns:
        keep_cols.append("unchanged")
    keep_cols += ["day_change_pct", "week_chg_pct", "month_chg_pct",
                  "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct"]

    disp = sector_df[keep_cols].copy()
    for c in ["day_change_pct", "week_chg_pct", "month_chg_pct",
              "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct"]:
        disp[c] = disp[c].map(_fmt_pct)

    rename_map = {
        "sector": "Sector", "num_companies": "# Stocks",
        "advances": "Adv", "declines": "Dec", "unchanged": "Flat",
        "day_change_pct": "1D%", "week_chg_pct": "1W%",
        "month_chg_pct": "30D%", "qtr_chg_pct": "60D%",
        "half_yr_chg_pct": "180D%", "year_chg_pct": "365D%",
    }
    disp = disp.rename(columns={k: v for k, v in rename_map.items() if k in disp.columns})
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.divider()

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


# ---------------------------------------------------------------------------
# Technical Analysis view — RSI, MACD, ADX, DMA signal table
# ---------------------------------------------------------------------------

def _fmt_volume_ind(v):
    """Format volume in Indian style: 12.3L (lakhs) or 12.3Cr (crores)."""
    if pd.isna(v) or v is None:
        return "—"
    v = int(v)
    if v >= 10_000_000:       # ≥ 1 crore
        return f"{v / 10_000_000:.1f}Cr"
    if v >= 100_000:           # ≥ 1 lakh
        return f"{v / 100_000:.1f}L"
    return f"{v:,}"


def _color_rsi(val):
    """Style RSI cell: red if overbought (>70), green if oversold (<30)."""
    if val == "—":
        return ""
    try:
        v = float(val)
        if v > 70:
            return "color: #ef4444; font-weight: 600"
        if v < 30:
            return "color: #22c55e; font-weight: 600"
    except (ValueError, TypeError):
        pass
    return ""


def _style_adx(val):
    """Highlight ADX > 25 in amber — signals strong trend."""
    if val == "—":
        return ""
    try:
        if float(val) > 25:
            return "font-weight: 700; color: #f59e0b"
    except (ValueError, TypeError):
        pass
    return ""


def _fmt_slope(v) -> str:
    """Format SMA200 slope as +1.2% / -0.8% or —."""
    if pd.isna(v) or v is None:
        return "—"
    return f"{v:+.2f}%"


def _color_slope(val) -> str:
    """Color SMA200 slope: green if rising >+1%, red if falling <-1%, gray otherwise."""
    if val == "—":
        return ""
    try:
        v = float(val.replace("%", "").replace("+", ""))
        if v > 1.0:
            return "color: #22c55e; font-weight: 600"
        if v < -1.0:
            return "color: #ef4444; font-weight: 600"
        return "color: #94a3b8"
    except (ValueError, TypeError):
        return ""


def _fmt_vol_ratio(v) -> str:
    """Format volume ratio as 1.8x or —."""
    if pd.isna(v) or v is None:
        return "—"
    return f"{v:.2f}x"


def _style_vol_ratio(val) -> str:
    """Bold volume ratio >= 1.5x."""
    if val == "—":
        return ""
    try:
        if float(val.replace("x", "")) >= 1.5:
            return "font-weight: 700; color: #f59e0b"
    except (ValueError, TypeError):
        pass
    return ""


def _render_technical_table(df: pd.DataFrame, key: str, show_v1: bool = False):
    """Build and render the formatted technical indicators table."""
    if df.empty:
        st.info("No stocks match the current filters.")
        return

    # ── Build display columns ─────────────────────────────────────────────────
    disp = pd.DataFrame()
    disp["Ticker"]    = df["symbol"]
    disp["Name"]      = df["name"]
    disp["CMP"]       = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["RSI (14)"]  = df["rsi_14"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "—")
    disp["MACD"]      = df.apply(
        lambda r: (
            f"L: {r['macd_line']:.2f} | S: {r['macd_signal']:.2f} | H: {r['macd_histogram']:.2f}"
            if pd.notna(r["macd_line"]) and pd.notna(r["macd_signal"]) and pd.notna(r["macd_histogram"])
            else "—"
        ),
        axis=1,
    )
    disp["ADX (14)"]  = df["adx_14"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    disp["50 DMA"]    = df["sma_50"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["200 DMA"]   = df["sma_200"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["Volume"]      = df["volume"].map(_fmt_volume_ind)
    disp["SMA200 Slope"] = df["sma_200_slope"].map(_fmt_slope) if "sma_200_slope" in df.columns else "—"
    disp["Vol Ratio"]   = df["volume_ratio"].map(_fmt_vol_ratio) if "volume_ratio" in df.columns else "—"
    disp["Chart"]       = df["tradingview_url"].where(df["tradingview_url"].notna(), other=None)
    disp["Status"]      = df["technical_status"]
    if show_v1 and "technical_status_v1" in df.columns:
        disp["v1 Signal"] = df["technical_status_v1"]

    # ── Styling ───────────────────────────────────────────────────────────────
    styled = disp.style
    styled = styled.map(_color_rsi,        subset=["RSI (14)"])
    styled = styled.map(_style_adx,        subset=["ADX (14)"])
    styled = styled.map(_color_slope,      subset=["SMA200 Slope"])
    styled = styled.map(_style_vol_ratio,  subset=["Vol Ratio"])

    total = len(disp)
    st.caption(f"{total} stocks")

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=700,
        column_config={
            "Chart": st.column_config.LinkColumn("Chart", display_text="📈 Chart"),
        },
    )

    # ── CSV download ──────────────────────────────────────────────────────────
    raw_cols = ["symbol", "name", "cmp", "rsi_14", "macd_line", "macd_signal",
                "macd_histogram", "adx_14", "sma_50", "sma_200", "volume",
                "sma_200_slope", "volume_ratio", "technical_status", "technical_status_v1"]
    csv_cols = [c for c in raw_cols if c in df.columns]
    csv_bytes = df[csv_cols].to_csv(index=False).encode()
    _, dl_col = st.columns([5, 1])
    with dl_col:
        st.download_button(
            "⬇ CSV", csv_bytes, f"technicals_{key}.csv", "text/csv",
            key=f"dl_tech_{key}", use_container_width=True,
        )


def render_technical_analysis_view():
    """Render the Technical Analysis tab: filters, summary cards, sub-tabs."""
    # ── Load data ─────────────────────────────────────────────────────────────
    df_all = load_latest_technicals()

    if df_all.empty:
        st.info(
            "No technical indicator data found. "
            "Run `python backend/compute_technicals.py` first "
            "or wait for the next daily refresh."
        )
        return

    # Latest computed date
    latest_date = "—"
    if "indicator_date" in df_all.columns and df_all["indicator_date"].notna().any():
        latest_date = pd.Timestamp(df_all["indicator_date"].dropna().max()).strftime("%d %b %Y")

    st.caption(f"Indicators as of {latest_date} · Refreshed daily after market close")

    # ── F&O subset ───────────────────────────────────────────────────────────
    membership    = _load_index_membership()
    fno_symbols   = set(membership.loc[membership["index_name"] == "FNO", "symbol"])
    df_fno        = df_all[df_all["symbol"].isin(fno_symbols)].copy()

    # ── Filters row 1: existing filters ──────────────────────────────────────
    fc1, fc2, fc3, fc4 = st.columns([2, 1, 2, 2])
    with fc1:
        rsi_range = st.slider(
            "RSI range", min_value=0, max_value=100, value=(0, 100),
            key="ta_rsi_range",
        )
    with fc2:
        adx_min = st.slider(
            "Min ADX", min_value=0, max_value=100, value=0,
            key="ta_adx_min",
        )
    with fc3:
        all_statuses = sorted(df_all["technical_status"].dropna().unique().tolist())
        sel_statuses = st.multiselect(
            "Status", all_statuses, default=[],
            key="ta_status", placeholder="All statuses",
        )
    with fc4:
        search = st.text_input(
            "Search symbol / name", placeholder="e.g. RELIANCE or Tata",
            key="ta_search", label_visibility="collapsed",
        )

    # ── Filters row 2: v2 slope + volume filters ──────────────────────────────
    fc5, fc6, fc7 = st.columns([2, 2, 1])
    with fc5:
        slope_filter = st.radio(
            "SMA200 Slope", ["Any", "Rising only (>+1%)", "Falling only (<-1%)"],
            horizontal=True, key="ta_slope_filter",
        )
    with fc6:
        vol_ratio_min = st.slider(
            "Min Volume Ratio", min_value=0.0, max_value=3.0, value=0.0, step=0.1,
            key="ta_vol_ratio_min",
        )
    with fc7:
        show_v1 = st.checkbox("Show v1 signal", value=False, key="ta_show_v1")

    def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        # RSI filter (exclude rows where RSI is null only if user moved slider away from default)
        if rsi_range != (0, 100):
            df = df[df["rsi_14"].isna() | (df["rsi_14"].between(rsi_range[0], rsi_range[1]))]
        # ADX min filter
        if adx_min > 0:
            df = df[df["adx_14"].notna() & (df["adx_14"] >= adx_min)]
        # Status filter
        if sel_statuses:
            df = df[df["technical_status"].isin(sel_statuses)]
        # SMA200 slope filter
        if slope_filter == "Rising only (>+1%)" and "sma_200_slope" in df.columns:
            df = df[df["sma_200_slope"].notna() & (df["sma_200_slope"] > 1.0)]
        elif slope_filter == "Falling only (<-1%)" and "sma_200_slope" in df.columns:
            df = df[df["sma_200_slope"].notna() & (df["sma_200_slope"] < -1.0)]
        # Volume ratio min filter
        if vol_ratio_min > 0.0 and "volume_ratio" in df.columns:
            df = df[df["volume_ratio"].notna() & (df["volume_ratio"] >= vol_ratio_min)]
        # Search
        if search.strip():
            q = search.strip().lower()
            mask = (
                df["symbol"].str.lower().str.contains(q, na=False) |
                df["name"].str.lower().str.contains(q, na=False)
            )
            df = df[mask]
        return df

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    tab_all_stocks, tab_fno_stocks = st.tabs(["All Stocks", "F&O Stocks"])

    # ── All Stocks sub-tab ────────────────────────────────────────────────────
    with tab_all_stocks:
        # Summary cards (from the full all-stocks universe, before user filters)
        n_strong_buy = int(df_all["technical_status"].str.contains("Strong Buy", na=False).sum())
        n_sell       = int(df_all["technical_status"].str.contains("Sell", na=False).sum())
        n_oversold   = int((df_all["rsi_14"].notna() & (df_all["rsi_14"] < 30)).sum())
        n_strong_trn = int((df_all["adx_14"].notna() & (df_all["adx_14"] > 25)).sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🚀 Strong Buys",    n_strong_buy)
        c2.metric("🔻 Sells / Avoid",  n_sell)
        c3.metric("🔥 Oversold (RSI<30)", n_oversold)
        c4.metric("💪 Strong Trends (ADX>25)", n_strong_trn)

        st.divider()
        _render_technical_table(_apply_filters(df_all), key="all", show_v1=show_v1)

    # ── F&O Stocks sub-tab ────────────────────────────────────────────────────
    with tab_fno_stocks:
        # Summary cards (from the F&O universe, before user filters)
        n_strong_buy_fno = int(df_fno["technical_status"].str.contains("Strong Buy", na=False).sum())
        n_sell_fno       = int(df_fno["technical_status"].str.contains("Sell", na=False).sum())
        n_oversold_fno   = int((df_fno["rsi_14"].notna() & (df_fno["rsi_14"] < 30)).sum())
        n_strong_trn_fno = int((df_fno["adx_14"].notna() & (df_fno["adx_14"] > 25)).sum())

        c1f, c2f, c3f, c4f = st.columns(4)
        c1f.metric("🚀 Strong Buys",    n_strong_buy_fno)
        c2f.metric("🔻 Sells / Avoid",  n_sell_fno)
        c3f.metric("🔥 Oversold (RSI<30)", n_oversold_fno)
        c4f.metric("💪 Strong Trends (ADX>25)", n_strong_trn_fno)

        st.divider()
        if df_fno.empty:
            st.info("No F&O stocks found. Ensure `index_membership` is seeded with `index_name = 'FNO'`.")
        else:
            _render_technical_table(_apply_filters(df_fno), key="fno", show_v1=show_v1)

    # ── v1 vs v2 Debug Panel ──────────────────────────────────────────────────
    if "technical_status_v1" in df_all.columns:
        changed = df_all[
            df_all["technical_status_v1"].notna() &
            (df_all["technical_status_v1"] != df_all["technical_status"])
        ].copy()
        with st.expander(f"🔍 v1 vs v2 Signal Comparison ({len(changed)} stocks differ)", expanded=False):
            if changed.empty:
                st.info("No label differences between v1 and v2 signals.")
            else:
                debug = pd.DataFrame()
                debug["Symbol"]       = changed["symbol"]
                debug["Name"]         = changed["name"]
                debug["v1 Signal"]    = changed["technical_status_v1"]
                debug["v2 Signal"]    = changed["technical_status"]
                debug["v1 Score"]     = changed["signal_score_v2"].map(
                    lambda v: f"{v:.1f}" if pd.notna(v) else "—"
                ) if "signal_score_v2" in changed.columns else "—"
                debug["Slope"]        = changed["sma_200_slope"].map(_fmt_slope) if "sma_200_slope" in changed.columns else "—"
                debug["Vol Ratio"]    = changed["volume_ratio"].map(_fmt_vol_ratio) if "volume_ratio" in changed.columns else "—"
                st.dataframe(debug, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Volume Spike screener — all stocks sorted by vol spike desc
# ---------------------------------------------------------------------------
_VS_COLS = {
    "symbol":        "Symbol",
    "name":          "Name",
    "sector":        "Sector",
    "cmp":           "CMP",
    "vol_spike":     "Vol Spike",
    "ret_1d":        "1D%",
    "ret_1w":        "1W%",
    "ret_30d":       "30D%",
    "ret_365d":      "1Y%",
    "market_cap_cr": "MCap (Cr)",
    "pe_ratio":      "P/E",
    "pct_from_52wh": "52W High%",
}
_VS_PCT_COLS = {"ret_1d", "ret_1w", "ret_30d", "ret_365d", "pct_from_52wh"}


def render_volspike_view(snap_date):
    df = _load_all_snapshots(snap_date)

    if "vol_spike" not in df.columns or df["vol_spike"].isna().all():
        st.info(
            "Volume spike data isn't available yet — it requires `prices_daily` "
            "data for this date. Try a more recent date or wait for the next refresh."
        )
        return

    df = df[df["vol_spike"].notna() & (df["vol_spike"] > 0)].copy()

    # ── Filters ─────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns([1, 1, 2])
    with fc1:
        spike_options = {"Any (all)": 0.0, "1.5×+": 1.5, "2×+": 2.0, "3×+": 3.0, "5×+": 5.0}
        min_label = st.selectbox("Min spike", list(spike_options.keys()), index=2, key="vs_min")
        min_val   = spike_options[min_label]
    with fc2:
        sectors    = sorted(df["sector"].dropna().unique().tolist())
        sel_sector = st.multiselect("Sector", sectors, default=[], key="vs_sector",
                                    placeholder="All sectors")
    with fc3:
        st.markdown(
            "<div style='font-size:11px;color:#374151;padding-top:28px;'>"
            "Stocks where today's volume significantly exceeds the 30-day average — "
            "often signals unusual activity, breakouts, or news-driven moves.</div>",
            unsafe_allow_html=True,
        )

    if min_val > 0:
        df = df[df["vol_spike"] >= min_val]
    if sel_sector:
        df = df[df["sector"].isin(sel_sector)]

    df = df.sort_values("vol_spike", ascending=False).reset_index(drop=True)

    total = len(df)
    if total == 0:
        st.warning("No stocks match the current filters.")
        return

    st.markdown(
        f"<div style='font-size:11.5px;color:#4a5568;margin:4px 0 8px;'>"
        f"{total} stocks · sorted highest Vol Spike first"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Build display df ────────────────────────────────────────────────────
    available = {k: v for k, v in _VS_COLS.items() if k in df.columns}
    disp = df[list(available.keys())].copy().rename(columns=available)

    for raw, pretty in available.items():
        if raw in _VS_PCT_COLS:
            disp[pretty] = df[raw].map(_fmt_pct)

    disp["CMP"]       = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["MCap (Cr)"] = df["market_cap_cr"].map(_fmt_mcap)
    disp["P/E"]       = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    if "Vol Spike" in disp.columns:
        disp["Vol Spike"] = df["vol_spike"].map(lambda v: f"{v:.1f}×" if pd.notna(v) else "—")

    # Chart link column
    disp["Chart"] = df["tradingview_url"].where(df["tradingview_url"].notna(), other=None)

    # ── Styling ─────────────────────────────────────────────────────────────
    styled = disp.style
    for raw, pretty in available.items():
        if raw in _VS_PCT_COLS and pretty in disp.columns:
            styled = styled.map(_color_return, subset=[pretty])
    if "Vol Spike" in disp.columns:
        styled = styled.map(_color_vol_spike, subset=["Vol Spike"])

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=700,
        column_config={
            "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
        },
    )

    # CSV export
    csv_cols  = [k for k in _VS_COLS.keys() if k in df.columns]
    csv_bytes = df[csv_cols].to_csv(index=False).encode()
    _, dl_col = st.columns([5, 1])
    with dl_col:
        st.download_button("⬇ CSV", csv_bytes, "vol_spikes.csv", "text/csv",
                           key="dl_vs", use_container_width=True)


# ---------------------------------------------------------------------------
# Data-freshness banner — shown when last refresh is >24 h old
# ---------------------------------------------------------------------------
if status:
    _last_run = status.get("finished_at") or status.get("started_at")
    if _last_run:
        try:
            _last_ts = pd.Timestamp(_last_run)
            _now_ts  = pd.Timestamp.now("UTC")
            if _last_ts.tzinfo is None:
                _last_ts = _last_ts.tz_localize("UTC")
            _age_h = (_now_ts - _last_ts).total_seconds() / 3600
            if _age_h > 24:
                st.warning(
                    f"Data may be stale — last refresh was **{int(_age_h)} hours ago**. "
                    "This typically happens over weekends or market holidays.",
                    icon="⚠️",
                )
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Auto-refresh at 3:35 PM IST — gives backend 5 min after market close to finish
# ---------------------------------------------------------------------------
try:
    import pytz as _pytz
    from datetime import datetime as _dt
    _ist = _pytz.timezone("Asia/Kolkata")
    _now_ist = _dt.now(_ist)
    # Only on weekdays (Mon=0 … Fri=4), and only if we're before 3:35 PM today
    if _now_ist.weekday() < 5:
        _trigger = _now_ist.replace(hour=16, minute=5, second=0, microsecond=0)
        if _now_ist < _trigger:
            _ms = int((_trigger - _now_ist).total_seconds() * 1000)
            components.html(
                f"""<script>
                    setTimeout(function() {{
                        // Clear Streamlit's own cache then reload
                        window.parent.location.reload();
                    }}, {_ms});
                </script>""",
                height=0,
            )
except Exception:
    pass

# ---------------------------------------------------------------------------
# Main — 5 top-level tabs
# ---------------------------------------------------------------------------
tab_gm, tab_idx, tab_sec, tab_analysis, tab_themes, tab_volspike, tab_technical, tab_upload = st.tabs([
    "Global Markets",
    "Indexes",
    "Sectors",
    "Sector Performance",
    "Themes",
    "Vol Spikes",
    "🔬 Technical Analysis",
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
            _frag_universe_view(key, selected_date)
    with sub_tabs[-2]:
        _frag_analysis_tab(selected_date, INDEX_TABS, "indexes")
    with sub_tabs[-1]:
        _frag_breadth_tab(selected_date, INDEX_TABS, "indexes")

# ── Tab 2: Sectors ──────────────────────────────────────────────────────────
with tab_sec:
    _page_header("Sector Views", selected_date)
    sub_tabs2 = st.tabs([label for _, label in SECTOR_TABS] + ["Analysis", "Breadth"])
    for tab, (key, _) in zip(sub_tabs2[:len(SECTOR_TABS)], SECTOR_TABS):
        with tab:
            _frag_universe_view(key, selected_date)
    with sub_tabs2[-2]:
        _frag_analysis_tab(selected_date, SECTOR_TABS, "sectors")
    with sub_tabs2[-1]:
        _frag_breadth_tab(selected_date, SECTOR_TABS, "sectors")

# ── Tab 3: Sector Performance ────────────────────────────────────────────────
with tab_analysis:
    _page_header("Sector Performance", selected_date)
    _frag_sector_performance(selected_date)

# ── Tab 4: Themes ────────────────────────────────────────────────────────────
with tab_themes:
    _page_header("Themes")
    _frag_themes()

# ── Tab 5: Vol Spikes ────────────────────────────────────────────────────────
with tab_volspike:
    _page_header("Volume Spike Screener", selected_date)
    _frag_volspike(selected_date)

# ── Tab 6: Custom Upload ─────────────────────────────────────────────────────
with tab_upload:
    _page_header("Custom Stock List")

    uploaded = st.file_uploader("Upload CSV", type=["csv"])
    if not uploaded:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #0c1220 0%, #0f1729 100%);
            border: 1px dashed #1e2d45;
            border-radius: 16px;
            padding: 48px 32px;
            text-align: center;
            margin-top: 16px;
        ">
            <div style="font-size:36px; margin-bottom:14px; opacity:0.6;">
                <svg xmlns='http://www.w3.org/2000/svg' width='36' height='36' viewBox='0 0 24 24'
                     fill='none' stroke='#2d5a9e' stroke-width='1.5'
                     stroke-linecap='round' stroke-linejoin='round'>
                    <path d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/>
                    <polyline points='14 2 14 8 20 8'/>
                    <line x1='16' y1='13' x2='8' y2='13'/>
                    <line x1='16' y1='17' x2='8' y2='17'/>
                    <polyline points='10 9 9 9 8 9'/>
                </svg>
            </div>
            <div style="font-size:15px; font-weight:600; color:#e2e8f0; margin-bottom:8px;">
                Analyse a custom watchlist
            </div>
            <div style="font-size:13px; color:#4a5568; max-width:380px; margin:0 auto 20px; line-height:1.6;">
                Upload a CSV with a
                <code style="background:#111827; padding:2px 7px; border-radius:4px; color:#94a3b8; font-size:12px;">symbol</code>
                column containing NSE tickers — no
                <code style="background:#111827; padding:2px 7px; border-radius:4px; color:#94a3b8; font-size:12px;">.NS</code>
                suffix needed.
            </div>
            <div style="font-size:11px; color:#1e3a5f; letter-spacing:0.06em; text-transform:uppercase; font-weight:600;">
                Example &nbsp;·&nbsp; RELIANCE &nbsp;·&nbsp; TCS &nbsp;·&nbsp; INFY &nbsp;·&nbsp; HDFCBANK
            </div>
        </div>
        """, unsafe_allow_html=True)
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
                    # Reuse bulk cache — no extra query
                    custom_df = _load_all_snapshots(selected_date)
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

# ── Tab 7: Technical Analysis ────────────────────────────────────────────────
with tab_technical:
    _page_header("Technical Analysis")
    _frag_technical_analysis()

# ── Tab 8: Global Markets ─────────────────────────────────────────────────────
with tab_gm:
    _page_header("Global Markets")
    if _GM_AVAILABLE:
        _render_global_markets()
    else:
        st.error(f"Global Markets module failed to load: {_GM_ERROR}")
        st.info("Make sure `frontend/global_markets_tab.py` exists and all dependencies are installed.")
