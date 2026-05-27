"""
app.py — Indian Equity Dashboard (Streamlit)

Reads primarily from Supabase. yfinance is used for live benchmark index returns.
All heavy stock computation happens in the daily refresh job.
"""

import base64
import json
import os
import pathlib
import sys
import concurrent.futures
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# components.html() deadlocks on Python 3.14+ due to threading changes.
# Skip all components.html calls on 3.14+ to prevent the app from hanging.
_COMPONENTS_HTML_SAFE = sys.version_info < (3, 14)
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv
import yfinance as yf
from PIL import Image, ImageDraw

load_dotenv()

# PERF: Timing instrumentation — gated behind DEBUG=true env var
from perf_logger import measure, show_perf_panel, reset_timings


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
# Custom CSS — theme-aware via CSS custom properties
# ---------------------------------------------------------------------------
_dark = st.session_state.get("dark_mode", False)

# Inline color tokens for f-string markdown (CSS vars can't reach inline styles)
_T = {
    "text_primary":   "#f1f5f9" if _dark else "#0f172a",
    "text_secondary": "#e2e8f0" if _dark else "#1e293b",
    "text_muted":     "#8b97a8" if _dark else "#64748b",
    "text_soft":      "#4a5568" if _dark else "#64748b",
    "text_label":     "#374151" if _dark else "#6b7280",
    "text_section":   "#475569" if _dark else "#64748b",
    "text_date_badge":"#2d4f8e" if _dark else "#3b82f6",
    "text_as_of":     "#2d4f6e" if _dark else "#64748b",
    "text_accent":    "#60a5fa" if _dark else "#1d4ed8",
    "text_hint":      "#1e3a5f" if _dark else "#3b82f6",
    "text_no_data":   "#2d3f57" if _dark else "#6b7280",
    "bg_tag":         "#0f1f3d" if _dark else "#dbeafe",
    "bg_code":        "#111827" if _dark else "#f1f5f9",
    "bd_tag":         "#1e3a5f" if _dark else "#bfdbfe",
    "bd_card":        "#1e2d45" if _dark else "#e2e8f0",
    "code_text":      "#94a3b8" if _dark else "#475569",
    "card_title":     "#f1f5f9" if _dark else "#0f172a",
    "card_subtitle":  "#64748b" if _dark else "#94a3b8",
    "sb_name":        "#f1f5f9" if _dark else "#0f172a",
}

# Inject CSS custom properties — swapped on every rerun when theme changes
st.markdown(f"""<style>:root {{
    --bg-main:           {"#080c14" if _dark else "#f0f4f8"};
    --bg-secondary:      {"#0b0f1a" if _dark else "#e8edf5"};
    --bg-card-start:     {"#0f1729" if _dark else "#ffffff"};
    --bg-card-end:       {"#111827" if _dark else "#f0f4f8"};
    --bg-accent:         {"#1e3a5f" if _dark else "#dbeafe"};
    --border:            {"#1a2236" if _dark else "#cbd5e1"};
    --border-tab:        {"#1e2d45" if _dark else "#e2e8f0"};
    --border-accent:     {"#2d5a9e" if _dark else "#3b82f6"};
    --border-hover:      {"#2a3a5c" if _dark else "#94a3b8"};
    --text-primary:      {"#f1f5f9" if _dark else "#0f172a"};
    --text-secondary:    {"#e2e8f0" if _dark else "#1e293b"};
    --text-muted:        {"#4a5568" if _dark else "#64748b"};
    --text-tab:          {"#475569" if _dark else "#64748b"};
    --text-caption:      {"#374151" if _dark else "#6b7280"};
    --tab-active-text:   {"#e2e8f0" if _dark else "#1e40af"};
    --sidebar-label:     {"#1e3050" if _dark else "#64748b"};
    --btn-primary-bg:    {"#1d3461" if _dark else "#1d4ed8"};
    --btn-primary-bd:    {"#2d4f8e" if _dark else "#3b82f6"};
    --radio-checked-bg:  {"#1d3461" if _dark else "#1d4ed8"};
    --radio-checked-bd:  {"#3b82f6" if _dark else "#3b82f6"};
    --radio-checked-txt: {"#e2e8f0" if _dark else "#ffffff"};
}}</style>""", unsafe_allow_html=True)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* ── Global ── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Hide Streamlit header (ticker bar is injected as position:fixed via JS) */
    header[data-testid="stHeader"] { display: none !important; }

    /* Collapse invisible autorefresh iframe */
    iframe[title="st_autorefresh.st_autorefresh"] { display: none !important; }

    /* Main background */
    .stApp { background-color: var(--bg-main); }

    .block-container,
    [data-testid="block-container"],
    [data-testid="stMainBlockContainer"] {
        padding-top: 0 !important;
        padding-bottom: 1rem;
        max-width: 100%;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background-color: var(--bg-secondary);
        border-right: 1px solid var(--border);
    }
    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stCaption {
        color: var(--text-muted);
        font-size: 11px;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: transparent;
        border-bottom: 1px solid var(--border-tab);
        padding: 0 0 8px 0;
        align-items: flex-end;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 7px 16px;
        font-size: 11.5px;
        font-weight: 600;
        color: var(--text-tab);
        border-radius: 4px;
        background: transparent;
        border: none;
        letter-spacing: 0.07em;
        text-transform: uppercase;
        transition: color 0.15s, background 0.15s;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: var(--text-secondary);
        background: transparent;
    }
    .stTabs [aria-selected="true"] {
        color: var(--tab-active-text) !important;
        background: var(--bg-accent) !important;
        border: 1px solid var(--border-accent) !important;
        border-radius: 4px !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1.25rem;
    }

    /* ── Metric cards ── */
    [data-testid="metric-container"] {
        background: linear-gradient(135deg, var(--bg-card-start) 0%, var(--bg-card-end) 100%);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 16px 20px;
        transition: border-color 0.2s;
    }
    [data-testid="metric-container"]:hover {
        border-color: var(--border-hover);
    }
    [data-testid="stMetricLabel"] {
        font-size: 11px !important;
        font-weight: 600 !important;
        color: var(--text-muted) !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    [data-testid="stMetricValue"] {
        font-size: 22px !important;
        font-weight: 700 !important;
        color: var(--text-primary) !important;
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
        border: 1px solid var(--border);
        color: var(--text-muted);
    }
    .stButton button[kind="secondary"]:hover {
        background: var(--bg-card-start);
        border-color: var(--border-hover);
        color: var(--text-secondary);
    }
    .stButton button[kind="primary"] {
        background: var(--btn-primary-bg);
        border: 1px solid var(--btn-primary-bd);
        color: var(--tab-active-text);
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
        border: 1px solid var(--border);
        border-radius: 10px;
        overflow: hidden;
    }
    [data-testid="stDataFrame"] th {
        background-color: var(--bg-secondary) !important;
        color: var(--text-muted) !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    [data-testid="stDataFrame"] td {
        font-size: 13px;
    }

    /* ── Input / Select ── */
    .stTextInput input {
        background-color: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        color: var(--text-secondary) !important;
        font-size: 13px !important;
    }
    .stTextInput input:focus {
        border-color: #3b82f6 !important;
        box-shadow: 0 0 0 2px rgba(59,130,246,0.15) !important;
    }

    /* Selectbox & Multiselect — target the inner control div that BaseUI renders */
    .stSelectbox div[data-baseweb="select"] > div:first-child,
    .stMultiSelect div[data-baseweb="select"] > div:first-child {
        background-color: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        color: var(--text-secondary) !important;
        font-size: 13px !important;
        min-height: 38px !important;
    }
    .stSelectbox div[data-baseweb="select"] > div:first-child:hover,
    .stMultiSelect div[data-baseweb="select"] > div:first-child:hover {
        border-color: var(--border-hover) !important;
    }
    /* Value text color inside selects */
    .stSelectbox div[data-baseweb="select"] [data-testid="stSelectboxVirtualDropdown"],
    .stSelectbox div[data-baseweb="select"] div[class*="singleValue"],
    .stSelectbox div[data-baseweb="select"] div[class*="placeholder"] {
        color: var(--text-secondary) !important;
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
        background: var(--bg-secondary);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 4px 12px;
        font-size: 12px;
        font-weight: 500;
        color: var(--text-muted);
        cursor: pointer;
        transition: all 0.15s;
    }
    .stRadio label:has(input:checked) {
        background: var(--radio-checked-bg);
        border-color: var(--radio-checked-bd);
        color: var(--radio-checked-txt);
    }

    /* ── Theme toggle icon button ── */
    [data-testid*="stButton-theme_toggle_main"] button {
        padding: 4px 10px !important;
        font-size: 16px !important;
        border-radius: 20px !important;
        border: 1px solid var(--border) !important;
        background: var(--bg-card-start) !important;
        color: var(--text-primary) !important;
        line-height: 1.4 !important;
        min-height: unset !important;
    }

    /* ── Selected (primary) button — must win over all other rules ── */
    .stButton button[kind="primary"] {
        background: var(--btn-primary-bg) !important;
        border: 1px solid var(--btn-primary-bd) !important;
        color: var(--tab-active-text) !important;
        font-weight: 600 !important;
    }

    /* ── Divider ── */
    hr {
        border: none;
        border-top: 1px solid var(--border);
        margin: 12px 0;
    }

    /* ── Captions & helpers ── */
    .stCaption, .stCaption p {
        color: var(--text-caption);
        font-size: 11.5px;
    }

    /* ── Alerts ── */
    .stAlert {
        border-radius: 8px;
        font-size: 13px;
    }

    /* ── Scrollbar (webkit) ── */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--bg-main); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-hover); }

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
        color: var(--sidebar-label);
        margin: 2px 0 8px 0;
    }

    /* ── Popover (About this tab) ── */
    div[data-baseweb="popover"] > div,
    div[data-baseweb="popover"] > div > div {
        background-color: var(--bg-card-start) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        color: var(--text-secondary) !important;
    }
    div[data-baseweb="popover"] p,
    div[data-baseweb="popover"] li,
    div[data-baseweb="popover"] span {
        color: var(--text-secondary) !important;
    }

    /* ── Selectbox / Multiselect dropdown menus ── */
    ul[data-baseweb="menu"],
    div[data-baseweb="menu"] {
        background-color: var(--bg-card-start) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
    }
    li[role="option"],
    div[role="option"] {
        background-color: var(--bg-card-start) !important;
        color: var(--text-secondary) !important;
    }
    li[role="option"]:hover,
    div[role="option"]:hover,
    li[aria-selected="true"],
    div[aria-selected="true"] {
        background-color: var(--bg-accent) !important;
        color: var(--text-primary) !important;
    }

    /* ── AG Grid (st.dataframe) dark/light theming ── */
    .ag-theme-streamlit {
        --ag-background-color: var(--bg-card-start) !important;
        --ag-odd-row-background-color: var(--bg-secondary) !important;
        --ag-header-background-color: var(--bg-secondary) !important;
        --ag-foreground-color: var(--text-secondary) !important;
        --ag-header-foreground-color: var(--text-muted) !important;
        --ag-border-color: var(--border) !important;
        --ag-row-hover-color: var(--bg-accent) !important;
        --ag-selected-row-background-color: var(--bg-accent) !important;
        --ag-modal-overlay-background-color: var(--bg-card-start) !important;
    }
    .ag-theme-streamlit .ag-root-wrapper {
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        overflow: hidden !important;
    }
    .ag-theme-streamlit .ag-header-cell-text {
        color: var(--text-muted) !important;
        font-size: 11px !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.06em !important;
    }
    .ag-theme-streamlit .ag-cell {
        color: var(--text-secondary) !important;
        font-size: 13px !important;
    }
</style>
""", unsafe_allow_html=True)

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
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #0f172a 100%) !important;
            background-attachment: fixed !important;
        }
        [data-testid="stAppViewContainer"]::before {
            content: '';
            position: fixed;
            inset: 0;
            background-image: url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%233b82f6' fill-opacity='0.06'%3E%3Cpath d='M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z'/%3E%3C/g%3E%3C/G%3E%3C/svg%3E");
            pointer-events: none;
            z-index: 0;
        }

        /* hide "Press Enter to apply" hint on password field */
        .stTextInput div[data-baseweb="input"] ~ div small,
        .stTextInput [class*="InputInstructions"],
        .stTextInput ~ div > small,
        .stTextInput small,
        [data-testid="InputInstructions"],
        .stTextInput [data-testid="InputInstructions"],
        .stTextInput div[role="status"],
        .stTextInput + div small { display: none !important; }

        /* Frosted glass card */
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(12, 18, 32, 0.82) !important;
            backdrop-filter: blur(24px) !important;
            -webkit-backdrop-filter: blur(24px) !important;
            border: 1px solid rgba(59, 130, 246, 0.22) !important;
            border-radius: 24px !important;
            box-shadow: 0 32px 80px rgba(0,0,0,0.55), 0 0 0 1px rgba(59,130,246,0.06) !important;
        }
        [data-testid="stVerticalBlockBorderWrapper"] > div {
            padding: 12px 32px 24px !important;
        }

        /* Input field styling */
        .stTextInput input {
            background: rgba(255,255,255,0.05) !important;
            border: 1px solid rgba(59,130,246,0.2) !important;
            border-radius: 10px !important;
            color: #94a3b8 !important;
        }
        .stTextInput input:focus {
            border-color: rgba(59,130,246,0.5) !important;
            box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important;
        }

        /* Sign In button */
        .stButton > button[kind="primary"] {
            background: linear-gradient(90deg, #1d4ed8, #3b82f6) !important;
            border: none !important;
            border-radius: 10px !important;
            font-weight: 700 !important;
            letter-spacing: 0.04em !important;
            color: white !important;
            box-shadow: 0 4px 20px rgba(37,99,235,0.35) !important;
            transition: box-shadow 0.2s !important;
        }
        .stButton > button[kind="primary"]:hover {
            box-shadow: 0 6px 28px rgba(37,99,235,0.55) !important;
        }
        .lp-sebi-disclaimer {
            text-align: center;
            font-size: 10px;
            color: #94a3b8;
            margin-top: 14px;
            line-height: 1.5;
            letter-spacing: 0.01em;
        }

        .lp-logo-icon {
            width: 60px; height: 60px; border-radius: 16px;
            background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%);
            display: inline-flex; align-items: center; justify-content: center;
            margin: 8px auto 18px;
            box-shadow: 0 8px 32px rgba(37,99,235,0.4);
        }
        .lp-name {
            font-size: 32px; font-weight: 800; color: #f1f5f9;
            letter-spacing: -0.05em; margin: 0 0 6px;
        }
        .lp-name span { color: #60a5fa; }
        .lp-tagline {
            font-size: 12px; color: #f1f5f9; letter-spacing: 0.1em;
            text-transform: uppercase; font-weight: 500; margin-bottom: 24px;
        }
        .lp-divider { border: none; border-top: 1px solid rgba(59,130,246,0.12); margin: 0 0 20px; }
        .lp-footer {
            text-align: center; font-size: 11px; color: #f1f5f9;
            margin-top: 12px; letter-spacing: 0.02em;
        }
        /* ── Creator card — fixed bottom-left ── */
        .creator-card {
            position: fixed;
            bottom: 24px;
            left: 24px;
            width: 260px;
            background: rgba(10, 16, 28, 0.88);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid rgba(59,130,246,0.18);
            border-radius: 16px;
            padding: 16px 16px 16px 16px;
            display: flex;
            align-items: center;
            gap: 14px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.5);
            z-index: 9999;
        }
        .creator-info { flex: 1; min-width: 0; }
        .creator-heading {
            font-size: 9px; font-weight: 700; color: #3b82f6;
            text-transform: uppercase; letter-spacing: 0.12em; margin-bottom: 5px;
        }
        .creator-name {
            font-size: 14px; font-weight: 800; color: #f1f5f9;
            letter-spacing: -0.02em; margin-bottom: 5px; white-space: nowrap;
            overflow: hidden; text-overflow: ellipsis;
        }
        .creator-link {
            display: flex; align-items: center; gap: 5px;
            font-size: 11px; color: #60a5fa; text-decoration: none;
            font-weight: 500; margin-bottom: 3px;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .creator-link:hover { color: #93c5fd; text-decoration: underline; }
        .creator-email {
            display: flex; align-items: center; gap: 5px;
            font-size: 11px; color: #64748b;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .creator-avatar {
            width: 64px; height: 64px; border-radius: 12px; flex-shrink: 0;
            background: linear-gradient(135deg, #1d4ed8 0%, #3b82f6 100%);
            display: flex; align-items: center; justify-content: center;
            font-size: 22px; font-weight: 800; color: #fff;
            letter-spacing: -0.03em;
            box-shadow: 0 4px 16px rgba(37,99,235,0.35);
            border: 2px solid rgba(59,130,246,0.3);
        }
        .creator-avatar-img {
            width: 72px; height: 72px; border-radius: 12px; flex-shrink: 0;
            object-fit: cover; object-position: center top;
            box-shadow: 0 4px 16px rgba(0,0,0,0.5);
            border: 2px solid rgba(59,130,246,0.35);
        }
        .lp-footer {
            text-align: center; font-size: 11px; color: #f1f5f9;
            margin-top: 12px; letter-spacing: 0.02em;
        }
    </style>
    """, unsafe_allow_html=True)

    # Creator card — fixed bottom-left, outside the centre column
    _photo_path = pathlib.Path(__file__).parent / "assets" / "sumit_meena.jpg"
    if _photo_path.exists():
        _photo_b64 = base64.b64encode(_photo_path.read_bytes()).decode()
        _avatar_html = f'<img src="data:image/jpeg;base64,{_photo_b64}" class="creator-avatar-img">'
    else:
        _avatar_html = '<div class="creator-avatar">SM</div>'

    st.markdown(f"""
    <div class="creator-card">
        <div class="creator-info">
            <div class="creator-heading">Created By</div>
            <div class="creator-name">Sumit Meena</div>
            <a class="creator-link" href="https://linkedin.com/in/sumit-meena-9559422a8" target="_blank">
                <svg width="11" height="11" viewBox="0 0 24 24" fill="#0a66c2"><path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 01-2.063-2.065 2.064 2.064 0 112.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/></svg>
                linkedin.com/in/sumit-meena-9559422a8
            </a>
            <div class="creator-email">
                <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#64748b" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/></svg>
                sumitmeena680@gmail.com
            </div>
        </div>
        {_avatar_html}
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='min-height:10vh'></div>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 1.4, 1])
    with col:
        with st.container(border=True):
            st.markdown("""
            <div style="text-align:center; padding-top:12px;">
                <div class="lp-logo-icon">
                    <svg xmlns="http://www.w3.org/2000/svg" width="30" height="30" viewBox="0 0 24 24"
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
            pw = st.text_input("Password", type="password", placeholder="Enter password…",
                               label_visibility="collapsed")
            if st.button("Sign In →", use_container_width=True, type="primary"):
                if pw == correct:
                    st.session_state["authenticated"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")
            st.markdown("""
                <div class='lp-sebi-disclaimer'>
                    <strong>Disclaimer:</strong> This platform is for informational and educational purposes only.
                    It does not constitute investment advice, research, or a recommendation to buy or sell any
                    securities. Users must conduct their own due diligence. Investments in securities markets are
                    subject to market risks. Please read all related documents carefully before investing.
                    <em>Not SEBI registered.</em>
                </div>
            """, unsafe_allow_html=True)
            st.markdown(
                "<div class='lp-footer'>Restricted access · Authorised users only</div>",
                unsafe_allow_html=True,
            )
    st.stop()


_check_password()

# PERF: Reset timing counters at the start of every full-page render.
reset_timings()

# Inject loading overlay — only for authenticated users, after login gate.
# Skipped on Python 3.14+ where components.html() deadlocks (threading changes).
if _COMPONENTS_HTML_SAFE:
  try:
    components.html("""
<script>
(function() {
    var pdoc = window.parent.document;
    if (pdoc.getElementById('eq-loader')) return;
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
    var overlay = pdoc.createElement('div');
    overlay.id = 'eq-loader';
    overlay.innerHTML = '<div id="eq-spinner"></div>';
    pdoc.body.appendChild(overlay);
    var hideTimer;
    var observer = new MutationObserver(function() {
        var stale = pdoc.querySelector('[data-stale="true"]');
        if (stale) { clearTimeout(hideTimer); overlay.classList.add('show'); }
        else { hideTimer = setTimeout(function() { overlay.classList.remove('show'); }, 120); }
    });
    observer.observe(pdoc.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['data-stale'] });
})();
</script>
""", height=0)
  except Exception:
    pass

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _get_engine():
    import time
    try:
        url = st.secrets["SUPABASE_DB_URL"]
    except Exception:
        url = os.environ.get("SUPABASE_DB_URL", "")
    if not url:
        st.error("SUPABASE_DB_URL not configured.")
        st.stop()
    tx_url = url.replace(":5432/", ":6543/")
    connect_args = {
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 3,
        "options": "-c statement_timeout=30000",
    }
    # Try transaction pooler (6543) first — designed for stateless apps like Streamlit.
    # Fall back to session pooler (5432) only if transaction pooler is unavailable.
    for attempt_url in [tx_url, url]:
        for attempt in range(3):
            try:
                eng = create_engine(attempt_url, poolclass=NullPool, connect_args=connect_args)
                with eng.connect() as c:
                    c.execute(text("SELECT 1"))
                return eng
            except Exception:
                if attempt < 2:
                    time.sleep(2 ** attempt)
    st.error("Could not connect to the database. Please try refreshing.")
    st.stop()


# PERF: Engine is cached via @st.cache_resource — near-zero on warm runs
with measure("get_engine"):
    engine = _get_engine()

# News headline ticker bar (below stock price ticker) — needs engine
try:
    from news_ticker import render_news_ticker
    render_news_ticker(engine)
except Exception:
    pass

# Health check endpoint — ?health=true returns JSON status without rendering the full app
if st.query_params.get("health") == "true":
    try:
        with engine.connect() as _hc_conn:
            _hc_conn.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    st.json({"status": "ok" if db_ok else "degraded", "version": "2.0.0", "db": db_ok})
    st.stop()

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
@st.cache_data(ttl=300, show_spinner=False)
def load_available_dates() -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT date FROM snapshots_daily ORDER BY date DESC LIMIT 90")
        ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_all_index_returns() -> dict:
    """Batch fetch 1D/1M/1Y returns for ALL index symbols in one yf.download call.
    Returns {yf_symbol: {"1D": float|None, "1M": float|None, "1Y": float|None}}.
    Replaces per-symbol fetch_index_returns to cut cold-start from ~30s to ~3s."""
    symbols = list({s for s in INDEX_YF_SYMBOL.values() if s is not None})
    if not symbols:
        return {}
    try:
        def _run():
            return yf.download(
                tickers=symbols, period='13mo', interval='1d',
                auto_adjust=True, progress=False, threads=False,
            )
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_run)
            try:
                raw = fut.result(timeout=30)
            except Exception:
                return {}
        if raw.empty:
            return {}
        close = (raw['Close'] if isinstance(raw.columns, pd.MultiIndex)
                 else raw[['Close']].rename(columns={'Close': symbols[0]}))
        result = {}
        for sym in symbols:
            try:
                if sym not in close.columns:
                    continue
                prices = close[sym].dropna()
                if len(prices) < 2:
                    continue
                last = float(prices.iloc[-1])
                prev = float(prices.iloc[-2])
                ret_1d = (last / prev - 1) if prev != 0 else None
                idx_1m = max(0, len(prices) - 22)
                c1m = float(prices.iloc[idx_1m])
                ret_1m = (last / c1m - 1) if c1m != 0 else None
                idx_1y = max(0, len(prices) - 253)
                c1y = float(prices.iloc[idx_1y])
                ret_1y = (last / c1y - 1) if c1y != 0 else None
                result[sym] = {"1D": ret_1d, "1M": ret_1m, "1Y": ret_1y}
            except Exception:
                pass
        return result
    except Exception as e:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
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
    # PERF: Measure the actual SQL round-trip — near-zero on cache hit, 300-800ms on cold
    with measure("_load_all_snapshots__sql"):
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

    # Derive status columns from raw DMA values where they are NULL.
    # This handles rows written before status columns existed (schema migration gap)
    # and any stocks whose status_200dma/status_50dma were not populated by the refresh.
    if "status_200dma" in df.columns and "dma_200" in df.columns and "cmp" in df.columns:
        mask = df["status_200dma"].isna() & df["dma_200"].notna() & df["cmp"].notna()
        if mask.any():
            df.loc[mask, "status_200dma"] = (
                df.loc[mask, "cmp"] >= df.loc[mask, "dma_200"]
            ).map({True: "Above 200DMA", False: "Below 200DMA"})

    if "status_50dma" in df.columns and "dma_50" in df.columns and "cmp" in df.columns:
        mask = df["status_50dma"].isna() & df["dma_50"].notna() & df["cmp"].notna()
        if mask.any():
            df.loc[mask, "status_50dma"] = (
                df.loc[mask, "cmp"] >= df.loc[mask, "dma_50"]
            ).map({True: "Above 50DMA", False: "Below 50DMA"})

    return df


@st.cache_data(ttl=3600, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
def load_sector_performance(snap_date, refresh_ts=None) -> pd.DataFrame:
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
    # PERF: PERCENTILE_CONT aggregation — expensive on large tables without indexes
    with measure("load_sector_performance__sql"):
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"date": str(snap_date)})
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def load_all_symbols() -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT symbol, name, sector FROM stocks WHERE is_active = TRUE"),
            conn,
        )
    return df


# ---------------------------------------------------------------------------
# Watchlist DB helpers
# ---------------------------------------------------------------------------
def _wl_load_watchlists() -> pd.DataFrame:
    sql = text("""
        SELECT w.id, w.name, w.created_at, COUNT(m.symbol) AS stock_count
        FROM watchlists w
        LEFT JOIN watchlist_members m ON w.id = m.watchlist_id
        GROUP BY w.id, w.name, w.created_at
        ORDER BY w.created_at DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


def _wl_load_symbols(watchlist_id: int) -> list[str]:
    sql = text("SELECT symbol FROM watchlist_members WHERE watchlist_id = :wid ORDER BY symbol")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"wid": watchlist_id})
    return df["symbol"].tolist()


def _wl_save(name: str, symbols: list[str]) -> None:
    with engine.begin() as conn:
        result = conn.execute(
            text("INSERT INTO watchlists (name) VALUES (:name) RETURNING id"),
            {"name": name},
        )
        wl_id = result.fetchone()[0]
        if symbols:
            conn.execute(
                text("INSERT INTO watchlist_members (watchlist_id, symbol) VALUES (:wid, :sym)"),
                [{"wid": wl_id, "sym": s} for s in symbols],
            )


def _wl_delete(watchlist_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM watchlists WHERE id = :wid"), {"wid": watchlist_id})


def _wl_rename(watchlist_id: int, new_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE watchlists SET name = :name WHERE id = :wid"),
            {"name": new_name, "wid": watchlist_id},
        )


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
def load_latest_technicals(refresh_ts=None) -> pd.DataFrame:
    """Load the most recent technical indicators for all active stocks."""
    sql_v2 = text("""
        SELECT
            s.symbol,
            s.name,
            s.sector,
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
            t.date AS indicator_date,
            h52.high_52w,
            CASE
                WHEN h52.high_52w IS NOT NULL AND h52.high_52w > 0
                THEN (t.cmp - h52.high_52w) / h52.high_52w * 100
                ELSE NULL
            END AS pct_from_52wh,
            ath.all_time_high,
            CASE
                WHEN ath.all_time_high IS NOT NULL AND ath.all_time_high > 0
                THEN (t.cmp - ath.all_time_high) / ath.all_time_high * 100
                ELSE NULL
            END AS pct_from_ath,
            rs.rs_excess_1w, rs.rs_excess_2w, rs.rs_excess_1m,
            rs.rs_excess_3m, rs.rs_excess_6m, rs.rs_excess_1y,
            rs.rs_bucket_1w, rs.rs_bucket_2w, rs.rs_bucket_1m,
            rs.rs_bucket_3m, rs.rs_bucket_6m, rs.rs_bucket_1y
        FROM stocks s
        JOIN latest_technicals t ON t.symbol = s.symbol
        LEFT JOIN (
            SELECT symbol, MAX(high) AS high_52w
            FROM prices_daily
            WHERE date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY symbol
        ) h52 ON h52.symbol = s.symbol
        LEFT JOIN (
            SELECT symbol, MAX(high) AS all_time_high
            FROM prices_daily
            GROUP BY symbol
        ) ath ON ath.symbol = s.symbol
        LEFT JOIN latest_relative_strength rs ON rs.symbol = s.symbol
        WHERE s.is_active = true
        ORDER BY s.symbol
    """)
    sql_v1 = text("""
        SELECT
            s.symbol,
            s.name,
            s.sector,
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
            t.date AS indicator_date,
            h52.high_52w,
            CASE
                WHEN h52.high_52w IS NOT NULL AND h52.high_52w > 0
                THEN (t.cmp - h52.high_52w) / h52.high_52w * 100
                ELSE NULL
            END AS pct_from_52wh,
            ath.all_time_high,
            CASE
                WHEN ath.all_time_high IS NOT NULL AND ath.all_time_high > 0
                THEN (t.cmp - ath.all_time_high) / ath.all_time_high * 100
                ELSE NULL
            END AS pct_from_ath,
            rs.rs_excess_1w, rs.rs_excess_2w, rs.rs_excess_1m,
            rs.rs_excess_3m, rs.rs_excess_6m, rs.rs_excess_1y,
            rs.rs_bucket_1w, rs.rs_bucket_2w, rs.rs_bucket_1m,
            rs.rs_bucket_3m, rs.rs_bucket_6m, rs.rs_bucket_1y
        FROM stocks s
        JOIN latest_technicals t ON t.symbol = s.symbol
        LEFT JOIN (
            SELECT symbol, MAX(high) AS high_52w
            FROM prices_daily
            WHERE date >= CURRENT_DATE - INTERVAL '365 days'
            GROUP BY symbol
        ) h52 ON h52.symbol = s.symbol
        LEFT JOIN (
            SELECT symbol, MAX(high) AS all_time_high
            FROM prices_daily
            GROUP BY symbol
        ) ath ON ath.symbol = s.symbol
        LEFT JOIN latest_relative_strength rs ON rs.symbol = s.symbol
        WHERE s.is_active = true
        ORDER BY s.symbol
    """)
    # PERF: Measure technicals SQL round-trip — ~1500 rows with 52W high join
    try:
        with measure("load_latest_technicals__sql_v2"):
            with engine.connect() as conn:
                df = pd.read_sql(sql_v2, conn)
    except Exception:
        # v2 columns not yet migrated — open a fresh connection for fallback
        with measure("load_latest_technicals__sql_v1_fallback"):
            with engine.connect() as conn:
                df = pd.read_sql(sql_v1, conn)
    # Cast NUMERIC → float (PostgreSQL returns Decimal objects)
    for c in ["cmp", "rsi_14", "macd_line", "macd_signal", "macd_histogram",
              "adx_14", "sma_50", "sma_200", "signal_score",
              "sma_200_slope", "volume_ratio", "signal_score_v2",
              "high_52w", "pct_from_52wh", "all_time_high", "pct_from_ath",
              "rs_excess_1w", "rs_excess_2w", "rs_excess_1m",
              "rs_excess_3m", "rs_excess_6m", "rs_excess_1y"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=1800, show_spinner=False)
def load_themes() -> pd.DataFrame:
    sql = text("""
        SELECT theme_slug, theme_name, theme_order, actual_stock_count
        FROM themes_with_counts
        ORDER BY theme_order
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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


@st.cache_data(ttl=300, show_spinner=False)
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

# ---------------------------------------------------------------------------
# Column visibility — persistent per-table, saved to .tmp/col_visibility.json
# ---------------------------------------------------------------------------
_COL_VIS_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", ".tmp", "col_visibility.json")


def _load_col_visibility(table_id: str) -> set:
    try:
        with open(_COL_VIS_CONFIG_PATH) as f:
            return set(json.load(f).get(table_id, []))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return set()


def _save_col_visibility(table_id: str, hidden_cols: set):
    os.makedirs(os.path.dirname(_COL_VIS_CONFIG_PATH), exist_ok=True)
    try:
        with open(_COL_VIS_CONFIG_PATH) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        data = {}
    data[table_id] = sorted(hidden_cols)
    with open(_COL_VIS_CONFIG_PATH, "w") as f:
        json.dump(data, f, indent=2)


def _render_col_visibility_ui(table_id: str, all_cols: list, ncols: int = 7) -> set:
    """Expander with checkboxes for every column. Returns current hidden-col set."""
    saved_hidden = _load_col_visibility(table_id)
    with st.expander("⚙ Column Visibility", expanded=False):
        btn_col, _ = st.columns([1, 5])
        with btn_col:
            if st.button("Show All Columns", key=f"_colvis_showall_{table_id}"):
                for col in all_cols:
                    sk = f"_colvis_{table_id}_{col}"
                    if sk in st.session_state:
                        del st.session_state[sk]
                _save_col_visibility(table_id, set())
                st.rerun()
        ui_cols = st.columns(ncols)
        new_hidden: set = set()
        for i, col in enumerate(all_cols):
            with ui_cols[i % ncols]:
                is_visible = st.checkbox(col, value=col not in saved_hidden, key=f"_colvis_{table_id}_{col}")
                if not is_visible:
                    new_hidden.add(col)
        if new_hidden != saved_hidden:
            _save_col_visibility(table_id, new_hidden)
    return _load_col_visibility(table_id)


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
    # BUGFIX: `if prev["close"]` lets NaN through (NaN is truthy in Python), producing
    # "nan%" on screen. Use pd.notna to correctly detect missing values.
    _pc = prev["close"]
    day_chg_pct = (last["close"] - _pc) / _pc * 100 if (pd.notna(_pc) and _pc != 0) else 0
    chg_color   = "#22c55e" if day_chg_pct >= 0 else "#ef4444"
    arrow       = "▲" if day_chg_pct >= 0 else "▼"

    _sym_info = load_all_symbols()
    _sym_row  = _sym_info[_sym_info["symbol"] == symbol]
    _sector   = _sym_row.iloc[0]["sector"] if not _sym_row.empty and pd.notna(_sym_row.iloc[0]["sector"]) else None
    _sector_tag = (
        f"<span style='font-size:11px;font-weight:600;color:{_T['text_accent']};"
        f"background:{_T['bg_tag']};padding:2px 8px;border-radius:4px;"
        f"border:1px solid {_T['bd_tag']};white-space:nowrap;'>{_sector}</span>"
        if _sector else ""
    )

    st.markdown(
        f"<div style='display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap;'>"
        f"<span style='font-size:20px;font-weight:700;color:{_T['text_secondary']}'>{symbol}</span>"
        f"<span style='color:{_T['text_muted']};font-size:13px'>{name}</span>"
        f"{_sector_tag}"
        f"<span style='font-size:26px;font-weight:700;color:{_T['text_secondary']};margin-left:4px;'>₹{last['close']:,.2f}</span>"
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
def render_summary_cards(df: pd.DataFrame, index_name: str | None = None, snap_date=None, show_returns: bool = True):
    valid_ret = df["ret_1d"].dropna()
    adv       = int((valid_ret > 0).sum())
    dec       = int((valid_ret < 0).sum())
    unch      = int((valid_ret == 0).sum())
    above_200 = int((df["status_200dma"] == "Above 200DMA").sum())
    total     = len(df)

    if not show_returns:
        # Watchlist mode — only show advance/decline metrics, no return cards
        c1, c2, c3, c4 = st.columns(4)
        with c1: st.metric("Total Stocks", total)
        with c2: st.metric("Advancing",    adv)
        with c3: st.metric("Declining",    dec)
        with c4: st.metric("Unchanged",    unch)
        if snap_date:
            st.markdown(
                f"<div style='font-size:10.5px;color:{_T['text_as_of']};margin-top:2px;"
                f"letter-spacing:0.04em;'>As of market close · "
                f"{pd.Timestamp(snap_date).strftime('%d %b %Y')}</div>",
                unsafe_allow_html=True,
            )
        return

    # Fetch index-level returns — prefer a benchmark yfinance symbol, else use
    # the median return of constituent stocks already in df.
    idx_rets: dict = {}
    _yf_fetch_error: str | None = None
    yf_sym = INDEX_YF_SYMBOL.get(index_name) if index_name else None
    if yf_sym:
        with measure("fetch_all_index_returns"):
            all_rets = fetch_all_index_returns()
        _raw = all_rets.get(yf_sym)
        if _raw is None:
            _yf_fetch_error = f"No data for {yf_sym}"
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
            f"<div style='font-size:10.5px;color:{_T['text_as_of']};margin-top:2px;"
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
    if df.empty:
        st.info("No stocks match the current filters. Try relaxing the RSI range, removing the sector filter, or switching to a broader index.")
        return
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
    _all_data_cols = list(display.columns) + ["Screener", "Chart"]
    _hidden = _render_col_visibility_ui(key, _all_data_cols)

    # Add link columns before filtering so they can also be hidden
    display["Screener"] = chunk["screener_url"].where(chunk["screener_url"].notna(), other=None)
    display["Chart"] = chunk["tradingview_url"].where(chunk["tradingview_url"].notna(), other=None)
    _visible_cols = [c for c in display.columns if c not in _hidden]
    display = display[_visible_cols]

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

    # PERF: Measure st.dataframe render for universe table (varies by row count)
    with measure(f"render_table__{key}"):
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
    # BUGFIX: only select columns that actually exist in df — the theme SQL uses LEFT JOINs
    # so a stock with no snapshot data would still produce all columns, but defensive
    # filtering prevents KeyError if the schema ever drifts.
    available_keys = [k for k in THEME_DISPLAY_COLS.keys() if k in df.columns]
    d = df[available_keys].copy()
    d = d.rename(columns=THEME_DISPLAY_COLS)
    for raw, pretty in THEME_DISPLAY_COLS.items():
        if raw in THEME_PCT_COLS and raw in df.columns:
            d[pretty] = df[raw].map(_fmt_pct)
    if "cmp" in df.columns:
        d["CMP"] = df["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    if "market_cap_cr" in df.columns:
        d["Market Cap (₹ Cr)"] = df["market_cap_cr"].map(
            lambda v: f"₹{v:,.2f} Cr" if pd.notna(v) else "—"
        )
    if "pe_ratio" in df.columns:
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

    # Initialise selected theme to the top performer under the current sort period
    if "selected_theme_slug" not in st.session_state:
        _default_sort_col = _THEME_SORT_OPTIONS.get(
            st.session_state.get("theme_sort_period", "1M"), ("avg_ret_30d", "")
        )[0]
        _sorted_init = themes_df.sort_values(_default_sort_col, ascending=False, na_position="last")
        st.session_state["selected_theme_slug"] = _sorted_init.iloc[0]["theme_slug"]

    left_col, right_col = st.columns([1, 3])

    # ── Left: theme picker ───────────────────────────────────────────────────
    with left_col:
        # Duration pill selector
        if "theme_sort_period" not in st.session_state:
            st.session_state["theme_sort_period"] = "1M"

        st.markdown(
            f"<div style='font-size:10px;font-weight:700;letter-spacing:0.12em;"
            f"text-transform:uppercase;color:{_T['text_label']};margin-bottom:6px;'>"
            "Return Period</div>",
            unsafe_allow_html=True,
        )
        dur_cols = st.columns(len(_THEME_SORT_OPTIONS))
        for i, dur_label in enumerate(_THEME_SORT_OPTIONS):
            with dur_cols[i]:
                if st.button(
                    dur_label,
                    key=f"theme_dur_{dur_label}",
                    use_container_width=True,
                ):
                    st.session_state["theme_sort_period"] = dur_label
                    st.rerun()

        _active_dur = st.session_state["theme_sort_period"]

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
            if st.button(
                label,
                key=f"theme_btn_{row['theme_slug']}",
                use_container_width=True,
            ):
                st.session_state["selected_theme_slug"] = row["theme_slug"]
                st.session_state["_theme_scroll_top"] = True
                st.rerun()

        # Highlight selected buttons via JS — CSS selectors can't reach them in Streamlit 1.53
        # (button kind/type is a React prop, not an HTML attribute; data-testid has no key)
        _active_slug = st.session_state["selected_theme_slug"]
        _active_theme_row = themes_df[themes_df["theme_slug"] == _active_slug]
        _active_theme_name = _active_theme_row.iloc[0]["theme_name"] if not _active_theme_row.empty else ""
        _sel_bg  = "#1d3461" if _dark else "#1d4ed8"
        _sel_bd  = "#2d4f8e" if _dark else "#3b82f6"
        _sel_txt = "#e2e8f0"
        if _COMPONENTS_HTML_SAFE:
         components.html(f"""
<script>
(function() {{
  var activeDur   = {repr(_active_dur)};
  var activeTheme = {repr(_active_theme_name)};
  var durLabels   = ['1W', '1M', '1Y'];
  var selBg  = '{_sel_bg}';
  var selBd  = '{_sel_bd}';
  var selTxt = '{_sel_txt}';

  function applyHighlights() {{
    var doc = window.parent.document;
    var buttons = doc.querySelectorAll('.stButton button');
    var durDone = false, themeDone = false;
    buttons.forEach(function(btn) {{
      var text = btn.innerText.trim();
      if (durLabels.indexOf(text) !== -1) {{
        if (text === activeDur) {{
          btn.style.setProperty('background', selBg, 'important');
          btn.style.setProperty('border', '1px solid ' + selBd, 'important');
          btn.style.setProperty('color', selTxt, 'important');
          btn.style.setProperty('font-weight', '700', 'important');
          durDone = true;
        }} else {{
          btn.style.removeProperty('background');
          btn.style.removeProperty('border');
          btn.style.removeProperty('color');
          btn.style.removeProperty('font-weight');
        }}
      }}
      if (activeTheme && text.startsWith(activeTheme)) {{
        btn.style.setProperty('background', selBg, 'important');
        btn.style.setProperty('border-left', '3px solid #3b82f6', 'important');
        btn.style.setProperty('color', selTxt, 'important');
        btn.style.setProperty('font-weight', '600', 'important');
        themeDone = true;
      }}
    }});
    return durDone && themeDone;
  }}

  var attempts = 0;
  function tryHighlight() {{
    if (!applyHighlights() && attempts < 30) {{
      attempts++;
      setTimeout(tryHighlight, 100);
    }}
  }}
  tryHighlight();

  var observer = new MutationObserver(function() {{ applyHighlights(); }});
  observer.observe(window.parent.document.body, {{subtree: true, childList: true, attributes: true}});
  setTimeout(function() {{ observer.disconnect(); }}, 8000);
}})();
</script>
""", height=0)

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

        _themes_all_cols = list(display.columns)
        _themes_hidden = _render_col_visibility_ui("themes", _themes_all_cols)
        _themes_visible = [c for c in display.columns if c not in _themes_hidden]
        display = display[_themes_visible]

        styled = display.style
        for raw, pretty in THEME_DISPLAY_COLS.items():
            if raw in THEME_PCT_COLS and pretty in display.columns:
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
_PLOT_BG    = "rgba(0,0,0,0)" if not _dark else "#080c14"
_GRID       = "#e2e8f0" if not _dark else "#111827"
_ZERO_LINE  = "#cbd5e1" if not _dark else "#1e293b"
# Theme-aware chart colors
_AXIS_TICK_COLOR  = "#64748b" if not _dark else "#94a3b8"
_YAXIS_FONT_COLOR = "#374151" if not _dark else "#cbd5e0"
_BAR_LABEL_COLOR  = "#1e293b" if not _dark else "#e2e8f0"
_HOVER_BG         = "#f1f5f9" if not _dark else "#1e2535"
_HOVER_BD         = "#cbd5e1" if not _dark else "#334155"
_HOVER_TXT        = "#0f172a" if not _dark else "#e2e8f0"
_DONUT_LINE_COLOR = "#f0f4f8" if not _dark else "#080c14"
_ANN_COLOR        = "#64748b" if not _dark else "#94a3b8"


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
        font=dict(family=_CHART_FONT, color=_AXIS_TICK_COLOR, size=11),
        margin=dict(l=90, r=72, t=6, b=6),
        bargap=0,
        xaxis=dict(
            range=x_range,
            gridcolor=_GRID,
            gridwidth=1,
            tickformat="+.1f",
            ticksuffix="%",
            tickfont=dict(size=9, color=_AXIS_TICK_COLOR),
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
                color=_YAXIS_FONT_COLOR,
                family=_CHART_FONT,
            ),
            showline=False,
        ),
        showlegend=False,
        hoverlabel=dict(
            bgcolor=_HOVER_BG,
            bordercolor=_HOVER_BD,
            font=dict(family=_CHART_FONT, color=_HOVER_TXT, size=12),
        ),
    )
    fig.update_traces(
        textfont=dict(
            family=_CHART_FONT,
            color=_BAR_LABEL_COLOR,
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
        f"<div style='font-size:11px;color:{_T['text_section']};margin-bottom:20px;'>"
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

    _pb_track = "#111827" if _dark else "#e2e8f0"
    def _progress_bar(pct: float, color: str) -> str:
        """Thin HTML progress bar."""
        return (
            f"<div style='background:{_pb_track};border-radius:3px;height:5px;"
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
                        f"border-bottom:1px solid {_T['bd_card']};'>"
                        f"<div style='width:4px;height:34px;background:{dominant_color};"
                        f"border-radius:2px;flex-shrink:0;opacity:0.85;'></div>"
                        f"<div>"
                        f"<div style='font-size:17px;font-weight:700;color:{_T['card_title']};"
                        f"letter-spacing:-0.02em;line-height:1.2;'>{label}</div>"
                        f"<div style='font-size:11px;color:{_T['card_subtitle']};font-weight:500;"
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
                                colors=["#22c55e", "#ef4444"] if total else ["#cbd5e1" if not _dark else "#1e293b"],
                                line=dict(color=_DONUT_LINE_COLOR, width=3),
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
                        ann.font.color  = _ANN_COLOR
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
                                font=dict(size=9, color=_ANN_COLOR, family=_CHART_FONT),
                                showarrow=False, xanchor="center", yanchor="middle",
                            ),
                        ]

                    fig.update_layout(
                        height=230,
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        font=dict(family=_CHART_FONT, color=_AXIS_TICK_COLOR, size=11),
                        margin=dict(l=10, r=10, t=34, b=4),
                        showlegend=False,
                        annotations=base_anns,
                        hoverlabel=dict(
                            bgcolor=_HOVER_BG, bordercolor=_HOVER_BD,
                            font=dict(family=_CHART_FONT, color=_HOVER_TXT, size=12),
                        ),
                    )
                    st.plotly_chart(fig, use_container_width=True,
                                    key=f"breadth_{section_key}_{key}",
                                    config={"displayModeBar": False})

                    # ── Stats strip ──────────────────────────────────────────
                    sc1, sc2 = st.columns(2)
                    for sc, above, below, total, pct, color, mood, dma_label, prev_pct in [
                        (sc1, a50,  b50,  t50,  pct50,  c50,  mood50,  "50 DMA",  _prev50),
                        (sc2, a200, b200, t200, pct200, c200, mood200, "200 DMA", _prev200),
                    ]:
                        with sc:
                            _div_sep_color = "#94a3b8" if _dark else "#64748b"
                            st.markdown(
                                f"<div style='text-align:center;padding:4px 0 8px;'>"
                                f"<div style='font-size:12px;font-weight:700;color:{_ANN_COLOR};"
                                f"text-transform:uppercase;letter-spacing:0.1em;"
                                f"margin-bottom:6px;'>{dma_label}</div>"
                                + _progress_bar(pct, color) +
                                f"<div style='font-size:13px;margin-top:6px;'>"
                                f"<span style='color:#22c55e;font-weight:600;'>{above}↑</span>"
                                f"<span style='color:{_div_sep_color};'> / </span>"
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
                             universe_label: str, ret_label: str, chart_key: str = ""):
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
                key=f"top_{chart_key}",
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
                key=f"bot_{chart_key}",
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
        f"<div style='font-size:10px;font-weight:700;letter-spacing:0.12em;"
        f"text-transform:uppercase;color:{_T['text_label']};margin-bottom:8px;'>"
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
            f"<span style='font-size:14px;font-weight:700;color:{_T['text_secondary']};"
            f"letter-spacing:-0.01em;'>{label}</span>"
            f"<span style='font-size:10px;color:{_T['text_label']};font-weight:500;"
            f"letter-spacing:0.04em;'>{valid_count} stocks · {selected_tf}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        _render_topbottom_chart(df, ret_col, n, label, ret_label, chart_key=f"{section_key}_{key}_{selected_tf}")
        st.divider()


# ---------------------------------------------------------------------------
# Universe view — inline filters + cards + sort + table
# ---------------------------------------------------------------------------
def render_universe_view(index_name: str, snap_date):
    # PERF: load_snapshot is an in-memory filter over the cached bulk snapshot
    with measure(f"load_snapshot__{index_name}"):
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
    # PERF: Summary cards include yfinance index return fetch (cached 1h)
    with measure(f"render_summary_cards__{index_name}"):
        render_summary_cards(df, index_name=index_name, snap_date=snap_date)
    st.divider()
    # PERF: Table render time — depends on row count and column count
    with measure(f"render_sort_and_table__{index_name}"):
        render_sort_and_table(df, key=index_name)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(f"""
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
            <div style="font-size:20px;font-weight:800;color:{_T['sb_name']};letter-spacing:-0.04em;">
                Stock<span style="color:#3b82f6;">Stack</span>
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    st.markdown("<div class='sidebar-section-label'>Last Refresh</div>", unsafe_allow_html=True)
    # PERF: Measure refresh status query (TTL=1800s, near-zero on cache hit)
    with measure("load_refresh_status"):
        status = load_refresh_status()
    if status:
        last_run = status.get("finished_at") or status.get("started_at")
        s   = status.get("status", "")
        ok  = status.get("stocks_success", 0)
        tot = status.get("stocks_total", 0)
        # BUGFIX: ts was only set inside `if last_run:` but used unconditionally below,
        # causing NameError when finished_at and started_at are both None.
        ts = pd.Timestamp(last_run).strftime("%d %b %Y · %H:%M") if last_run else "—"
        dot_color = "#22c55e" if s == "success" else "#f59e0b"
        status_text = f"{ok}/{tot} stocks" if s == "success" else s.title()
        st.markdown(
            f"<div style='font-size:11.5px;color:{_T['text_soft']};display:flex;align-items:center;gap:6px;'>"
            f"<span style='width:6px;height:6px;border-radius:50%;background:{dot_color};"
            f"display:inline-block;flex-shrink:0;'></span>"
            f"<span>{status_text} · {ts}</span></div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(f"<div style='font-size:11.5px;color:{_T['text_no_data']};'>No refresh data yet</div>", unsafe_allow_html=True)

    if st.button("↻ Refresh All Tabs", key="refresh_all_btn", use_container_width=True, help="Clear cached data and reload all tabs"):
        st.cache_data.clear()
        st.rerun()

    st.divider()

    st.markdown("<div class='sidebar-section-label'>Data</div>", unsafe_allow_html=True)
    # PERF: Measure available-dates query (TTL=1800s, near-zero on cache hit)
    with measure("load_available_dates"):
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

    st.divider()
    _theme_icon = "☀️" if _dark else "🌙"
    _theme_label = "Light Mode" if _dark else "Dark Mode"
    if st.button(f"{_theme_icon}  {_theme_label}", key="theme_toggle_btn", use_container_width=True):
        st.session_state["dark_mode"] = not _dark
        st.rerun()

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


def _color_score(val):
    if pd.isna(val) or val == "—":
        return "color: #4a5568"
    try:
        n = float(val)
        if n >= 70:
            return "color: #22c55e; font-weight:700"
        if n >= 40:
            return "color: #f59e0b; font-weight:700"
        return "color: #ef4444; font-weight:700"
    except (ValueError, TypeError):
        return ""


def _render_earnings_table(df: pd.DataFrame, mode: str):
    """Render an earnings results table.

    mode='today'  → symbol, name, score, market_cap_cr, announcement_day_return, cmp
    mode='season' → adds result_date and return_since_announcement
    """
    disp = pd.DataFrame()
    disp["Symbol"] = df["symbol"]
    disp["Company"] = df["name"]
    disp["Score"] = pd.to_numeric(df["score"], errors="coerce").map(
        lambda x: int(x) if pd.notna(x) else "—"
    )
    disp["MCap (Cr)"] = df["market_cap_cr"].map(_fmt_mcap)
    if mode == "season":
        disp["Result Date"] = pd.to_datetime(df["result_date"]).dt.strftime("%d %b %Y")
    # Store as float (×100 = percentage points) so st.dataframe sorts numerically
    disp["Ann. Day Return"] = pd.to_numeric(df["announcement_day_return"], errors="coerce") * 100
    if mode == "season":
        disp["Return Since Ann."] = pd.to_numeric(df["return_since_announcement"], errors="coerce") * 100
        disp["Today's Return"] = pd.to_numeric(df["today_return"], errors="coerce") * 100
    if "presentation_url" in df.columns:
        disp["PPT"] = df["presentation_url"].fillna("")
    if "result_pdf_url" in df.columns:
        disp["PDF"] = df["result_pdf_url"].fillna("")
    disp["Chart"] = df.apply(
        lambda r: f"https://www.tradingview.com/chart/?symbol=NSE%3A{r['symbol']}", axis=1
    )
    disp["Screener"] = df.apply(
        lambda r: f"https://www.screener.in/company/{r['symbol']}/consolidated/", axis=1
    )

    styled = disp.style.map(_color_score, subset=["Score"])
    styled = styled.map(_color_return, subset=["Ann. Day Return"])
    if mode == "season" and "Return Since Ann." in disp.columns:
        styled = styled.map(_color_return, subset=["Return Since Ann."])
        styled = styled.map(_color_return, subset=["Today's Return"])

    col_cfg = {
        "PPT": st.column_config.LinkColumn("PPT", display_text="📊"),
        "PDF": st.column_config.LinkColumn("PDF", display_text="📄"),
        "Chart": st.column_config.LinkColumn("Chart", display_text="📈"),
        "Screener": st.column_config.LinkColumn("Screener", display_text="🔍"),
        "Ann. Day Return": st.column_config.NumberColumn("Ann. Day Return", format="%.2f%%"),
        "Return Since Ann.": st.column_config.NumberColumn("Return Since Ann.", format="%.2f%%"),
        "Today's Return": st.column_config.NumberColumn("Today's Return", format="%.2f%%"),
    }
    st.dataframe(styled, use_container_width=True, hide_index=True, height=600,
                 column_config=col_cfg)

    csv_bytes = df.to_csv(index=False).encode()
    _, dl_col = st.columns([5, 1])
    with dl_col:
        fname = "earnings_today.csv" if mode == "today" else "earnings_season.csv"
        st.download_button("⬇ CSV", csv_bytes, fname, "text/csv",
                           key=f"dl_earn_{mode}", use_container_width=True)


@st.fragment
def _frag_quarterly_results(snap_date):
    sub_today, sub_season = st.tabs(["Today's Results", "Season to Date"])

    with sub_today:
        try:
            rows = engine.connect().execute(
                text("""
                    SELECT
                        ec.symbol,
                        s.name,
                        sd.market_cap_cr,
                        sd.ret_1d  AS announcement_day_return,
                        sd.cmp,
                        ROUND(
                            COALESCE(mt.criteria_count, 0) / 8.0 * 25
                            + COALESCE(mt.rs_rank_12m, 0) / 99.0 * 15
                            + CASE WHEN sd.cmp > COALESCE(td.sma_200, 0) AND td.sma_200 IS NOT NULL THEN 5 ELSE 0 END
                            + CASE WHEN COALESCE(td.sma_200_slope, 0) > 0 THEN 5 ELSE 0 END
                            + CASE
                                WHEN COALESCE(sd.ret_1d, 0) >= 0.10 THEN 20
                                WHEN COALESCE(sd.ret_1d, 0) >= 0.05 THEN 15
                                WHEN COALESCE(sd.ret_1d, 0) >= 0.02 THEN 10
                                WHEN COALESCE(sd.ret_1d, 0) >  0    THEN 5
                                ELSE 0
                              END
                            + CASE
                                WHEN COALESCE(td.volume_ratio, 0) >= 3.0 THEN 10
                                WHEN COALESCE(td.volume_ratio, 0) >= 2.0 THEN 7
                                WHEN COALESCE(td.volume_ratio, 0) >= 1.5 THEN 4
                                ELSE 0
                              END
                            + CASE WHEN COALESCE(fs.roe, 0) >= 0.20 THEN 8
                                   WHEN COALESCE(fs.roe, 0) >= 0.15 THEN 6
                                   WHEN COALESCE(fs.roe, 0) >= 0.10 THEN 4 ELSE 0 END
                            + CASE WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.20 THEN 6
                                   WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.10 THEN 4
                                   WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.05 THEN 2 ELSE 0 END
                            + CASE WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.20 THEN 6
                                   WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.10 THEN 4
                                   WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.05 THEN 2 ELSE 0 END
                        , 0) AS score,
                        ec.presentation_url,
                        ec.result_pdf_url
                    FROM earnings_calendar ec
                    JOIN stocks s ON ec.symbol = s.symbol
                    LEFT JOIN snapshots_daily sd
                        ON ec.symbol = sd.symbol AND sd.date = ec.result_date
                    LEFT JOIN LATERAL (
                        SELECT criteria_count, rs_rank_12m
                        FROM minervini_template_daily
                        WHERE symbol = ec.symbol
                        ORDER BY date DESC LIMIT 1
                    ) mt ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT sma_200, sma_200_slope, volume_ratio
                        FROM technicals_daily
                        WHERE symbol = ec.symbol
                        ORDER BY date DESC LIMIT 1
                    ) td ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT roe, revenue_growth_yoy, pat_growth_yoy
                        FROM financials_snapshots
                        WHERE symbol = ec.symbol
                        ORDER BY fetched_at DESC LIMIT 1
                    ) fs ON TRUE
                    WHERE ec.result_date = :today
                    ORDER BY score DESC NULLS LAST
                """),
                {"today": snap_date},
            ).fetchall()
        except Exception as e:
            st.error(f"Could not load today's earnings data: {e}")
            return
        df_today = pd.DataFrame(rows, columns=["symbol", "name", "market_cap_cr",
                                               "announcement_day_return", "cmp", "score",
                                               "presentation_url", "result_pdf_url"])
        if df_today.empty:
            st.info(
                f"No quarterly result announcements scheduled for "
                f"{pd.Timestamp(snap_date).strftime('%d %b %Y')}."
            )
        else:
            n = len(df_today)
            announced = df_today["announcement_day_return"].notna().sum()
            st.markdown(
                f"<div style='font-size:11.5px;color:{_T['text_soft']};margin:4px 0 8px;'>"
                f"{n} companies scheduled · {announced} with price data · "
                f"sorted by Post-Result Strength Score ↓</div>",
                unsafe_allow_html=True,
            )
            _render_earnings_table(df_today, mode="today")

    with sub_season:
        try:
            rows = engine.connect().execute(
                text("""
                    SELECT
                        ec.symbol,
                        s.name,
                        ec.result_date,
                        COALESCE(sd_ann.market_cap_cr, next_td.market_cap_cr) AS market_cap_cr,
                        COALESCE(sd_ann.ret_1d, next_td.ret_1d) AS announcement_day_return,
                        CASE
                            WHEN sd_ann.cmp > 0 AND latest.cmp > 0
                            THEN ROUND(
                                CAST((latest.cmp - sd_ann.cmp) / sd_ann.cmp AS NUMERIC), 6
                            )
                            WHEN COALESCE(prev_td.cmp, 0) > 0 AND latest.cmp > 0
                            THEN ROUND(
                                CAST((latest.cmp - prev_td.cmp) / prev_td.cmp AS NUMERIC), 6
                            )
                            ELSE NULL
                        END AS return_since_announcement,
                        latest.ret_1d AS today_return,
                        ROUND(
                            COALESCE(mt.criteria_count, 0) / 8.0 * 25
                            + COALESCE(mt.rs_rank_12m, 0) / 99.0 * 15
                            + CASE WHEN COALESCE(sd_ann.cmp, next_td.cmp) > COALESCE(td.sma_200, 0) AND td.sma_200 IS NOT NULL THEN 5 ELSE 0 END
                            + CASE WHEN COALESCE(td.sma_200_slope, 0) > 0 THEN 5 ELSE 0 END
                            + CASE
                                WHEN COALESCE(sd_ann.ret_1d, next_td.ret_1d, 0) >= 0.10 THEN 20
                                WHEN COALESCE(sd_ann.ret_1d, next_td.ret_1d, 0) >= 0.05 THEN 15
                                WHEN COALESCE(sd_ann.ret_1d, next_td.ret_1d, 0) >= 0.02 THEN 10
                                WHEN COALESCE(sd_ann.ret_1d, next_td.ret_1d, 0) >  0    THEN 5
                                ELSE 0
                              END
                            + CASE
                                WHEN COALESCE(td.volume_ratio, 0) >= 3.0 THEN 10
                                WHEN COALESCE(td.volume_ratio, 0) >= 2.0 THEN 7
                                WHEN COALESCE(td.volume_ratio, 0) >= 1.5 THEN 4
                                ELSE 0
                              END
                            + CASE WHEN COALESCE(fs.roe, 0) >= 0.20 THEN 8
                                   WHEN COALESCE(fs.roe, 0) >= 0.15 THEN 6
                                   WHEN COALESCE(fs.roe, 0) >= 0.10 THEN 4 ELSE 0 END
                            + CASE WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.20 THEN 6
                                   WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.10 THEN 4
                                   WHEN COALESCE(fs.revenue_growth_yoy, 0) >= 0.05 THEN 2 ELSE 0 END
                            + CASE WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.20 THEN 6
                                   WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.10 THEN 4
                                   WHEN COALESCE(fs.pat_growth_yoy, 0) >= 0.05 THEN 2 ELSE 0 END
                        , 0) AS score,
                        ec.presentation_url,
                        ec.result_pdf_url
                    FROM earnings_calendar ec
                    JOIN stocks s ON ec.symbol = s.symbol
                    LEFT JOIN snapshots_daily sd_ann
                        ON ec.symbol = sd_ann.symbol AND sd_ann.date = ec.result_date
                    LEFT JOIN LATERAL (
                        SELECT cmp
                        FROM snapshots_daily
                        WHERE symbol = ec.symbol AND date < ec.result_date
                        ORDER BY date DESC LIMIT 1
                    ) prev_td ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT ret_1d, market_cap_cr, cmp
                        FROM snapshots_daily
                        WHERE symbol = ec.symbol AND date > ec.result_date
                        ORDER BY date ASC LIMIT 1
                    ) next_td ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT cmp, ret_1d FROM snapshots_daily
                        WHERE symbol = ec.symbol
                        ORDER BY date DESC LIMIT 1
                    ) latest ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT criteria_count, rs_rank_12m
                        FROM minervini_template_daily
                        WHERE symbol = ec.symbol
                        ORDER BY date DESC LIMIT 1
                    ) mt ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT sma_200, sma_200_slope, volume_ratio
                        FROM technicals_daily
                        WHERE symbol = ec.symbol
                        ORDER BY date DESC LIMIT 1
                    ) td ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT roe, revenue_growth_yoy, pat_growth_yoy
                        FROM financials_snapshots
                        WHERE symbol = ec.symbol
                        ORDER BY fetched_at DESC LIMIT 1
                    ) fs ON TRUE
                    WHERE ec.result_date <= :today
                    ORDER BY score DESC NULLS LAST, ec.result_date DESC
                """),
                {"today": snap_date},
            ).fetchall()
        except Exception as e:
            st.error(f"Could not load season earnings data: {e}")
            return
        df_season = pd.DataFrame(rows, columns=["symbol", "name", "result_date", "market_cap_cr",
                                                 "announcement_day_return",
                                                 "return_since_announcement", "today_return", "score",
                                                 "presentation_url", "result_pdf_url"])
        if df_season.empty:
            st.info("No results announced yet this season.")
        else:
            n = len(df_season)
            dates = df_season["result_date"].nunique()
            st.markdown(
                f"<div style='font-size:11.5px;color:{_T['text_soft']};margin:4px 0 8px;'>"
                f"{n} companies across {dates} result dates · "
                f"sorted by Post-Result Strength Score ↓</div>",
                unsafe_allow_html=True,
            )
            _render_earnings_table(df_season, mode="season")


@st.fragment
def _frag_volspike(snap_date):
    render_volspike_view(snap_date)


@st.fragment
def _frag_global_markets():
    if _GM_AVAILABLE:
        _render_global_markets()
    else:
        st.error(f"Global Markets module failed to load: {_GM_ERROR}")
        st.info("Make sure `frontend/global_markets_tab.py` exists and all dependencies are installed.")


@st.fragment
def _frag_technical_analysis(refresh_ts=None):
    render_technical_analysis_view(refresh_ts)


# ---------------------------------------------------------------------------
# News helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600, show_spinner=False)
def _load_news(query: str, sources_tuple: tuple, symbol: str) -> pd.DataFrame:
    from datetime import timezone as _tz
    try:
        sources_list = list(sources_tuple)
        params: dict = {
            "query":        query.strip(),
            "sources_json": sources_list,
            "symbol":       symbol.strip(),
        }
        with engine.connect() as conn:
            if not query.strip() and not sources_list and not symbol.strip():
                sql = text("""
                    SELECT a.article_id, a.title, a.url,
                           ns.display_name AS source_name,
                           a.published_at, a.summary,
                           STRING_AGG(nas.symbol, ', ' ORDER BY nas.symbol) AS symbols
                    FROM news_articles a
                    JOIN news_sources ns ON ns.source_id = a.source_id
                    LEFT JOIN news_article_symbols nas ON nas.article_id = a.article_id
                    WHERE a.published_at >= NOW() - INTERVAL '7 days'
                    GROUP BY a.article_id, a.title, a.url, ns.display_name, a.published_at, a.summary
                    ORDER BY a.published_at DESC
                    LIMIT 100
                """)
                df = pd.read_sql(sql, conn)
            else:
                conditions = ["a.published_at >= NOW() - INTERVAL '7 days'"]
                if query.strip():
                    conditions.append("a.ts_vector @@ plainto_tsquery('english', :query)")
                if sources_list:
                    conditions.append("ns.display_name = ANY(:sources_arr)")
                if symbol.strip():
                    conditions.append(
                        "EXISTS (SELECT 1 FROM news_article_symbols x "
                        "WHERE x.article_id = a.article_id AND x.symbol = :symbol)"
                    )
                where = " AND ".join(conditions)
                raw_params: dict = {}
                if query.strip():
                    raw_params["query"] = query.strip()
                if sources_list:
                    raw_params["sources_arr"] = sources_list
                if symbol.strip():
                    raw_params["symbol"] = symbol.strip()
                sql = text(f"""
                    SELECT a.article_id, a.title, a.url,
                           ns.display_name AS source_name,
                           a.published_at, a.summary,
                           STRING_AGG(nas.symbol, ', ' ORDER BY nas.symbol) AS symbols
                    FROM news_articles a
                    JOIN news_sources ns ON ns.source_id = a.source_id
                    LEFT JOIN news_article_symbols nas ON nas.article_id = a.article_id
                    WHERE {where}
                    GROUP BY a.article_id, a.title, a.url, ns.display_name, a.published_at, a.summary
                    ORDER BY a.published_at DESC
                    LIMIT 100
                """)
                df = pd.read_sql(sql, conn, params=raw_params)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _load_news_source_stats() -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text("""
                    SELECT ns.display_name AS source_name, COUNT(*) AS article_count
                    FROM news_articles a
                    JOIN news_sources ns ON ns.source_id = a.source_id
                    WHERE a.published_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY ns.display_name
                    ORDER BY article_count DESC
                """),
                conn,
            )
        return df
    except Exception:
        return pd.DataFrame()


def _fmt_news_age(published_at) -> str:
    from datetime import datetime, timezone
    if published_at is None:
        return ""
    try:
        now = datetime.now(timezone.utc)
        if hasattr(published_at, "tzinfo") and published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)
        delta = now - published_at
        secs = delta.total_seconds()
        if secs < 3600:
            return f"{int(secs / 60)}m ago"
        if secs < 86400:
            return f"{int(secs / 3600)}h ago"
        if secs < 172800:
            return "Yesterday"
        return published_at.strftime("%b %-d")
    except Exception:
        return ""


def _render_news_card(row, col):
    with col:
        src     = row.get("source_name", "")
        title   = row.get("title", "")
        url     = row.get("url", "#")
        summary = row.get("summary", "") or ""
        _sym_raw = row.get("symbols")
        symbols  = "" if _sym_raw is None or (isinstance(_sym_raw, float) and _sym_raw != _sym_raw) else str(_sym_raw)
        age     = _fmt_news_age(row.get("published_at"))

        # Symbol pills
        sym_pills = ""
        if symbols:
            for sym in symbols.split(", ")[:5]:
                sym_pills += (
                    f"<span style='background:{_T['bg_tag']};color:{_T['text_accent']};"
                    f"border:1px solid {_T['bd_tag']};border-radius:4px;"
                    f"font-size:9px;font-weight:700;padding:1px 6px;margin-right:4px;"
                    f"letter-spacing:0.5px;'>{sym}</span>"
                )

        card_html = f"""
<div style='background:{"#0f1729" if _dark else "#ffffff"};
     border:1px solid {_T["bd_card"]};border-radius:10px;
     padding:14px 16px 12px;margin-bottom:10px;'>
  <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;'>
    <span style='font-size:9px;font-weight:800;letter-spacing:1.2px;
          color:#f59e0b;text-transform:uppercase;'>{src}</span>
    <span style='font-size:10px;color:{_T["text_muted"]};'>{age}</span>
  </div>
  <div style='margin-bottom:6px;'>
    <a href='{url}' target='_blank' rel='noopener noreferrer'
       style='font-size:13.5px;font-weight:600;color:{_T["card_title"]};
              text-decoration:none;line-height:1.45;'>
      {title}
    </a>
  </div>
  <div style='font-size:11.5px;color:{_T["text_muted"]};line-height:1.55;
       display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;
       overflow:hidden;margin-bottom:8px;'>
    {summary[:300]}
  </div>
  <div style='display:flex;align-items:center;justify-content:space-between;'>
    <div>{sym_pills}</div>
    <a href='{url}' target='_blank' rel='noopener noreferrer'
       style='font-size:10px;color:{_T["text_accent"]};text-decoration:none;
              font-weight:600;'>Read →</a>
  </div>
</div>"""
        st.markdown(card_html, unsafe_allow_html=True)


@st.fragment
def _frag_news():
    _ALL_SOURCES = [
        "Economic Times", "Moneycontrol", "Business Standard",
        "Livemint", "Financial Express", "NDTV Profit", "Business Line",
    ]

    sub_all, sub_company, sub_source = st.tabs(["All News", "By Company", "By Source"])

    # ── All News ─────────────────────────────────────────────────────────────
    with sub_all:
        col_q, col_src, col_sym = st.columns([3, 2, 2])
        with col_q:
            query = st.text_input(
                "Search", placeholder="RBI, Nifty, earnings, SEBI…",
                label_visibility="collapsed", key="news_search",
            )
        with col_src:
            sel_sources = st.multiselect(
                "Sources", _ALL_SOURCES, placeholder="All sources", key="news_sources",
                label_visibility="collapsed",
            )
        with col_sym:
            syms_df = load_all_symbols()
            sym_opts = [""] + sorted(syms_df["symbol"].tolist())
            sel_sym = st.selectbox(
                "Company", sym_opts,
                format_func=lambda x: x if x else "All companies",
                key="news_sym", label_visibility="collapsed",
            )

        df = _load_news(query, tuple(sel_sources), sel_sym)

        if df.empty:
            st.info("No articles found. News refreshes every 30 minutes — check back soon.")
        else:
            st.caption(f"{len(df)} articles (last 7 days)")
            col_a, col_b = st.columns(2)
            for i, (_, row) in enumerate(df.iterrows()):
                _render_news_card(row, col_a if i % 2 == 0 else col_b)

    # ── By Company ───────────────────────────────────────────────────────────
    with sub_company:
        syms_df2 = load_all_symbols()
        sym_opts2 = [""] + sorted(syms_df2["symbol"].tolist())
        sel_sym2  = st.selectbox(
            "Select company / symbol", sym_opts2,
            format_func=lambda x: x if x else "Choose a company…",
            key="news_company_sym",
        )
        if sel_sym2:
            df2 = _load_news("", (), sel_sym2)
            if df2.empty:
                st.info(f"No recent articles mentioning **{sel_sym2}**.")
            else:
                st.caption(f"{len(df2)} articles mentioning {sel_sym2}")
                col_a2, col_b2 = st.columns(2)
                for i, (_, row) in enumerate(df2.iterrows()):
                    _render_news_card(row, col_a2 if i % 2 == 0 else col_b2)
        else:
            st.info("Select a company to see all related news articles.")

    # ── By Source ────────────────────────────────────────────────────────────
    with sub_source:
        stats_df = _load_news_source_stats()
        if not stats_df.empty:
            fig = px.bar(
                stats_df, x="article_count", y="source_name",
                orientation="h", text="article_count",
                color_discrete_sequence=["#f59e0b"],
                labels={"article_count": "Articles (last 24h)", "source_name": ""},
            )
            fig.update_layout(
                height=240, margin=dict(l=0, r=20, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color=_T["text_muted"], size=11),
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False),
            )
            fig.update_traces(textposition="outside", textfont_size=11)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        sel_src_only = st.selectbox(
            "Filter by source", ["All"] + _ALL_SOURCES, key="news_src_only",
        )
        src_filter = [] if sel_src_only == "All" else [sel_src_only]
        df3 = _load_news("", tuple(src_filter), "")
        if df3.empty:
            st.info("No articles for this source in the last 7 days.")
        else:
            st.caption(f"{len(df3)} articles")
            col_a3, col_b3 = st.columns(2)
            for i, (_, row) in enumerate(df3.iterrows()):
                _render_news_card(row, col_a3 if i % 2 == 0 else col_b3)


@st.fragment
def _frag_sector_performance(snap_date, refresh_ts=None):
    sector_df = load_sector_performance(snap_date, refresh_ts)
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
    _sector_all_cols = list(disp.columns)
    _sector_hidden = _render_col_visibility_ui("sector", _sector_all_cols)
    _sector_visible = [c for c in disp.columns if c not in _sector_hidden]
    st.dataframe(disp[_sector_visible], use_container_width=True, hide_index=True)
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
# Minervini Trend Template — query helpers + renderer
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, show_spinner=False)
def _load_minervini_pass() -> pd.DataFrame:
    query = """
        SELECT m.*, s.name, s.sector
        FROM latest_minervini_template m
        JOIN stocks s ON m.symbol = s.symbol
        WHERE m.template_pass = true AND s.is_active = true
        ORDER BY m.template_score DESC, m.rs_rank_12m DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(__import__("sqlalchemy").text(query), conn)


@st.cache_data(ttl=300, show_spinner=False)
def _load_minervini_by_count(min_count: int) -> pd.DataFrame:
    query = """
        SELECT m.*, s.name, s.sector
        FROM latest_minervini_template m
        JOIN stocks s ON m.symbol = s.symbol
        WHERE m.criteria_count >= :min_count AND s.is_active = true
        ORDER BY m.criteria_count DESC, m.template_score DESC, m.rs_rank_12m DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(
            __import__("sqlalchemy").text(query), conn,
            params={"min_count": min_count},
        )


@st.cache_data(ttl=300, show_spinner=False)
def _load_minervini_all() -> pd.DataFrame:
    query = """
        SELECT m.*, s.name, s.sector
        FROM latest_minervini_template m
        JOIN stocks s ON m.symbol = s.symbol
        WHERE s.is_active = true
        ORDER BY m.criteria_count DESC, m.template_score DESC
    """
    with engine.connect() as conn:
        return pd.read_sql(__import__("sqlalchemy").text(query), conn)


def _render_minervini_screener():
    st.markdown("### ⭐ Mark Minervini Trend Template")
    st.caption(
        "Stocks meeting all 8 criteria of Mark Minervini's Trend Template — "
        "the framework he used to win the U.S. Investing Championship. "
        "These stocks are in confirmed Stage 2 uptrends."
    )

    try:
        df_pass    = _load_minervini_pass()
        df_partial = _load_minervini_by_count(6)
        df_partial = df_partial[df_partial["criteria_count"].between(6, 7)] if not df_partial.empty else df_partial
    except Exception as e:
        st.warning(
            f"Minervini data not available yet. Run the backfill first: "
            f"`python backend/backfill_minervini_template.py`\n\n_{e}_"
        )
        return

    # ── Summary cards ─────────────────────────────────────────────────────────
    strong = int(df_pass[df_pass["template_score"] >= 8].shape[0]) if not df_pass.empty else 0
    avg_rs = f"{df_pass['rs_rank_12m'].mean():.0f}" if not df_pass.empty else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Pass All 8 Criteria", len(df_pass))
    c2.metric("Strong Setups (Score 8+)", strong)
    c3.metric("Partial Pass (6-7 criteria)", len(df_partial))
    c4.metric("Avg RS Rank (Passing)", avg_rs)

    st.divider()

    # ── Filters ───────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        view_mode = st.radio(
            "View",
            ["Strict (All 8 Pass)", "Lenient (7+ Pass)", "All Criteria Counts"],
            index=0, horizontal=False, key="minervini_view",
        )
    with fc2:
        min_score = st.slider("Min Template Score", 0.0, 10.0, 5.0, 0.5, key="minervini_min_score")
    with fc3:
        min_rs = st.slider("Min RS Rank", 0, 99, 70, 1, key="minervini_min_rs")

    # ── Load + filter ─────────────────────────────────────────────────────────
    if view_mode == "Strict (All 8 Pass)":
        df = df_pass.copy()
        df = df[df["template_score"] >= min_score]
    elif view_mode == "Lenient (7+ Pass)":
        df = _load_minervini_by_count(7)
    else:
        df = _load_minervini_all()

    if not df.empty and "rs_rank_12m" in df.columns:
        df = df[df["rs_rank_12m"] >= min_rs]
    if not df.empty and "template_score" in df.columns and view_mode != "Strict (All 8 Pass)":
        df = df.sort_values(["template_score", "rs_rank_12m"], ascending=[False, False])

    if df.empty:
        st.info("No stocks match the current filters. Try lowering the thresholds.")
    else:
        st.caption(f"Showing {len(df)} stocks")

        disp_cols = {
            "symbol":            "Symbol",
            "name":              "Company",
            "sector":            "Sector",
            "cmp":               "CMP",
            "template_score":    "Score",
            "criteria_count":    "Criteria",
            "rs_rank_12m":       "RS Rank",
            "pct_from_52w_high": "% from 52W High",
            "pct_above_52w_low": "% above 52W Low",
            "sma_200_slope_22d": "200 DMA Slope (1M)",
            "return_12m":        "12M Return",
        }
        available = [c for c in disp_cols if c in df.columns]
        display_df = df[available].copy()

        def _fmt(col, val):
            if pd.isna(val):
                return "—"
            if col == "template_score":
                return f"{val:.1f}/10"
            if col == "criteria_count":
                return f"{int(val)}/8"
            if col == "rs_rank_12m":
                return f"{val:.0f}"
            if col == "pct_from_52w_high":
                return f"{val:+.1f}%"
            if col in ("pct_above_52w_low", "return_12m"):
                return f"{val:+.1f}%"
            if col == "sma_200_slope_22d":
                return f"{val:+.2f}%"
            if col == "cmp":
                return f"₹{val:,.1f}"
            return val

        for col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda v, c=col: _fmt(c, v))

        display_df.columns = [disp_cols[c] for c in available]
        st.dataframe(display_df, use_container_width=True, hide_index=True, height=580)

        st.download_button(
            "📥 Download as CSV",
            data=display_df.to_csv(index=False),
            file_name=f"minervini_screener_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
            key="minervini_csv_download",
        )

    st.divider()

    with st.expander("📋 The 8 Criteria Explained"):
        st.markdown("""
1. **Price > 150 DMA AND > 200 DMA** — confirms medium and long-term uptrend
2. **150 DMA > 200 DMA** — proper moving average alignment
3. **200 DMA trending up for 1+ month** — long-term trend confirmed
4. **50 DMA > 150 DMA AND > 200 DMA** — short-term trend leading
5. **Price > 50 DMA** — currently in short-term uptrend
6. **Price ≥ 30% above 52-week low** — meaningful strength shown
7. **Price within 25% of 52-week high** — near highs, not lagging
8. **Relative Strength Rank ≥ 70** — outperforming most stocks over 12 months

A stock passing all 8 is in a Stan Weinstein **Stage 2** advancing uptrend.
*Source: Trade Like a Stock Market Wizard — Mark Minervini*
        """)

    st.info(
        "ℹ️ This screen identifies stocks in confirmed uptrends. It does NOT predict future performance. "
        "Always combine technical screening with fundamental analysis and risk management."
    )


# ---------------------------------------------------------------------------
# Technical Analysis view — RSI, MACD, ADX, DMA signal table
# ---------------------------------------------------------------------------

_ALL_TECH_COLS = [
    "Ticker", "Name", "Sector", "CMP", "RSI (14)", "MACD", "ADX (14)",
    "% from ATH", "% from 52W High", "50 DMA", "200 DMA", "Volume",
    "SMA200 Slope", "Vol Ratio", "Chart", "Status", "Relative Strength",
]

_RS_TIMEFRAME_MAP = {
    "1W": "1w", "2W": "2w", "1M": "1m", "3M": "3m", "6M": "6m", "1Y": "1y",
}


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


def _color_52wh(val) -> str:
    """Color % from 52W High: green if < -5%, orange if -20% to -5%, red if < -20%."""
    if val == "—":
        return ""
    try:
        v = float(val.replace("%", ""))
        if v >= -5:
            return "color: #22c55e; font-weight: 600"
        if v >= -20:
            return "color: #f59e0b; font-weight: 600"
        return "color: #ef4444; font-weight: 600"
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


def _render_technical_table(
    df: pd.DataFrame,
    key: str,
    show_v1: bool = False,
    hidden_cols: set = None,
    rs_timeframe_col: str = "1m",
    show_rs_value: bool = False,
    total_before_filter: int = None,
):
    """Build and render the formatted technical indicators table with pagination."""
    if hidden_cols is None:
        hidden_cols = set()
    if df.empty:
        st.info("No stocks match the current filters.")
        return

    # PERF: Paginate BEFORE building display columns and before .style.map().
    # Styling runs a Python function on every cell — 1500 rows × N columns is slow.
    # Paginating first means styling runs on 100 rows instead of 1500: ~15x speedup.
    # Also reduces DOM elements sent to the browser by the same factor.
    _PAGE_SIZE = 100
    total = len(df)
    pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)

    # Reset to page 1 when the result set size changes (e.g. after a filter change)
    _page_key  = f"ta_page_{key}"
    _total_key = f"ta_total_{key}"
    if st.session_state.get(_total_key) != total:
        st.session_state[_total_key] = total
        st.session_state[_page_key]  = 1

    hdr_left, hdr_right = st.columns([4, 1])
    with hdr_left:
        if total_before_filter is not None and total_before_filter != total:
            count_text = f"Showing {total} stocks (filtered from {total_before_filter})"
        else:
            count_text = f"{total} stocks"
        st.caption(count_text + (f" · page {st.session_state.get(_page_key, 1)} of {pages}" if pages > 1 else ""))
    with hdr_right:
        if pages > 1:
            # BUGFIX: use namespaced key ta_page_{key} to avoid collisions between
            # All Stocks and F&O sub-tabs sharing a generic "page_1" key
            page = st.number_input(
                "Page", min_value=1, max_value=pages,
                value=st.session_state.get(_page_key, 1),
                step=1, key=_page_key,
            )
        else:
            page = 1

    start = (page - 1) * _PAGE_SIZE
    end   = min(start + _PAGE_SIZE, total)
    # Slice the raw df BEFORE formatting so column builders only touch this page
    df_page = df.iloc[start:end].copy()

    # ── Build display columns (only for the current page) ────────────────────
    disp = pd.DataFrame()
    disp["Ticker"]    = df_page["symbol"]
    disp["Name"]      = df_page["name"]
    disp["Sector"]    = df_page["sector"].fillna("—") if "sector" in df_page.columns else "—"
    disp["CMP"]       = df_page["cmp"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["RSI (14)"]  = df_page["rsi_14"].map(lambda v: f"{v:.2f}" if pd.notna(v) else "—")
    disp["MACD"]      = df_page.apply(
        lambda r: (
            f"L: {r['macd_line']:.2f} | S: {r['macd_signal']:.2f} | H: {r['macd_histogram']:.2f}"
            if pd.notna(r["macd_line"]) and pd.notna(r["macd_signal"]) and pd.notna(r["macd_histogram"])
            else "—"
        ),
        axis=1,
    )
    disp["ADX (14)"]  = df_page["adx_14"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    disp["% from ATH"] = df_page["pct_from_ath"].map(lambda v: f"{v:.1f}%" if pd.notna(v) else "—") if "pct_from_ath" in df_page.columns else "—"
    disp["% from 52W High"] = df_page["pct_from_52wh"].map(lambda v: f"{v:.1f}%" if pd.notna(v) else "—") if "pct_from_52wh" in df_page.columns else "—"
    disp["50 DMA"]    = df_page["sma_50"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["200 DMA"]   = df_page["sma_200"].map(lambda v: f"₹{v:,.2f}" if pd.notna(v) else "—")
    disp["Volume"]      = df_page["volume"].map(_fmt_volume_ind)
    disp["SMA200 Slope"] = df_page["sma_200_slope"].map(_fmt_slope) if "sma_200_slope" in df_page.columns else "—"
    disp["Vol Ratio"]   = df_page["volume_ratio"].map(_fmt_vol_ratio) if "volume_ratio" in df_page.columns else "—"
    disp["Chart"]       = df_page["tradingview_url"].where(df_page["tradingview_url"].notna(), other=None)
    disp["Status"]      = df_page["technical_status"]
    if show_v1 and "technical_status_v1" in df_page.columns:
        disp["v1 Signal"] = df_page["technical_status_v1"]

    # Relative Strength column (uses whichever timeframe is selected in the sidebar)
    _excess_col = f"rs_excess_{rs_timeframe_col}"
    _bucket_col = f"rs_bucket_{rs_timeframe_col}"
    if _bucket_col in df_page.columns:
        if show_rs_value and _excess_col in df_page.columns:
            disp["Relative Strength"] = df_page.apply(
                lambda r: f"{r[_bucket_col]} ({r[_excess_col]:+.2f}%)"
                if pd.notna(r[_bucket_col]) and pd.notna(r[_excess_col]) else "—",
                axis=1,
            )
        else:
            disp["Relative Strength"] = df_page[_bucket_col].fillna("—")
    else:
        disp["Relative Strength"] = "—"

    # ── Apply column visibility ────────────────────────────────────────────────
    visible_cols = [c for c in disp.columns if c not in hidden_cols]
    disp = disp[visible_cols]

    # ── Styling (runs on page slice only — ~100 rows, not 1500) ──────────────
    styled = disp.style
    if "RSI (14)" in disp.columns:
        styled = styled.map(_color_rsi,        subset=["RSI (14)"])
    if "ADX (14)" in disp.columns:
        styled = styled.map(_style_adx,        subset=["ADX (14)"])
    if "SMA200 Slope" in disp.columns:
        styled = styled.map(_color_slope,      subset=["SMA200 Slope"])
    if "Vol Ratio" in disp.columns:
        styled = styled.map(_style_vol_ratio,  subset=["Vol Ratio"])
    if "% from ATH" in disp.columns:
        styled = styled.map(_color_52wh,       subset=["% from ATH"])
    if "% from 52W High" in disp.columns:
        styled = styled.map(_color_52wh,       subset=["% from 52W High"])

    # PERF: Measure st.dataframe render time — now only renders 100 rows
    with measure(f"render_technical_table__{key}_{len(disp)}rows"):
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=700,
            column_config={
                "Chart": st.column_config.LinkColumn("Chart", display_text="📈 Chart"),
            },
        )

    # ── CSV download — exports ALL filtered rows, not just the current page ───
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


@st.cache_data(ttl=300, show_spinner=False)
def load_setup_candidates(as_of_date: str) -> pd.DataFrame:
    sql = text("""
        SELECT
            sc.symbol,
            s.name,
            s.sector,
            sc.pattern_code,
            sc.pattern_category,
            sc.setup_strength,
            sc.cmp,
            sc.trigger_level,
            sc.pct_from_trigger,
            sc.days_in_base,
            sc.volume_ratio,
            sc.notes,
            sc.date
        FROM setup_candidates_daily sc
        JOIN stocks s ON s.symbol = sc.symbol
        WHERE sc.date = :dt
          AND sc.is_candidate = TRUE
        ORDER BY sc.pattern_category, sc.setup_strength DESC, sc.symbol
    """)
    with _get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params={"dt": as_of_date})
    return df


_PATTERN_LABELS = {
    "BR_52W_RETEST":    "52W High Retest",
    "BR_ATH_RETEST":    "ATH Retest",
    "BR_VOLUME_DRYUP":  "Volume Dry-Up",
    "RV_DIVERGENCE":    "RSI Divergence",
    "RV_DOUBLE_BOTTOM": "Double Bottom",
    "RV_MACD_CROSS":    "MACD Crossover",
}


def render_my_watchlist_tab(snap_date, refresh_ts=None):
    # ── Section 1: Upload & Stage ────────────────────────────────────────────
    st.markdown(
        f"<h3 style='font-size:15px;font-weight:700;color:{_T['card_title']};margin:0 0 4px;'>"
        f"Upload a Stock List</h3>",
        unsafe_allow_html=True,
    )

    uploaded = st.file_uploader("Upload CSV", type=["csv"], key="wl_upload",
                                 label_visibility="collapsed")

    if not uploaded:
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {_T['bg_tag']} 0%, {_T['bd_card']} 100%);
            border: 1px dashed {_T['bd_card']};
            border-radius: 16px;
            padding: 36px 32px;
            text-align: center;
            margin-top: 12px;
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
            <div style="font-size:15px; font-weight:600; color:{_T['card_title']}; margin-bottom:8px;">
                Upload a stock list to analyse or save as a watchlist
            </div>
            <div style="font-size:13px; color:{_T['text_soft']}; max-width:380px; margin:0 auto 20px; line-height:1.6;">
                Upload a CSV with a
                <code style="background:{_T['bg_code']}; padding:2px 7px; border-radius:4px; color:{_T['code_text']}; font-size:12px;">symbol</code>
                column containing NSE tickers — no
                <code style="background:{_T['bg_code']}; padding:2px 7px; border-radius:4px; color:{_T['code_text']}; font-size:12px;">.NS</code>
                suffix needed.
            </div>
            <div style="font-size:11px; color:{_T['text_hint']}; letter-spacing:0.06em; text-transform:uppercase; font-weight:600;">
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
                    staged_df = _load_all_snapshots(snap_date)
                    staged_df = staged_df[staged_df["symbol"].isin(known)]
                    if staged_df.empty:
                        st.info("No snapshot data for these symbols on the selected date.")
                    else:
                        st.success(f"{len(known)} symbols matched.")
                        st.divider()
                        render_summary_cards(staged_df, snap_date=snap_date, show_returns=False)
                        st.divider()
                        render_sort_and_table(staged_df, key="wl_staged")
                        st.divider()

                        # ── Add to Watchlist ─────────────────────────────
                        st.markdown(
                            f"<h4 style='font-size:14px;font-weight:700;color:{_T['card_title']};margin:8px 0 4px;'>"
                            f"Save as Watchlist</h4>",
                            unsafe_allow_html=True,
                        )
                        wl_name_col, wl_btn_col = st.columns([3, 1])
                        with wl_name_col:
                            wl_name = st.text_input(
                                "Watchlist name", placeholder="e.g. My Top Picks",
                                key="wl_new_name", label_visibility="collapsed",
                            )
                        with wl_btn_col:
                            if st.button("➕ Add to Watchlist", use_container_width=True, type="primary"):
                                if not wl_name.strip():
                                    st.error("Please enter a name for the watchlist.")
                                else:
                                    try:
                                        _wl_save(wl_name.strip(), known)
                                        st.success(f'Watchlist "{wl_name.strip()}" saved with {len(known)} stocks!')
                                        st.rerun()
                                    except Exception as ex:
                                        if "unique" in str(ex).lower() or "duplicate" in str(ex).lower():
                                            st.error(f'A watchlist named "{wl_name.strip()}" already exists. Choose a different name.')
                                        else:
                                            st.error(f"Error saving watchlist: {ex}")
        except Exception as e:
            st.error(f"Error reading CSV: {e}")

    # ── Section 2: Saved Watchlists ─────────────────────────────────────────
    st.divider()
    st.markdown(
        f"<h3 style='font-size:15px;font-weight:700;color:{_T['card_title']};margin:0 0 8px;'>"
        f"My Saved Watchlists</h3>",
        unsafe_allow_html=True,
    )

    try:
        wl_df = _wl_load_watchlists()
    except Exception as ex:
        st.error(f"Could not load watchlists: {ex}")
        return

    if wl_df.empty:
        st.info("No saved watchlists yet. Upload a CSV above and click 'Add to Watchlist' to create one.")
        return

    for _, row in wl_df.iterrows():
        wl_id   = int(row["id"])
        wl_name = str(row["name"])
        wl_cnt  = int(row["stock_count"])
        created = pd.Timestamp(row["created_at"]).strftime("%d %b %Y")

        with st.expander(f"**{wl_name}** — {wl_cnt} stocks · added {created}"):
            # Rename / Delete controls
            mgmt_c1, mgmt_c2, mgmt_c3 = st.columns([3, 1, 1])
            with mgmt_c1:
                new_name = st.text_input(
                    "Rename", value=wl_name, key=f"wl_rename_input_{wl_id}",
                    label_visibility="collapsed",
                )
            with mgmt_c2:
                if st.button("💾 Rename", key=f"wl_rename_btn_{wl_id}", use_container_width=True):
                    if new_name.strip() and new_name.strip() != wl_name:
                        try:
                            _wl_rename(wl_id, new_name.strip())
                            st.success("Renamed.")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Error: {ex}")
            with mgmt_c3:
                # Two-click delete: first click arms, second click confirms
                arm_key = f"wl_delete_armed_{wl_id}"
                if st.session_state.get(arm_key):
                    if st.button("⚠️ Confirm Delete", key=f"wl_delete_confirm_{wl_id}",
                                 type="primary", use_container_width=True):
                        _wl_delete(wl_id)
                        st.session_state.pop(arm_key, None)
                        st.success("Deleted.")
                        st.rerun()
                else:
                    if st.button("🗑️ Delete", key=f"wl_delete_btn_{wl_id}",
                                 use_container_width=True):
                        st.session_state[arm_key] = True
                        st.rerun()

            # Stock table for this watchlist
            symbols = _wl_load_symbols(wl_id)
            if symbols:
                detail_df = _load_all_snapshots(snap_date)
                detail_df = detail_df[detail_df["symbol"].isin(symbols)]
                if detail_df.empty:
                    st.info("No snapshot data for this watchlist on the selected date.")
                else:
                    render_summary_cards(detail_df, snap_date=snap_date, show_returns=False)
                    st.divider()
                    render_sort_and_table(detail_df, key=f"wl_detail_{wl_id}")

    # ── Section 3: Watchlist Analysis ───────────────────────────────────────
    st.divider()
    st.markdown(
        f"<h3 style='font-size:15px;font-weight:700;color:{_T['card_title']};margin:0 0 8px;'>"
        f"📊 Watchlist Analysis</h3>",
        unsafe_allow_html=True,
    )

    if wl_df.empty:
        st.info("No watchlists to analyse. Save one above.")
        return

    # Timeframe selector — same style as sector/index Analysis tab
    tf_key = "wl_analysis_tf"
    if tf_key not in st.session_state:
        st.session_state[tf_key] = "1D"

    st.markdown(
        f"<div style='font-size:10px;font-weight:700;letter-spacing:0.12em;"
        f"text-transform:uppercase;color:{_T['text_label']};margin-bottom:8px;'>"
        "Timeframe</div>",
        unsafe_allow_html=True,
    )
    tf_cols = st.columns(len(RETURN_COLS))
    for i, tf_label in enumerate(RETURN_COLS):
        with tf_cols[i]:
            if st.button(
                tf_label,
                key=f"wl_tf_{tf_label}",
                type="primary" if st.session_state[tf_key] == tf_label else "secondary",
                use_container_width=True,
            ):
                st.session_state[tf_key] = tf_label
                st.rerun()

    selected_tf = st.session_state[tf_key]
    ret_col, ret_label = RETURN_COLS[selected_tf]

    st.divider()

    # One block per watchlist — same bar chart pattern as sector/index analysis
    all_snap = _load_all_snapshots(snap_date)
    for _, row in wl_df.iterrows():
        wl_id   = int(row["id"])
        wl_name = str(row["name"])
        symbols = _wl_load_symbols(wl_id)
        if not symbols:
            continue
        sub = all_snap[all_snap["symbol"].isin(symbols)].copy()
        if sub.empty:
            st.caption(f"No snapshot data for {wl_name} on this date.")
            continue

        n = min(5, len(sub))
        valid_count = sub[ret_col].notna().sum()
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:10px;margin-bottom:6px;'>"
            f"<span style='font-size:14px;font-weight:700;color:{_T['text_secondary']};"
            f"letter-spacing:-0.01em;'>{wl_name}</span>"
            f"<span style='font-size:10px;color:{_T['text_label']};font-weight:500;"
            f"letter-spacing:0.04em;'>{valid_count} stocks · {selected_tf}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
        _render_topbottom_chart(sub, ret_col, n, wl_name, ret_label)
        st.divider()


def render_scanner_tab():
    _page_header("Breakout & Reversal Scanner", desc_key=None)

    engine = _get_engine()
    with engine.connect() as conn:
        latest_date = conn.execute(
            text("SELECT MAX(date) FROM setup_candidates_daily WHERE is_candidate = TRUE")
        ).scalar()

    if not latest_date:
        st.info("No scanner data yet. Run `python backend/backfill_setup_candidates.py` first.")
        return

    st.caption(f"Data as of **{latest_date}**")

    df = load_setup_candidates(str(latest_date))
    if df.empty:
        st.info("No candidates found for the latest date.")
        return

    # ── Filters ──────────────────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    with col1:
        categories = ["All"] + sorted(df["pattern_category"].unique().tolist())
        cat_filter = st.selectbox("Category", categories, key="scanner_cat")
    with col2:
        patterns = ["All"] + sorted(df["pattern_code"].unique().tolist())
        pat_filter = st.selectbox("Pattern", patterns, key="scanner_pat")
    with col3:
        sectors = ["All"] + sorted(df["sector"].dropna().unique().tolist())
        sec_filter = st.selectbox("Sector", sectors, key="scanner_sec")

    filtered = df.copy()
    if cat_filter != "All":
        filtered = filtered[filtered["pattern_category"] == cat_filter]
    if pat_filter != "All":
        filtered = filtered[filtered["pattern_code"] == pat_filter]
    if sec_filter != "All":
        filtered = filtered[filtered["sector"] == sec_filter]

    # ── Summary cards ────────────────────────────────────────────────────────
    total = len(filtered)
    breakouts = len(filtered[filtered["pattern_category"] == "breakout"])
    reversals = len(filtered[filtered["pattern_category"] == "reversal"])

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Candidates", total)
    m2.metric("Breakout Setups", breakouts)
    m3.metric("Reversal Setups", reversals)

    st.divider()

    if filtered.empty:
        st.info("No setup candidates match the current filters. Try selecting 'All' for Category, Pattern, or Sector.")
        return

    # ── Table ────────────────────────────────────────────────────────────────
    display = filtered[[
        "symbol", "name", "sector", "pattern_code", "pattern_category",
        "setup_strength", "cmp", "trigger_level", "pct_from_trigger",
        "days_in_base", "volume_ratio", "notes",
    ]].copy()

    display["pattern_code"] = display["pattern_code"].map(
        lambda x: _PATTERN_LABELS.get(x, x)
    )
    display.columns = [
        "Symbol", "Name", "Sector", "Pattern", "Category",
        "Strength", "CMP", "Trigger", "% from Trigger",
        "Days in Base", "Vol Ratio", "Notes",
    ]

    display["CMP"] = display["CMP"].astype(float).round(2)
    display["Trigger"] = display["Trigger"].astype(float).round(2)
    display["% from Trigger"] = display["% from Trigger"].astype(float).round(2)
    display["Vol Ratio"] = display["Vol Ratio"].astype(float).round(2)
    display["Strength"] = display["Strength"].astype(float)

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Strength": st.column_config.ProgressColumn(
                "Strength", min_value=0, max_value=10, format="%.0f"
            ),
            "% from Trigger": st.column_config.NumberColumn(
                "% from Trigger", format="%.2f%%"
            ),
            "Vol Ratio": st.column_config.NumberColumn("Vol Ratio", format="%.2f×"),
        },
    )

    csv = display.to_csv(index=False).encode()
    st.download_button("⬇ CSV", csv, f"scanner_{latest_date}.csv", "text/csv", key="dl_scanner")


def render_technical_analysis_view(refresh_ts=None):
    """Render the Technical Analysis tab: filters, summary cards, sub-tabs."""
    # ── Load data ─────────────────────────────────────────────────────────────
    # PERF: ~1500 rows with 52W high join — cached 5 min, ~800ms on cold load
    if refresh_ts is None:
        _rs = load_refresh_status()
        refresh_ts = str(_rs.get("finished_at")) if _rs else None
    with measure("load_latest_technicals"):
        df_all = load_latest_technicals(refresh_ts)

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

    # ── Filters row 1b: Sector filter ────────────────────────────────────────
    if "sector" in df_all.columns:
        all_sectors = sorted(df_all["sector"].dropna().unique().tolist())
        sel_sectors_ta = st.multiselect(
            "Sector", all_sectors, default=[],
            key="ta_sector", placeholder="All sectors",
        )
    else:
        sel_sectors_ta = []

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

    # ── Filters row 3: Relative Strength ─────────────────────────────────────
    st.markdown("**Relative Strength vs Nifty 50**")
    fc_rs1, fc_rs2, fc_rs3 = st.columns([1, 2, 1])
    with fc_rs1:
        rs_timeframe = st.selectbox(
            "RS Timeframe",
            options=["1W", "2W", "1M", "3M", "6M", "1Y"],
            index=2,
            key="ta_rs_timeframe",
            help="Timeframe for comparing stock return vs Nifty 50",
        )
    with fc_rs2:
        rs_filter = st.selectbox(
            "RS Filter",
            options=[
                "All",
                "🚀 Strong Outperformer",
                "✅ Outperformer",
                "⚖️ In-line",
                "📉 Underperformer",
                "🔻 Strong Underperformer",
            ],
            index=0,
            key="ta_rs_filter",
            help="Filter stocks by their RS category",
        )
    with fc_rs3:
        show_rs_value = st.toggle(
            "Show RS % Value",
            value=False,
            key="ta_show_rs_value",
            help="Display the excess return % alongside the label",
        )

    # ── Filters row 4: Outperformance % ───────────────────────────────────────
    fc_op1, fc_op2, fc_op3 = st.columns([1, 2, 1])
    with fc_op1:
        rs_outperf_min = st.selectbox(
            "Min Outperformance %",
            options=["Any", ">0%", ">2%", ">5%", ">10%", ">15%", ">20%"],
            index=0,
            key="ta_rs_outperf_min",
            help="Show only stocks with excess return above this threshold vs Nifty 50",
        )

    rs_tf_col = _RS_TIMEFRAME_MAP[rs_timeframe]

    _OUTPERF_MIN_MAP = {
        "Any": None, ">0%": 0.0, ">2%": 2.0, ">5%": 5.0,
        ">10%": 10.0, ">15%": 15.0, ">20%": 20.0,
    }

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
        # Sector filter
        if sel_sectors_ta and "sector" in df.columns:
            df = df[df["sector"].isin(sel_sectors_ta)]
        # SMA200 slope filter
        if slope_filter == "Rising only (>+1%)" and "sma_200_slope" in df.columns:
            df = df[df["sma_200_slope"].notna() & (df["sma_200_slope"] > 1.0)]
        elif slope_filter == "Falling only (<-1%)" and "sma_200_slope" in df.columns:
            df = df[df["sma_200_slope"].notna() & (df["sma_200_slope"] < -1.0)]
        # Volume ratio min filter
        if vol_ratio_min > 0.0 and "volume_ratio" in df.columns:
            df = df[df["volume_ratio"].notna() & (df["volume_ratio"] >= vol_ratio_min)]
        # RS bucket filter
        if rs_filter != "All":
            _bucket_col = f"rs_bucket_{rs_tf_col}"
            if _bucket_col in df.columns:
                df = df[df[_bucket_col] == rs_filter]
        # Outperformance % min filter
        _excess_col = f"rs_excess_{rs_tf_col}"
        _op_min = _OUTPERF_MIN_MAP[rs_outperf_min]
        if _op_min is not None and _excess_col in df.columns:
            df = df[df[_excess_col].notna() & (df[_excess_col] > _op_min)]
        # Search
        if search.strip():
            q = search.strip().lower()
            mask = (
                df["symbol"].str.lower().str.contains(q, na=False) |
                df["name"].str.lower().str.contains(q, na=False)
            )
            df = df[mask]
        return df

    # ── Column Visibility ─────────────────────────────────────────────────────
    hidden_cols = _render_col_visibility_ui("technical", _ALL_TECH_COLS)

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    tab_all_stocks, tab_fno_stocks, tab_minervini = st.tabs(["All Stocks", "F&O Stocks", "⭐ Minervini Screener"])

    # ── All Stocks sub-tab ────────────────────────────────────────────────────
    with tab_all_stocks:
        # Summary cards (from the full all-stocks universe, before user filters)
        n_strong_bull = int(df_all["technical_status"].str.contains("Strong Bullish", na=False).sum())
        n_sell        = int(df_all["technical_status"].str.contains("Sell / Avoid", na=False).sum())
        n_oversold    = int((df_all["rsi_14"].notna() & (df_all["rsi_14"] < 30)).sum())
        n_strong_trn  = int((df_all["adx_14"].notna() & (df_all["adx_14"] > 25)).sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🟢 Strong Bullish",        n_strong_bull)
        c2.metric("🔻 Sell / Avoid",           n_sell)
        c3.metric("🔥 Oversold (RSI<30)",      n_oversold)
        c4.metric("💪 Strong Trends (ADX>25)", n_strong_trn)

        st.divider()
        _df_all_filtered = _apply_filters(df_all)
        _render_technical_table(
            _df_all_filtered, key="all", show_v1=show_v1, hidden_cols=hidden_cols,
            rs_timeframe_col=rs_tf_col, show_rs_value=show_rs_value,
            total_before_filter=len(df_all),
        )

    # ── F&O Stocks sub-tab ────────────────────────────────────────────────────
    with tab_fno_stocks:
        # Summary cards (from the F&O universe, before user filters)
        n_strong_bull_fno = int(df_fno["technical_status"].str.contains("Strong Bullish", na=False).sum())
        n_sell_fno        = int(df_fno["technical_status"].str.contains("Sell / Avoid", na=False).sum())
        n_oversold_fno    = int((df_fno["rsi_14"].notna() & (df_fno["rsi_14"] < 30)).sum())
        n_strong_trn_fno  = int((df_fno["adx_14"].notna() & (df_fno["adx_14"] > 25)).sum())

        c1f, c2f, c3f, c4f = st.columns(4)
        c1f.metric("🟢 Strong Bullish",        n_strong_bull_fno)
        c2f.metric("🔻 Sell / Avoid",           n_sell_fno)
        c3f.metric("🔥 Oversold (RSI<30)",      n_oversold_fno)
        c4f.metric("💪 Strong Trends (ADX>25)", n_strong_trn_fno)

        st.divider()
        if df_fno.empty:
            st.info("No F&O stocks found. Ensure `index_membership` is seeded with `index_name = 'FNO'`.")
        else:
            _df_fno_filtered = _apply_filters(df_fno)
            _render_technical_table(
                _df_fno_filtered, key="fno", show_v1=show_v1, hidden_cols=hidden_cols,
                rs_timeframe_col=rs_tf_col, show_rs_value=show_rs_value,
                total_before_filter=len(df_fno),
            )

    # ── Minervini Screener sub-tab ────────────────────────────────────────────
    with tab_minervini:
        _render_minervini_screener()

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
            f"<div style='font-size:11px;color:{_T['text_label']};padding-top:28px;'>"
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
        f"<div style='font-size:11.5px;color:{_T['text_soft']};margin:4px 0 8px;'>"
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

    # ── Column visibility ────────────────────────────────────────────────────
    _vs_hidden = _render_col_visibility_ui("volspike", list(disp.columns))
    _vs_visible = [c for c in disp.columns if c not in _vs_hidden]
    disp = disp[_vs_visible]

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
        if _now_ist < _trigger and _COMPONENTS_HTML_SAFE:
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
# Refresh timestamp — used as cache-bust key for technical/sector tabs.
# Derived from the last completed daily refresh so that when a new refresh
# runs, caches for those tabs are invalidated within the next TTL window.
# ---------------------------------------------------------------------------
_rs_now = load_refresh_status()
_refresh_ts_key = str(_rs_now.get("finished_at")) if _rs_now else None

# ---------------------------------------------------------------------------
# Main — 5 top-level tabs
# ---------------------------------------------------------------------------
_tc_space, _tc_btn = st.columns([20, 1])
with _tc_btn:
    if st.button("☀️" if _dark else "🌙", key="theme_toggle_main", help="Switch to light mode" if _dark else "Switch to dark mode"):
        st.session_state["dark_mode"] = not _dark
        st.rerun()

tab_gm, tab_idx, tab_sec, tab_analysis, tab_themes, tab_earnings, tab_volspike, tab_technical, tab_scanner, tab_upload, tab_news = st.tabs([
    "Global Markets",
    "Indexes",
    "Sectors",
    "Sector Performance",
    "Themes",
    "📅 Quaterly Results",
    "Vol Spikes",
    "🔬 Technical Analysis",
    "📡 Scanner",
    "My Watchlist",
    "📰 News",
])

_TAB_DESCRIPTIONS = {
    "global_markets": {
        "what": [
            "**Session Timeline** — live view of which markets are open across 7 global cities, with a real-time cursor",
            "**Overnight Futures** — S&P 500, Nasdaq, and Dow futures with USD/INR, displayed outside Indian market hours",
            "**Overview Bar** — global advance/decline tally, best/worst index, and a cross-asset snapshot (DXY, Brent, Gold, US 10Y, BTC)",
            "**Regional Index Cards** — price, 1-day change, and intraday sparkline for every major index across 8 regions: India, US, Europe, China/HK, Japan, Korea, Asia-Pacific, and EM & Americas",
            "**Commodities** — Gold, Silver, WTI, Brent, Copper, Natural Gas, Platinum, Palladium, and agricultural futures",
            "**Global Bonds** — US Treasury yields (10Y / 20Y / 30Y) and India government bond yields",
            "**Crypto** — Bitcoin, Ethereum, Solana, XRP, and 8 other major tokens, updated 24/7",
            "**Interactive Chart** — price history for any tracked symbol across horizons from 1D to 5Y",
            "**World Heatmap** — choropleth of 1-day index returns across 19 countries",
        ],
        "how": "A complete pre-market briefing on one screen. Overnight futures signal the opening mood before NSE opens. Cross-asset data — yields, oil, and the dollar index — explains why Nifty may gap up or down. Commodity prices drive sector rotation calls: rising oil pressures aviation and logistics; rising yields compress banking valuations. The world heatmap instantly identifies which regions are leading or lagging global markets so you can position accordingly.",
    },
    "indexes": {
        "what": [
            "**Universe Tabs** — Nifty 50, Nifty 500, Nifty Bank, and F&O, each as a separate sub-tab",
            "**Summary Cards** — index-level 1D / 1M / 1Y return, advance/decline count, and the number of stocks above the 200 DMA",
            "**Stock Table** — full sortable list with CMP, returns across 1D to 365D, Market Cap, P/E, 50/200 DMA status, 52W High %, and Volume Spike",
            "**Screener & Chart links** — direct links to Screener.in and TradingView for every stock",
            "**Analysis sub-tab** — horizontal bar charts of top and bottom performers within each index for any timeframe",
            "**Breadth sub-tab** — donut charts showing what percentage of stocks trade above their 50 DMA and 200 DMA, with day-over-day delta",
        ],
        "how": "The primary screener for index-aware investors. Sort Nifty 50 or Nifty 500 by 30-day return to identify momentum leaders or oversold laggards. The breadth donuts reveal whether a rally is broad-based — which is healthy — or narrow, which often signals fragility. The Analysis sub-tab makes the top and bottom performers immediately visual so you spend less time scanning and more time deciding.",
    },
    "sectors": {
        "what": [
            "**12 Sector Sub-tabs** — Banks, NBFCs, Pharma, Defence, Auto, Chemicals, Consumer Durables, FMCG, Healthcare, IT, Media, and Metal",
            "**Per-Sector View** — same layout as Indexes: summary cards plus a fully sortable stock table with all return and valuation columns",
            "**Analysis sub-tab** — top and bottom performer bar charts per sector across any timeframe",
            "**Breadth sub-tab** — 50/200 DMA participation donuts per sector with session-over-session changes",
        ],
        "how": "Sector rotation is one of the most reliable tools in an active investor's toolkit. This tab lets you compare every stock within a sector on any timeframe — useful for spotting which names are driving the sector move and which are quietly lagging. The breadth view shows whether an entire sector is in a structural uptrend (most stocks above 200 DMA) or entering distribution. Together, Analysis and Breadth give you both the signal and the context.",
    },
    "sector_performance": {
        "what": [
            "**Sector Summary Table** — all sectors side-by-side with stock count, advance/decline/flat count, and median returns for 1D, 1W, 30D, 60D, 180D, and 365D",
            "**Horizontal Bar Chart** — median 30-day return by sector, colour-coded from red (negative) to green (positive)",
            "**Column Visibility Toggle** — show or hide any column to tailor the view",
        ],
        "how": "The fastest way to read sector rotation at a glance. Which sectors have led over the past month? Which have been in a multi-month downtrend? Use this view to tilt your portfolio toward structurally strong sectors and reduce exposure to weakening ones. The advance/decline breakdown within each sector tells you whether a move is broad — most stocks participating — or concentrated in just a few names, which changes how much conviction you should place in the signal.",
    },
    "themes": {
        "what": [
            "**Searchable Theme List** — investment themes such as EV & Battery, Data Centres, and Defence PSUs, sorted by 1W / 1M / 1Y average return",
            "**Stock Count & Return** — each theme shows the number of constituent stocks and the current average return for the selected period",
            "**Theme Stock Table** — full breakdown of every stock in the selected theme: CMP, 1D to 1Y returns, Market Cap, P/E, Screener and Chart links",
            "**Period Toggle** — switch the sorting and display between 1W, 1M, and 1Y performance",
        ],
        "how": "Themes cut across traditional sector boundaries — a 'Data Centre' theme spans real estate, power infrastructure, and IT hardware stocks. This view is built for thematic and narrative-driven investors who want to track a macro idea rather than an index. Sort themes by 1-month return to find which narratives the market is currently rewarding. Drill into a theme to identify the strongest and weakest constituents, which helps you concentrate exposure in the right names rather than holding the entire basket.",
    },
    "quarterly_results": {
        "what": [
            "**Today's Results** — companies announcing quarterly results today, sorted by their 1-day return on the announcement date",
            "**Season to Date** — all companies that have announced results so far this season, showing announcement-day return and return-since-announcement",
        ],
        "how": "Track how stocks react the moment quarterly results hit. The announcement-day return tells you whether the market liked the numbers — a big positive move signals a beat; a sharp fall signals disappointment. The return-since-announcement shows the subsequent drift: stocks that keep rising after results often reflect genuine fundamental improvement, while those that fade may have been a one-day knee-jerk. Sort by either column to quickly identify the strongest and weakest earnings reactions of the season.",
    },
    "vol_spikes": {
        "what": [
            "**Volume Spike Screener** — all stocks where today's volume significantly exceeds their 30-day average, sorted by spike magnitude",
            "**Spike Threshold Filter** — narrow results to 1.5×, 2×, 3×, or 5× above average volume",
            "**Sector Filter** — focus on volume anomalies within a specific sector",
            "**Columns** — Symbol, Name, Sector, CMP, Vol Spike ratio, 1D / 1W / 30D / 1Y returns, Market Cap, P/E, and 52W High %",
        ],
        "how": "Volume is the market's most honest signal. A 3× or 5× spike almost always means something is happening — a breakout, institutional accumulation or distribution, a news catalyst, or a stop-loss cascade. Use this screener as a daily watchlist for stocks worth investigating further. A large spike combined with a positive price move suggests a potential breakout with conviction. A spike on a sharp decline may indicate panic selling or a major exit by a large holder, which can present an opportunity or a warning depending on the context.",
    },
    "technical": {
        "what": [
            "**Full Technical Table** — RSI (14), MACD line/signal/histogram, ADX (14), % from 52W High, 50/200 DMA status, Volume, SMA200 Slope, Volume Ratio, and a composite signal status for every active stock",
            "**F&O Sub-tab** — the same view filtered to futures and options eligible stocks only",
            "**Relative Strength** — each stock's excess return vs Nifty 50 across six timeframes (1W, 2W, 1M, 3M, 6M, 1Y), bucketed as Strong / Neutral / Weak",
            "**Filters** — screen by technical status, RSI range, ADX threshold, signal score, volume ratio, and relative strength bucket",
            "**Column Visibility Toggle** — show or hide any indicator column",
        ],
        "how": "The most granular screener on the platform. RSI highlights overbought (>70) or oversold (<30) conditions. MACD shows momentum direction and the strength of the current move. ADX above 25 confirms a trend is strong enough to trade. The SMA200 slope tells you whether the long-term trend is accelerating or decelerating — a rising slope in an uptrend is a sign of strength. Relative Strength vs Nifty 50 is particularly powerful: stocks persistently outperforming the benchmark are the ones institutions are accumulating, and those are usually the best candidates to hold in a bullish environment or sell short when the market turns.",
    },
    "custom_upload": {
        "what": [
            "**CSV Upload** — import a stock list containing a `symbol` column with NSE tickers (no `.NS` suffix required)",
            "**Automatic Matching** — uploaded symbols are validated against the full master stock list; unrecognised tickers are flagged",
            "**Advance / Decline Summary** — instant advance/decline count for the uploaded list, without return noise",
            "**Full Stock Table** — complete sortable table with CMP, market cap, P/E, DMA status, RSI, 52W High%, volume spike and more",
            "**Save as Watchlist** — name and persist the uploaded list to your saved watchlists with one click",
            "**Saved Watchlists** — view, rename, delete, and drill into any previously saved watchlist",
            "**Watchlist Analysis** — side-by-side comparison of all saved watchlists: advance/decline counts plus best and worst performer across any timeframe",
        ],
        "how": "Build and manage persistent named watchlists from any CSV of stocks. Upload a portfolio, a broker recommendation, or a curated screener result — validate the symbols, review the data, then save it permanently. The Analysis section lets you compare multiple watchlists at a glance: which list is seeing more breadth today, which has the strongest performer, and which is under pressure. Switch timeframes (1D to 365D) to see how each watchlist has performed across different horizons.",
    },
    "news": {
        "what": [
            "**7 Live Feeds** — Economic Times, Moneycontrol, Business Standard, Livemint, Financial Express, NDTV Profit, and Business Line — refreshed every 30 minutes",
            "**Stock-Linked Articles** — articles are auto-tagged with the NSE symbols mentioned in their title or summary",
            "**Full-Text Search** — search across all article titles and summaries (company name, topic, keyword)",
            "**Source Filter** — focus on one or more specific publications",
            "**Company Filter** — see every article mentioning a specific NSE-listed stock",
            "**Article Cards** — source, publication time, headline, 2-line summary, and a direct link to the original article",
            "**Scrolling Headline Ticker** — the amber bar at the top of every page shows the latest headlines at all times",
        ],
        "how": "News is the fastest-moving input for short-term traders. Use the company filter to pull all recent coverage of a stock before taking a position. The headline ticker keeps you aware of breaking stories even while you are on other tabs. Articles are matched to ~1500 NSE-listed companies using keyword and symbol detection, so filtering by RELIANCE or HDFCBANK instantly surfaces only relevant articles.",
    },
}


def _render_tab_description(desc_key: str):
    desc = _TAB_DESCRIPTIONS.get(desc_key)
    if not desc:
        return
    with st.popover("ⓘ  About this tab", use_container_width=False):
        st.markdown(
            f"<div style='font-size:11px;font-weight:700;letter-spacing:0.10em;"
            f"text-transform:uppercase;color:{_T['text_section']};margin-bottom:6px;'>"
            "What's included</div>",
            unsafe_allow_html=True,
        )
        for point in desc["what"]:
            st.markdown(point)
        st.markdown(
            f"<div style='font-size:11px;font-weight:700;letter-spacing:0.10em;"
            f"text-transform:uppercase;color:{_T['text_section']};margin-top:10px;margin-bottom:4px;'>"
            "How it helps</div>",
            unsafe_allow_html=True,
        )
        st.markdown(desc["how"])


def _page_header(title: str, date=None, desc_key: str | None = None):
    date_str = f" <span style='color:{_T['text_date_badge']};font-size:13px;font-weight:500;margin-left:10px;'>{pd.Timestamp(date).strftime('%d %b %Y')}</span>" if date else ""
    col_title, col_btn = st.columns([7, 1])
    with col_title:
        st.markdown(
            f"<div style='display:flex;align-items:baseline;gap:0;margin-bottom:0.75rem;padding-top:4px;'>"
            f"<span style='font-size:18px;font-weight:700;color:{_T['text_secondary']};letter-spacing:-0.02em;'>{title}</span>"
            f"{date_str}</div>",
            unsafe_allow_html=True,
        )
    with col_btn:
        if desc_key:
            _render_tab_description(desc_key)
        else:
            st.markdown("<div style='margin-bottom:1rem;'></div>", unsafe_allow_html=True)


# ── Tab 1: Indexes ──────────────────────────────────────────────────────────
with tab_idx:
    _page_header("Broad Market Indexes", selected_date, desc_key="indexes")
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
    _page_header("Sector Views", selected_date, desc_key="sectors")
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
    _page_header("Sector Performance", selected_date, desc_key="sector_performance")
    _frag_sector_performance(selected_date, _refresh_ts_key)

# ── Tab 4: Themes ────────────────────────────────────────────────────────────
with tab_themes:
    _page_header("Themes", desc_key="themes")
    _frag_themes()

# ── Tab 5: Quarterly Results ─────────────────────────────────────────────────
with tab_earnings:
    _page_header("Quaterly Results", selected_date, desc_key="quarterly_results")
    _frag_quarterly_results(selected_date)

# ── Tab 6: Vol Spikes ────────────────────────────────────────────────────────
with tab_volspike:
    _page_header("Volume Spike Screener", selected_date, desc_key="vol_spikes")
    _frag_volspike(selected_date)

# ── Tab 6: My Watchlist ───────────────────────────────────────────────────────
with tab_upload:
    _page_header("My Watchlist", desc_key="custom_upload")
    render_my_watchlist_tab(selected_date, _refresh_ts_key)

# ── Tab 7: Technical Analysis ────────────────────────────────────────────────
with tab_technical:
    _page_header("Technical Analysis", desc_key="technical")
    _frag_technical_analysis(_refresh_ts_key)

# ── Tab 8: Breakout & Reversal Scanner ───────────────────────────────────────
with tab_scanner:
    render_scanner_tab()

# ── Tab 9: Global Markets ─────────────────────────────────────────────────────
with tab_gm:
    _page_header("Global Markets", desc_key="global_markets")
    _frag_global_markets()

# ── Tab 10: News ──────────────────────────────────────────────────────────────
with tab_news:
    _page_header("Market News", desc_key="news")
    _frag_news()

# PERF: Show timing panel at the bottom — only visible when DEBUG=true
show_perf_panel()
