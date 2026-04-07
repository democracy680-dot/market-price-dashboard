"""
app.py — Indian Equity Dashboard (Streamlit)

Reads exclusively from Supabase. No yfinance calls here.
All heavy computation happens in the daily refresh job.
"""

import os
import hashlib
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Indian Equity Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Auth — password gate
# ---------------------------------------------------------------------------
def _check_password() -> bool:
    """Returns True once the correct password has been entered."""
    # Read from st.secrets (Streamlit Cloud) or .env (local)
    try:
        correct = st.secrets["DASHBOARD_PASSWORD"]
    except Exception:
        correct = os.environ.get("DASHBOARD_PASSWORD", "")

    if not correct:
        st.error("DASHBOARD_PASSWORD not configured.")
        st.stop()

    def _submit():
        entered = st.session_state.get("pw_input", "")
        if hashlib.sha256(entered.encode()).hexdigest() == hashlib.sha256(correct.encode()).hexdigest():
            st.session_state["authenticated"] = True
        else:
            st.session_state["auth_error"] = True

    if st.session_state.get("authenticated"):
        return True

    st.title("Indian Equity Dashboard")
    st.text_input("Password", type="password", key="pw_input", on_change=_submit)
    st.button("Login", on_click=_submit)
    if st.session_state.get("auth_error"):
        st.error("Incorrect password.")
    st.stop()


_check_password()

# ---------------------------------------------------------------------------
# Database connection
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
# Data loaders (cached)
# ---------------------------------------------------------------------------
INDEXES = [
    "NIFTY_50",
    "NIFTY_500",
    "NIFTY_BANK",
    "FNO",
    "BANKS",
    "NBFCS",
    "PHARMA",
    "DEFENCE",
]

DISPLAY_NAMES = {
    "NIFTY_50":   "Nifty 50",
    "NIFTY_500":  "Nifty 500",
    "NIFTY_BANK": "Nifty Bank",
    "FNO":        "F&O",
    "BANKS":      "Banks",
    "NBFCS":      "NBFCs",
    "PHARMA":     "Pharma",
    "DEFENCE":    "Defence",
}


@st.cache_data(ttl=300)
def load_available_dates() -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT DISTINCT date FROM snapshots_daily ORDER BY date DESC LIMIT 90")
        ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_snapshot(snap_date, index_name: str | None = None) -> pd.DataFrame:
    """Load snapshots_daily joined with stocks and index_membership."""
    params: dict = {"date": str(snap_date)}

    index_join = ""
    index_filter = ""
    if index_name:
        index_join = """
            JOIN index_membership im
              ON s.symbol = im.symbol
             AND im.index_name = :index_name
        """
        index_filter = ""
        params["index_name"] = index_name

    sql = text(f"""
        SELECT
            sd.symbol,
            s.name,
            sd.cmp,
            sd.ret_1d,
            sd.ret_1w,
            sd.ret_30d,
            sd.ret_60d,
            sd.ret_180d,
            sd.ret_365d,
            sd.dma_50,
            sd.dma_200,
            sd.status_50dma,
            sd.status_200dma,
            sd.pe_ratio,
            sd.market_cap_cr,
            s.sector,
            s.screener_url,
            s.tradingview_url
        FROM snapshots_daily sd
        JOIN stocks s ON sd.symbol = s.symbol
        {index_join}
        WHERE sd.date = :date
          AND s.is_active = TRUE
        ORDER BY sd.symbol
    """)

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_sector_performance(snap_date) -> pd.DataFrame:
    sql = text("""
        SELECT * FROM sector_performance_daily
        WHERE date = :date
        ORDER BY month_chg_pct DESC
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"date": str(snap_date)})
    return df


@st.cache_data(ttl=300)
def load_all_symbols() -> pd.DataFrame:
    """Used for CSV upload validation."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT symbol, name, sector FROM stocks WHERE is_active = TRUE"),
            conn,
        )
    return df


@st.cache_data(ttl=300)
def load_refresh_status() -> dict | None:
    sql = text("""
        SELECT started_at, finished_at, stocks_total, stocks_success, stocks_failed, status
        FROM refresh_log
        ORDER BY started_at DESC
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    if row:
        return dict(row._mapping)
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PCT_COLS = ["ret_1d", "ret_1w", "ret_30d", "ret_60d", "ret_180d", "ret_365d"]

DISPLAY_COLS = {
    "symbol":        "Symbol",
    "name":          "Name",
    "cmp":           "CMP",
    "ret_1d":        "1D%",
    "ret_1w":        "1W%",
    "ret_30d":       "30D%",
    "ret_60d":       "60D%",
    "ret_180d":      "180D%",
    "ret_365d":      "365D%",
    "market_cap_cr": "MCap (Cr)",
    "pe_ratio":      "P/E",
    "sector":        "Sector",
    "status_50dma":  "50DMA Status",
    "status_200dma": "200DMA Status",
}

SORT_OPTIONS = {
    "Symbol (A-Z)":     ("symbol",        False),
    "CMP (High→Low)":   ("cmp",           True),
    "1D% (Best)":       ("ret_1d",        True),
    "1W% (Best)":       ("ret_1w",        True),
    "30D% (Best)":      ("ret_30d",       True),
    "60D% (Best)":      ("ret_60d",       True),
    "180D% (Best)":     ("ret_180d",      True),
    "365D% (Best)":     ("ret_365d",      True),
    "MCap (High→Low)":  ("market_cap_cr", True),
}


def _color_return(val):
    if pd.isna(val) or val == "—":
        return "color: grey"
    try:
        numeric = float(str(val).replace("%", "").replace("+", ""))
        return "color: #16a34a" if numeric >= 0 else "color: #dc2626"
    except (ValueError, TypeError):
        return ""


def _fmt_pct(val):
    if pd.isna(val):
        return "—"
    return f"{val * 100:+.2f}%"


def _fmt_mcap(val):
    if pd.isna(val):
        return "—"
    if val >= 1_00_000:
        return f"₹{val/1_00_000:.1f}L Cr"
    if val >= 1_000:
        return f"₹{val/1_000:.1f}K Cr"
    return f"₹{val:.0f} Cr"


def prepare_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format the dataframe for display (doesn't mutate original)."""
    d = df[list(DISPLAY_COLS.keys())].copy()
    d = d.rename(columns=DISPLAY_COLS)
    for raw, pretty in DISPLAY_COLS.items():
        if raw in PCT_COLS:
            d[pretty] = df[raw].map(_fmt_pct)
    d["MCap (Cr)"] = df["market_cap_cr"].map(_fmt_mcap)
    d["P/E"] = df["pe_ratio"].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    return d


def render_table(df: pd.DataFrame, page_size: int = 100):
    """Paginated, styled table with symbol expander links."""
    total = len(df)
    pages = max(1, (total + page_size - 1) // page_size)

    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"{total} stocks")
    with col2:
        page = st.number_input("Page", min_value=1, max_value=pages, value=1, step=1, label_visibility="collapsed")

    start = (page - 1) * page_size
    end = min(start + page_size, total)
    chunk = df.iloc[start:end].reset_index(drop=True)

    display = prepare_display(chunk)

    # Style return columns
    styled = display.style
    for raw, pretty in DISPLAY_COLS.items():
        if raw in PCT_COLS:
            styled = styled.map(_color_return, subset=[pretty])

    st.dataframe(styled, use_container_width=True, hide_index=True)

    # CSV download
    csv_bytes = df[list(DISPLAY_COLS.keys())].to_csv(index=False).encode()
    st.download_button("Download CSV", csv_bytes, "stocks.csv", "text/csv")

    # Symbol detail expanders
    st.markdown("---")
    st.markdown("**Stock links** (expand to open Screener / TradingView)")
    for _, row in chunk.iterrows():
        with st.expander(f"{row['symbol']} — {row['name']}"):
            c1, c2 = st.columns(2)
            with c1:
                if pd.notna(row.get("screener_url")):
                    st.markdown(f"[Screener.in]({row['screener_url']})")
            with c2:
                if pd.notna(row.get("tradingview_url")):
                    st.markdown(f"[TradingView]({row['tradingview_url']})")


def render_summary_cards(df: pd.DataFrame):
    c1, c2, c3, c4 = st.columns(4)

    med_1w = df["ret_1w"].median()
    med_30d = df["ret_30d"].median()

    adv = (df["ret_1d"] > 0).sum()
    dec = (df["ret_1d"] < 0).sum()
    above_200 = (df["status_200dma"] == "Above 200DMA").sum()

    with c1:
        st.metric("Median 1W Return", _fmt_pct(med_1w))
    with c2:
        st.metric("Median 30D Return", _fmt_pct(med_30d))
    with c3:
        ratio = f"{adv}A / {dec}D"
        st.metric("Advance / Decline", ratio)
    with c4:
        st.metric("Above 200 DMA", f"{above_200} / {len(df)}")


def render_sector_chart(sector_df: pd.DataFrame, universe_filter: list | None = None):
    if sector_df.empty:
        st.info("No sector data for this date.")
        return

    df = sector_df.copy()
    if universe_filter is not None:
        df = df[df["sector"].isin(universe_filter)]

    df = df.sort_values("month_chg_pct", ascending=False)
    df["month_chg_pct_pct"] = df["month_chg_pct"] * 100

    fig = px.bar(
        df,
        x="month_chg_pct_pct",
        y="sector",
        orientation="h",
        color="month_chg_pct_pct",
        color_continuous_scale=["#dc2626", "#f9fafb", "#16a34a"],
        color_continuous_midpoint=0,
        labels={"month_chg_pct_pct": "Median 30D Return (%)", "sector": "Sector"},
        title="Sector Performance — Median 30D Return",
    )
    fig.update_layout(
        coloraxis_showscale=False,
        yaxis={"categoryorder": "total ascending"},
        height=max(300, len(df) * 30),
    )
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("📊 Equity Dashboard")

    # Refresh status banner
    status = load_refresh_status()
    if status:
        last_run = status.get("finished_at") or status.get("started_at")
        if last_run:
            st.caption(f"Last refresh: {pd.Timestamp(last_run).strftime('%d %b %Y %H:%M IST')}")
        if status.get("status") != "success":
            st.warning(f"Last refresh status: {status.get('status')}")

    st.divider()

    # Universe selector
    universe_choice = st.radio(
        "Universe",
        options=INDEXES + ["Custom Upload"],
        format_func=lambda x: DISPLAY_NAMES.get(x, x),
    )

    # Date picker
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

    # Filters (shown for non-custom tabs)
    if universe_choice != "Custom Upload":
        raw_df = load_snapshot(selected_date, index_name=universe_choice)

        sectors = sorted(raw_df["sector"].dropna().unique().tolist())
        selected_sectors = st.multiselect("Sectors", sectors, default=[])

        mcap_min_val = float(raw_df["market_cap_cr"].min(skipna=True) or 0)
        mcap_max_val = float(raw_df["market_cap_cr"].max(skipna=True) or 1_00_000)
        if mcap_min_val < mcap_max_val:
            mcap_range = st.slider(
                "MCap range (Cr)",
                min_value=mcap_min_val,
                max_value=mcap_max_val,
                value=(mcap_min_val, mcap_max_val),
                format="₹%.0f",
            )
        else:
            mcap_range = (mcap_min_val, mcap_max_val)

        sort_choice = st.selectbox("Sort by", list(SORT_OPTIONS.keys()))
    else:
        raw_df = pd.DataFrame()
        selected_sectors = []
        mcap_range = (0, 1e12)
        sort_choice = list(SORT_OPTIONS.keys())[0]


# ---------------------------------------------------------------------------
# Main area — tabs
# ---------------------------------------------------------------------------
tab_main, tab_sector, tab_upload, tab_timetravel = st.tabs(
    ["Main Table", "Sector View", "Custom Upload", "Time Travel"]
)

# --- Tab 1: Main Table ---
with tab_main:
    if universe_choice == "Custom Upload":
        st.info("Switch to the **Custom Upload** tab to load your stock list.")
    else:
        st.subheader(f"{DISPLAY_NAMES.get(universe_choice, universe_choice)} — {pd.Timestamp(selected_date).strftime('%d %b %Y')}")

        df = raw_df.copy()

        # Apply sector filter
        if selected_sectors:
            df = df[df["sector"].isin(selected_sectors)]

        # Apply mcap filter
        if "market_cap_cr" in df.columns:
            df = df[
                df["market_cap_cr"].isna() |
                ((df["market_cap_cr"] >= mcap_range[0]) & (df["market_cap_cr"] <= mcap_range[1]))
            ]

        # Apply sort
        sort_col, sort_desc = SORT_OPTIONS[sort_choice]
        if sort_col in df.columns:
            df = df.sort_values(sort_col, ascending=not sort_desc, na_position="last")

        if df.empty:
            st.warning("No stocks match the current filters.")
        else:
            render_summary_cards(df)
            st.divider()
            render_table(df)


# --- Tab 2: Sector View ---
with tab_sector:
    st.subheader(f"Sector Performance — {pd.Timestamp(selected_date).strftime('%d %b %Y')}")

    sector_df = load_sector_performance(selected_date)

    if not sector_df.empty:
        # Summary table
        display_sector = sector_df[[
            "sector", "num_companies", "advances", "declines",
            "day_change_pct", "week_chg_pct", "month_chg_pct",
            "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct",
        ]].copy()

        for col in ["day_change_pct", "week_chg_pct", "month_chg_pct",
                    "qtr_chg_pct", "half_yr_chg_pct", "year_chg_pct"]:
            display_sector[col] = display_sector[col].map(_fmt_pct)

        display_sector = display_sector.rename(columns={
            "sector": "Sector",
            "num_companies": "# Stocks",
            "advances": "Advances",
            "declines": "Declines",
            "day_change_pct": "1D%",
            "week_chg_pct": "1W%",
            "month_chg_pct": "30D%",
            "qtr_chg_pct": "60D%",
            "half_yr_chg_pct": "180D%",
            "year_chg_pct": "365D%",
        })
        st.dataframe(display_sector, use_container_width=True, hide_index=True)

    render_sector_chart(sector_df)


# --- Tab 3: Custom Upload ---
with tab_upload:
    st.subheader("Custom Stock List")
    st.markdown(
        "Upload a CSV with a single column named `symbol` containing NSE symbols (without `.NS`)."
    )

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

                known = [s for s in user_symbols if s in valid]
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
                        render_summary_cards(custom_df)
                        st.divider()
                        render_table(custom_df)
        except Exception as e:
            st.error(f"Error reading CSV: {e}")


# --- Tab 4: Time Travel ---
with tab_timetravel:
    st.subheader("Time-Travel Comparison")
    st.markdown(
        "Compare two snapshot dates side-by-side. Find stocks that flipped DMA status."
    )

    dates_list = load_available_dates()
    if len(dates_list) < 2:
        st.info("Need at least 2 snapshot dates for comparison. Check back after a few daily refreshes.")
    else:
        col_a, col_b = st.columns(2)
        with col_a:
            date_a = st.selectbox(
                "Date A (earlier)",
                options=dates_list,
                index=min(len(dates_list) - 1, 30),
                format_func=lambda d: pd.Timestamp(d).strftime("%d %b %Y"),
                key="tt_date_a",
            )
        with col_b:
            date_b = st.selectbox(
                "Date B (later)",
                options=dates_list,
                index=0,
                format_func=lambda d: pd.Timestamp(d).strftime("%d %b %Y"),
                key="tt_date_b",
            )

        tt_index = st.selectbox(
            "Universe",
            options=INDEXES,
            format_func=lambda x: DISPLAY_NAMES.get(x, x),
            key="tt_index",
        )

        if st.button("Compare"):
            df_a = load_snapshot(date_a, index_name=tt_index)
            df_b = load_snapshot(date_b, index_name=tt_index)

            if df_a.empty or df_b.empty:
                st.warning("No data for one or both dates.")
            else:
                merged = df_a[["symbol", "name", "cmp", "status_200dma", "ret_30d"]].merge(
                    df_b[["symbol", "cmp", "status_200dma", "ret_30d"]],
                    on="symbol",
                    suffixes=("_a", "_b"),
                )

                merged["cmp_chg"] = ((merged["cmp_b"] - merged["cmp_a"]) / merged["cmp_a"]).map(_fmt_pct)
                merged["200DMA flip"] = merged.apply(
                    lambda r: "Below → Above" if r["status_200dma_a"] == "Below 200DMA" and r["status_200dma_b"] == "Above 200DMA"
                    else ("Above → Below" if r["status_200dma_a"] == "Above 200DMA" and r["status_200dma_b"] == "Below 200DMA"
                    else ""),
                    axis=1,
                )

                flipped = merged[merged["200DMA flip"] != ""].sort_values("200DMA flip")

                st.markdown(f"### 200DMA Status Changes ({len(flipped)} stocks flipped)")
                if not flipped.empty:
                    st.dataframe(
                        flipped[["symbol", "name", "cmp_a", "cmp_b", "cmp_chg", "200DMA flip"]].rename(columns={
                            "cmp_a": f"CMP ({pd.Timestamp(date_a).strftime('%d %b')})",
                            "cmp_b": f"CMP ({pd.Timestamp(date_b).strftime('%d %b')})",
                            "cmp_chg": "CMP Change",
                        }),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("No stocks changed 200DMA status between these two dates.")

                st.divider()
                st.markdown("### Full Comparison Table")
                full_display = merged[[
                    "symbol", "name", "cmp_a", "cmp_b", "cmp_chg",
                    "status_200dma_a", "status_200dma_b", "200DMA flip",
                ]].rename(columns={
                    "cmp_a": f"CMP {pd.Timestamp(date_a).strftime('%d %b')}",
                    "cmp_b": f"CMP {pd.Timestamp(date_b).strftime('%d %b')}",
                    "cmp_chg": "Change",
                    "status_200dma_a": f"200DMA ({pd.Timestamp(date_a).strftime('%d %b')})",
                    "status_200dma_b": f"200DMA ({pd.Timestamp(date_b).strftime('%d %b')})",
                })
                st.dataframe(full_display, use_container_width=True, hide_index=True)
