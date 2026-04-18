"""
global_markets_tab.py (v2) — Global Markets tab, redesigned to match target UI.

Visual design:
  - City-named session timeline (Sydney → Tokyo → HK → India → Frankfurt → London → NY)
  - Cards with embedded SVG sparklines, status badge, abs+pct change
  - Single-line overview bar with cross-asset values inline
  - Overnight futures bar showing price + change%
  - Auto-refresh every 5 min with elapsed-time counter
"""

import logging
from datetime import datetime, time as dtime

import pandas as pd
import plotly.colors as pc
import plotly.express as px
import plotly.graph_objects as go
import pytz
import streamlit as st
import yfinance as yf

logger = logging.getLogger(__name__)

try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False


# ─── SESSION TIMELINE CONFIG (city-level bars) ───────────────────────────────

SESSIONS = [
    {'name': 'Sydney',    'tz': 'Australia/Sydney', 'open': dtime(10, 0),  'close': dtime(16, 0),  'color': '#06b6d4'},
    {'name': 'Tokyo',     'tz': 'Asia/Tokyo',        'open': dtime(9,  0),  'close': dtime(15, 30), 'color': '#f87171'},
    {'name': 'HK/China',  'tz': 'Asia/Shanghai',     'open': dtime(9,  30), 'close': dtime(15, 0),  'color': '#fb923c'},
    {'name': 'India',     'tz': 'Asia/Kolkata',      'open': dtime(9,  15), 'close': dtime(15, 30), 'color': '#f97316'},
    {'name': 'Frankfurt', 'tz': 'Europe/Berlin',     'open': dtime(9,  0),  'close': dtime(17, 30), 'color': '#8b5cf6'},
    {'name': 'London',    'tz': 'Europe/London',     'open': dtime(8,  0),  'close': dtime(16, 30), 'color': '#6366f1'},
    {'name': 'New York',  'tz': 'America/New_York',  'open': dtime(9,  30), 'close': dtime(16, 0),  'color': '#3b82f6'},
]


# ─── REGIONS + INDICES CONFIG ─────────────────────────────────────────────────

REGIONS = [
    {
        'id': 'india', 'code': 'IN', 'name': 'India', 'flag': '🇮🇳', 'is_home': True,
        'tz': 'Asia/Kolkata', 'open': dtime(9, 15), 'close': dtime(15, 30),
        'indices': [
            {'sym': '^NSEI',             'name': 'Nifty 50',          'short': 'NIFTY'},
            {'sym': '^BSESN',            'name': 'BSE Sensex',        'short': 'SENSEX'},
            {'sym': '^NSEBANK',          'name': 'Nifty Bank',        'short': 'BANK NIFTY'},
            {'sym': 'NIFTYIT.NS',        'name': 'Nifty IT',          'short': 'NIFTY IT'},
            {'sym': 'NIFTYMIDCAP150.NS', 'name': 'Nifty Midcap 150', 'short': 'MIDCAP'},
            {'sym': 'NIFTYSMLCAP250.NS', 'name': 'Nifty Smlcap 250', 'short': 'SMALLCAP'},
            {'sym': '^INDIAVIX',         'name': 'India VIX',         'short': 'INDIA VIX'},
        ],
    },
    {
        'id': 'us', 'code': 'US', 'name': 'United States', 'flag': '🇺🇸', 'is_home': False,
        'tz': 'America/New_York', 'open': dtime(9, 30), 'close': dtime(16, 0),
        'pre': dtime(4, 0), 'after': dtime(20, 0),
        'indices': [
            {'sym': '^GSPC', 'name': 'S&P 500',           'short': 'S&P 500'},
            {'sym': '^DJI',  'name': 'Dow Jones',         'short': 'DOW'},
            {'sym': '^IXIC', 'name': 'Nasdaq Composite',  'short': 'NASDAQ'},
            {'sym': '^NDX',  'name': 'Nasdaq 100',        'short': 'NDX 100'},
            {'sym': '^RUT',  'name': 'Russell 2000',      'short': 'RUSSELL'},
            {'sym': '^VIX',  'name': 'CBOE VIX',          'short': 'VIX'},
            {'sym': 'ES=F',  'name': 'S&P 500 Futures',   'short': 'S&P FUT'},
            {'sym': 'NQ=F',  'name': 'Nasdaq Futures',    'short': 'NQ FUT'},
            {'sym': 'YM=F',  'name': 'Dow Futures',       'short': 'DOW FUT'},
            {'sym': '^SOX',  'name': 'Philadelphia Semi', 'short': 'SOX'},
        ],
    },
    {
        'id': 'europe', 'code': 'EU', 'name': 'Europe', 'flag': '🇪🇺', 'is_home': False,
        'tz': 'Europe/London', 'open': dtime(8, 0), 'close': dtime(16, 30),
        'indices': [
            {'sym': '^STOXX50E', 'name': 'EURO STOXX 50', 'short': 'STOXX 50'},
            {'sym': '^FTSE',     'name': 'FTSE 100',      'short': 'FTSE 100'},
            {'sym': '^GDAXI',    'name': 'DAX 40',        'short': 'DAX'},
            {'sym': '^FCHI',     'name': 'CAC 40',        'short': 'CAC 40'},
            {'sym': '^IBEX',     'name': 'IBEX 35',       'short': 'IBEX'},
            {'sym': '^AEX',      'name': 'AEX',           'short': 'AEX'},
            {'sym': '^SSMI',     'name': 'Swiss SMI',     'short': 'SMI'},
            {'sym': '^STOXX',    'name': 'STOXX 600',     'short': 'STOXX 600'},
        ],
    },
    {
        'id': 'china_hk', 'code': 'CN', 'name': 'China & Hong Kong', 'flag': '🇨🇳', 'is_home': False,
        'tz': 'Asia/Shanghai', 'open': dtime(9, 30), 'close': dtime(15, 0),
        'indices': [
            {'sym': '000001.SS', 'name': 'Shanghai Composite', 'short': 'SHCOMP'},
            {'sym': '000300.SS', 'name': 'CSI 300',            'short': 'CSI 300'},
            {'sym': '^HSI',      'name': 'Hang Seng',          'short': 'HSI'},
            {'sym': '^HSTECH',   'name': 'Hang Seng Tech',     'short': 'HS TECH'},
        ],
    },
    {
        'id': 'japan', 'code': 'JP', 'name': 'Japan', 'flag': '🇯🇵', 'is_home': False,
        'tz': 'Asia/Tokyo', 'open': dtime(9, 0), 'close': dtime(15, 30),
        'indices': [
            {'sym': '^N225', 'name': 'Nikkei 225', 'short': 'NIKKEI'},
            {'sym': '^TOPX', 'name': 'TOPIX',      'short': 'TOPIX'},
        ],
    },
    {
        'id': 'korea', 'code': 'KR', 'name': 'South Korea', 'flag': '🇰🇷', 'is_home': False,
        'tz': 'Asia/Seoul', 'open': dtime(9, 0), 'close': dtime(15, 30),
        'indices': [
            {'sym': '^KS11', 'name': 'KOSPI',  'short': 'KOSPI'},
            {'sym': '^KQ11', 'name': 'KOSDAQ', 'short': 'KOSDAQ'},
        ],
    },
    {
        'id': 'apac', 'code': 'AP', 'name': 'Asia Pacific', 'flag': '🌏', 'is_home': False,
        'tz': 'Australia/Sydney', 'open': dtime(10, 0), 'close': dtime(16, 0),
        'indices': [
            {'sym': '^TWII', 'name': 'TAIEX (Taiwan)',      'short': 'TAIEX'},
            {'sym': '^AXJO', 'name': 'ASX 200 (Australia)', 'short': 'ASX 200'},
            {'sym': '^STI',  'name': 'Straits Times (SGP)', 'short': 'STI'},
            {'sym': '^JKSE', 'name': 'Jakarta Comp (IDN)',  'short': 'JKSE'},
        ],
    },
    {
        'id': 'em', 'code': 'EM', 'name': 'EM & Americas', 'flag': '🌍', 'is_home': False,
        'tz': 'America/Sao_Paulo', 'open': dtime(10, 0), 'close': dtime(17, 0),
        'indices': [
            {'sym': '^BVSP',   'name': 'Bovespa (Brazil)', 'short': 'BOVESPA'},
            {'sym': '^GSPTSE', 'name': 'S&P/TSX (Canada)', 'short': 'TSX'},
            {'sym': '^TASI',   'name': 'Tadawul (Saudi)',   'short': 'TASI'},
        ],
    },
    {
        'id': 'cross_asset', 'code': 'FX', 'name': 'Cross-Asset', 'flag': '🌐', 'is_home': False,
        'tz': None,
        'indices': [
            {'sym': 'DX-Y.NYB', 'name': 'US Dollar Index', 'short': 'DXY'},
            {'sym': 'GC=F',     'name': 'Gold Futures',    'short': 'GOLD'},
            {'sym': 'BZ=F',     'name': 'Brent Crude',     'short': 'BRENT'},
            {'sym': '^TNX',     'name': 'US 10Y Treasury', 'short': 'US 10Y'},
            {'sym': 'BTC-USD',  'name': 'Bitcoin',         'short': 'BTC'},
            {'sym': 'USDINR=X', 'name': 'USD/INR',         'short': 'USD/INR'},
        ],
    },
]

# Country → primary index for world heatmap
HEATMAP_COUNTRIES = [
    ('IND', '^NSEI',    'India'),       ('USA', '^GSPC',    'United States'),
    ('GBR', '^FTSE',    'UK'),          ('DEU', '^GDAXI',   'Germany'),
    ('FRA', '^FCHI',    'France'),      ('ESP', '^IBEX',    'Spain'),
    ('NLD', '^AEX',     'Netherlands'), ('CHE', '^SSMI',    'Switzerland'),
    ('CHN', '000001.SS','China'),       ('HKG', '^HSI',     'Hong Kong'),
    ('JPN', '^N225',    'Japan'),       ('KOR', '^KS11',    'South Korea'),
    ('TWN', '^TWII',    'Taiwan'),      ('AUS', '^AXJO',    'Australia'),
    ('SGP', '^STI',     'Singapore'),   ('IDN', '^JKSE',    'Indonesia'),
    ('BRA', '^BVSP',    'Brazil'),      ('CAN', '^GSPTSE',  'Canada'),
    ('SAU', '^TASI',    'Saudi Arabia'),
]

_YIELD_SYMS   = {'^TNX', '^TYX', '^FVX', '^IRX', '^USGG20YR',
                 'IN10YT=RR', 'IN20YT=RR', 'IN30YT=RR'}
_FUTURES_SYMS = ['ES=F', 'NQ=F', 'YM=F']


# ─── COMMODITIES CONFIG ───────────────────────────────────────────────────────

COMMODITIES = [
    {'sym': 'GC=F',  'name': 'Gold',          'short': 'GOLD'},
    {'sym': 'SI=F',  'name': 'Silver',         'short': 'SILVER'},
    {'sym': 'CL=F',  'name': 'WTI Crude Oil',  'short': 'WTI CRUDE'},
    {'sym': 'BZ=F',  'name': 'Brent Crude',    'short': 'BRENT CRUDE'},
    {'sym': 'HG=F',  'name': 'Copper',         'short': 'COPPER'},
    {'sym': 'NG=F',  'name': 'Natural Gas',    'short': 'NAT GAS'},
    {'sym': 'PL=F',  'name': 'Platinum',       'short': 'PLATINUM'},
    {'sym': 'PA=F',  'name': 'Palladium',      'short': 'PALLADIUM'},
    {'sym': 'ZW=F',  'name': 'Wheat',          'short': 'WHEAT'},
    {'sym': 'ZC=F',  'name': 'Corn',           'short': 'CORN'},
    {'sym': 'ZS=F',  'name': 'Soybeans',       'short': 'SOYBEANS'},
    {'sym': 'ALI=F', 'name': 'Aluminium',      'short': 'ALUMINIUM'},
]


# ─── GLOBAL BONDS CONFIG ─────────────────────────────────────────────────────

BONDS = [
    {'sym': '^TNX',       'name': 'US 10Y Treasury',  'short': 'US 10Y'},
    {'sym': '^USGG20YR',  'name': 'US 20Y Treasury',  'short': 'US 20Y'},
    {'sym': '^TYX',       'name': 'US 30Y Treasury',  'short': 'US 30Y'},
    {'sym': 'IN10YT=RR',  'name': 'India 10Y Bond',   'short': 'IN 10Y'},
    {'sym': 'IN20YT=RR',  'name': 'India 20Y Bond',   'short': 'IN 20Y'},
    {'sym': 'IN30YT=RR',  'name': 'India 30Y Bond',   'short': 'IN 30Y'},
]


# ─── CRYPTO CONFIG ───────────────────────────────────────────────────────────

CRYPTO = [
    {'sym': 'BTC-USD',  'name': 'Bitcoin',   'short': 'BTC'},
    {'sym': 'ETH-USD',  'name': 'Ethereum',  'short': 'ETH'},
    {'sym': 'BNB-USD',  'name': 'BNB',       'short': 'BNB'},
    {'sym': 'SOL-USD',  'name': 'Solana',    'short': 'SOL'},
    {'sym': 'XRP-USD',  'name': 'XRP',       'short': 'XRP'},
    {'sym': 'ADA-USD',  'name': 'Cardano',   'short': 'ADA'},
    {'sym': 'DOGE-USD', 'name': 'Dogecoin',  'short': 'DOGE'},
    {'sym': 'AVAX-USD', 'name': 'Avalanche', 'short': 'AVAX'},
    {'sym': 'DOT-USD',  'name': 'Polkadot',  'short': 'DOT'},
    {'sym': 'LINK-USD', 'name': 'Chainlink', 'short': 'LINK'},
    {'sym': 'MATIC-USD','name': 'Polygon',   'short': 'MATIC'},
    {'sym': 'LTC-USD',  'name': 'Litecoin',  'short': 'LTC'},
]


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _ist_now() -> datetime:
    return datetime.now(pytz.timezone('Asia/Kolkata'))


def _session_hours_ist(tz_str: str, open_t: dtime, close_t: dtime):
    """Return (open_h, close_h) as fractional IST hours (0-28 range)."""
    try:
        ist    = pytz.timezone('Asia/Kolkata')
        mkt_tz = pytz.timezone(tz_str)
        today  = _ist_now().date()
        oh = mkt_tz.localize(datetime.combine(today, open_t)).astimezone(ist)
        ch = mkt_tz.localize(datetime.combine(today, close_t)).astimezone(ist)
        oh_f = oh.hour + oh.minute / 60
        ch_f = ch.hour + ch.minute / 60
        if ch_f < oh_f:
            ch_f += 24        # crosses midnight IST (e.g. US)
        return oh_f, ch_f
    except Exception:
        return None, None


def _region_status(region: dict) -> str:
    tz_str = region.get('tz')
    if not tz_str:
        return 'UNKNOWN'
    try:
        tz  = pytz.timezone(tz_str)
        now = datetime.now(tz)
    except Exception:
        return 'UNKNOWN'
    if now.weekday() >= 5:
        return 'CLOSED'
    now_t = now.time().replace(second=0, microsecond=0)
    op, cl = region.get('open'), region.get('close')
    pr, af = region.get('pre'), region.get('after')
    if not (op and cl):
        return 'UNKNOWN'
    if op <= now_t <= cl:   return 'OPEN'
    if pr and pr <= now_t < op: return 'PRE'
    if af and cl < now_t <= af: return 'AFTER'
    return 'CLOSED'


def _india_open() -> bool:
    india = next(r for r in REGIONS if r['id'] == 'india')
    return _region_status(india) == 'OPEN'


def _fmt_price(price, sym: str = '') -> str:
    if price is None:
        return '—'
    if sym in _YIELD_SYMS:
        return f"{price:.2f}%"
    if price < 1:
        return f"{price:.4f}"
    return f"{price:,.2f}"


def _status_info(status: str) -> tuple:
    """Return (label, text_color, bg_color)."""
    return {
        'OPEN':    ('Open',        '#22c55e', '#052e16'),
        'PRE':     ('Pre-Market',  '#f59e0b', '#1c1007'),
        'AFTER':   ('After-Hours', '#f59e0b', '#1c1007'),
        'CLOSED':  ('Closed',      '#6b7280', '#1f2937'),
    }.get(status, ('—', '#6b7280', '#1f2937'))


# ─── DATA FETCHING ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _fetch_quotes() -> tuple:
    """Batch daily quotes for all symbols. Returns (dict, fetched_at)."""
    all_syms = list(dict.fromkeys(
        [i['sym'] for r in REGIONS for i in r['indices']]
        + [c['sym'] for c in COMMODITIES]
        + [b['sym'] for b in BONDS]
        + [c['sym'] for c in CRYPTO]
    ))
    results  = {s: None for s in all_syms}
    try:
        raw = yf.download(
            tickers=all_syms, period='2d', interval='1d',
            auto_adjust=True, progress=False, threads=True,
        )
        if not raw.empty:
            close = (
                raw['Close'] if isinstance(raw.columns, pd.MultiIndex)
                else raw[['Close']].rename(columns={'Close': all_syms[0]})
            )
            for sym in all_syms:
                try:
                    if sym not in close.columns:
                        continue
                    prices = close[sym].dropna()
                    if len(prices) == 0:
                        continue
                    price = float(prices.iloc[-1])
                    prev  = float(prices.iloc[-2]) if len(prices) >= 2 else price
                    chg   = price - prev
                    pct   = (chg / prev * 100) if prev else 0.0
                    results[sym] = {'price': price, 'prev': prev, 'change': chg, 'pct': pct}
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Quote fetch failed: {e}")
    return results, datetime.now()


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_intraday_all() -> dict:
    """Batch 5-min intraday for all symbols. Returns {sym: [close_prices]}."""
    all_syms = list(dict.fromkeys(
        [i['sym'] for r in REGIONS for i in r['indices']]
        + [c['sym'] for c in COMMODITIES]
        + [b['sym'] for b in BONDS]
        + [c['sym'] for c in CRYPTO]
    ))
    result   = {s: [] for s in all_syms}
    try:
        raw = yf.download(
            tickers=all_syms, period='1d', interval='5m',
            auto_adjust=True, progress=False, threads=True,
        )
        if not raw.empty:
            close = (
                raw['Close'] if isinstance(raw.columns, pd.MultiIndex)
                else raw[['Close']].rename(columns={'Close': all_syms[0]})
            )
            for sym in all_syms:
                if sym in close.columns:
                    result[sym] = [float(p) for p in close[sym].dropna()]
    except Exception as e:
        logger.error(f"Intraday batch failed: {e}")
    return result


# ─── SVG SPARKLINE ────────────────────────────────────────────────────────────

def _svg_spark(prices: list, color: str, w: int = 120, h: int = 40) -> str:
    """Generate a filled SVG sparkline from a list of prices."""
    if not prices or len(prices) < 2:
        return (
            f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}">'
            f'<line x1="0" y1="{h//2}" x2="{w}" y2="{h//2}" '
            f'stroke="#374151" stroke-width="1" stroke-dasharray="4,3"/></svg>'
        )
    mn, mx = min(prices), max(prices)
    rng    = mx - mn or 1
    pad    = 2
    xs = [i * w / (len(prices) - 1) for i in range(len(prices))]
    ys = [h - pad - (p - mn) / rng * (h - 2 * pad) for p in prices]
    pts  = ' '.join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys))
    # Hex → rgba for fill
    r_h, g_h, b_h = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    fill = f"rgba({r_h},{g_h},{b_h},0.10)"
    poly_pts = f"0,{h} " + pts + f" {w},{h}"
    return (
        f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" '
        f'preserveAspectRatio="none" style="display:block;">'
        f'<defs><linearGradient id="sg_{r_h}{g_h}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0%" stop-color="{color}" stop-opacity="0.18"/>'
        f'<stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>'
        f'</linearGradient></defs>'
        f'<polygon points="{poly_pts}" fill="url(#sg_{r_h}{g_h})"/>'
        f'<polyline points="{pts}" fill="none" stroke="{color}" '
        f'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
        f'</svg>'
    )


# ─── SESSION TIMELINE ─────────────────────────────────────────────────────────

def _html_timeline() -> str:
    ist   = _ist_now()
    now_h = ist.hour + ist.minute / 60
    SPAN  = 28      # 00:00 → 28:00 IST (i.e. next-day 04:00)
    ROW_H = 28
    GAP   = 6

    rows = []
    for sess in SESSIONS:
        oh, ch = _session_hours_ist(sess['tz'], sess['open'], sess['close'])
        if oh is None:
            continue
        is_open = oh <= now_h <= ch
        rows.append({
            'name':    sess['name'],
            'left':    oh / SPAN * 100,
            'width':   (ch - oh) / SPAN * 100,
            'color':   sess['color'],
            'opacity': '1' if is_open else '0.38',
        })

    total_h = len(rows) * (ROW_H + GAP)
    now_pct = min(now_h / SPAN * 100, 99.5)

    # Grid lines + tick labels
    grid  = ''
    ticks = ''
    for h in range(0, 29, 4):
        pct   = h / SPAN * 100
        label = f"{h % 24:02d}:00"
        grid  += (f'<div style="position:absolute;left:{pct:.2f}%;top:0;bottom:0;'
                  f'border-left:1px solid #1e2535;pointer-events:none;"></div>')
        ticks += (f'<span style="position:absolute;left:{pct:.2f}%;'
                  f'transform:translateX(-50%);font-size:11px;font-weight:500;color:#64748b;'
                  f'white-space:nowrap;">{label}</span>')

    # Session bars
    bars = ''
    for i, row in enumerate(rows):
        top = i * (ROW_H + GAP)
        bars += (
            f'<div style="position:absolute;top:{top}px;left:{row["left"]:.2f}%;'
            f'width:{row["width"]:.2f}%;height:{ROW_H}px;background:{row["color"]};'
            f'opacity:{row["opacity"]};border-radius:4px;display:flex;align-items:center;'
            f'padding:0 8px;overflow:hidden;min-width:2px;">'
            f'<span style="font-size:11px;font-weight:600;color:#fff;'
            f'white-space:nowrap;overflow:hidden;">{row["name"]}</span>'
            f'</div>'
        )

    # NOW vertical line (animated pulse)
    now_line = (
        f'<div class="gm-now-line" style="position:absolute;left:{now_pct:.2f}%;top:0;'
        f'height:{total_h}px;width:2px;background:#ef4444;z-index:20;pointer-events:none;">'
        f'</div>'
    )

    return f"""
<div style="background:#0b0f1a;border-radius:10px;padding:16px 20px 14px;margin-top:1rem;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
    <span style="font-size:11px;font-weight:700;color:#64748b;letter-spacing:0.12em;
                 text-transform:uppercase;">GLOBAL SESSION TIMELINE (IST)</span>
    <span style="font-size:11px;font-weight:600;color:#64748b;">
      Now:&nbsp;<span style="color:#94a3b8;">{ist.strftime('%H:%M')} IST</span>
    </span>
  </div>
  <div style="position:relative;height:{total_h}px;margin-bottom:16px;">
    {grid}{bars}{now_line}
  </div>
  <div style="position:relative;height:14px;">{ticks}</div>
</div>
"""


# ─── OVERNIGHT FUTURES BAR ────────────────────────────────────────────────────

def _html_futures(quotes: dict) -> str:
    if _india_open():
        return ''
    labels = [('ES=F', 'S&P FUT'), ('NQ=F', 'NQ FUT'), ('YM=F', 'DOW FUT')]
    parts  = []
    for sym, label in labels:
        q = quotes.get(sym)
        if not q:
            continue
        color = '#22c55e' if q['pct'] >= 0 else '#ef4444'
        sign  = '+' if q['pct'] >= 0 else ''
        parts.append(
            f'<span style="margin-right:28px;white-space:nowrap;">'
            f'<span style="color:#94a3b8;font-size:12px;font-weight:600;">{label}:&nbsp;</span>'
            f'<span style="color:#f1f5f9;font-size:13px;font-weight:700;">'
            f'{_fmt_price(q["price"])}&nbsp;</span>'
            f'<span style="color:{color};font-size:13px;font-weight:700;">'
            f'{sign}{q["pct"]:.2f}%</span>'
            f'</span>'
        )
    usdinr = quotes.get('USDINR=X')
    if usdinr:
        color = '#ef4444' if usdinr['pct'] > 0 else '#22c55e'
        parts.append(
            f'<span style="white-space:nowrap;">'
            f'<span style="color:#94a3b8;font-size:12px;font-weight:600;">USD/INR:&nbsp;</span>'
            f'<span style="color:#f1f5f9;font-size:13px;font-weight:700;">'
            f'₹{usdinr["price"]:.2f}&nbsp;</span>'
            f'<span style="color:{color};font-size:13px;font-weight:700;">'
            f'({usdinr["pct"]:+.2f}%)</span>'
            f'</span>'
        )
    if not parts:
        return ''
    return f"""
<div style="background:#080e1f;border:1px solid #1a3061;border-radius:8px;
            padding:10px 20px;margin-top:10px;display:flex;align-items:center;
            flex-wrap:wrap;gap:6px;">
  <span style="color:#475569;font-size:11px;font-weight:700;letter-spacing:0.08em;
               text-transform:uppercase;margin-right:14px;">🌙 OVERNIGHT FUTURES</span>
  {''.join(parts)}
</div>
"""


# ─── OVERVIEW BAR ─────────────────────────────────────────────────────────────

def _html_overview(structured: list, quotes: dict, elapsed_s: int) -> str:
    ups = downs = flat = 0
    best_pct, best_name  = -999.0, ''
    worst_pct, worst_name = 999.0, ''

    for r in structured:
        if r['id'] == 'cross_asset':
            continue
        for idx in r['data']:
            q = idx['q']
            if not q:
                continue
            pct = q['pct']
            if pct > 0.1:    ups   += 1
            elif pct < -0.1: downs += 1
            else:            flat  += 1
            if pct > best_pct:   best_pct,  best_name  = pct, idx['short']
            if pct < worst_pct:  worst_pct, worst_name = pct, idx['short']

    # Cross-asset inline
    cross_items = [
        ('DX-Y.NYB', 'DXY'), ('BZ=F', 'Brent'), ('GC=F', 'Gold'),
        ('^TNX', 'US10Y'), ('BTC-USD', 'BTC'),
    ]
    cross_html = ''
    for sym, lbl in cross_items:
        q = quotes.get(sym)
        if not q:
            continue
        c = '#22c55e' if q['pct'] >= 0 else '#ef4444'
        cross_html += (
            f'<span style="margin-left:16px;white-space:nowrap;">'
            f'<span style="color:#94a3b8;font-size:12px;font-weight:600;">{lbl}:&nbsp;</span>'
            f'<span style="color:#f1f5f9;font-size:12px;font-weight:700;">'
            f'{_fmt_price(q["price"], sym)}&nbsp;</span>'
            f'<span style="color:{c};font-size:12px;font-weight:700;">({q["pct"]:+.2f}%)</span>'
            f'</span>'
        )

    bc = '#22c55e' if best_pct  >= 0 else '#ef4444'
    wc = '#22c55e' if worst_pct >= 0 else '#ef4444'
    elapsed_str = (f"{elapsed_s}s ago" if elapsed_s < 60
                   else f"{elapsed_s // 60}m {elapsed_s % 60}s ago")

    return f"""
<div style="display:flex;align-items:center;justify-content:space-between;
            flex-wrap:wrap;gap:4px;padding:8px 2px;">
  <div style="display:flex;align-items:center;flex-wrap:wrap;gap:0;">
    <span style="color:#94a3b8;font-size:12px;font-weight:600;margin-right:6px;">Global:</span>
    <span style="color:#22c55e;font-size:12px;font-weight:700;margin-right:8px;">● {ups} Up</span>
    <span style="color:#ef4444;font-size:12px;font-weight:700;margin-right:8px;">● {downs} Down</span>
    <span style="color:#64748b;font-size:12px;font-weight:600;margin-right:12px;">● {flat} Flat</span>
    <span style="color:#475569;font-size:12px;margin-right:12px;">│</span>
    <span style="margin-right:12px;white-space:nowrap;">
      <span style="color:#94a3b8;font-size:12px;font-weight:600;">Best:&nbsp;</span>
      <span style="color:{bc};font-size:12px;font-weight:700;">{best_name} {best_pct:+.2f}%</span>
    </span>
    <span style="color:#475569;font-size:12px;margin-right:12px;">│</span>
    <span style="white-space:nowrap;">
      <span style="color:#94a3b8;font-size:12px;font-weight:600;">Worst:&nbsp;</span>
      <span style="color:{wc};font-size:12px;font-weight:700;">{worst_name} {worst_pct:+.2f}%</span>
    </span>
    {cross_html}
  </div>
  <span style="font-size:11px;color:#475569;white-space:nowrap;">↻ Updated {elapsed_str}</span>
</div>
"""


# ─── INDEX CARD HTML ─────────────────────────────────────────────────────────

def _card_html(idx: dict, q, spark_prices: list, status: str, anim_delay: float = 0.0) -> str:
    lbl, txt_clr, bg_clr = _status_info(status)
    badge_extra = ' gm-badge-open' if status == 'OPEN' else ''

    if q:
        price_str = _fmt_price(q['price'], idx['sym'])
        prev_str  = _fmt_price(q['prev'],  idx['sym'])
        pct       = q['pct']
        chg       = q['change']
        pct_clr   = '#22c55e' if pct >= 0 else '#ef4444'
        arrow     = '▲' if pct >= 0 else '▼'
        spark_clr = '#22c55e' if pct >= 0 else '#ef4444'
        chg_line  = f"{arrow} {abs(chg):,.1f} ({pct:+.2f}%)"
    else:
        price_str = '—'
        prev_str  = '—'
        pct_clr   = '#64748b'
        spark_clr = '#334155'
        chg_line  = '—'

    spark_svg = _svg_spark(spark_prices, spark_clr) if spark_prices else _svg_spark([], spark_clr)

    return f"""
<div class="gm-card" style="background:#0f1520;border:1px solid #1e2535;border-radius:10px;
            padding:13px 13px 10px;position:relative;margin-bottom:4px;
            animation-delay:{anim_delay:.2f}s;">
  <span class="{badge_extra}" style="position:absolute;top:9px;right:9px;background:{bg_clr};
               color:{txt_clr};font-size:9px;font-weight:700;padding:2px 7px;border-radius:10px;
               letter-spacing:0.04em;">{lbl}</span>
  <div style="font-size:15px;font-weight:700;color:#f1f5f9;padding-right:54px;
              margin-bottom:1px;white-space:nowrap;overflow:hidden;
              text-overflow:ellipsis;">{idx['short']}</div>
  <div style="font-size:10px;color:#6b7280;margin-bottom:9px;white-space:nowrap;
              overflow:hidden;text-overflow:ellipsis;">{idx['name']}</div>
  <div style="font-size:20px;font-weight:700;color:#f9fafb;
              font-family:'JetBrains Mono','Courier New',monospace;
              letter-spacing:-0.01em;line-height:1.1;">{price_str}</div>
  <div style="font-size:12px;font-weight:600;color:{pct_clr};margin-top:3px;">{chg_line}</div>
  <div style="margin:8px 0 5px;">{spark_svg}</div>
  <div style="font-size:10px;color:#4b5563;">Prev&nbsp;{prev_str}</div>
</div>
"""


# ─── REGION SECTION ──────────────────────────────────────────────────────────

def _render_region(region: dict, intraday: dict):
    status          = region['status']
    lbl, txt_c, _   = _status_info(status)
    code            = region.get('code', '')
    flag            = region['flag']
    name            = region['name']
    data            = region['data']
    is_home         = region.get('is_home', False)

    expander_label = f"{flag}  {name}"

    with st.expander(expander_label, expanded=is_home):
        per_row = 4
        for row_idx, start in enumerate(range(0, len(data), per_row)):
            chunk = data[start : start + per_row]
            cols  = st.columns(per_row, gap="medium")
            for col_idx, (col, idx) in enumerate(zip(cols, chunk)):
                # Stagger animation: each card gets a small delay
                delay = (row_idx * per_row + col_idx) * 0.06
                spark = intraday.get(idx['sym'], [])
                with col:
                    st.markdown(_card_html(idx, idx['q'], spark, status, delay),
                                unsafe_allow_html=True)
            # Vertical gap between rows of cards
            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)


# ─── GENERIC ASSET SECTION (Commodities / Bonds / Crypto) ───────────────────

def _asset_status(always_open: bool = False) -> str:
    """Simple open/closed based on weekday; always_open for 24/7 markets."""
    if always_open:
        return 'OPEN'
    return 'OPEN' if datetime.now().weekday() < 5 else 'CLOSED'


def _render_asset_section(
    title: str,
    flag: str,
    items: list,
    quotes: dict,
    intraday: dict,
    status: str,
    expanded: bool = False,
    note: str = '',
):
    """Renders a commodity / bond / crypto section as a collapsible card grid."""
    label = f"{flag}  {title}"
    with st.expander(label, expanded=expanded):
        if note:
            st.caption(note)
        per_row = 4
        for row_idx, start in enumerate(range(0, len(items), per_row)):
            chunk = items[start : start + per_row]
            cols  = st.columns(per_row, gap="medium")
            for col_idx, (col, item) in enumerate(zip(cols, chunk)):
                delay = (row_idx * per_row + col_idx) * 0.06
                spark = intraday.get(item['sym'], [])
                q     = quotes.get(item['sym'])
                with col:
                    st.markdown(
                        _card_html(item, q, spark, status, delay),
                        unsafe_allow_html=True,
                    )
            st.markdown('<div style="height:12px;"></div>', unsafe_allow_html=True)


# ─── WORLD HEATMAP ───────────────────────────────────────────────────────────

# ── Kashmir disputed-territory polygons (India's official claim line) ─────────
# Azad Kashmir (Pakistan-administered J&K)
_AJK_LON = [73.99, 73.76, 73.47, 73.18, 73.10, 73.42, 73.82,
             74.04, 74.38, 74.92, 75.05, 74.72, 74.51, 74.22, 73.99]
_AJK_LAT = [33.20, 33.56, 33.87, 34.22, 34.71, 35.06, 35.10,
             34.92, 34.53, 34.65, 34.73, 34.23, 33.72, 33.31, 33.20]

# Gilgit-Baltistan (Pakistan-administered, larger northern territory)
_GB_LON  = [73.82, 73.42, 73.10, 72.50, 72.00, 71.80, 72.02,
             73.05, 74.08, 75.52, 77.02, 77.82, 77.48, 76.05,
             75.22, 74.92, 73.82]
_GB_LAT  = [35.10, 35.06, 34.71, 35.98, 36.52, 36.51, 37.10,
             37.32, 37.72, 37.50, 37.20, 36.52, 35.83, 35.22,
             34.82, 34.73, 35.10]

# Aksai Chin (China-administered, Indian claim)
_AC_LON  = [79.02, 79.52, 80.05, 80.82, 81.20, 80.60, 79.82, 78.82, 79.02]
_AC_LAT  = [35.52, 36.22, 36.52, 36.02, 34.82, 33.82, 33.52, 34.52, 35.52]


def _interpolate_colorscale(colorscale, t: float) -> str:
    """Return hex color for position t ∈ [0,1] on colorscale."""
    stops = colorscale
    for i in range(len(stops) - 1):
        s0, c0 = stops[i]
        s1, c1 = stops[i + 1]
        if s0 <= t <= s1:
            lt = (t - s0) / (s1 - s0) if s1 > s0 else 0.0
            r0, g0, b0 = int(c0[1:3], 16), int(c0[3:5], 16), int(c0[5:7], 16)
            r1, g1, b1 = int(c1[1:3], 16), int(c1[3:5], 16), int(c1[5:7], 16)
            r = int(r0 + (r1 - r0) * lt)
            g = int(g0 + (g1 - g0) * lt)
            b = int(b0 + (b1 - b0) * lt)
            return f'#{r:02x}{g:02x}{b:02x}'
    return stops[-1][1]


def _render_heatmap(quotes: dict):
    rows = [
        {'iso': iso, 'country': c, 'sym': sym,
         'pct': quotes[sym]['pct'] if quotes.get(sym) else None}
        for iso, sym, c in HEATMAP_COUNTRIES
    ]
    df = pd.DataFrame(rows)
    df['pct_fill'] = df['pct'].fillna(0)
    df['label']    = df['pct'].apply(lambda x: f"{x:+.2f}%" if x is not None else "N/A")

    color_scale = [
        [0.00, '#7f1d1d'],
        [0.20, '#dc2626'],
        [0.40, '#b91c1c'],
        [0.46, '#374151'],
        [0.50, '#1e293b'],
        [0.54, '#374151'],
        [0.60, '#15803d'],
        [0.80, '#16a34a'],
        [1.00, '#14532d'],
    ]

    # Compute India's current color so disputed territories match exactly
    india_pct = next((r['pct'] for r in rows if r['iso'] == 'IND'), None)
    if india_pct is not None:
        t = max(0.0, min(1.0, (india_pct + 3) / 6.0))
        kashmir_color = _interpolate_colorscale(color_scale, t)
    else:
        kashmir_color = '#1e293b'   # neutral grey when no data

    fig = go.Figure(go.Choropleth(
        locations=df['iso'],
        z=df['pct_fill'],
        text=df['country'],
        customdata=df[['label', 'sym']],
        hovertemplate=(
            '<b>%{text}</b><br>'
            'Index: %{customdata[1]}<br>'
            'Change: %{customdata[0]}'
            '<extra></extra>'
        ),
        colorscale=color_scale,
        zmin=-3, zmax=3,
        locationmode='ISO-3',
        marker_line_color='#0d1117',
        marker_line_width=0.6,
        colorbar=dict(
            title=dict(text='1D %', font=dict(color='#64748b', size=11)),
            tickfont=dict(color='#64748b', size=10),
            len=0.55, thickness=10,
            x=1.01, y=0.5,
            bgcolor='rgba(11,15,26,0)',
            borderwidth=0,
            tickvals=[-3, -2, -1, 0, 1, 2, 3],
            ticktext=['-3%', '-2%', '-1%', '0', '+1%', '+2%', '+3%'],
        ),
    ))

    # ── Overlay disputed Kashmir territories (India's claim line) ─────────────
    hover_txt = (
        f"<b>India (Jammu & Kashmir)</b><br>"
        f"Index: ^NSEI<br>"
        f"Change: {india_pct:+.2f}%" if india_pct is not None else
        "<b>India (Jammu & Kashmir)</b><br>No data"
    )
    for lons, lats, region_name in [
        (_AJK_LON, _AJK_LAT, 'Azad Kashmir'),
        (_GB_LON,  _GB_LAT,  'Gilgit-Baltistan'),
        (_AC_LON,  _AC_LAT,  'Aksai Chin'),
    ]:
        fig.add_trace(go.Scattergeo(
            lon=lons, lat=lats,
            mode='lines',
            fill='toself',
            fillcolor=kashmir_color,
            line=dict(color='#0d1117', width=0.5),
            hovertemplate=hover_txt + '<extra></extra>',
            showlegend=False,
        ))

    fig.update_layout(
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor='#1e293b',
            coastlinewidth=0.5,
            bgcolor='#080c14',
            landcolor='#161e2e',
            showocean=True,
            oceancolor='#080c14',
            showlakes=False,
            showrivers=False,
            showcountries=True,
            countrycolor='#0d1117',
            countrywidth=0.4,
            projection_type='natural earth',
            resolution=50,
        ),
        paper_bgcolor='#080c14',
        font_color='#94a3b8',
        margin=dict(l=0, r=40, t=4, b=0),
        height=460,
    )

    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    st.markdown(
        '<div style="font-size:11px;color:#374151;text-align:right;margin-top:-8px;">'
        'Colour = 1-day change &nbsp;·&nbsp; Grey = no tracked index &nbsp;·&nbsp; '
        'Boundaries per India\'s official claim</div>',
        unsafe_allow_html=True,
    )


# ─── DETAIL MODAL ────────────────────────────────────────────────────────────

@st.dialog("Index Detail", width="large")
def _show_detail(sym: str, name: str):
    st.markdown(f"#### {name} ({sym}) — Intraday")
    intraday = _fetch_intraday_all()
    prices = intraday.get(sym, [])
    if len(prices) < 2:
        st.info("No intraday data available for this symbol.")
    else:
        pct_chg = (prices[-1] / prices[0] - 1) * 100 if prices[0] else 0
        lc = '#22c55e' if pct_chg >= 0 else '#ef4444'
        fc = 'rgba(34,197,94,0.07)' if pct_chg >= 0 else 'rgba(239,68,68,0.07)'
        fig = go.Figure(go.Scatter(
            y=prices, mode='lines',
            line=dict(color=lc, width=2),
            fill='tozeroy', fillcolor=fc,
        ))
        fig.update_layout(
            plot_bgcolor='#0f1117', paper_bgcolor='#0f1117',
            font_color='#cbd5e0', height=280, showlegend=False,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis=dict(gridcolor='#1a2236', showgrid=True, showline=False),
            yaxis=dict(gridcolor='#1a2236', showgrid=True, showline=False),
        )
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    quotes, _ = _fetch_quotes()
    q = quotes.get(sym)
    if q:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Price",      _fmt_price(q['price'], sym))
        c2.metric("Change",     f"{q['change']:+.2f}")
        c3.metric("Change %",   f"{q['pct']:+.2f}%")
        c4.metric("Prev Close", _fmt_price(q['prev'],  sym))


# ─── CHART HORIZON CONFIG + FETCH ────────────────────────────────────────────

HORIZON_OPTIONS = {
    '1D': ('1d',  '5m'),
    '5D': ('5d',  '30m'),
    '1M': ('1mo', '1d'),
    '3M': ('3mo', '1d'),
    '6M': ('6mo', '1d'),
    '1Y': ('1y',  '1d'),
    '5Y': ('5y',  '1wk'),
}


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_chart_data(sym: str, period: str, interval: str):
    """Fetch OHLCV for a single symbol. Returns (timestamps_list, closes_list)."""
    try:
        df = yf.Ticker(sym).history(period=period, interval=interval, auto_adjust=True)
        if df.empty:
            return [], []
        df = df.dropna(subset=['Close'])
        timestamps = df.index.tolist()   # list of Timestamps
        closes     = [float(v) for v in df['Close']]
        return timestamps, closes
    except Exception as e:
        logger.error(f"Chart fetch failed for {sym} ({period}/{interval}): {e}")
        return [], []


# ─── MAIN ────────────────────────────────────────────────────────────────────

def render_global_markets_tab():
    """Entry point called from app.py."""

    # ── Inject tab-scoped CSS ──
    st.markdown("""
    <style>
      /* Remove top padding inside this tab's columns for card layout */
      div[data-testid="column"] > div[data-testid="stVerticalBlock"] {
        gap: 0.5rem;
      }

      /* ── Region expander styling ── */
      div[data-testid="stExpander"] > details > summary {
        background: #0b0f1a !important;
        border: 1px solid #1e2535 !important;
        border-radius: 8px !important;
        padding: 10px 16px !important;
        font-size: 14px !important;
        font-weight: 700 !important;
        color: #e2e8f0 !important;
        transition: background 0.2s ease, border-color 0.2s ease !important;
      }
      div[data-testid="stExpander"] > details[open] > summary {
        border-radius: 8px 8px 0 0 !important;
        border-bottom-color: #0b0f1a !important;
        background: #111827 !important;
      }
      div[data-testid="stExpander"] > details > summary:hover {
        background: #111827 !important;
        border-color: #2d3f5e !important;
      }
      div[data-testid="stExpander"] > details > div[data-testid="stExpanderDetails"] {
        background: #0b0f1a !important;
        border: 1px solid #1e2535 !important;
        border-top: none !important;
        border-radius: 0 0 8px 8px !important;
        padding: 12px 8px !important;
      }

      /* ── Card fade-in animation ── */
      @keyframes gmFadeUp {
        from { opacity: 0; transform: translateY(10px); }
        to   { opacity: 1; transform: translateY(0); }
      }

      /* ── OPEN badge pulse ── */
      @keyframes gmPulse {
        0%, 100% { opacity: 1; }
        50%       { opacity: 0.55; }
      }

      /* ── Card hover glow ── */
      @keyframes gmHoverGlow {
        from { box-shadow: 0 0 0 0 rgba(99,102,241,0); }
        to   { box-shadow: 0 0 12px 2px rgba(99,102,241,0.18); }
      }

      .gm-card {
        animation: gmFadeUp 0.35s ease both;
        transition: transform 0.18s ease, box-shadow 0.18s ease !important;
      }
      .gm-card:hover {
        transform: translateY(-3px) !important;
        box-shadow: 0 6px 24px rgba(0,0,0,0.45) !important;
        border-color: #2d3f5e !important;
      }
      .gm-badge-open {
        animation: gmPulse 2s ease-in-out infinite;
      }

      /* ── Timeline NOW line pulse ── */
      @keyframes gmNowPulse {
        0%, 100% { opacity: 1; }
        50%       { opacity: 0.4; }
      }
      .gm-now-line { animation: gmNowPulse 1.4s ease-in-out infinite; }
    </style>
    """, unsafe_allow_html=True)

    # ── Auto-refresh ──
    refresh_count = 0
    if _HAS_AUTOREFRESH:
        refresh_count = st_autorefresh(interval=300_000, key="gm_autorefresh")

    if refresh_count != st.session_state.get('gm_prev_count', -1) \
            or 'gm_last_fetch' not in st.session_state:
        st.session_state['gm_prev_count'] = refresh_count
        st.session_state['gm_last_fetch'] = datetime.now()

    elapsed_s = int((datetime.now() - st.session_state['gm_last_fetch']).total_seconds())

    # ── Fetch data (both calls are cached) ──
    quotes, _  = _fetch_quotes()
    intraday   = _fetch_intraday_all()

    # Build structured list
    structured = [
        {
            **r,
            'status': _region_status(r),
            'data': [
                {'sym': i['sym'], 'name': i['name'], 'short': i['short'],
                 'q': quotes.get(i['sym'])}
                for i in r['indices']
            ],
        }
        for r in REGIONS
    ]

    n_regions = len([r for r in structured if r['id'] != 'cross_asset'])
    n_indices = sum(len(r['data']) for r in structured if r['id'] != 'cross_asset')

    # ── HEADER ──
    col_h, col_ctrl = st.columns([5, 4])
    with col_h:
        st.markdown(
            f'<div style="margin-bottom:2px;">'
            f'<span style="font-size:22px;font-weight:700;color:#f1f5f9;'
            f'letter-spacing:-0.02em;">Global Markets</span></div>'
            f'<div style="font-size:12px;color:#374151;">'
            f'15-20 min delayed &nbsp;·&nbsp; {n_regions} regions &nbsp;·&nbsp; {n_indices} indices'
            f'</div>',
            unsafe_allow_html=True,
        )

    with col_ctrl:
        dot    = '<span style="color:#22c55e;font-size:9px;">●</span>&nbsp;' if _HAS_AUTOREFRESH else ''
        e_str  = (f"{elapsed_s}s ago" if elapsed_s < 60
                  else f"{elapsed_s // 60}m {elapsed_s % 60}s ago")
        view   = st.session_state.get('gm_view', 'Cards')

        ctrl_left, ctrl_mid, ctrl_right_a, ctrl_right_b = st.columns([3, 1, 1, 1.4])

        with ctrl_left:
            st.markdown(
                f'<div style="padding-top:10px;font-size:12px;color:#4a5568;">'
                f'{dot}{e_str}</div>',
                unsafe_allow_html=True,
            )

        with ctrl_mid:
            if st.button("↺", key="gm_refresh", help="Refresh data now",
                         use_container_width=True):
                _fetch_quotes.clear()
                _fetch_intraday_all.clear()
                st.session_state['gm_last_fetch'] = datetime.now()
                st.rerun()

        with ctrl_right_a:
            if st.button(
                "Index",
                key="gm_view_cards",
                type="primary" if view == 'Cards' else "secondary",
                use_container_width=True,
            ):
                st.session_state['gm_view'] = 'Cards'
                st.rerun()

        with ctrl_right_b:
            if st.button(
                "Heatmap",
                key="gm_view_heatmap",
                type="primary" if view == 'Heatmap' else "secondary",
                use_container_width=True,
            ):
                st.session_state['gm_view'] = 'Heatmap'
                st.rerun()

    # ── SESSION TIMELINE ──
    st.markdown(_html_timeline(), unsafe_allow_html=True)

    # ── OVERNIGHT FUTURES BAR ──
    futures_html = _html_futures(quotes)
    if futures_html:
        st.markdown(futures_html, unsafe_allow_html=True)

    # ── OVERVIEW / STATS BAR ──
    st.markdown(_html_overview(structured, quotes, elapsed_s), unsafe_allow_html=True)

    st.divider()

    # ── CARD VIEW or HEATMAP ──
    if view == 'Heatmap':
        _render_heatmap(quotes)
    else:
        # Build symbol list for chart selector (regions + commodities + crypto)
        all_idx = (
            [(i['sym'], f"{r['flag']} {i['short']} — {i['name']}")
             for r in structured if r['id'] != 'cross_asset'
             for i in r['data']]
            + [(c['sym'], f"🛢️ {c['short']} — {c['name']}") for c in COMMODITIES]
            + [(c['sym'], f"₿  {c['short']} — {c['name']}") for c in CRYPTO]
        )
        syms, labels = zip(*all_idx) if all_idx else ([], [])

        # Render all region card grids
        for region_data in structured:
            if region_data['id'] == 'cross_asset':
                continue
            _render_region(region_data, intraday)
            st.markdown('<div style="margin-bottom:4px;"></div>', unsafe_allow_html=True)

        # ── COMMODITIES ──
        _render_asset_section(
            title='Major Commodities',
            flag='🛢️',
            items=COMMODITIES,
            quotes=quotes,
            intraday=intraday,
            status=_asset_status(always_open=False),
            expanded=False,
            note='Futures prices · CME/COMEX · 10-15 min delayed',
        )
        st.markdown('<div style="margin-bottom:4px;"></div>', unsafe_allow_html=True)

        # ── GLOBAL BONDS ──
        _render_asset_section(
            title='Global Bonds',
            flag='🏦',
            items=BONDS,
            quotes=quotes,
            intraday=intraday,
            status=_asset_status(always_open=False),
            expanded=False,
            note='Yields in % · US bonds via CBOE; India bonds may show — if unavailable on Yahoo Finance',
        )
        st.markdown('<div style="margin-bottom:4px;"></div>', unsafe_allow_html=True)

        # ── CRYPTO ──
        _render_asset_section(
            title='Crypto',
            flag='₿',
            items=CRYPTO,
            quotes=quotes,
            intraday=intraday,
            status=_asset_status(always_open=True),
            expanded=False,
            note='USD prices · 24 / 7 · near real-time',
        )
        st.markdown('<div style="margin-bottom:4px;"></div>', unsafe_allow_html=True)

        # ── CHART SECTION ──
        st.divider()

        chart_sym_col, chart_hz_col = st.columns([5, 4])
        with chart_sym_col:
            st.markdown(
                '<span style="font-size:11px;font-weight:700;color:#4a5568;'
                'letter-spacing:0.08em;text-transform:uppercase;">View Chart</span>',
                unsafe_allow_html=True,
            )
            chosen = st.selectbox(
                "Select symbol", options=list(syms),
                format_func=lambda s: dict(zip(syms, labels)).get(s, s),
                key="gm_chart_select", label_visibility="collapsed",
            )
        with chart_hz_col:
            st.markdown(
                '<span style="font-size:11px;font-weight:700;color:#4a5568;'
                'letter-spacing:0.08em;text-transform:uppercase;">Horizon</span>',
                unsafe_allow_html=True,
            )
            horizon = st.pills(
                "Horizon", options=list(HORIZON_OPTIONS.keys()),
                default='1M', key="gm_chart_horizon",
                label_visibility="collapsed",
            )
        horizon = horizon or '1M'

        if chosen:
            period, interval = HORIZON_OPTIONS[horizon]
            timestamps, prices = _fetch_chart_data(chosen, period, interval)
            chosen_q = quotes.get(chosen)

            if len(prices) >= 2:
                pct_chg = (prices[-1] / prices[0] - 1) * 100 if prices[0] else 0
                lc = '#22c55e' if pct_chg >= 0 else '#ef4444'
                fc = ('rgba(34,197,94,0.06)' if pct_chg >= 0
                      else 'rgba(239,68,68,0.06)')

                # Tight y-range so price moves fill the chart
                y_lo = min(prices) * 0.997
                y_hi = max(prices) * 1.003

                # x-axis tick format depends on horizon
                if horizon == '1D':
                    tick_fmt = '%H:%M'
                elif horizon in ('5D', '1M'):
                    tick_fmt = '%b %d'
                elif horizon in ('3M', '6M'):
                    tick_fmt = '%b %d'
                else:
                    tick_fmt = '%b %Y'

                # y-axis number format
                y_max_val = max(prices)
                y_fmt = (',.0f' if y_max_val >= 100
                         else '.2f'  if y_max_val >= 1
                         else '.4f')

                fig = go.Figure(go.Scatter(
                    x=timestamps, y=prices,
                    mode='lines',
                    line=dict(color=lc, width=1.8),
                    fill='tozeroy', fillcolor=fc,
                    hovertemplate='<b>%{y:,.4g}</b>  %{x}<extra></extra>',
                ))
                fig.update_layout(
                    plot_bgcolor='#0b0f1a', paper_bgcolor='#0b0f1a',
                    font_color='#94a3b8', height=320, showlegend=False,
                    margin=dict(l=10, r=10, t=10, b=10),
                    hovermode='x unified',
                    xaxis=dict(
                        showgrid=False, showline=False, zeroline=False,
                        tickformat=tick_fmt,
                        tickfont=dict(size=10, color='#475569'),
                        nticks=8,
                    ),
                    yaxis=dict(
                        showgrid=False, showline=False, zeroline=False,
                        range=[y_lo, y_hi],
                        tickformat=y_fmt,
                        tickfont=dict(size=10, color='#475569'),
                        side='right',
                    ),
                )
                st.plotly_chart(fig, use_container_width=True,
                                config={'displayModeBar': False})

                # ── Horizon-aware metrics ──
                cur_price  = prices[-1]
                start_price = prices[0]
                period_high = max(prices)
                period_low  = min(prices)
                chg_sign    = '+' if pct_chg >= 0 else ''

                m1, m2, m3, m4 = st.columns(4)
                m1.metric(
                    "Current Price",
                    _fmt_price(cur_price, chosen),
                )
                m2.metric(
                    f"{horizon} Return",
                    f"{chg_sign}{pct_chg:.2f}%",
                    delta=f"{chg_sign}{cur_price - start_price:,.4g}",
                )
                m3.metric(
                    f"{horizon} High",
                    _fmt_price(period_high, chosen),
                )
                m4.metric(
                    f"{horizon} Low",
                    _fmt_price(period_low, chosen),
                )
            else:
                st.info(f"No data available for **{chosen}** over the **{horizon}** horizon.")

    # ── FOOTER ──
    ist = _ist_now()
    note = "Auto-refresh: 5 min" if _HAS_AUTOREFRESH else "pip install streamlit-autorefresh for live auto-refresh"
    st.markdown(
        f'<div style="margin-top:1.2rem;font-size:10px;color:#1f2937;text-align:right;">'
        f'Data: Yahoo Finance · 15-20 min delayed · '
        f'{ist.strftime("%d %b %Y %H:%M IST")} · {note}'
        f'</div>',
        unsafe_allow_html=True,
    )
