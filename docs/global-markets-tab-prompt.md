# Claude Code Prompt — Add Global Equity Markets Tab to India Macro Dashboard

Paste everything below this line into Claude Code (in the same session where the dashboard is already built):

---

Add a new **Tab 8: Global Markets** to the existing India Macro Dashboard. This tab should provide a real-time overview of every major equity index across the world, organized by region and market session, with 15-20 minute delayed data. This is used by an Indian equity research analyst who needs to see how global markets are moving to assess impact on Indian markets (Nifty/Sensex).

## CORE REQUIREMENTS

1. **Live/delayed index prices** (15-20 min delay is acceptable) for 50+ global indices
2. **Organized by geography** — not just a flat list, but grouped into meaningful regions
3. **Market session awareness** — clearly show which markets are OPEN right now, which are CLOSED, and which are in PRE/POST market
4. **Auto-refresh** every 60 seconds without full page reload (silent background fetch, UI updates smoothly)
5. **World heatmap** — a visual geographic map where countries are colored by how their market is performing today
6. **India-centric context** — always show Indian indices (Nifty 50, Sensex, Nifty Bank, Nifty IT, Nifty Midcap) at the top since that is home market

## DATA SOURCE STRATEGY

Use **multiple free data sources** in priority order. The backend should try them in sequence and use whichever responds:

### Primary: Yahoo Finance API (unofficial)
```
https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}?interval=1d&range=1d
https://query1.finance.yahoo.com/v7/finance/quote?symbols={SYMBOL1},{SYMBOL2},{SYMBOL3}
```
- The v7 quote endpoint accepts comma-separated symbols (batch up to 20 per request)
- Returns: price, change, changePercent, marketState (PRE/REGULAR/POST/CLOSED), regularMarketTime
- Yahoo symbols for indices use `^` prefix: `^GSPC` (S&P 500), `^DJI` (Dow), `^NSEI` (Nifty 50), `^BSESN` (Sensex), etc.
- **This is the most reliable free source.** Prioritize this.

### Secondary: Google Finance (scrape)
```
https://www.google.com/finance/quote/{SYMBOL}:{EXCHANGE}
```
- Scrape the price, change, and change% from the page using cheerio
- Use as fallback when Yahoo is rate-limited

### Tertiary: Twelve Data API (free tier)
```
https://api.twelvedata.com/quote?symbol={SYMBOL}&apikey={KEY}
```
- Free tier: 800 requests/day, 8 requests/minute
- Sign up at twelvedata.com for free API key
- Good for real-time forex and some indices

### Quaternary: Alpha Vantage (already connected as MCP)
- You already have Alpha Vantage connected — use it as a final fallback
- `GLOBAL_QUOTE` endpoint for individual index quotes

### For historical intraday charts:
```
Yahoo Finance: https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}?interval=5m&range=1d
```
- Returns 5-minute candles for the current trading day
- Use this to build the mini sparkline charts on each index card

## COMPLETE INDEX LIST

### 🇮🇳 India (Home Market — Always show first, prominently)
| Index | Yahoo Symbol | Description |
|---|---|---|
| Nifty 50 | `^NSEI` | Benchmark large-cap |
| BSE Sensex | `^BSESN` | BSE 30 benchmark |
| Nifty Bank | `^NSEBANK` | Banking sector index |
| Nifty IT | `NIFTYIT.NS` | IT sector index |
| Nifty Midcap 150 | `NIFTYMIDCAP150.NS` | Mid-cap benchmark |
| Nifty Smallcap 250 | `NIFTYSMLCAP250.NS` | Small-cap benchmark |
| India VIX | `^INDIAVIX` | Volatility/fear gauge |

### 🇺🇸 United States
| Index | Yahoo Symbol | Description |
|---|---|---|
| S&P 500 | `^GSPC` | US large-cap benchmark |
| Dow Jones Industrial | `^DJI` | 30 blue-chip stocks |
| Nasdaq Composite | `^IXIC` | Tech-heavy benchmark |
| Nasdaq 100 | `^NDX` | Top 100 non-financial Nasdaq stocks |
| Russell 2000 | `^RUT` | US small-cap benchmark |
| S&P 500 VIX | `^VIX` | Fear index |
| S&P 500 Futures | `ES=F` | Overnight futures (critical for India opening) |
| Nasdaq Futures | `NQ=F` | Tech futures |
| Dow Futures | `YM=F` | Dow futures |
| Philadelphia Semiconductor | `^SOX` | Semiconductor index |

### 🇪🇺 Europe
| Index | Yahoo Symbol | Description |
|---|---|---|
| EURO STOXX 50 | `^STOXX50E` | Eurozone blue-chip |
| FTSE 100 | `^FTSE` | UK benchmark |
| DAX 40 | `^GDAXI` | Germany benchmark |
| CAC 40 | `^FCHI` | France benchmark |
| IBEX 35 | `^IBEX` | Spain benchmark |
| FTSE MIB | `FTSEMIB.MI` | Italy benchmark |
| AEX | `^AEX` | Netherlands benchmark |
| Swiss Market Index | `^SSMI` | Switzerland benchmark |
| STOXX 600 | `^STOXX` | Broad European index |

### 🇨🇳 China & Hong Kong
| Index | Yahoo Symbol | Description |
|---|---|---|
| Shanghai Composite | `000001.SS` | Mainland China A-shares |
| Shenzhen Composite | `399001.SZ` | Shenzhen benchmark |
| CSI 300 | `000300.SS` | Top 300 A-shares |
| Hang Seng | `^HSI` | Hong Kong benchmark |
| Hang Seng Tech | `^HSTECH` | HK tech index |

### 🇯🇵 Japan
| Index | Yahoo Symbol | Description |
|---|---|---|
| Nikkei 225 | `^N225` | Japan benchmark |
| TOPIX | `^TOPX` | Broad Tokyo index |

### 🇰🇷 South Korea
| Index | Yahoo Symbol | Description |
|---|---|---|
| KOSPI | `^KS11` | Korea benchmark |
| KOSDAQ | `^KQ11` | Korea tech/growth |

### 🇹🇼 Taiwan
| Index | Yahoo Symbol | Description |
|---|---|---|
| TAIEX | `^TWII` | Taiwan benchmark (TSMC heavyweight) |

### 🇦🇺 Australia
| Index | Yahoo Symbol | Description |
|---|---|---|
| ASX 200 | `^AXJO` | Australia benchmark |

### 🇸🇬 Singapore
| Index | Yahoo Symbol | Description |
|---|---|---|
| Straits Times | `^STI` | Singapore benchmark |

### 🇮🇩 Indonesia
| Index | Yahoo Symbol | Description |
|---|---|---|
| Jakarta Composite | `^JKSE` | Indonesia benchmark |

### 🇧🇷 Brazil
| Index | Yahoo Symbol | Description |
|---|---|---|
| Bovespa | `^BVSP` | Brazil benchmark |

### 🇨🇦 Canada
| Index | Yahoo Symbol | Description |
|---|---|---|
| S&P/TSX Composite | `^GSPTSE` | Canada benchmark |

### 🇷🇺 Russia
| Index | Yahoo Symbol | Description |
|---|---|---|
| MOEX | `IMOEX.ME` | Moscow Exchange index |

### 🇸🇦 Saudi Arabia
| Index | Yahoo Symbol | Description |
|---|---|---|
| Tadawul All Share | `^TASI` | Saudi benchmark |

### 🔵 Global/Thematic
| Index | Yahoo Symbol | Description |
|---|---|---|
| MSCI World | `URTH` | Global developed markets ETF proxy |
| MSCI Emerging Markets | `EEM` | EM benchmark ETF proxy |
| US Dollar Index (DXY) | `DX-Y.NYB` | Dollar strength |
| Gold Futures | `GC=F` | Gold price |
| Brent Crude | `BZ=F` | Oil benchmark |
| US 10Y Treasury Yield | `^TNX` | US bond yield |
| Bitcoin | `BTC-USD` | Crypto benchmark |

## BACKEND IMPLEMENTATION

### New files:
```
/server
  /routes
    markets.js              — Express routes for market data
  /scrapers
    yahoo-finance.js        — Yahoo Finance batch quote fetcher
    google-finance.js       — Google Finance scraper (fallback)
    market-session.js       — Market open/close time logic for all exchanges
  /config
    indices-config.js       — Master config: all indices, symbols, regions, exchange info
  /cache
    markets-cache.json      — Latest prices cache (updated every 60 sec)
    intraday-cache/         — Folder with per-index 1-day chart data
  /fallback-data
    markets.json            — Static snapshot of all indices for offline use
```

### indices-config.js structure:
```javascript
module.exports = {
  regions: [
    {
      id: 'india',
      name: 'India',
      flag: '🇮🇳',
      isHome: true,  // always show first
      exchanges: [
        {
          name: 'NSE/BSE',
          timezone: 'Asia/Kolkata',
          openTime: '09:15',
          closeTime: '15:30',
          tradingDays: [1,2,3,4,5], // Mon-Fri
        }
      ],
      indices: [
        { symbol: '^NSEI', name: 'Nifty 50', shortName: 'NIFTY', description: 'Benchmark large-cap' },
        { symbol: '^BSESN', name: 'Sensex', shortName: 'SENSEX', description: 'BSE 30 benchmark' },
        // ... all India indices
      ]
    },
    {
      id: 'us',
      name: 'United States',
      flag: '🇺🇸',
      exchanges: [
        {
          name: 'NYSE/NASDAQ',
          timezone: 'America/New_York',
          openTime: '09:30',
          closeTime: '16:00',
          preMarketOpen: '04:00',
          afterHoursClose: '20:00',
          tradingDays: [1,2,3,4,5],
        }
      ],
      indices: [ /* ... */ ]
    },
    // ... all regions
  ]
};
```

### yahoo-finance.js:
```javascript
// Batch fetch up to 20 symbols per request
// GET https://query1.finance.yahoo.com/v7/finance/quote?symbols=^GSPC,^DJI,^IXIC,...
// Parse response for each symbol:
//   regularMarketPrice, regularMarketChange, regularMarketChangePercent,
//   regularMarketPreviousClose, regularMarketOpen, regularMarketDayHigh,
//   regularMarketDayLow, regularMarketVolume, marketState
// Batch all 50+ indices into 3 API calls (20 symbols each)
// Implement retry with exponential backoff if rate-limited (HTTP 429)
```

### API endpoints:
```
GET /api/markets/quotes
  Returns all index quotes with latest prices, change, change%, market state
  Response: { lastUpdated, regions: [{ id, name, flag, indices: [{...quote data}] }] }

GET /api/markets/chart/:symbol
  Returns intraday 5-min chart data for a specific index
  Query params: ?range=1d (default) | 5d | 1mo | 3mo | 1y
  Response: { symbol, timestamps: [...], prices: [...], volume: [...] }

GET /api/markets/session-status
  Returns which markets are currently open/closed/pre/post
  Response: { sessions: [{ region, exchange, status, nextOpen, nextClose }] }
```

### Auto-refresh logic:
- Backend fetches all quotes every 60 seconds during Indian market hours (9:00 AM — 11:30 PM IST, covering India + Europe + US sessions)
- During off-hours (11:30 PM — 6:00 AM IST), fetch every 5 minutes (only Asia-Pacific markets are active)
- Frontend polls `/api/markets/quotes` every 60 seconds using `setInterval`, updates state without re-mounting components
- Price changes should animate (flash green on uptick, red on downtick, then fade back)

## FRONTEND IMPLEMENTATION

### New files:
```
/src
  /tabs
    MarketsTab.jsx           — Main markets tab
  /components
    /markets
      MarketSessionBar.jsx   — Shows timeline of global sessions (which are open now)
      RegionSection.jsx      — Collapsible region group with its indices
      IndexCard.jsx          — Individual index card with price + sparkline
      WorldHeatmap.jsx       — Geographic heatmap visualization
      MarketOverview.jsx     — Summary bar: global gainers/losers count, best/worst performers
      IndexDetailModal.jsx   — Popup when clicking an index: full chart + details
      SparklineChart.jsx     — Tiny inline chart (50px tall) showing today's price movement
      FuturesBar.jsx         — Prominent bar showing US/EU futures (critical for pre-market India context)
```

### MarketsTab.jsx layout (top to bottom):

---

#### Section 1: Global Session Timeline Bar
A horizontal timeline showing the current time (IST) and overlaid colored bars for each market's trading session:
```
|----Sydney----|     |---Tokyo/HK---|     |---India---|     |---Europe---|     |-----US-----|
04:00          10:00              15:30              19:00             22:00            04:30
                                    ▲ NOW (IST)
```
- Color the bars: GREEN if currently open, GRAY if closed, YELLOW if in pre/post market
- Show a vertical red line for "NOW" (current IST time)
- This instantly tells the analyst: "Right now, India is closed, Europe is open, US is in pre-market"

---

#### Section 2: Futures Bar (only visible when Indian market is closed)
When Indian markets are closed (after 3:30 PM IST or before 9:15 AM), show a prominent bar:
```
┌─────────────────────────────────────────────────────────────┐
│  🌙 OVERNIGHT FUTURES    S&P 500: +0.3%  |  Nasdaq: +0.5%  |  Dow: +0.2%  |  SGX Nifty: +0.4%  │
└─────────────────────────────────────────────────────────────┘
```
This is the most important thing an Indian analyst checks before market open — where are the US futures and SGX Nifty pointing.

Try to include SGX Nifty if Yahoo Finance has it (symbol: `SGX_NIFTY` or similar). If not available via free APIs, skip it.

---

#### Section 3: Market Overview Summary
A single row of summary stats:
```
Global Markets:  🟢 32 Up  🔴 18 Down  ⚪ 3 Unchanged  |  Best: KOSDAQ +2.4%  |  Worst: Hang Seng -1.8%  |  DXY: 104.2 (+0.1%)  |  Brent: $82.4 (-0.3%)  |  Gold: $2,340 (+0.5%)
```

---

#### Section 4: View Toggle
Two view modes (toggle buttons):
- **Card View** (default): Index cards in a grid layout
- **Heatmap View**: World map colored by performance

---

#### Section 5A: Card View (default)

Organized by region. Each region is a collapsible section:

```
🇮🇳 INDIA (Home Market)                                              [Market Open 🟢]
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ NIFTY 50     │  │ SENSEX       │  │ BANK NIFTY   │  │ NIFTY IT     │
│ 23,456.70    │  │ 77,234.50    │  │ 49,876.30    │  │ 38,234.10    │
│ ▲ +1.23%     │  │ ▲ +1.15%     │  │ ▼ -0.34%     │  │ ▲ +2.10%     │
│ ~~sparkline~~│  │ ~~sparkline~~│  │ ~~sparkline~~│  │ ~~sparkline~~│
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘

🇺🇸 UNITED STATES                                                    [Pre-Market 🟡]
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ S&P 500      │  │ NASDAQ       │  │ DOW JONES    │  │ RUSSELL 2000 │
│ 5,234.56     │  │ 16,789.12    │  │ 39,456.78    │  │ 2,045.67     │
│ ▲ +0.45%     │  │ ▲ +0.67%     │  │ ▲ +0.23%     │  │ ▼ -0.12%     │
│ ~~sparkline~~│  │ ~~sparkline~~│  │ ~~sparkline~~│  │ ~~sparkline~~│
└──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘

🇪🇺 EUROPE ...
🇨🇳 CHINA & HONG KONG ...
🇯🇵 JAPAN ...
(... all regions)
```

**Each IndexCard contains:**
- Index name (bold, top)
- Current price (large font, monospace)
- Change amount and change % (green if positive, red if negative, with ▲/▼ arrow)
- A tiny sparkline chart (50px tall, fills card width) showing today's intraday price movement
- Market state badge: 🟢 Open, 🔴 Closed, 🟡 Pre-Market, 🟠 After-Hours
- Previous close price in small muted text
- **On click:** Opens IndexDetailModal with a larger interactive chart (line chart with 1D/5D/1M/3M/1Y range selector, OHLC data, volume bars below)

**Card grid layout:**
- India section: 4 columns on desktop (since it is home market, give more space)
- Other regions: 4-5 columns on desktop
- 2 columns on tablet, 1 column on mobile

**Price animation:**
When a price updates on auto-refresh:
- If price went UP from last update: briefly flash the price text green (0.5s), then fade back to normal
- If price went DOWN: briefly flash red
- This gives a live-ticker feel without being distracting

---

#### Section 5B: Heatmap View (toggle)

A **world map** (use a simple SVG world map or react-simple-maps) where each country/region is colored based on its primary index's performance today:
- Dark green: > +2%
- Light green: +0.5% to +2%
- Neutral gray: -0.5% to +0.5%
- Light red: -0.5% to -2%
- Dark red: < -2%

Hovering over a country shows a tooltip with: Country name, Primary index name, Current value, Change %.
Clicking a country scrolls to that region in the Card View.

This gives an instant visual read: "Asia is green, Europe is mixed, US futures are slightly red" — useful morning context.

---

#### Section 6: Asset Correlation Table (bottom of tab)

A small section at the bottom showing a simple **correlation/context table** — manually updated daily through the data fetch:

```
┌───────────────────────────────────────────────────────┐
│  CROSS-ASSET SNAPSHOT                                  │
│                                                        │
│  USD/INR: ₹83.45 (+0.1%)  │  Brent Crude: $82.4      │
│  US 10Y:  4.32% (+2bps)   │  Gold: $2,340 (+0.5%)    │
│  DXY:     104.2 (+0.1%)   │  Bitcoin: $67,890 (-1.2%) │
│  VIX:     14.5 (-0.3)     │  India VIX: 12.8 (+0.1)  │
└───────────────────────────────────────────────────────┘
```

This cross-asset bar is useful because as an equity analyst, I don't just look at indices — I need to see the dollar, oil, gold, and bond yields in the same view to understand what's driving markets.

## DESIGN SPECIFICATIONS

- Follow the existing dashboard theme (dark default, light toggle)
- **Dark theme cards:** Background #1a1d26, border 1px solid #2a2d36, hover border brightens to accent
- **Green prices:** #2ecc71 for positive, **Red prices:** #e74c3c for negative
- **Sparklines:** Single-color line, no axes, no labels — just the shape. Green line if index is up, red if down.
- **Monospace for all numbers:** JetBrains Mono — prices, changes, percentages
- **Region headers:** Flag emoji + region name in bold, with market status badge on the right
- **Auto-refresh indicator:** Small pulsing green dot in the tab header or top bar, with "Last updated: 2 sec ago" that counts up and resets on each refresh

## ERROR HANDLING

1. If Yahoo Finance returns 429 (rate limit): Wait 5 seconds, retry once, then fall back to Google Finance scraper
2. If all live sources fail: Serve cached data from markets-cache.json with a yellow banner "Showing data from X minutes ago — live feed temporarily unavailable"
3. If a specific index symbol returns no data: Show the card with "—" for price and a muted "Unavailable" label. Don't hide the card.
4. If intraday chart data fails: Show the card without sparkline (just price + change)
5. On fresh install with no cache: Load fallback-data/markets.json which should contain a realistic snapshot of all 50+ indices

## PERFORMANCE OPTIMIZATION

- **Batch Yahoo API calls:** Fetch all symbols in 3 requests (20 symbols each), NOT 50+ individual requests
- **Stagger chart fetches:** Don't fetch all 50+ intraday charts at once. Fetch chart data only when a card is visible in viewport (intersection observer) or when the user clicks a card for the detail modal
- **Memoize components:** Use React.memo for IndexCard since most cards won't change on every refresh cycle (only the ones with live markets will have new data)
- **Debounce re-renders:** When new quote data arrives, batch all state updates into a single setState call

## INTEGRATION

- Add "Global Markets" as Tab 8 in the existing TabBar.jsx
- Add `/api/markets/*` routes to the existing server.js
- The market session timeline and futures bar components can optionally be shown on the dashboard header (across all tabs) if you can fit them without cluttering — otherwise, just keep them in the Markets tab.
- Share the same ThemeContext and design system as existing tabs.

Build this now. Start with the backend (indices-config → yahoo-finance fetcher → API routes → caching), then the frontend (IndexCard → RegionSection → MarketsTab → WorldHeatmap → IndexDetailModal). Get the Card View with live prices working first, then add the heatmap and detail modals.
