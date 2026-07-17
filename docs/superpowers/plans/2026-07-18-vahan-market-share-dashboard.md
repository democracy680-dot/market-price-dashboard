# Vahan Market-Share Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-command tool that scrapes the Government of India Vahan dashboard, stores monthly maker-wise registration data, and generates a self-contained interactive HTML dashboard showing which manufacturers are gaining/losing market share by vehicle category.

**Architecture:** A `vahan-dashboard/` folder with four focused packages — `scraper` (Playwright automation + HTML parser), `store` (SQLite persistence), `analysis` (pure market-share/delta math), and `report` (self-contained HTML generation) — wired together by a `vahan_refresh.py` orchestrator. Deterministic layers (store, analysis, report, parser) are TDD'd with no network; the live Playwright navigation is validated by a real smoke run.

**Tech Stack:** Python 3.11, Playwright (Chromium), BeautifulSoup4, SQLite (stdlib), pytest.

## Global Constraints

- Python 3.11.4 (already installed); no other runtime.
- All commands run with the working directory set to `vahan-dashboard/`.
- Month keys are strings in `YYYY-MM` format everywhere.
- Target month = **2026-06** (June 2026); MoM comparison = **2026-05**; YoY comparison = **2025-06**.
- All-India totals only — no state/RTO/fuel/class breakdown in v1.
- The generated HTML must be fully self-contained: all CSS/JS inline, **no CDN or external requests**.
- **Never fabricate data.** A missing comparison month renders `—`, and a failed/empty scrape must never overwrite good stored data or a good `dashboard.html`.
- Shared row type is `SaleRow(month, category, maker, units)` from `models.py`.

---

### Task 1: Project scaffold, dependencies, shared model, and SQLite store

**Files:**
- Create: `vahan-dashboard/requirements.txt`
- Create: `vahan-dashboard/.gitignore`
- Create: `vahan-dashboard/conftest.py`
- Create: `vahan-dashboard/models.py`
- Create: `vahan-dashboard/store/__init__.py`
- Create: `vahan-dashboard/store/db.py`
- Test: `vahan-dashboard/tests/test_db.py`

**Interfaces:**
- Produces: `models.SaleRow` = `namedtuple("SaleRow", ["month", "category", "maker", "units"])`.
- Produces: `store.db.open_db(path) -> sqlite3.Connection`, `init_schema(conn) -> None`, `upsert_sales(conn, rows: Iterable[SaleRow]) -> int` (returns count written), `get_rows_for_month(conn, month: str) -> list[SaleRow]`, `has_month(conn, month: str) -> bool`.

- [ ] **Step 1: Create dependency and ignore files**

`vahan-dashboard/requirements.txt`:
```
playwright==1.48.0
beautifulsoup4==4.12.3
pytest==8.3.3
```

`vahan-dashboard/.gitignore`:
```
store/vahan.db
__pycache__/
*.pyc
.pytest_cache/
```

`vahan-dashboard/conftest.py` (empty file — makes the folder the pytest rootdir so `models`, `store`, etc. import cleanly):
```python
# Intentionally empty: marks vahan-dashboard/ as the pytest rootdir.
```

- [ ] **Step 2: Install dependencies**

Run (from `vahan-dashboard/`):
```bash
pip install -r requirements.txt
python -m playwright install chromium
```
Expected: pip reports the three packages installed; Playwright downloads Chromium successfully.

- [ ] **Step 3: Create the shared model**

`vahan-dashboard/models.py`:
```python
from collections import namedtuple

# One row of registration data: month "YYYY-MM", category label, maker name, unit count.
SaleRow = namedtuple("SaleRow", ["month", "category", "maker", "units"])
```

- [ ] **Step 4: Write the failing store test**

`vahan-dashboard/store/__init__.py`: (empty file)

`vahan-dashboard/tests/test_db.py`:
```python
from models import SaleRow
from store import db


def _conn(tmp_path):
    conn = db.open_db(str(tmp_path / "t.db"))
    db.init_schema(conn)
    return conn


def test_upsert_and_read_back(tmp_path):
    conn = _conn(tmp_path)
    rows = [
        SaleRow("2026-06", "TWO WHEELER", "HERO", 100),
        SaleRow("2026-06", "TWO WHEELER", "HONDA", 80),
    ]
    written = db.upsert_sales(conn, rows)
    assert written == 2
    got = db.get_rows_for_month(conn, "2026-06")
    assert set(got) == set(rows)


def test_upsert_is_idempotent(tmp_path):
    conn = _conn(tmp_path)
    row = SaleRow("2026-06", "TWO WHEELER", "HERO", 100)
    db.upsert_sales(conn, [row])
    db.upsert_sales(conn, [SaleRow("2026-06", "TWO WHEELER", "HERO", 150)])
    got = db.get_rows_for_month(conn, "2026-06")
    assert got == [SaleRow("2026-06", "TWO WHEELER", "HERO", 150)]


def test_has_month(tmp_path):
    conn = _conn(tmp_path)
    assert db.has_month(conn, "2026-06") is False
    db.upsert_sales(conn, [SaleRow("2026-06", "TWO WHEELER", "HERO", 100)])
    assert db.has_month(conn, "2026-06") is True
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `python -m pytest tests/test_db.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'store.db'` (or `db` attribute missing).

- [ ] **Step 6: Implement the store**

`vahan-dashboard/store/db.py`:
```python
import sqlite3
from typing import Iterable

from models import SaleRow


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            month      TEXT NOT NULL,
            category   TEXT NOT NULL,
            maker      TEXT NOT NULL,
            units      INTEGER NOT NULL,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (month, category, maker)
        )
        """
    )
    conn.commit()


def upsert_sales(conn: sqlite3.Connection, rows: Iterable[SaleRow]) -> int:
    count = 0
    for r in rows:
        conn.execute(
            """
            INSERT INTO sales (month, category, maker, units)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(month, category, maker)
            DO UPDATE SET units = excluded.units, scraped_at = datetime('now')
            """,
            (r.month, r.category, r.maker, int(r.units)),
        )
        count += 1
    conn.commit()
    return count


def get_rows_for_month(conn: sqlite3.Connection, month: str) -> list[SaleRow]:
    cur = conn.execute(
        "SELECT month, category, maker, units FROM sales WHERE month = ? "
        "ORDER BY category, units DESC, maker",
        (month,),
    )
    return [SaleRow(row["month"], row["category"], row["maker"], row["units"]) for row in cur]


def has_month(conn: sqlite3.Connection, month: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sales WHERE month = ? LIMIT 1", (month,))
    return cur.fetchone() is not None
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `python -m pytest tests/test_db.py -v`
Expected: PASS (3 passed).

- [ ] **Step 8: Commit**

```bash
git add vahan-dashboard/requirements.txt vahan-dashboard/.gitignore vahan-dashboard/conftest.py vahan-dashboard/models.py vahan-dashboard/store vahan-dashboard/tests/test_db.py
git commit -m "feat(vahan): project scaffold, SaleRow model, and SQLite store"
```

---

### Task 2: Market-share and delta computation (pure, no I/O)

**Files:**
- Create: `vahan-dashboard/analysis/__init__.py`
- Create: `vahan-dashboard/analysis/compute.py`
- Test: `vahan-dashboard/tests/test_compute.py`

**Interfaces:**
- Consumes: `models.SaleRow`.
- Produces: `compute.compute_category_view(category: str, target_rows, mom_rows, yoy_rows) -> dict` with keys `category`, `total_units`, `makers` (list of maker dicts), `movers` (`{"gainers": [...], "losers": [...]}`). Each maker dict has: `maker`, `units`, `share_pct`, `mom_units_delta`, `mom_share_pp_delta`, `yoy_units_delta`, `yoy_share_pp_delta`, `rank`, `rank_change` (positive = moved up; `None` if no prior month). Deltas are `None` when the comparison month lacks that maker.
- Produces: `compute.build_dashboard_view(rows_by_month: dict[str, list[SaleRow]], target: str, mom: str, yoy: str, generated_at: str) -> dict` with keys `target_month`, `mom_month`, `yoy_month`, `generated_at`, `total_units`, `categories` (list of category views).

- [ ] **Step 1: Write the failing test**

`vahan-dashboard/analysis/__init__.py`: (empty file)

`vahan-dashboard/tests/test_compute.py`:
```python
from models import SaleRow
from analysis import compute

CAT = "TWO WHEELER"


def _rows(month, pairs):
    return [SaleRow(month, CAT, maker, units) for maker, units in pairs]


def test_shares_and_ranks():
    target = _rows("2026-06", [("HERO", 60), ("HONDA", 40)])
    view = compute.compute_category_view(CAT, target, [], [])
    assert view["total_units"] == 100
    hero, honda = view["makers"]
    assert hero["maker"] == "HERO" and hero["rank"] == 1 and hero["share_pct"] == 60.0
    assert honda["maker"] == "HONDA" and honda["rank"] == 2 and honda["share_pct"] == 40.0


def test_mom_and_yoy_deltas():
    target = _rows("2026-06", [("HERO", 60), ("HONDA", 40)])
    mom = _rows("2026-05", [("HERO", 50), ("HONDA", 50)])   # HERO 50% -> 60%
    yoy = _rows("2025-06", [("HERO", 70), ("HONDA", 30)])   # HERO 70% -> 60%
    view = compute.compute_category_view(CAT, target, mom, yoy)
    hero = view["makers"][0]
    assert hero["mom_units_delta"] == 10
    assert hero["mom_share_pp_delta"] == 10.0
    assert hero["yoy_units_delta"] == -10
    assert hero["yoy_share_pp_delta"] == -10.0


def test_missing_comparison_maker_yields_none():
    target = _rows("2026-06", [("HERO", 60), ("OLA", 40)])
    mom = _rows("2026-05", [("HERO", 50)])  # OLA absent last month
    view = compute.compute_category_view(CAT, target, mom, [])
    ola = [m for m in view["makers"] if m["maker"] == "OLA"][0]
    assert ola["mom_units_delta"] is None
    assert ola["mom_share_pp_delta"] is None


def test_rank_change_positive_means_moved_up():
    target = _rows("2026-06", [("HONDA", 60), ("HERO", 40)])   # HONDA now #1
    mom = _rows("2026-05", [("HERO", 60), ("HONDA", 40)])      # HONDA was #2
    view = compute.compute_category_view(CAT, target, mom, [])
    honda = [m for m in view["makers"] if m["maker"] == "HONDA"][0]
    assert honda["rank"] == 1
    assert honda["rank_change"] == 1  # 2 -> 1, moved up one


def test_movers_ordered_by_mom_share_delta():
    target = _rows("2026-06", [("A", 50), ("B", 30), ("C", 20)])
    mom = _rows("2026-05", [("A", 30), ("B", 40), ("C", 30)])
    view = compute.compute_category_view(CAT, target, mom, [])
    assert view["movers"]["gainers"][0]["maker"] == "A"   # +20pp
    assert view["movers"]["losers"][0]["maker"] == "B"    # -10pp


def test_build_dashboard_view_aggregates_categories():
    rows_by_month = {
        "2026-06": [
            SaleRow("2026-06", "TWO WHEELER", "HERO", 100),
            SaleRow("2026-06", "PASSENGER", "MARUTI", 50),
        ],
        "2026-05": [],
        "2025-06": [],
    }
    view = compute.build_dashboard_view(rows_by_month, "2026-06", "2026-05", "2025-06", "2026-07-18T10:00")
    assert view["total_units"] == 150
    assert {c["category"] for c in view["categories"]} == {"TWO WHEELER", "PASSENGER"}
    assert view["generated_at"] == "2026-07-18T10:00"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_compute.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'analysis.compute'`.

- [ ] **Step 3: Implement the computation**

`vahan-dashboard/analysis/compute.py`:
```python
from models import SaleRow


def _share(units, total):
    return (units / total * 100.0) if total else 0.0


def _ranks(units_by_maker):
    """maker -> 1-based rank, ordered by units desc then name."""
    ordered = sorted(units_by_maker.items(), key=lambda kv: (-kv[1], kv[0]))
    return {maker: i + 1 for i, (maker, _) in enumerate(ordered)}


def compute_category_view(category, target_rows, mom_rows, yoy_rows):
    target = {r.maker: r.units for r in target_rows}
    mom = {r.maker: r.units for r in mom_rows}
    yoy = {r.maker: r.units for r in yoy_rows}

    total = sum(target.values())
    mom_total = sum(mom.values())
    yoy_total = sum(yoy.values())
    mom_ranks = _ranks(mom)

    makers = []
    for maker, units in sorted(target.items(), key=lambda kv: (-kv[1], kv[0])):
        rank = _ranks(target)[maker]
        share = _share(units, total)
        mom_units = mom.get(maker)
        yoy_units = yoy.get(maker)
        mom_share = _share(mom_units, mom_total) if mom_units is not None else None
        yoy_share = _share(yoy_units, yoy_total) if yoy_units is not None else None
        prev_rank = mom_ranks.get(maker)
        makers.append({
            "maker": maker,
            "units": units,
            "share_pct": round(share, 2),
            "mom_units_delta": (units - mom_units) if mom_units is not None else None,
            "mom_share_pp_delta": round(share - mom_share, 2) if mom_share is not None else None,
            "yoy_units_delta": (units - yoy_units) if yoy_units is not None else None,
            "yoy_share_pp_delta": round(share - yoy_share, 2) if yoy_share is not None else None,
            "rank": rank,
            "rank_change": (prev_rank - rank) if prev_rank is not None else None,
        })

    return {
        "category": category,
        "total_units": total,
        "makers": makers,
        "movers": _movers(makers),
    }


def _movers(makers, top_n=3):
    scored = [m for m in makers if m["mom_share_pp_delta"] is not None]
    gainers = sorted(scored, key=lambda m: -m["mom_share_pp_delta"])[:top_n]
    losers = sorted(scored, key=lambda m: m["mom_share_pp_delta"])[:top_n]
    return {"gainers": gainers, "losers": losers}


def build_dashboard_view(rows_by_month, target, mom, yoy, generated_at):
    target_rows = rows_by_month.get(target, [])
    mom_rows = rows_by_month.get(mom, [])
    yoy_rows = rows_by_month.get(yoy, [])

    categories = sorted({r.category for r in target_rows})
    cat_views = []
    for cat in categories:
        cat_views.append(compute_category_view(
            cat,
            [r for r in target_rows if r.category == cat],
            [r for r in mom_rows if r.category == cat],
            [r for r in yoy_rows if r.category == cat],
        ))

    return {
        "target_month": target,
        "mom_month": mom,
        "yoy_month": yoy,
        "generated_at": generated_at,
        "total_units": sum(cv["total_units"] for cv in cat_views),
        "categories": cat_views,
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_compute.py -v`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add vahan-dashboard/analysis vahan-dashboard/tests/test_compute.py
git commit -m "feat(vahan): market-share, MoM/YoY delta, and rank-change computation"
```

---

### Task 3: Self-contained interactive HTML report

**Files:**
- Create: `vahan-dashboard/report/__init__.py`
- Create: `vahan-dashboard/report/template.py`
- Create: `vahan-dashboard/report/build_html.py`
- Test: `vahan-dashboard/tests/test_build_html.py`

**Interfaces:**
- Consumes: a dashboard view dict from `compute.build_dashboard_view`.
- Produces: `template.render(view) -> str` (a complete HTML document string).
- Produces: `build_html.write_dashboard(view, out_path: str) -> None` — raises `ValueError` if `view["categories"]` is empty (guard against clobbering good output with an empty run).

- [ ] **Step 1: Write the failing test**

`vahan-dashboard/report/__init__.py`: (empty file)

`vahan-dashboard/tests/test_build_html.py`:
```python
import pytest

from models import SaleRow
from analysis import compute
from report import template, build_html


def _view():
    rows_by_month = {
        "2026-06": [
            SaleRow("2026-06", "TWO WHEELER", "HERO", 60),
            SaleRow("2026-06", "TWO WHEELER", "HONDA", 40),
        ],
        "2026-05": [
            SaleRow("2026-05", "TWO WHEELER", "HERO", 50),
            SaleRow("2026-05", "TWO WHEELER", "HONDA", 50),
        ],
        "2025-06": [],
    }
    return compute.build_dashboard_view(rows_by_month, "2026-06", "2026-05", "2025-06", "2026-07-18T10:00")


def test_render_is_self_contained_and_has_content():
    html = template.render(_view())
    assert html.strip().startswith("<!DOCTYPE html>")
    assert "HERO" in html and "HONDA" in html
    assert "TWO WHEELER" in html
    assert "2026-06" in html
    # No external resources allowed.
    assert "http://" not in html and "https://" not in html
    assert "<script" in html  # sorting JS is inline


def test_write_dashboard_creates_file(tmp_path):
    out = tmp_path / "dashboard.html"
    build_html.write_dashboard(_view(), str(out))
    assert out.exists()
    assert "HERO" in out.read_text(encoding="utf-8")


def test_write_dashboard_refuses_empty_view(tmp_path):
    empty = {"target_month": "2026-06", "mom_month": "2026-05", "yoy_month": "2025-06",
             "generated_at": "x", "total_units": 0, "categories": []}
    out = tmp_path / "dashboard.html"
    with pytest.raises(ValueError):
        build_html.write_dashboard(empty, str(out))
    assert not out.exists()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_build_html.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'report.template'`.

- [ ] **Step 3: Implement the HTML template**

`vahan-dashboard/report/template.py`:
```python
import html as _html


def _fmt_int(n):
    return f"{n:,}" if n is not None else "—"


def _chip(value, unit=""):
    """Render a signed delta chip; None -> em dash."""
    if value is None:
        return '<span class="chip flat">—</span>'
    cls = "up" if value > 0 else ("down" if value < 0 else "flat")
    sign = "+" if value > 0 else ""
    return f'<span class="chip {cls}">{sign}{value}{unit}</span>'


def _maker_row(m):
    bar = f'<div class="bar"><span style="width:{min(m["share_pct"], 100):.2f}%"></span></div>'
    rc = m["rank_change"]
    rc_html = "" if not rc else (f' <span class="rc up">▲{rc}</span>' if rc > 0
                                 else f' <span class="rc down">▼{abs(rc)}</span>')
    return (
        "<tr>"
        f'<td class="num">{m["rank"]}{rc_html}</td>'
        f'<td>{_html.escape(m["maker"])}</td>'
        f'<td class="num" data-sort="{m["units"]}">{_fmt_int(m["units"])}</td>'
        f'<td class="num" data-sort="{m["share_pct"]}">{m["share_pct"]:.2f}%{bar}</td>'
        f'<td class="num">{_chip(m["mom_units_delta"])}</td>'
        f'<td class="num">{_chip(m["mom_share_pp_delta"], "pp")}</td>'
        f'<td class="num">{_chip(m["yoy_units_delta"])}</td>'
        f'<td class="num">{_chip(m["yoy_share_pp_delta"], "pp")}</td>'
        "</tr>"
    )


def _movers_block(movers):
    def li(m):
        return f'<li>{_html.escape(m["maker"])} {_chip(m["mom_share_pp_delta"], "pp")}</li>'
    g = "".join(li(m) for m in movers["gainers"])
    l = "".join(li(m) for m in movers["losers"])
    return (f'<div class="movers"><div><h4>Top gainers (MoM share)</h4><ul>{g}</ul></div>'
            f'<div><h4>Top losers (MoM share)</h4><ul>{l}</ul></div></div>')


def _category_section(cv, idx):
    rows = "".join(_maker_row(m) for m in cv["makers"])
    return f"""
    <section class="cat" data-idx="{idx}" {'hidden' if idx else ''}>
      <div class="cat-head">
        <h2>{_html.escape(cv['category'])}</h2>
        <span class="total">Total: {_fmt_int(cv['total_units'])}</span>
      </div>
      {_movers_block(cv['movers'])}
      <table class="sortable">
        <thead><tr>
          <th>#</th><th>Maker</th><th class="num">Units</th><th class="num">Share</th>
          <th class="num">MoM Δ units</th><th class="num">MoM Δ share</th>
          <th class="num">YoY Δ units</th><th class="num">YoY Δ share</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </section>"""


def render(view):
    tabs = "".join(
        f'<button class="tab {"active" if i == 0 else ""}" data-idx="{i}">'
        f'{_html.escape(cv["category"])}</button>'
        for i, cv in enumerate(view["categories"])
    )
    sections = "".join(_category_section(cv, i) for i, cv in enumerate(view["categories"]))
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vahan Market Share — {view['target_month']}</title>
<style>
:root {{ color-scheme: dark; }}
* {{ box-sizing: border-box; }}
body {{ margin:0; font:14px/1.5 system-ui,Segoe UI,Arial,sans-serif; background:#0e1116; color:#e6edf3; }}
header {{ padding:20px 24px; border-bottom:1px solid #222b36; }}
header h1 {{ margin:0 0 4px; font-size:20px; }}
header .meta {{ color:#8b98a5; font-size:13px; }}
.tabs {{ display:flex; flex-wrap:wrap; gap:6px; padding:12px 24px; position:sticky; top:0; background:#0e1116; border-bottom:1px solid #222b36; }}
.tab {{ background:#161b22; color:#c9d1d9; border:1px solid #30363d; border-radius:999px; padding:6px 14px; cursor:pointer; font-size:13px; }}
.tab.active {{ background:#1f6feb; border-color:#1f6feb; color:#fff; }}
main {{ padding:20px 24px; }}
.cat-head {{ display:flex; align-items:baseline; gap:12px; }}
.cat-head h2 {{ margin:0; font-size:17px; }}
.total {{ color:#8b98a5; }}
.movers {{ display:flex; gap:24px; margin:12px 0 16px; flex-wrap:wrap; }}
.movers h4 {{ margin:0 0 4px; font-size:12px; color:#8b98a5; text-transform:uppercase; letter-spacing:.04em; }}
.movers ul {{ margin:0; padding-left:16px; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ padding:8px 10px; border-bottom:1px solid #222b36; text-align:left; }}
th.num, td.num {{ text-align:right; }}
th {{ cursor:pointer; user-select:none; color:#c9d1d9; font-size:12px; text-transform:uppercase; letter-spacing:.03em; }}
tbody tr:hover {{ background:#161b22; }}
.bar {{ height:4px; background:#222b36; border-radius:2px; margin-top:3px; }}
.bar span {{ display:block; height:100%; background:#1f6feb; border-radius:2px; }}
.chip {{ font-variant-numeric:tabular-nums; padding:1px 6px; border-radius:6px; font-size:12px; }}
.chip.up {{ color:#3fb950; }} .chip.down {{ color:#f85149; }} .chip.flat {{ color:#8b98a5; }}
.rc.up {{ color:#3fb950; }} .rc.down {{ color:#f85149; }} .rc {{ font-size:11px; }}
</style></head><body>
<header>
  <h1>Vahan Market Share — {view['target_month']}</h1>
  <div class="meta">Total registrations: {_fmt_int(view['total_units'])} &nbsp;·&nbsp;
  MoM vs {view['mom_month']} · YoY vs {view['yoy_month']} · generated {view['generated_at']}</div>
</header>
<nav class="tabs">{tabs}</nav>
<main>{sections}</main>
<script>
document.querySelectorAll('.tab').forEach(function(t){{
  t.addEventListener('click', function(){{
    var idx = t.dataset.idx;
    document.querySelectorAll('.tab').forEach(function(x){{ x.classList.toggle('active', x===t); }});
    document.querySelectorAll('.cat').forEach(function(s){{ s.hidden = (s.dataset.idx !== idx); }});
  }});
}});
document.querySelectorAll('table.sortable th').forEach(function(th, col){{
  th.addEventListener('click', function(){{
    var tb = th.closest('table').querySelector('tbody');
    var rows = Array.prototype.slice.call(tb.querySelectorAll('tr'));
    var asc = !(th.dataset.asc === 'true'); th.dataset.asc = asc;
    rows.sort(function(a, b){{
      var x = a.children[col], y = b.children[col];
      var xv = x.dataset.sort !== undefined ? parseFloat(x.dataset.sort) : x.textContent.trim();
      var yv = y.dataset.sort !== undefined ? parseFloat(y.dataset.sort) : y.textContent.trim();
      if (xv < yv) return asc ? -1 : 1; if (xv > yv) return asc ? 1 : -1; return 0;
    }});
    rows.forEach(function(r){{ tb.appendChild(r); }});
  }});
}});
</script>
</body></html>"""
```

- [ ] **Step 4: Implement the writer with the empty-view guard**

`vahan-dashboard/report/build_html.py`:
```python
import os
import tempfile

from report import template


def write_dashboard(view, out_path: str) -> None:
    if not view.get("categories"):
        raise ValueError("Refusing to write dashboard: view has no categories (empty/failed run).")
    html = template.render(view)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    # Write to a temp file then atomically replace, so a good file is never left half-written.
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(out_path)), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(html)
        os.replace(tmp, out_path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python -m pytest tests/test_build_html.py -v`
Expected: PASS (3 passed).

- [ ] **Step 6: Commit**

```bash
git add vahan-dashboard/report vahan-dashboard/tests/test_build_html.py
git commit -m "feat(vahan): self-contained interactive HTML dashboard renderer"
```

---

### Task 4: Live recon of the Vahan dashboard (discover selectors + capture fixtures)

This task is **exploratory and network-dependent**. Its deliverables are (a) confirmation that automated access works from this machine, (b) verified selector/category constants written into `scraper/config.py`, and (c) a saved HTML fixture plus `recon_notes.json` that later tests use. If Vahan blocks automated access even headed, **stop and report** — do not proceed to Task 5/6 with invented values.

**Files:**
- Create: `vahan-dashboard/scraper/__init__.py`
- Create: `vahan-dashboard/scraper/config.py`
- Create: `vahan-dashboard/scraper/recon.py`
- Create (generated): `vahan-dashboard/data/fixtures/<category>_2026-06.html`
- Create (generated): `vahan-dashboard/data/fixtures/recon_notes.json`

**Interfaces:**
- Produces: `scraper/config.py` constants — `DASHBOARD_URL: str`, `SELECTORS: dict` (Playwright selectors for the year menu, month menu, vehicle-category menu, and refresh button), `CATEGORY_OPTIONS: dict[str, str]` (canonical label → the exact option text in Vahan's category dropdown), `TABLE_SELECTOR: str`, `MAKER_COL: int`, `COUNT_COL: int`.
- Produces: `data/fixtures/recon_notes.json` shaped `{"month": "2026-06", "categories": {"<canonical>": {"row_count": int, "top_maker": str, "top_units": int, "total_units": int}}}`.

- [ ] **Step 1: Create the config module with best-effort defaults**

`vahan-dashboard/scraper/__init__.py`: (empty file)

`vahan-dashboard/scraper/config.py`:
```python
# Vahan dashboard report view. Verified/updated during recon (Task 4).
DASHBOARD_URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"

# PrimeFaces widgets are custom; recon confirms the trigger element selectors below.
# Each menu is opened by clicking its label element, then an option is clicked by text.
SELECTORS = {
    "year_menu": "#yaxisVar_label",       # placeholder id — CONFIRM in recon
    "month_menu": "#selectedMonth_label", # placeholder id — CONFIRM in recon
    "category_menu": "#vhCatg_label",     # placeholder id — CONFIRM in recon
    "refresh_button": "#j_idt_refresh",   # placeholder id — CONFIRM in recon
    "data_table": "#vchgroupTable",       # placeholder id — CONFIRM in recon
    "option_item": ".ui-selectonemenu-item",  # PrimeFaces option row (stable pattern)
}

# canonical category label -> exact dropdown option text (fill from recon)
CATEGORY_OPTIONS = {}

TABLE_SELECTOR = "#vchgroupTable"  # CONFIRM in recon
MAKER_COL = 1   # 0-based column index of the maker name (CONFIRM in recon)
COUNT_COL = 2   # 0-based column index of the unit count (CONFIRM in recon)
```

- [ ] **Step 2: Write the recon script**

`vahan-dashboard/scraper/recon.py`:
```python
"""One-off recon: open Vahan, dump the report page + a data-table fixture, and
print the widget structure so config.py selectors/categories can be confirmed.

Run headed first so you can watch it:  python -m scraper.recon --headed
"""
import json
import os
import sys

from playwright.sync_api import sync_playwright

from scraper import config

FIXDIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "fixtures")


def main(headed: bool):
    os.makedirs(FIXDIR, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        page = browser.new_page()
        print("Opening", config.DASHBOARD_URL)
        page.goto(config.DASHBOARD_URL, wait_until="networkidle", timeout=90_000)
        # Save the full landing page so its DOM can be inspected offline.
        with open(os.path.join(FIXDIR, "landing.html"), "w", encoding="utf-8") as f:
            f.write(page.content())
        # Print every PrimeFaces select label + id, and any table ids, to identify controls.
        info = page.eval_on_selector_all(
            "[id]",
            "els => els.filter(e => /selectonemenu|Table|refresh|Month|Catg|Year/i.test(e.id))"
            ".map(e => ({id: e.id, cls: e.className, tag: e.tagName}))",
        )
        print(json.dumps(info, indent=2)[:6000])
        print("\nSaved landing.html to", FIXDIR)
        print("Next: update scraper/config.py SELECTORS/CATEGORY_OPTIONS/TABLE_SELECTOR/columns")
        print("from the ids above, then re-run this script to capture a data fixture (Step 4).")
        browser.close()


if __name__ == "__main__":
    main(headed="--headed" in sys.argv)
```

- [ ] **Step 3: Run recon to confirm access and inspect controls**

Run: `python -m scraper.recon --headed`
Expected: A Chromium window opens the Vahan dashboard, `data/fixtures/landing.html` is saved, and the console prints element ids for the year/month/category menus, refresh button, and data table.

**If the page fails to load or is blocked** (captcha, 403, endless spinner): retry once headed; if still blocked, **STOP** and report to the user that automated access is unavailable from this machine, per the spec's honesty rule. Do not continue.

- [ ] **Step 4: Update `config.py` from what recon showed, then capture a real data fixture**

Using the printed ids and `landing.html`, set the real values in `scraper/config.py`: `SELECTORS` (year/month/category/refresh/table), `TABLE_SELECTOR`, `MAKER_COL`, `COUNT_COL`, and populate `CATEGORY_OPTIONS` with every canonical category → its exact dropdown option text.

Then extend `recon.py` `main()` to, for **one** category (e.g. TWO WHEELER), select Year=2026, Month=JUNE, that category, click Refresh, wait for the table, and save:
```python
        # (append inside main(), after selectors are confirmed)
        from scraper.vahan_scraper import _select, _wait_table  # available after Task 6
```
Since Task 6 does not exist yet, instead capture the fixture manually in this run by driving the confirmed selectors inline:
```python
        def open_and_pick(label_sel, option_text):
            page.click(label_sel)
            page.click(f'{config.SELECTORS["option_item"]}:has-text("{option_text}")')
            page.wait_for_load_state("networkidle")

        open_and_pick(config.SELECTORS["year_menu"], "2026")
        open_and_pick(config.SELECTORS["month_menu"], "JUNE")
        open_and_pick(config.SELECTORS["category_menu"], config.CATEGORY_OPTIONS["TWO WHEELER"])
        page.click(config.SELECTORS["refresh_button"])
        page.wait_for_selector(config.TABLE_SELECTOR, timeout=60_000)
        page.wait_for_load_state("networkidle")
        table_html = page.inner_html(config.TABLE_SELECTOR)
        with open(os.path.join(FIXDIR, "TWO WHEELER_2026-06.html"), "w", encoding="utf-8") as f:
            f.write(table_html)
```
Run again: `python -m scraper.recon --headed`
Expected: `data/fixtures/TWO WHEELER_2026-06.html` is saved and contains a table of maker names + counts.

- [ ] **Step 5: Record ground-truth values for the parser test**

Open the saved `TWO WHEELER_2026-06.html`, read off the number of maker rows, the top maker and its unit count, and the column total. Write `data/fixtures/recon_notes.json`:
```json
{
  "month": "2026-06",
  "categories": {
    "TWO WHEELER": {
      "row_count": 0,
      "top_maker": "REPLACE_WITH_ACTUAL",
      "top_units": 0,
      "total_units": 0
    }
  }
}
```
Fill each field with the actual values read from the fixture (these are real observed numbers, not guesses — they become the parser's expected assertions).

- [ ] **Step 6: Commit the recon artifacts and confirmed config**

```bash
git add vahan-dashboard/scraper/__init__.py vahan-dashboard/scraper/config.py vahan-dashboard/scraper/recon.py "vahan-dashboard/data/fixtures/TWO WHEELER_2026-06.html" vahan-dashboard/data/fixtures/recon_notes.json
git commit -m "chore(vahan): recon — confirmed selectors, category options, and data fixture"
```

---

### Task 5: Maker-table parser (TDD against the recon fixture)

**Files:**
- Create: `vahan-dashboard/scraper/parser.py`
- Test: `vahan-dashboard/tests/test_parser.py`

**Interfaces:**
- Consumes: `scraper/config.py` (`MAKER_COL`, `COUNT_COL`), the fixture HTML, `recon_notes.json`.
- Produces: `parser.parse_maker_table(html: str, month: str, category: str) -> list[SaleRow]`.

- [ ] **Step 1: Write the failing test (reads ground truth from recon_notes.json)**

`vahan-dashboard/tests/test_parser.py`:
```python
import json
import os

import pytest

from scraper import parser

FIX = os.path.join(os.path.dirname(__file__), "..", "data", "fixtures")


def _load(category, month):
    with open(os.path.join(FIX, f"{category}_{month}.html"), encoding="utf-8") as f:
        html = f.read()
    with open(os.path.join(FIX, "recon_notes.json"), encoding="utf-8") as f:
        notes = json.load(f)["categories"][category]
    return html, notes


@pytest.mark.skipif(
    not os.path.exists(os.path.join(FIX, "recon_notes.json")),
    reason="recon fixtures not captured yet (Task 4)",
)
def test_parses_fixture_rows():
    html, notes = _load("TWO WHEELER", "2026-06")
    rows = parser.parse_maker_table(html, "2026-06", "TWO WHEELER")
    assert len(rows) == notes["row_count"]
    assert all(r.month == "2026-06" and r.category == "TWO WHEELER" for r in rows)
    assert all(isinstance(r.units, int) and r.units >= 0 for r in rows)
    assert all(r.maker.strip() for r in rows)
    top = max(rows, key=lambda r: r.units)
    assert top.maker == notes["top_maker"]
    assert top.units == notes["top_units"]
    assert sum(r.units for r in rows) == notes["total_units"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scraper.parser'`.

- [ ] **Step 3: Implement the parser**

`vahan-dashboard/scraper/parser.py`:
```python
import re

from bs4 import BeautifulSoup

from models import SaleRow
from scraper import config


def _to_int(text):
    digits = re.sub(r"[^\d]", "", text or "")
    return int(digits) if digits else None


def parse_maker_table(html: str, month: str, category: str) -> list[SaleRow]:
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for tr in soup.select("tr"):
        cells = tr.find_all(["td"])
        if len(cells) <= max(config.MAKER_COL, config.COUNT_COL):
            continue
        maker = cells[config.MAKER_COL].get_text(strip=True)
        units = _to_int(cells[config.COUNT_COL].get_text(strip=True))
        # Skip header/total/blank rows.
        if not maker or units is None:
            continue
        if maker.lower() in {"total", "grand total"}:
            continue
        rows.append(SaleRow(month, category, maker, units))
    return rows
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_parser.py -v`
Expected: PASS (1 passed). If column indices are off, correct `MAKER_COL`/`COUNT_COL` in `config.py` and re-run.

- [ ] **Step 5: Commit**

```bash
git add vahan-dashboard/scraper/parser.py vahan-dashboard/tests/test_parser.py
git commit -m "feat(vahan): maker-table HTML parser with fixture-backed test"
```

---

### Task 6: Playwright scraper — iterate categories for a month

**Files:**
- Create: `vahan-dashboard/scraper/vahan_scraper.py`

**Interfaces:**
- Consumes: `scraper/config.py`, `scraper/parser.py`.
- Produces: `vahan_scraper.scrape_month(year: int, month: int, headless: bool = True) -> list[SaleRow]` — iterates all `CATEGORY_OPTIONS`, returns combined rows tagged with month `f"{year:04d}-{month:02d}"`. Raises `RuntimeError` if zero rows are produced (so the orchestrator never stores an empty month).

- [ ] **Step 1: Implement the scraper**

`vahan-dashboard/scraper/vahan_scraper.py`:
```python
import calendar

from playwright.sync_api import sync_playwright

from scraper import config, parser

MONTH_NAMES = {i: calendar.month_name[i].upper() for i in range(1, 13)}


def _pick(page, label_sel, option_text):
    """Open a PrimeFaces selectOneMenu and choose an option by its visible text."""
    page.click(label_sel)
    page.click(f'{config.SELECTORS["option_item"]}:has-text("{option_text}")')
    page.wait_for_load_state("networkidle", timeout=60_000)


def _scrape_one_category(page, year, month, canonical, option_text):
    _pick(page, config.SELECTORS["year_menu"], str(year))
    _pick(page, config.SELECTORS["month_menu"], MONTH_NAMES[month])
    _pick(page, config.SELECTORS["category_menu"], option_text)
    page.click(config.SELECTORS["refresh_button"])
    page.wait_for_selector(config.TABLE_SELECTOR, timeout=60_000)
    page.wait_for_load_state("networkidle", timeout=60_000)
    table_html = page.inner_html(config.TABLE_SELECTOR)
    return parser.parse_maker_table(table_html, f"{year:04d}-{month:02d}", canonical)


def scrape_month(year: int, month: int, headless: bool = True) -> list[SaleRow]:
    all_rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(config.DASHBOARD_URL, wait_until="networkidle", timeout=90_000)
        try:
            for canonical, option_text in config.CATEGORY_OPTIONS.items():
                for attempt in (1, 2):
                    try:
                        rows = _scrape_one_category(page, year, month, canonical, option_text)
                        all_rows.extend(rows)
                        print(f"  {canonical}: {len(rows)} makers")
                        break
                    except Exception as e:  # noqa: BLE001 — retry once, then re-raise
                        if attempt == 2:
                            raise RuntimeError(f"Failed scraping {canonical}: {e}") from e
                        page.reload(wait_until="networkidle")
        finally:
            browser.close()
    if not all_rows:
        raise RuntimeError("Scrape produced zero rows — refusing to return empty data.")
    return all_rows
```

- [ ] **Step 2: Live smoke run (validates against the real site)**

Run:
```bash
python -c "from scraper.vahan_scraper import scrape_month; r=scrape_month(2026,6,headless=True); print('rows:',len(r)); print(r[:3])"
```
Expected: prints per-category maker counts and a nonzero total; the first few `SaleRow`s show real maker names and units for `2026-06`. If a category intermittently fails, the single retry should recover it; if the whole run fails, revisit the Task 4 selectors.

- [ ] **Step 3: Commit**

```bash
git add vahan-dashboard/scraper/vahan_scraper.py
git commit -m "feat(vahan): Playwright scraper iterating all categories for a month"
```

---

### Task 7: Orchestrator — `vahan_refresh.py` (scrape → store → compute → HTML)

**Files:**
- Create: `vahan-dashboard/vahan_refresh.py`
- Create (generated): `vahan-dashboard/data/dashboard.html`

**Interfaces:**
- Consumes: `store.db`, `scraper.vahan_scraper`, `analysis.compute`, `report.build_html`.
- Produces: `vahan_refresh.main() -> None`.

- [ ] **Step 1: Implement the orchestrator**

`vahan-dashboard/vahan_refresh.py`:
```python
"""Vahan market-share refresh: scrape the target month (+ MoM/YoY comparison
months if missing), store them, compute shares/deltas, and regenerate the HTML.

Run from vahan-dashboard/:  python vahan_refresh.py
"""
import datetime as dt
import os

from analysis import compute
from report import build_html
from scraper.vahan_scraper import scrape_month
from store import db

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "store", "vahan.db")
OUT_PATH = os.path.join(HERE, "data", "dashboard.html")


def _last_completed_month(today=None):
    today = today or dt.date.today()
    first = today.replace(day=1)
    last_month = first - dt.timedelta(days=1)
    return last_month.year, last_month.month


def _month_key(year, month):
    return f"{year:04d}-{month:02d}"


def _prev_month(year, month):
    return (year - 1, 12) if month == 1 else (year, month - 1)


def main():
    ty, tm = _last_completed_month()
    target = _month_key(ty, tm)
    my, mm = _prev_month(ty, tm)
    mom = _month_key(my, mm)
    yoy = _month_key(ty - 1, tm)

    conn = db.open_db(DB_PATH)
    db.init_schema(conn)

    for (yy, mmn), key in [((ty, tm), target), ((my, mm), mom), ((ty - 1, tm), yoy)]:
        if db.has_month(conn, key):
            print(f"{key}: already in store, skipping scrape")
            continue
        print(f"{key}: scraping...")
        try:
            rows = scrape_month(yy, mmn, headless=True)
            db.upsert_sales(conn, rows)
            print(f"{key}: stored {len(rows)} rows")
        except Exception as e:  # noqa: BLE001
            print(f"{key}: scrape failed ({e}) — comparisons for this month will show '—'")

    rows_by_month = {m: db.get_rows_for_month(conn, m) for m in (target, mom, yoy)}
    if not rows_by_month[target]:
        raise SystemExit(f"No data for target month {target}; aborting without touching {OUT_PATH}.")

    view = compute.build_dashboard_view(
        rows_by_month, target, mom, yoy,
        generated_at=dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
    )
    build_html.write_dashboard(view, OUT_PATH)
    print(f"Wrote {OUT_PATH} — {view['total_units']:,} total registrations across "
          f"{len(view['categories'])} categories.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the full pipeline for real**

Run: `python vahan_refresh.py`
Expected: scrapes 2026-06 (and 2026-05 / 2025-06 for comparisons), stores rows, and writes `data/dashboard.html`. Console prints per-month row counts and a final total.

- [ ] **Step 3: Open and eyeball the dashboard**

Run: `start data/dashboard.html` (Windows)
Expected: the dashboard opens; category tabs switch, tables sort on header click, share bars and MoM/YoY chips render, and top gainers/losers show sensible values. Spot-check one maker's share against the raw Vahan site.

- [ ] **Step 4: Commit**

```bash
git add vahan-dashboard/vahan_refresh.py vahan-dashboard/data/dashboard.html
git commit -m "feat(vahan): refresh orchestrator + generated June 2026 dashboard"
```

---

### Task 8: README, full test run, and push

**Files:**
- Create: `vahan-dashboard/README.md`

- [ ] **Step 1: Write the README**

`vahan-dashboard/README.md`:
```markdown
# Vahan Market-Share Dashboard

Scrapes the Government of India Vahan dashboard and generates a self-contained
interactive HTML report of maker-wise registrations by vehicle category, with
month-over-month and year-over-year market-share changes.

## Setup (once)
```
cd vahan-dashboard
pip install -r requirements.txt
python -m playwright install chromium
```

## Refresh (run whenever you want fresh data)
```
python vahan_refresh.py
start data/dashboard.html
```
It scrapes the last completed month plus the MoM and YoY comparison months
(only scraping months not already stored), updates `store/vahan.db`, and
regenerates `data/dashboard.html`.

## Tests
```
python -m pytest -v
```

## Layout
- `scraper/` — Playwright automation (`vahan_scraper.py`), HTML parser (`parser.py`), site config (`config.py`), one-off recon (`recon.py`)
- `store/` — SQLite persistence (`db.py`)
- `analysis/` — market-share / delta math (`compute.py`)
- `report/` — self-contained HTML renderer (`template.py`, `build_html.py`)
- `vahan_refresh.py` — the orchestrator command

## Notes
- All-India totals only (v1). No fabricated data — missing comparison months render as `—`.
- If Vahan changes its dashboard, re-run `python -m scraper.recon --headed` and update `scraper/config.py`.
```

- [ ] **Step 2: Run the entire test suite**

Run: `python -m pytest -v`
Expected: all tests pass (db, compute, build_html, parser).

- [ ] **Step 3: Commit and push**

```bash
git add vahan-dashboard/README.md
git commit -m "docs(vahan): README with setup, refresh, and test instructions"
git push origin main
```

---

## Self-Review Notes

- **Spec coverage:** data source = Playwright (Tasks 4/6); MoM+YoY (Task 2); all categories (Task 6 iterates `CATEGORY_OPTIONS`); all-India (config, no state filter); interactive HTML (Task 3); one command (Task 7); accumulate history (SQLite store, Task 1 + orchestrator only scrapes missing months); standalone folder (all tasks under `vahan-dashboard/`); error handling / no fabricated data (empty-view guard Task 3, missing-month `—` in compute Task 2, scrape-failure handling Task 7); recon-first (Task 4); tests (Tasks 1,2,3,5). All covered.
- **Type consistency:** `SaleRow(month, category, maker, units)` used identically across store, compute, parser, scraper. `build_dashboard_view` / `compute_category_view` output keys match what `template.render` reads. `scrape_month(year, month, headless)` signature matches the orchestrator call.
- **Known network dependency:** Tasks 4, 6, 7 hit the live Vahan site; their "expected" outputs are validated at run time, and the honesty rule (stop + report if blocked) is stated in Task 4.
