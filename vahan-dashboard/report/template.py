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
