"""
Tests for Market Summary tab pure helpers.
Run: pytest frontend/tests/test_market_summary_tab.py -v
"""
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from market_summary_tab import ALL_SYMS, build_rows, group_breadth


def _q(price, prev):
    chg = price - prev
    return {"price": price, "prev": prev, "change": chg, "pct": chg / prev * 100}


def test_formats_index_row():
    [r] = build_rows([("^NSEI", "Nifty 50")], {"^NSEI": _q(25100.5, 25000.0)})
    assert r["price"] == "25,100.50"
    assert r["change"] == "+100.50"
    assert r["pct"] == "+0.40%"
    assert r["dir"] == 1


def test_yield_row_shows_percent_price_and_plain_change():
    [r] = build_rows([("^TNX", "US 10Y")], {"^TNX": _q(4.25, 4.30)})
    assert r["price"] == "4.25%"
    assert r["change"] == "-0.05"
    assert r["dir"] == -1


def test_sub_one_price_keeps_four_decimal_change():
    [r] = build_rows([("JPYINR=X", "JPY / INR")], {"JPYINR=X": _q(0.6050, 0.6045)})
    assert r["price"] == "0.6050"
    assert r["change"] == "+0.0005"


def test_missing_quote_renders_placeholders():
    [r] = build_rows([("XYZ", "Missing")], {"XYZ": None})
    assert (r["price"], r["change"], r["pct"], r["dir"]) == ("—", "—", "—", 0)


def test_group_breadth_counts_up_and_down_only():
    rows = [{"dir": 1}, {"dir": 1}, {"dir": -1}, {"dir": 0}]
    assert group_breadth(rows) == (2, 1)


def test_symbols_are_unique():
    assert len(ALL_SYMS) == len(set(ALL_SYMS))
