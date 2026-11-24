"""Tests for fetch_index_valuation pure helpers. Run: pytest backend/tests/test_fetch_index_valuation.py"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datetime import date
from fetch_index_valuation import parse_rows, year_windows, INDICES


def test_parse_rows_converts_and_cleans():
    rows = [
        {"DATE": "23 Nov 2026", "pe": "20.65", "pb": "3.11", "divYield": "1.35"},
        {"DATE": "21 Nov 2026", "pe": "-", "pb": "0", "divYield": "1,234.5"},
        {"DATE": "bad", "pe": "1"},
        {"DATE": "23 Nov 2026", "pe": "20.70", "pb": "3.11", "divYield": "1.35"},   # duplicate date: keep last
    ]
    df = parse_rows(rows)
    assert list(df["date"]) == [date(2026, 11, 21), date(2026, 11, 23)]
    assert df.loc[0, "pe"] is None or df["pe"].isna().iloc[0]
    assert df["pb"].isna().iloc[0]                       # 0 -> missing
    assert abs(df.loc[0, "div_yield"] - 1234.5) < 1e-9
    assert abs(df.loc[1, "pe"] - 20.70) < 1e-9


def test_year_windows_cover_range_without_gaps_or_overlap():
    w = year_windows(date(2020, 2, 29), date(2023, 1, 10))
    assert w[0][0] == date(2020, 2, 29) and w[-1][1] == date(2023, 1, 10)
    for (a1, b1), (a2, b2) in zip(w, w[1:]):
        assert (a2 - b1).days == 1
    assert all((b - a).days <= 364 for a, b in w)


def test_index_list_is_unique_and_grouped():
    names = [a for a, _, _ in INDICES]
    assert len(names) == len(set(names)) == 57
    assert {g for _, _, g in INDICES} == {"Broad", "Sectoral", "Thematic"}
