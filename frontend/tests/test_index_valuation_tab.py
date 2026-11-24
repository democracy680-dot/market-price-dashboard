"""Tests for index_valuation_tab pure helpers. Run: pytest frontend/tests/test_index_valuation_tab.py"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from index_valuation_tab import band_stats, build_valuation_figure, summary_table, weekly, window, zone


def _weekly_series(values, start="2020-01-03"):
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq="W-FRI"), dtype=float)


def test_band_stats_mean_std_and_percentile():
    s = _weekly_series([10, 20, 30, 40, 50])
    b = band_stats(s)
    assert abs(b["mean"] - 30) < 1e-9
    assert abs(b["std"] - np.std([10, 20, 30, 40, 50])) < 1e-9
    assert abs(b["z"] - (50 - 30) / np.std([10, 20, 30, 40, 50])) < 1e-9
    assert b["pctile"] == 100.0 and abs(b["dev_pct"] - (50 / 30 - 1) * 100) < 1e-9
    assert band_stats(_weekly_series([1, 2, 3])) is None          # too short


def test_weekly_takes_last_value_per_week():
    s = pd.Series([1.0, 2.0, 3.0], index=pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-13"]))
    w = weekly(s)
    assert list(w.values) == [2.0, 3.0]


def test_zone_reverses_for_dividend_yield():
    assert zone(1.5, "pe") == "Expensive" and zone(-1.2, "pe") == "Cheap" and zone(0.3, "pb") == "Fair"
    assert zone(1.5, "div_yield") == "Cheap" and zone(-1.5, "div_yield") == "Expensive"


def test_window_years():
    df = pd.DataFrame({"date": pd.date_range("2015-01-01", "2026-01-01", freq="MS")})
    assert window(df, 1)["date"].min() == pd.Timestamp("2025-01-01")
    assert len(window(df, None)) == len(df)


def test_summary_table_one_row_per_index():
    d = pd.date_range("2024-01-05", periods=60, freq="W-FRI")
    df = pd.concat([
        pd.DataFrame({"api_name": "A", "display_name": "Alpha", "grp": "Broad", "sort_order": 0, "date": d,
                      "pe": np.linspace(10, 20, 60), "pb": 2.0, "div_yield": 1.0}),
        pd.DataFrame({"api_name": "B", "display_name": "Beta", "grp": "Sectoral", "sort_order": 1, "date": d,
                      "pe": np.linspace(30, 15, 60), "pb": 3.0, "div_yield": 2.0}),
    ])
    t = summary_table(df, "pe", None)
    assert t["Index"].tolist() == ["Alpha", "Beta"]
    assert t.loc[0, "Zone"] == "Expensive" and t.loc[1, "Zone"] == "Cheap"
    assert abs(t.loc[0, "Current"] - 20) < 1e-9


def test_figure_builds_with_bands():
    d = pd.date_range("2024-01-05", periods=30, freq="W-FRI")
    g = pd.DataFrame({"date": d, "pe": np.linspace(15, 25, 30)})
    b = band_stats(pd.Series(g["pe"].values, index=d))
    fig = build_valuation_figure(g, "pe", "P/E", b, dark=True)
    assert len(fig.layout.shapes) >= 4            # avg + ±1σ + ±2σ lines
