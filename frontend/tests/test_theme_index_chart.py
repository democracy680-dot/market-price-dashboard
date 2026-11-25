"""
Tests for theme_index_chart pure helpers.
Run: pytest frontend/tests/test_theme_index_chart.py -v
"""
import os, sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from theme_index_chart import (
    build_index_figure, chart_window, period_return, rangebreaks, returns_table, window_start,
)


def _series(values, start="2026-01-01", freq="D"):
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq=freq), dtype=float)


def test_window_start_periods():
    last = pd.Timestamp("2026-11-13")
    assert window_start(last, "1W") == pd.Timestamp("2026-11-06")
    assert window_start(last, "1M") == pd.Timestamp("2026-10-14")
    assert window_start(last, "YTD") == pd.Timestamp("2025-12-31")
    assert window_start(last, "Max") is None


def test_period_return_1d_and_1w():
    s = _series([100, 101, 102, 103, 104, 105, 106, 110.0])     # 8 daily values
    assert abs(period_return(s, "1D") - (110 / 106 - 1) * 100) < 1e-9
    # 1W base = value on or before last-7d = first value (100)
    assert abs(period_return(s, "1W") - 10.0) < 1e-9


def test_period_return_uses_last_value_on_or_before_start():
    # Weekly points ending 2026-03-05; 1M back is 2026-02-03 -> base is the 2026-01-29 value (130)
    s = _series([100, 110, 120, 130, 140, 150, 160, 170, 180.0], start="2026-01-08", freq="7D")
    assert abs(period_return(s, "1M") - (180 / 130 - 1) * 100) < 1e-9


def test_period_return_insufficient_history_is_none():
    s = _series([100, 105.0])
    assert period_return(s, "1Y") is None
    assert period_return(_series([100.0]), "1D") is None


def test_period_return_max_and_ytd():
    s = pd.Series([100, 90, 120.0], index=pd.to_datetime(["2025-06-30", "2025-12-31", "2026-03-31"]))
    assert abs(period_return(s, "Max") - 20.0) < 1e-9
    assert abs(period_return(s, "YTD") - (120 / 90 - 1) * 100) < 1e-9


def test_returns_table_per_key():
    long = pd.DataFrame({
        "k": ["a"] * 3 + ["b"] * 3,
        "date": list(pd.date_range("2026-01-01", periods=3)) * 2,
        "v": [100, 100, 110.0, 50, 50, 45.0],
    })
    t = returns_table(long, "k", "v", ["1D"]).set_index("k")
    assert abs(t.loc["a", "1D"] - 10.0) < 1e-9
    assert abs(t.loc["b", "1D"] + 10.0) < 1e-9


def test_chart_window_starts_at_base_point():
    df = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=60), "index_ew": range(60)})
    w = chart_window(df, "1M")       # last 2026-03-01 -> start 2026-01-30 (inclusive base)
    assert w["date"].iloc[0] == pd.Timestamp("2026-01-30")
    assert w["date"].iloc[-1] == pd.Timestamp("2026-03-01")
    assert len(chart_window(df, "Max")) == 60


def test_rangebreaks_hide_weekends_and_holidays():
    d = pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-08", "2026-01-09", "2026-01-12"])
    rb = rangebreaks(d)
    assert rb[0] == dict(bounds=["sat", "mon"])
    assert rb[1]["values"] == ["2026-01-07"]          # weekday holiday


def test_figure_colour_follows_return():
    up = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=3), "index_ew": [100, 101, 103.0],
                       "turnover_cr": [10, 20, 30.0]})
    down = up.assign(index_ew=[100, 99, 97.0])
    f_up, f_down = build_index_figure(up, dark=False), build_index_figure(down, dark=False)
    line_up = [t for t in f_up.data if t.name == "Theme"][0].line.color
    line_down = [t for t in f_down.data if t.name == "Theme"][0].line.color
    assert line_up != line_down
    assert any(t.type == "bar" for t in f_up.data)          # turnover bars present


def test_display_name_drops_index_suffix():
    from theme_index_chart import display_name
    assert display_name("Cements Index") == "Cements"
    assert display_name("Cables - Power Index") == "Cables - Power"
    assert display_name("Indexed Things") == "Indexed Things"


def test_rebase_benchmark_starts_at_theme_value_and_aligns_dates():
    from theme_index_chart import rebase_benchmark
    win = pd.DataFrame({"date": pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-08"]),
                        "index_ew": [80.0, 82.0, 84.0]})
    bench = pd.DataFrame({"date": pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]),
                          "close": [990.0, 1000.0, 1010.0, 1020.0]})
    r = rebase_benchmark(win, bench)
    assert list(r["rebased"].round(6)) == [80.0, 80.8, 81.6]     # 1000->80, 1010 -> +1%, 01-08 uses 01-07 close
    assert list(r["close"]) == [1000.0, 1010.0, 1020.0]


def test_figure_with_benchmark_has_dotted_nifty_and_no_gridlines():
    win = pd.DataFrame({"date": pd.date_range("2026-01-05", periods=4, freq="B"),
                        "index_ew": [100, 101, 99, 103.0], "turnover_cr": [5, 6, 7, 8.0]})
    bench = pd.DataFrame({"date": win["date"], "close": [20000, 20100, 20050, 20300.0]})
    fig = build_index_figure(win, dark=False, bench=bench, label="Cements")
    nifty = [t for t in fig.data if t.name == "Nifty 50"][0]
    assert nifty.line.dash == "dot"
    assert fig.layout.xaxis.showgrid is False and fig.layout.yaxis.showgrid is False
