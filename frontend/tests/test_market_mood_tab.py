"""
Tests for Market Mood tab pure helpers.
Run: pytest frontend/tests/test_market_mood_tab.py -v
"""
import os, sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from market_mood_tab import split_at_midline


def _dates(n):
    t0 = datetime(2026, 1, 1)
    return [t0 + timedelta(days=i) for i in range(n)]


def test_inserts_point_exactly_on_midline_at_crossings():
    xs, ys = split_at_midline(_dates(3), [40.0, 60.0, 45.0])
    assert ys == [40.0, 50.0, 60.0, 50.0, 45.0]
    # 40 → 60 crosses 50 halfway through the day
    assert xs[1] == datetime(2026, 1, 1, 12)
    # 60 → 45 crosses 50 two-thirds of the way
    assert xs[3] == datetime(2026, 1, 2) + timedelta(days=2 / 3)


def test_no_crossing_leaves_series_untouched():
    d = _dates(4)
    xs, ys = split_at_midline(d, [55.0, 70.0, 60.0, 51.0])
    assert xs == d and ys == [55.0, 70.0, 60.0, 51.0]


def test_touching_midline_is_not_a_crossing():
    d = _dates(3)
    xs, ys = split_at_midline(d, [40.0, 50.0, 60.0])
    assert xs == d and ys == [40.0, 50.0, 60.0]


def test_empty_series():
    assert split_at_midline([], []) == ([], [])


# ── mood_regime_stats ────────────────────────────────────────────────────────
import pandas as pd
from market_mood_tab import mood_regime_stats


def test_regime_streak_and_last_seen():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05", "2026-01-06"]),
        "mood_label": ["Bull", "Dull", None, "Bear", "Bear", "Bear"],
    })
    s = mood_regime_stats(df)
    assert s["label"] == "Bear" and s["streak"] == 3
    assert s["since"] == pd.Timestamp("2026-01-04")
    assert s["last_seen"] == {"Dull": pd.Timestamp("2026-01-02"), "Bull": pd.Timestamp("2026-01-01")}


def test_regime_never_seen_and_empty():
    df = pd.DataFrame({"date": pd.to_datetime(["2026-01-01", "2026-01-02"]), "mood_label": ["Dull", "Dull"]})
    s = mood_regime_stats(df)
    assert s["streak"] == 2 and s["last_seen"] == {"Bear": None, "Bull": None}
    assert mood_regime_stats(df.iloc[0:0])["label"] is None


# ── symmetric_ticks ──────────────────────────────────────────────────────────
from market_mood_tab import symmetric_ticks


def test_symmetric_ticks_mirror_and_absolute_labels():
    vals, text = symmetric_ticks(577)
    assert vals == [-400, -200, 0, 200, 400]
    assert text == ["400", "200", "0", "200", "400"]


def test_symmetric_ticks_small_peak():
    vals, text = symmetric_ticks(11)
    assert vals == [-10, -5, 0, 5, 10]
    assert "-" not in "".join(text)


# ── finalize_chart ───────────────────────────────────────────────────────────
import plotly.graph_objects as go
from market_mood_tab import finalize_chart, CHART_CONFIG, _palette


def test_finalize_chart_disables_zoom_and_darkens_light_axes():
    C = _palette(False)
    fig = go.Figure(go.Scatter(x=[1, 2], y=[3, 4]))
    fig.update_layout(yaxis2=dict(overlaying="y", side="right"))
    finalize_chart(fig, C)
    assert fig.layout.dragmode is False
    assert fig.layout.xaxis.fixedrange and fig.layout.yaxis.fixedrange and fig.layout.yaxis2.fixedrange
    assert fig.layout.xaxis.tickfont.color == C["tick"] == "#0b0f19"
    assert CHART_CONFIG["scrollZoom"] is False and CHART_CONFIG["doubleClick"] is False
