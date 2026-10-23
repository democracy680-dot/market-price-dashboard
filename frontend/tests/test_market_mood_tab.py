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
