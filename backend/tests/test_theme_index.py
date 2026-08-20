"""
Unit tests for compute_theme_index.py pure functions.
Run: pytest backend/tests/test_theme_index.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd

from compute_theme_index import (
    compute_equal_weight_index,
    compute_mcap_weight_index,
    compute_theme_index_frame,
)

DATES = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])


def _pivot(data):
    return pd.DataFrame(data, index=DATES)


def test_equal_weight_base_100_and_average_returns():
    # A: +10%, +10% ; B: +10%, -10%  -> EW: 100, 110, 110
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, 49.5]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[0] - 100.0) < 1e-9
    assert abs(ew.iloc[1] - 110.0) < 1e-9
    assert abs(ew.iloc[2] - 110.0) < 1e-6


def test_mcap_weight_favours_larger_cap_member():
    # shares A=1, B=10. B (heavier) falls on d2 -> index down to ~102.667
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, 49.5]})
    mcap = compute_mcap_weight_index(p, {"A": 1.0, "B": 10.0})
    assert abs(mcap.iloc[0] - 100.0) < 1e-9
    assert abs(mcap.iloc[1] - 110.0) < 1e-6
    assert abs(mcap.iloc[2] - 102.6667) < 1e-3


def test_missing_member_data_handled():
    # B missing on d2 -> EW d2 uses only A (+10%) -> 121
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, np.nan]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[2] - 121.0) < 1e-6


def test_member_listed_midwindow_contributes_from_first_valid_return():
    # C lists on d1 (NaN at d0); its first return is on d2. EW with A & C:
    # d1 = A only (+10%) -> 110 ; d2 = mean(A +10%, C +10%) -> 121
    p = _pivot({"A": [100, 110, 121.0], "C": [np.nan, 200, 220.0]})
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[1] - 110.0) < 1e-6
    assert abs(ew.iloc[2] - 121.0) < 1e-6


def test_frame_shape_and_n_members():
    long = pd.DataFrame({
        "symbol": ["A", "A", "A", "B", "B", "B"],
        "date":   list(DATES) * 2,
        "close":  [100, 110, 121.0, 50, 55, 49.5],
    })
    out = compute_theme_index_frame(long, {"A": 1.0, "B": 10.0})
    assert list(out.columns) == ["date", "index_ew", "index_mcap", "n_members"]
    assert len(out) == 3
    assert out["n_members"].tolist() == [2, 2, 2]
    assert abs(out["index_ew"].iloc[1] - 110.0) < 1e-6
