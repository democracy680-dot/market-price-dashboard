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
    cap_weights,
    member_returns,
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
    assert list(out.columns) == ["date", "index_ew", "index_mcap", "n_members", "turnover_cr"]
    assert len(out) == 3
    assert out["n_members"].tolist() == [2, 2, 2]
    assert abs(out["index_ew"].iloc[1] - 110.0) < 1e-6


D5 = pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"])


def test_split_like_move_is_excluded():
    # B has an unadjusted 1:5 split on d2 (-80%) -> excluded; EW d2 = A only (+10%)
    p = pd.DataFrame({"A": [100, 100, 110.0], "B": [500, 500, 100.0]}, index=DATES)
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[2] - 110.0) < 1e-9


def test_inside_gap_is_bridged_not_dropped():
    # B misses d1 then trades +20% vs its last close on d2.
    # d1: A +0% (B carried flat) -> 100 ; d2: mean(A 0%, B +20%) = +10% -> 110
    p = pd.DataFrame({"A": [100, 100, 100.0], "B": [50, np.nan, 60.0]}, index=DATES)
    ew = compute_equal_weight_index(p)
    assert abs(ew.iloc[1] - 100.0) < 1e-9
    assert abs(ew.iloc[2] - 110.0) < 1e-9


def test_trailing_missing_price_is_not_forward_filled():
    # B has no price yet today -> it must not contribute a fake 0%.
    r = member_returns(pd.DataFrame({"A": [100, 110.0, 121.0], "B": [50, 55, np.nan]}, index=DATES))
    assert np.isnan(r["B"].iloc[2])


def test_cap_weights_redistributes_excess():
    w = cap_weights(np.array([70.0, 10.0, 10.0, 10.0]), 0.4)
    assert abs(w.sum() - 1.0) < 1e-12
    assert abs(w[0] - 0.4) < 1e-12
    assert np.allclose(w[1:], 0.2)


def test_cap_weights_infeasible_cap_falls_back_to_equal():
    w = cap_weights(np.array([90.0, 10.0]), 0.25)   # 2 members can't each be <= 25%
    assert np.allclose(w, [0.5, 0.5])


def test_capped_mcap_index():
    # shares A=1, B=10 -> uncapped B weight 0.909; capped at 0.6 -> A 0.4, B 0.6.
    # d2 returns: A +10%, B -10% -> 0.4*0.1 + 0.6*-0.1 = -2% -> 110 * 0.98 = 107.8
    p = _pivot({"A": [100, 110, 121.0], "B": [50, 55, 49.5]})
    mcap = compute_mcap_weight_index(p, {"A": 1.0, "B": 10.0}, cap=0.6)
    assert abs(mcap.iloc[2] - 107.8) < 1e-6


def test_calendar_drops_ghost_holiday_rows_and_turnover():
    # d2 is a holiday (not in calendar) where Yahoo wrote a flat row for A only.
    long = pd.DataFrame({
        "symbol": ["A"] * 5 + ["B"] * 4,
        "date":   list(D5) + [D5[0], D5[1], D5[3], D5[4]],
        "close":  [100, 100, 100, 110, 110.0, 50, 50, 55, 55.0],
        "volume": [1e7] * 5 + [2e7] * 4,
    })
    cal = [D5[0], D5[1], D5[3], D5[4]]
    out = compute_theme_index_frame(long, {}, calendar=cal)
    assert len(out) == 4                                   # holiday row removed
    # d3 (row 2): A +10%, B +10% -> 110 (B's return is not lost to the ghost day)
    assert abs(out["index_ew"].iloc[2] - 110.0) < 1e-9
    assert out["n_members"].tolist() == [2, 2, 2, 2]
    # turnover on d0 = (100*1e7 + 50*2e7) / 1e7 = 200 cr
    assert abs(out["turnover_cr"].iloc[0] - 200.0) < 1e-9


def test_member_returns_table_periods_and_ghost_rows():
    from compute_theme_index import member_returns_table
    dates = pd.date_range("2026-01-01", periods=40, freq="D")
    closes = [100.0 + i for i in range(40)]                 # last = 139 on 2026-02-09
    long = pd.DataFrame({"symbol": "A", "date": dates, "close": closes})
    # ghost holiday row on the last-but-one date: excluded via calendar
    cal = [d for d in dates if d != dates[-2]]
    t = member_returns_table(long, calendar=cal).set_index("symbol")
    assert abs(t.loc["A", "ret_1d"] - (139 / 137 - 1) * 100) < 1e-9   # vs previous *session*
    assert abs(t.loc["A", "ret_1w"] - (139 / 132 - 1) * 100) < 1e-9   # base 2026-02-02 = 132
    assert abs(t.loc["A", "ret_1m"] - (139 / 109 - 1) * 100) < 1e-9   # base 2026-01-10 = 109
    assert pd.isna(t.loc["A", "ret_1y"])                               # not enough history
