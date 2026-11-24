"""
Unit tests for split_repair.py pure functions.
Run: pytest backend/tests/test_split_repair.py -v
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd

from split_repair import compare_closes, repair_rows

D = list(pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]).date)


def _rows(dates, closes, vol=1000):
    return pd.DataFrame({"date": dates, "open": closes, "high": closes, "low": closes,
                         "close": closes, "volume": [vol] * len(dates)})


def test_compare_flags_only_mismatching_symbol():
    db = pd.DataFrame({"symbol": ["A"] * 4 + ["B"] * 4, "date": D * 2,
                       "close": [500, 500, 100, 101, 10, 11, 12, 13]})      # A: 1:5 split on d2, unadjusted
    y = pd.DataFrame({"symbol": ["A"] * 4 + ["B"] * 4, "date": D * 2,
                      "close": [100, 100, 100, 101, 10, 11, 12.1, 13]})     # B within tolerance
    r = compare_closes(db, y)
    assert r["symbol"].tolist() == ["A"]
    assert int(r["n_bad"].iloc[0]) == 2 and abs(r["ratio_med"].iloc[0] - 5.0) < 1e-9
    assert r["first_bad"].iloc[0] == D[0] and r["last_bad"].iloc[0] == D[1]


def test_repair_uses_yahoo_and_rescales_older_rows():
    older = list(pd.to_datetime(["2026-01-01", "2026-01-02"]).date)
    db = _rows(older + D, [480, 490, 500, 500, 100, 101])                     # all pre-split until d2
    y = _rows(D, [100, 100, 100, 101], vol=5000)                              # Yahoo starts at d0, adjusted
    up, dele = repair_rows(db, y)
    up = up.set_index("date")
    # first common date d0: ours 500, Yahoo 100 -> factor 0.2 for rows older than Yahoo's range
    assert abs(up.loc[older[0], "close"] - 96.0) < 1e-9
    assert abs(up.loc[older[1], "close"] - 98.0) < 1e-9
    assert up.loc[older[0], "volume"] == 5000                                 # 1000 / 0.2
    assert up.loc[D[0], "close"] == 100 and up.loc[D[3], "close"] == 101      # Yahoo rows win
    assert dele == []


def test_repair_deletes_our_rows_yahoo_dropped_inside_its_range():
    db = _rows(D, [100, 100, 100, 101])
    y = _rows([D[0], D[2], D[3]], [100, 100, 101])                            # Yahoo has no d1 (ghost row)
    up, dele = repair_rows(db, y)
    assert dele == [D[1]]
    assert len(up) == 3


def test_repair_without_overlap_leaves_older_rows_alone():
    older = list(pd.to_datetime(["2025-12-30", "2025-12-31"]).date)
    db = _rows(older, [50, 51])
    y = _rows(D, [100, 100, 100, 101])
    up, dele = repair_rows(db, y)
    assert set(up["date"]) == set(D)                                          # no factor -> untouched
