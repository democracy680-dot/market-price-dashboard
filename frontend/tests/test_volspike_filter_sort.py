"""
Tests for the Vol Spikes tab filter+sort helper (_filter_sort_volspike).

Reuses the AST-loader harness from test_display_sorting (app.py runs Streamlit at
module scope and can't be imported directly).
"""
import numpy as np
import pandas as pd

from test_display_sorting import APP  # loads app.py's pure functions

_filter_sort = APP["_filter_sort_volspike"]
SORT_COLS = APP["VOLSPIKE_SORT_COLUMNS"]


def _df():
    return pd.DataFrame(
        {
            "symbol": ["A", "B", "C", "D"],
            "sector": ["Industrials", "Industrials", "Pharma", "Auto"],
            "vol_spike": [2.2, 13.2, 5.5, 1.4],
            "pct_from_52wh": [-0.006, -0.50, -0.08, np.nan],
            "ret_1d": [0.03, 0.19, 0.07, -0.02],
            "cmp": [9926.5, 109.35, 115.65, 143.29],
            "market_cap_cr": [122103.0, 1036.0, 1101.0, 1495.0],
            "pe_ratio": [46.7, np.nan, 17.1, 28.8],
            "ret_1w": [0.04, 0.27, 0.09, 0.01],
            "ret_30d": [0.08, 0.57, 0.14, 0.29],
            "ret_365d": [0.64, 0.55, 0.27, 0.87],
        }
    )


def test_min_spike_filter():
    out = _filter_sort(_df(), min_spike=5.0)
    assert set(out["symbol"]) == {"B", "C"}


def test_sector_filter():
    out = _filter_sort(_df(), sectors=["Industrials"])
    assert set(out["symbol"]) == {"A", "B"}


def test_near_high_filter_keeps_within_threshold():
    # Within 10% of high → pct_from_52wh >= -0.10 → A (-0.006) and C (-0.08); NaN (D) excluded
    out = _filter_sort(_df(), near_high_thr=0.10)
    assert set(out["symbol"]) == {"A", "C"}


def test_combined_filters_match_user_example():
    # 5x+ spike, Industrials, within 10% of 52W high
    out = _filter_sort(_df(), min_spike=5.0, sectors=["Industrials"], near_high_thr=0.10)
    # B is Industrials & 5x+ but 50% below high → excluded; result empty
    assert out.empty


def test_sort_by_52wh_highest_first():
    out = _filter_sort(_df(), sort_col="pct_from_52wh", ascending=False)
    # highest (closest to high) first, NaN last
    assert out["symbol"].tolist() == ["A", "C", "B", "D"]


def test_sort_lowest_first_nan_still_last():
    out = _filter_sort(_df(), sort_col="pe_ratio", ascending=True)
    assert out["symbol"].tolist()[-1] == "B"  # NaN P/E sorts last even ascending


def test_default_sort_is_vol_spike_desc():
    out = _filter_sort(_df())
    assert out["symbol"].tolist() == ["B", "C", "A", "D"]


def test_sort_columns_mapping_exists():
    assert SORT_COLS["Vol Spike"] == "vol_spike"
    assert SORT_COLS["52W High%"] == "pct_from_52wh"
