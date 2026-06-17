"""
Tests for the Vol Spikes timeline helper (_apply_timeline).

Reuses the AST-loader harness from test_display_sorting (app.py runs Streamlit
at module scope and can't be imported directly).
"""
import numpy as np
import pandas as pd

from test_display_sorting import APP  # loads app.py's pure functions

_apply_timeline = APP["_apply_timeline"]
TIMELINE_COLS = APP["VOLSPIKE_TIMELINE_COLUMNS"]


def _df():
    return pd.DataFrame(
        {
            "symbol": ["A", "B", "C"],
            "vol_spike": [2.0, 5.0, 1.0],
            "vol_spike_weekly": [3.5, np.nan, 0.8],
            "vol_spike_monthly": [1.2, 4.4, 9.9],
        }
    )


def test_mapping_exists():
    assert TIMELINE_COLS["Today"] == "vol_spike"
    assert TIMELINE_COLS["Weekly"] == "vol_spike_weekly"
    assert TIMELINE_COLS["Monthly"] == "vol_spike_monthly"


def test_today_is_noop():
    out = _apply_timeline(_df(), "Today")
    assert out["vol_spike"].tolist() == [2.0, 5.0, 1.0]


def test_weekly_swaps_in_weekly_column():
    out = _apply_timeline(_df(), "Weekly")
    # NaN preserved for B so it sorts last / filters out downstream
    assert out["vol_spike"].tolist()[0] == 3.5
    assert np.isnan(out["vol_spike"].tolist()[1])
    assert out["vol_spike"].tolist()[2] == 0.8


def test_monthly_swaps_in_monthly_column():
    out = _apply_timeline(_df(), "Monthly")
    assert out["vol_spike"].tolist() == [1.2, 4.4, 9.9]


def test_missing_source_column_falls_back():
    df = pd.DataFrame({"symbol": ["A"], "vol_spike": [2.0]})  # no weekly/monthly cols
    out = _apply_timeline(df, "Weekly")
    assert out["vol_spike"].tolist() == [2.0]  # unchanged, no raise


def test_unknown_period_is_noop():
    out = _apply_timeline(_df(), "Quarterly")
    assert out["vol_spike"].tolist() == [2.0, 5.0, 1.0]


def test_does_not_mutate_input():
    df = _df()
    _apply_timeline(df, "Weekly")
    assert df["vol_spike"].tolist() == [2.0, 5.0, 1.0]  # original untouched
