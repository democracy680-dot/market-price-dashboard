"""
Unit tests for compute_relative_strength.py pure functions.

Run with:
    pytest backend/tests/test_relative_strength.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from compute_relative_strength import classify_rs, compute_excess_return, RS_THRESHOLDS


def test_classify_rs_strong_outperformer_1m():
    result = classify_rs(5.0, "1m")
    assert result == "🚀 Strong Outperformer"


def test_classify_rs_inline_1w():
    result = classify_rs(0.3, "1w")
    assert result == "⚖️ In-line"


def test_classify_rs_strong_underperformer_1y():
    result = classify_rs(-20.0, "1y")
    assert result == "🔻 Strong Underperformer"


def test_classify_rs_boundary_exact():
    # Exactly +4% for 1m: threshold is > 4.0 for Strong Outperformer,
    # so +4.0 is NOT > 4.0 — should fall into Outperformer bucket (> 1.0).
    result = classify_rs(4.0, "1m")
    assert result == "✅ Outperformer"


def test_classify_rs_none_input():
    result = classify_rs(None, "1m")
    assert result is None


def test_compute_excess_return_basic():
    result = compute_excess_return(5.0, 2.0)
    assert abs(result - 3.0) < 1e-9


def test_compute_excess_return_none_stock():
    result = compute_excess_return(None, 2.0)
    assert result is None


def test_compute_excess_return_none_nifty():
    result = compute_excess_return(5.0, None)
    assert result is None


def test_thresholds_are_consistent():
    """All threshold sets must be in descending order: strong_out > out > under > strong_under."""
    for tf, t in RS_THRESHOLDS.items():
        assert t["strong_out"] > t["out"],    f"{tf}: strong_out must be > out"
        assert t["out"]        > t["under"],  f"{tf}: out must be > under"
        assert t["under"]      > t["strong_under"], f"{tf}: under must be > strong_under"
