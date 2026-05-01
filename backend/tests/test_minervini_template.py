"""
test_minervini_template.py — 12 unit tests for Minervini Trend Template logic.

No network, no database. Pure math validation.
Run with: pytest backend/tests/test_minervini_template.py -v
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from compute_minervini_template import (
    evaluate_minervini_template,
    compute_rs_ranks_for_universe,
    compute_sma_slope_pct,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _perfect_kwargs(**overrides):
    """
    Returns kwargs for a stock in a perfect Stage 2 uptrend:
    price=150, 50DMA=140, 150DMA=130, 200DMA=120,
    200DMA rising, 52w high=155 (price within 3%), 52w low=100 (price 50% above),
    RS Rank=95, 50DMA rising, volume ratio 1.5.
    """
    base = dict(
        cmp=150.0,
        sma_50=140.0,
        sma_150=130.0,
        sma_200=120.0,
        sma_200_slope_22d=0.5,
        sma_200_slope_110d=2.0,
        high_52w=155.0,
        low_52w=100.0,
        rs_rank_12m=95.0,
        sma_50_slope_22d=0.3,
        recent_volume_ratio=1.5,
    )
    base.update(overrides)
    return base


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_perfect_setup():
    """All criteria met, strong bonuses → template_pass=True, score >= 9."""
    r = evaluate_minervini_template(**_perfect_kwargs())
    assert r["template_pass"] is True
    assert r["template_score"] >= 9.0
    assert r["criteria_count"] == 8


def test_below_200dma():
    """Price below 200 DMA → criterion_1_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(cmp=115.0))
    assert r["criterion_1_pass"] is False
    assert r["template_pass"] is False
    assert r["template_score"] == 0.0


def test_150dma_below_200dma():
    """150 DMA below 200 DMA → criterion_2_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(sma_150=115.0))
    assert r["criterion_2_pass"] is False
    assert r["template_pass"] is False


def test_200dma_declining():
    """Negative 200 DMA slope → criterion_3_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(sma_200_slope_22d=-0.1))
    assert r["criterion_3_pass"] is False
    assert r["template_pass"] is False


def test_50dma_below_150dma():
    """50 DMA below 150 DMA → criterion_4_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(sma_50=125.0))
    assert r["criterion_4_pass"] is False
    assert r["template_pass"] is False


def test_below_50dma():
    """Price below 50 DMA → criterion_5_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(cmp=135.0, sma_50=136.0))
    assert r["criterion_5_pass"] is False
    assert r["template_pass"] is False


def test_only_25_pct_above_low():
    """Price only 25% above 52w low (< 30% threshold) → criterion_6_pass=False."""
    # low=100, cmp=125 → 25% above → fails
    r = evaluate_minervini_template(**_perfect_kwargs(
        cmp=125.0, low_52w=100.0, high_52w=155.0,
        sma_50=120.0, sma_150=110.0, sma_200=100.0,
    ))
    assert r["criterion_6_pass"] is False
    assert r["template_pass"] is False


def test_30_pct_below_high():
    """Price 30% below 52w high (> 25% threshold) → criterion_7_pass=False."""
    # high=200, cmp=140 → 30% below → fails
    r = evaluate_minervini_template(**_perfect_kwargs(
        cmp=140.0, high_52w=200.0, low_52w=80.0,
    ))
    assert r["criterion_7_pass"] is False
    assert r["template_pass"] is False


def test_rs_rank_below_70():
    """RS rank 65 → criterion_8_pass=False, template_pass=False."""
    r = evaluate_minervini_template(**_perfect_kwargs(rs_rank_12m=65.0))
    assert r["criterion_8_pass"] is False
    assert r["template_pass"] is False


def test_rs_rank_computation():
    """10 stocks: highest return gets rank ~99, lowest gets rank ~10."""
    returns = {f"S{i}": float(i * 10) for i in range(1, 11)}  # S1=10, S10=100
    ranks = compute_rs_ranks_for_universe(returns)
    assert len(ranks) == 10
    assert ranks["S10"] == pytest.approx(99.0, abs=1.0)   # best
    assert ranks["S1"]  == pytest.approx(9.9, abs=2.0)    # worst


def test_score_caps_at_10():
    """All bonuses present → score must not exceed 10.0."""
    r = evaluate_minervini_template(**_perfect_kwargs(
        rs_rank_12m=95.0,           # triggers +1 (>=80) + +1 (>=90)
        sma_200_slope_110d=3.0,     # triggers +1
        sma_50_slope_22d=0.5,       # triggers +1
        recent_volume_ratio=2.0,    # triggers +1
        cmp=154.0, high_52w=155.0,  # within 10% → +1
    ))
    assert r["template_pass"] is True
    assert r["template_score"] <= 10.0


def test_none_inputs_fail_safely():
    """None for any required input → template_pass=False, no crash."""
    for field in ["cmp", "sma_50", "sma_150", "sma_200", "high_52w", "low_52w", "rs_rank_12m"]:
        kwargs = _perfect_kwargs()
        kwargs[field] = None
        r = evaluate_minervini_template(**kwargs)
        assert r["template_pass"] is False, f"Should fail with {field}=None"
        assert r["template_score"] == 0.0
        assert r["criteria_count"] == 0
