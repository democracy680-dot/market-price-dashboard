from models import SaleRow
from analysis import compute

CAT = "TWO WHEELER"


def _rows(month, pairs):
    return [SaleRow(month, CAT, maker, units) for maker, units in pairs]


def test_shares_and_ranks():
    target = _rows("2026-06", [("HERO", 60), ("HONDA", 40)])
    view = compute.compute_category_view(CAT, target, [], [])
    assert view["total_units"] == 100
    hero, honda = view["makers"]
    assert hero["maker"] == "HERO" and hero["rank"] == 1 and hero["share_pct"] == 60.0
    assert honda["maker"] == "HONDA" and honda["rank"] == 2 and honda["share_pct"] == 40.0


def test_mom_and_yoy_deltas():
    target = _rows("2026-06", [("HERO", 60), ("HONDA", 40)])
    mom = _rows("2026-05", [("HERO", 50), ("HONDA", 50)])   # HERO 50% -> 60%
    yoy = _rows("2025-06", [("HERO", 70), ("HONDA", 30)])   # HERO 70% -> 60%
    view = compute.compute_category_view(CAT, target, mom, yoy)
    hero = view["makers"][0]
    assert hero["mom_units_delta"] == 10
    assert hero["mom_share_pp_delta"] == 10.0
    assert hero["yoy_units_delta"] == -10
    assert hero["yoy_share_pp_delta"] == -10.0


def test_missing_comparison_maker_yields_none():
    target = _rows("2026-06", [("HERO", 60), ("OLA", 40)])
    mom = _rows("2026-05", [("HERO", 50)])  # OLA absent last month
    view = compute.compute_category_view(CAT, target, mom, [])
    ola = [m for m in view["makers"] if m["maker"] == "OLA"][0]
    assert ola["mom_units_delta"] is None
    assert ola["mom_share_pp_delta"] is None


def test_rank_change_positive_means_moved_up():
    target = _rows("2026-06", [("HONDA", 60), ("HERO", 40)])   # HONDA now #1
    mom = _rows("2026-05", [("HERO", 60), ("HONDA", 40)])      # HONDA was #2
    view = compute.compute_category_view(CAT, target, mom, [])
    honda = [m for m in view["makers"] if m["maker"] == "HONDA"][0]
    assert honda["rank"] == 1
    assert honda["rank_change"] == 1  # 2 -> 1, moved up one


def test_movers_ordered_by_mom_share_delta():
    target = _rows("2026-06", [("A", 50), ("B", 30), ("C", 20)])
    mom = _rows("2026-05", [("A", 30), ("B", 40), ("C", 30)])
    view = compute.compute_category_view(CAT, target, mom, [])
    assert view["movers"]["gainers"][0]["maker"] == "A"   # +20pp
    assert view["movers"]["losers"][0]["maker"] == "B"    # -10pp


def test_build_dashboard_view_aggregates_categories():
    rows_by_month = {
        "2026-06": [
            SaleRow("2026-06", "TWO WHEELER", "HERO", 100),
            SaleRow("2026-06", "PASSENGER", "MARUTI", 50),
        ],
        "2026-05": [],
        "2025-06": [],
    }
    view = compute.build_dashboard_view(rows_by_month, "2026-06", "2026-05", "2025-06", "2026-07-18T10:00")
    assert view["total_units"] == 150
    assert {c["category"] for c in view["categories"]} == {"TWO WHEELER", "PASSENGER"}
    assert view["generated_at"] == "2026-07-18T10:00"
