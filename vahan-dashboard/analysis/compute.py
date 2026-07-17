from models import SaleRow


def _share(units, total):
    return (units / total * 100.0) if total else 0.0


def _ranks(units_by_maker):
    """maker -> 1-based rank, ordered by units desc then name."""
    ordered = sorted(units_by_maker.items(), key=lambda kv: (-kv[1], kv[0]))
    return {maker: i + 1 for i, (maker, _) in enumerate(ordered)}


def compute_category_view(category, target_rows, mom_rows, yoy_rows):
    target = {r.maker: r.units for r in target_rows}
    mom = {r.maker: r.units for r in mom_rows}
    yoy = {r.maker: r.units for r in yoy_rows}

    total = sum(target.values())
    mom_total = sum(mom.values())
    yoy_total = sum(yoy.values())
    mom_ranks = _ranks(mom)

    makers = []
    for maker, units in sorted(target.items(), key=lambda kv: (-kv[1], kv[0])):
        rank = _ranks(target)[maker]
        share = _share(units, total)
        mom_units = mom.get(maker)
        yoy_units = yoy.get(maker)
        mom_share = _share(mom_units, mom_total) if mom_units is not None else None
        yoy_share = _share(yoy_units, yoy_total) if yoy_units is not None else None
        prev_rank = mom_ranks.get(maker)
        makers.append({
            "maker": maker,
            "units": units,
            "share_pct": round(share, 2),
            "mom_units_delta": (units - mom_units) if mom_units is not None else None,
            "mom_share_pp_delta": round(share - mom_share, 2) if mom_share is not None else None,
            "yoy_units_delta": (units - yoy_units) if yoy_units is not None else None,
            "yoy_share_pp_delta": round(share - yoy_share, 2) if yoy_share is not None else None,
            "rank": rank,
            "rank_change": (prev_rank - rank) if prev_rank is not None else None,
        })

    return {
        "category": category,
        "total_units": total,
        "makers": makers,
        "movers": _movers(makers),
    }


def _movers(makers, top_n=3):
    scored = [m for m in makers if m["mom_share_pp_delta"] is not None]
    gainers = sorted(scored, key=lambda m: -m["mom_share_pp_delta"])[:top_n]
    losers = sorted(scored, key=lambda m: m["mom_share_pp_delta"])[:top_n]
    return {"gainers": gainers, "losers": losers}


def build_dashboard_view(rows_by_month, target, mom, yoy, generated_at):
    target_rows = rows_by_month.get(target, [])
    mom_rows = rows_by_month.get(mom, [])
    yoy_rows = rows_by_month.get(yoy, [])

    categories = sorted({r.category for r in target_rows})
    cat_views = []
    for cat in categories:
        cat_views.append(compute_category_view(
            cat,
            [r for r in target_rows if r.category == cat],
            [r for r in mom_rows if r.category == cat],
            [r for r in yoy_rows if r.category == cat],
        ))

    return {
        "target_month": target,
        "mom_month": mom,
        "yoy_month": yoy,
        "generated_at": generated_at,
        "total_units": sum(cv["total_units"] for cv in cat_views),
        "categories": cat_views,
    }
