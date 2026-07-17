import pytest

from models import SaleRow
from analysis import compute
from report import template, build_html


def _view():
    rows_by_month = {
        "2026-06": [
            SaleRow("2026-06", "TWO WHEELER", "HERO", 60),
            SaleRow("2026-06", "TWO WHEELER", "HONDA", 40),
        ],
        "2026-05": [
            SaleRow("2026-05", "TWO WHEELER", "HERO", 50),
            SaleRow("2026-05", "TWO WHEELER", "HONDA", 50),
        ],
        "2025-06": [],
    }
    return compute.build_dashboard_view(rows_by_month, "2026-06", "2026-05", "2025-06", "2026-07-18T10:00")


def test_render_is_self_contained_and_has_content():
    html = template.render(_view())
    assert html.strip().startswith("<!DOCTYPE html>")
    assert "HERO" in html and "HONDA" in html
    assert "TWO WHEELER" in html
    assert "2026-06" in html
    # No external resources allowed.
    assert "http://" not in html and "https://" not in html
    assert "<script" in html  # sorting JS is inline


def test_write_dashboard_creates_file(tmp_path):
    out = tmp_path / "dashboard.html"
    build_html.write_dashboard(_view(), str(out))
    assert out.exists()
    assert "HERO" in out.read_text(encoding="utf-8")


def test_write_dashboard_refuses_empty_view(tmp_path):
    empty = {"target_month": "2026-06", "mom_month": "2026-05", "yoy_month": "2025-06",
             "generated_at": "x", "total_units": 0, "categories": []}
    out = tmp_path / "dashboard.html"
    with pytest.raises(ValueError):
        build_html.write_dashboard(empty, str(out))
    assert not out.exists()
