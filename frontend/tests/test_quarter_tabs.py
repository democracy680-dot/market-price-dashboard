"""
Tests for the Quarterly Results quarter-tab helpers.

The quarter tabs used to be hardcoded constants (Q1FY27 / Q4FY26) that needed
a code change every season. They are now derived: the live quarter from the
selected date, archive tabs from the distinct quarters in earnings_calendar.

Reuses the AST-loader harness from test_display_sorting (app.py runs Streamlit
at module scope and can't be imported directly).
"""
from datetime import date

import pandas as pd
import pytest

from test_display_sorting import APP  # loads app.py's pure functions

_derive_quarter = APP["_derive_quarter"]
_quarter_sort_key = APP["_quarter_sort_key"]
_prev_quarter = APP["_prev_quarter"]
_quarter_tabs = APP["_quarter_tabs"]


class TestDeriveQuarter:
    @pytest.mark.parametrize(
        "d, expected",
        [
            (date(2026, 7, 1), "Q1FY27"),
            (date(2026, 9, 30), "Q1FY27"),
            (date(2026, 10, 1), "Q2FY27"),
            (date(2026, 10, 16), "Q2FY27"),
            (date(2026, 12, 31), "Q2FY27"),
            (date(2027, 1, 15), "Q3FY27"),
            (date(2026, 5, 7), "Q4FY26"),
            (date(2027, 4, 10), "Q4FY27"),
        ],
    )
    def test_matches_backend_rule(self, d, expected):
        assert _derive_quarter(d) == expected

    def test_accepts_pandas_timestamp(self):
        # snap_date arrives from the date picker; the fragment wraps it in
        # pd.Timestamp so str / date / datetime all work.
        assert _derive_quarter(pd.Timestamp("2026-10-16")) == "Q2FY27"


class TestQuarterOrdering:
    def test_sort_key_is_chronological(self):
        qs = ["Q1FY27", "Q4FY26", "Q2FY27", "Q3FY26"]
        assert sorted(qs, key=_quarter_sort_key) == ["Q3FY26", "Q4FY26", "Q1FY27", "Q2FY27"]

    @pytest.mark.parametrize(
        "q, expected",
        [("Q2FY27", "Q1FY27"), ("Q1FY27", "Q4FY26"), ("Q4FY26", "Q3FY26")],
    )
    def test_prev_quarter(self, q, expected):
        assert _prev_quarter(q) == expected


class TestQuarterTabs:
    def test_live_first_then_archive_newest_first(self):
        assert _quarter_tabs("Q2FY27", ["Q4FY26", "Q1FY27"]) == ["Q2FY27", "Q1FY27", "Q4FY26"]

    def test_live_tab_shown_even_when_db_has_no_rows_for_it(self):
        # Early October: Q2FY27 has no filings yet but must still be the live tab.
        assert _quarter_tabs("Q2FY27", ["Q1FY27", "Q4FY26"])[0] == "Q2FY27"

    def test_live_not_duplicated_when_present_in_db(self):
        tabs = _quarter_tabs("Q2FY27", ["Q2FY27", "Q1FY27", "Q2FY27"])
        assert tabs == ["Q2FY27", "Q1FY27"]

    def test_ignores_null_quarters(self):
        assert _quarter_tabs("Q2FY27", [None, "", "Q1FY27"]) == ["Q2FY27", "Q1FY27"]

    def test_empty_db(self):
        assert _quarter_tabs("Q2FY27", []) == ["Q2FY27"]
