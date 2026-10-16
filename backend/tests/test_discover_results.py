"""Tests for the pure helpers in discover_bse_results.py."""

import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from discover_bse_results import (
    derive_quarter,
    match_announcements,
    parse_news_date,
    season_start,
)


class TestDeriveQuarter:
    @pytest.mark.parametrize(
        "announce_date, expected",
        [
            (date(2026, 7, 1), "Q1FY27"),
            (date(2026, 7, 16), "Q1FY27"),
            (date(2026, 9, 30), "Q1FY27"),
            (date(2026, 10, 1), "Q2FY27"),
            (date(2026, 10, 16), "Q2FY27"),
            (date(2026, 12, 31), "Q2FY27"),
            (date(2027, 1, 15), "Q3FY27"),
            (date(2026, 6, 30), "Q4FY26"),
            (date(2026, 4, 5), "Q4FY26"),
            (date(2027, 4, 10), "Q4FY27"),
        ],
    )
    def test_boundaries(self, announce_date, expected):
        assert derive_quarter(announce_date) == expected


class TestSeasonStart:
    @pytest.mark.parametrize(
        "today, expected",
        [
            (date(2026, 10, 1), date(2026, 10, 1)),
            (date(2026, 10, 16), date(2026, 10, 1)),
            (date(2026, 12, 31), date(2026, 10, 1)),
            (date(2027, 1, 1), date(2027, 1, 1)),
            (date(2026, 7, 9), date(2026, 7, 1)),
            (date(2026, 5, 20), date(2026, 4, 1)),
        ],
    )
    def test_first_day_of_calendar_quarter(self, today, expected):
        assert season_start(today) == expected

    def test_backfill_window_labels_current_season(self):
        today = date(2026, 10, 16)
        assert derive_quarter(season_start(today)) == derive_quarter(today) == "Q2FY27"


class TestParseNewsDate:
    def test_iso_with_fractional_seconds(self):
        assert parse_news_date("2026-07-16T16:22:05.53") == date(2026, 7, 16)

    def test_iso_without_fractional_seconds(self):
        assert parse_news_date("2026-07-10T09:00:00") == date(2026, 7, 10)

    def test_none_returns_none(self):
        assert parse_news_date(None) is None

    def test_garbage_returns_none(self):
        assert parse_news_date("not a date") is None

    def test_empty_string_returns_none(self):
        assert parse_news_date("") is None


def _ann(scrip_cd, news_dt, subcat="Financial Results", newssub="", name="Some Co Ltd"):
    return {
        "SCRIP_CD": scrip_cd,
        "NEWS_DT": news_dt,
        "SUBCATNAME": subcat,
        "NEWSSUB": newssub,
        "SLONGNAME": name,
    }


class TestMatchAnnouncements:
    def test_dedups_to_earliest_date_within_quarter(self):
        anns = [
            _ann(500325, "2026-07-14T18:00:00", name="Reliance Industries Ltd"),
            _ann(500325, "2026-07-12T18:00:00", name="Reliance Industries Ltd"),
        ]
        matches = match_announcements(anns, {"500325": "RELIANCE"})
        assert matches == {
            ("RELIANCE", "Q1FY27"): (date(2026, 7, 12), "Reliance Industries Ltd")
        }

    def test_quarter_derived_per_filing_across_seasons(self):
        """A window straddling two seasons keeps one row per (symbol, quarter)."""
        anns = [
            _ann(500325, "2026-07-18T18:00:00", name="Reliance Industries Ltd"),
            _ann(500325, "2026-10-14T18:00:00", name="Reliance Industries Ltd"),
        ]
        matches = match_announcements(anns, {"500325": "RELIANCE"})
        assert matches == {
            ("RELIANCE", "Q1FY27"): (date(2026, 7, 18), "Reliance Industries Ltd"),
            ("RELIANCE", "Q2FY27"): (date(2026, 10, 14), "Reliance Industries Ltd"),
        }

    def test_explicit_quarter_override_forces_label(self):
        anns = [
            _ann(500325, "2026-07-18T18:00:00"),
            _ann(500325, "2026-10-14T18:00:00"),
        ]
        matches = match_announcements(anns, {"500325": "RELIANCE"}, quarter="Q2FY27")
        # Forced label collapses both filings into one key, earliest date wins
        assert list(matches) == [("RELIANCE", "Q2FY27")]
        assert matches[("RELIANCE", "Q2FY27")][0] == date(2026, 7, 18)

    def test_skips_unknown_scrip(self):
        anns = [_ann(999999, "2026-07-14T18:00:00")]
        assert match_announcements(anns, {"500325": "RELIANCE"}) == {}

    def test_accepts_via_subcat(self):
        anns = [_ann(500325, "2026-07-14T18:00:00", subcat="Results", newssub="Board meeting")]
        matches = match_announcements(anns, {"500325": "RELIANCE"})
        assert ("RELIANCE", "Q1FY27") in matches

    def test_accepts_via_headline_keyword(self):
        anns = [
            _ann(
                500325,
                "2026-07-14T18:00:00",
                subcat="Company Update",
                newssub="Unaudited Financial Results for the quarter ended June 30, 2026",
            )
        ]
        matches = match_announcements(anns, {"500325": "RELIANCE"})
        assert ("RELIANCE", "Q1FY27") in matches

    def test_rejects_when_neither_matches(self):
        anns = [
            _ann(
                500325,
                "2026-07-14T18:00:00",
                subcat="Company Update",
                newssub="Appointment of Company Secretary",
            )
        ]
        assert match_announcements(anns, {"500325": "RELIANCE"}) == {}

    def test_skips_unparseable_news_date(self):
        anns = [_ann(500325, None)]
        assert match_announcements(anns, {"500325": "RELIANCE"}) == {}
