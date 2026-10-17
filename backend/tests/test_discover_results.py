"""Tests for the pure helpers in discover_bse_results.py."""

import os
import sys
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from discover_bse_results import (
    date_chunks,
    derive_quarter,
    match_announcements,
    parse_news_date,
    results_phrase_hits,
    season_start,
    within_results_deadline,
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


class TestWithinResultsDeadline:
    @pytest.mark.parametrize(
        "d, expected",
        [
            (date(2026, 10, 1), True),    # Q2 season opens
            (date(2026, 10, 16), True),
            (date(2026, 11, 14), True),   # day 44
            (date(2026, 11, 15), True),   # day 45 — SEBI deadline, inclusive
            (date(2026, 11, 16), False),  # past deadline: outcomes are not results
            (date(2026, 12, 20), False),
            (date(2026, 7, 9), True),     # Q1 season
            (date(2026, 8, 14), True),
            (date(2026, 8, 20), False),
            (date(2026, 5, 28), True),    # Q4 (annual) gets 60 days
            (date(2026, 5, 30), True),
            (date(2026, 6, 1), False),
        ],
    )
    def test_window(self, d, expected):
        assert within_results_deadline(d) is expected


class TestDateChunks:
    def test_short_window_is_one_chunk(self):
        assert date_chunks(date(2026, 10, 14), date(2026, 10, 17)) == [
            (date(2026, 10, 14), date(2026, 10, 17))
        ]

    def test_season_backfill_is_split_and_contiguous(self):
        chunks = date_chunks(date(2026, 7, 1), date(2026, 10, 17), days=30)
        assert chunks[0] == (date(2026, 7, 1), date(2026, 7, 30))
        assert chunks[-1][1] == date(2026, 10, 17)
        for (_, end), (nxt, _) in zip(chunks, chunks[1:]):
            assert nxt == end + timedelta(days=1)
        assert all((end - start).days < 30 for start, end in chunks)

    def test_single_day(self):
        assert date_chunks(date(2026, 10, 16), date(2026, 10, 16)) == [
            (date(2026, 10, 16), date(2026, 10, 16))
        ]


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


def _ann(scrip_cd, news_dt, subcat="Financial Results", newssub="", name="Some Co Ltd",
         headline="", attachment="x.pdf"):
    return {
        "SCRIP_CD": scrip_cd,
        "NEWS_DT": news_dt,
        "SUBCATNAME": subcat,
        "NEWSSUB": newssub,
        "HEADLINE": headline,
        "ATTACHMENTNAME": attachment,
        "SLONGNAME": name,
    }


class TestResultsPhraseHits:
    def test_counts_distinct_phrases_case_insensitively(self):
        text = "approved the Unaudited Financial Results for the quarter ended 30 Sep 2026"
        assert results_phrase_hits(text) >= 3

    def test_zero_for_capital_raise(self):
        assert results_phrase_hits("Capital raise through QIP and Preferential Issue.") == 0

    def test_zero_for_empty(self):
        assert results_phrase_hits("") == 0
        assert results_phrase_hits(None) == 0


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

    # E2E Networks, PNB etc. file results only as a board-meeting outcome, with
    # a headline that says nothing about results — the PDF has to be checked.
    _E2E = dict(
        subcat="Outcome of Board Meeting",
        newssub="E2E Networks Ltd - 544783 - Board Meeting Outcome for Outcome Of Board Meeting",
        headline="Outcome of Board Meeting of E2E Networks Limited ('the Company')",
        name="E2E Networks Ltd",
        attachment="e2e.pdf",
    )

    def test_outcome_with_results_headline_needs_no_pdf(self):
        anns = [_ann(500000, "2026-10-08T18:00:00", subcat="Outcome of Board Meeting",
                     newssub="Un-Audited Financial Results for the Quarter ended 30-09-2026",
                     name="GM Breweries")]
        calls = []
        matches = match_announcements(anns, {"500000": "GMBREW"}, verify_outcome=calls.append)
        assert ("GMBREW", "Q2FY27") in matches
        assert calls == []

    def test_outcome_without_keyword_accepted_when_pdf_verifies(self):
        anns = [_ann(544783, "2026-10-16T13:58:42", **self._E2E)]
        seen = []
        def verify(att):
            seen.append(att)
            return True
        matches = match_announcements(anns, {"544783": "E2E"}, verify_outcome=verify)
        assert matches == {("E2E", "Q2FY27"): (date(2026, 10, 16), "E2E Networks Ltd")}
        assert seen == ["e2e.pdf"]

    def test_outcome_without_keyword_rejected_when_pdf_fails(self):
        anns = [_ann(500034, "2026-10-01T20:00:00", subcat="Outcome of Board Meeting",
                     newssub="Board Meeting Outcome for Capital Raise",
                     headline="Capital raise through QIP and Preferential Issue.")]
        assert match_announcements(anns, {"500034": "BAJFINANCE"},
                                   verify_outcome=lambda a: False) == {}

    def test_outcome_without_keyword_rejected_when_no_verifier(self):
        anns = [_ann(544783, "2026-10-16T13:58:42", **self._E2E)]
        assert match_announcements(anns, {"544783": "E2E"}) == {}

    def test_outcome_of_board_meeting_ignored_after_deadline(self):
        anns = [_ann(544783, "2026-12-10T13:58:42", **self._E2E)]
        calls = []
        assert match_announcements(anns, {"544783": "E2E"},
                                   verify_outcome=lambda a: calls.append(a) or True) == {}
        assert calls == []  # no PDF fetched off-season

    def test_outcome_skipped_when_results_filing_already_matched(self):
        # Same day: Financial Results row + Outcome row → one match, no PDF fetch
        anns = [
            _ann(532540, "2026-10-08T16:30:00", subcat="Outcome of Board Meeting",
                 newssub="TCS - Board Meeting Outcome", headline="Outcome", attachment="o.pdf"),
            _ann(532540, "2026-10-08T16:00:00", name="TCS",
                 newssub="Financial Results for the quarter ended September 30, 2026"),
        ]
        calls = []
        matches = match_announcements(anns, {"532540": "TCS"},
                                      verify_outcome=lambda a: calls.append(a) or True)
        assert matches == {("TCS", "Q2FY27"): (date(2026, 10, 8), "TCS")}
        assert calls == []

    def test_outcome_skipped_for_keys_already_in_db(self):
        anns = [_ann(544783, "2026-10-16T13:58:42", **self._E2E)]
        calls = []
        matches = match_announcements(anns, {"544783": "E2E"},
                                      verify_outcome=lambda a: calls.append(a) or True,
                                      skip_keys={("E2E", "Q2FY27")})
        assert matches == {} and calls == []

    def test_financial_results_filing_not_gated_by_deadline(self):
        # A late "Financial Results" filing is still a result
        anns = [_ann(500325, "2026-12-10T18:00:00")]
        assert ("RELIANCE", "Q2FY27") in match_announcements(anns, {"500325": "RELIANCE"})

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
