"""Tests for the pure helpers in fetch_bse_orders.py (no network / no DB)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fetch_bse_orders import (
    classify_announcement,
    parse_order_value,
    build_order_id,
    clean_headline,
)


# ── parse_order_value ────────────────────────────────────────────────────────

class TestParseOrderValue:
    @pytest.mark.parametrize(
        "text, expected_cr",
        [
            ("Company bags order worth Rs. 1,250 crore", 1250.0),
            ("Order worth Rs 500 Crore from client", 500.0),
            ("Receipt of order valued at ₹2,340.50 crore", 2340.50),
            ("INR 75 lakh order received", 0.75),
            ("Rs 1.5 billion contract secured", 150.0),
            ("Bagged an order of Rs. 12,000 cr", 12000.0),
        ],
    )
    def test_parses_inr_values(self, text, expected_cr):
        value_cr, raw = parse_order_value(text)
        assert value_cr == pytest.approx(expected_cr)
        assert raw  # non-empty raw match preserved

    def test_usd_million_converted_to_inr_crore(self):
        # 40 million USD = 4 crore USD * 83 INR/USD = 332 crore INR
        value_cr, raw = parse_order_value("Received USD 40 million export order")
        assert value_cr == pytest.approx(4.0 * 83.0)

    def test_picks_largest_value_when_multiple(self):
        # The 500-crore order should win over an incidental "5 lakh shares"
        value_cr, _ = parse_order_value("Allotment of 5 lakh shares; order worth Rs 500 crore")
        assert value_cr == pytest.approx(500.0)

    @pytest.mark.parametrize(
        "text",
        [
            "Announcement under Regulation 30 (LODR)-Award_of_Order_Receipt_of_Order",
            "Appointment of Company Secretary / Compliance Officer",
            "Unaudited Financial Results for the quarter ended 30.06.2026",
            "",
            None,
        ],
    )
    def test_no_value_returns_none(self, text):
        assert parse_order_value(text) == (None, None)


# ── classify_announcement ────────────────────────────────────────────────────

class TestClassifyBySubcategory:
    def test_award_of_order(self):
        assert classify_announcement("Award of Order / Receipt of Order", "anything") == "order"

    def test_acquisition_subcat(self):
        assert classify_announcement("Acquisition", "Updates on Acquisition") == "acquisition"

    def test_joint_venture_subcat(self):
        assert classify_announcement("Joint Venture", "Updates on Joint Venture") == "jv"

    def test_subcat_wins_regardless_of_headline(self):
        # subcat is authoritative even if the headline is generic boilerplate
        assert classify_announcement(
            "Award of Order / Receipt of Order",
            "Announcement under Regulation 30 (LODR)",
        ) == "order"


class TestClassifyByHeadlineKeyword:
    def test_expansion_capex(self):
        assert classify_announcement("General", "Proposed Capex") == "expansion"

    def test_expansion_commissioning_capacity(self):
        assert classify_announcement(
            "General", "Commissioning Of 200 MW Solar Power Capacity"
        ) == "expansion"

    def test_expansion_capacity_addition(self):
        assert classify_announcement(
            "General", "Announcement Under Regulation 30-Capacity Addition"
        ) == "expansion"

    def test_order_via_keyword_under_general(self):
        assert classify_announcement(
            "General", "Company bags order worth Rs 500 crore"
        ) == "order"

    def test_jv_via_keyword(self):
        assert classify_announcement(
            "Memorandum of Understanding /Agreements",
            "MoU for a joint venture with XYZ Ltd",
        ) == "jv"


class TestClassifyRejects:
    @pytest.mark.parametrize(
        "subcat, headline",
        [
            ("Newspaper Publication", "Announcement under Regulation 30 (LODR)-Newspaper Publication"),
            ("Change in Directorate", "Appointment of Independent Director"),
            ("Memorandum of Understanding /Agreements", "Execution of a generic supply agreement"),
            ("General", "Submission of reminder notice to shareholders"),
            ("Investor Presentation", "Investor Presentation"),
        ],
    )
    def test_unrelated_returns_none(self, subcat, headline):
        assert classify_announcement(subcat, headline) is None


# ── build_order_id ───────────────────────────────────────────────────────────

class TestCleanHeadline:
    def test_strips_company_and_scrip_prefix(self):
        newssub = "Coal India Ltd - 533278 - Commissioning Of 200 MW Solar Power Capacity"
        assert clean_headline(newssub, "533278") == "Commissioning Of 200 MW Solar Power Capacity"

    def test_underscores_become_spaces(self):
        newssub = "Avantel Ltd - 532406 - Announcement under Regulation 30 (LODR)-Award_of_Order_Receipt_of_Order"
        out = clean_headline(newssub, "532406")
        assert "_" not in out
        assert out.startswith("Announcement under Regulation 30")

    def test_company_name_with_dash_suffix(self):
        newssub = "T T Ltd-$ - 514142 - Announcement under Regulation 30 (LODR)-Updates on Acquisition"
        assert clean_headline(newssub, "514142") == "Announcement under Regulation 30 (LODR)-Updates on Acquisition"

    def test_collapses_embedded_newlines_and_spaces(self):
        newssub = "ABC Ltd - 111111 - Execution Of Strategic Partnership Between\nABC   And XYZ"
        assert clean_headline(newssub, "111111") == "Execution Of Strategic Partnership Between ABC And XYZ"

    def test_no_marker_returns_stripped_original(self):
        assert clean_headline("Some free-form headline", "999999") == "Some free-form headline"

    def test_empty(self):
        assert clean_headline(None, "1") == ""


class TestBuildOrderId:
    def test_prefers_newsid(self):
        row = {"NEWSID": "abc123", "SCRIP_CD": "500325", "NEWS_DT": "2026-07-16T10:00:00",
               "ATTACHMENTNAME": "x.pdf"}
        assert build_order_id(row) == "abc123"

    def test_hash_fallback_is_deterministic(self):
        row = {"NEWSID": "", "SCRIP_CD": "500325", "NEWS_DT": "2026-07-16T10:00:00",
               "ATTACHMENTNAME": "x.pdf"}
        a = build_order_id(row)
        b = build_order_id(dict(row))
        assert a == b and len(a) == 40
