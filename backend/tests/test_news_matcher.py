"""Tests for news_fetcher symbol matching. Run: pytest backend/tests/test_news_matcher.py"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from news_fetcher import build_lookup, match_symbols

ROWS = [
    ("ARSSBL", "Anand Rathi Share and Stock Brokers Limited"),
    ("GLOBAL", "Global Education Limited"), ("OIL", "Oil India Ltd"), ("DOLLAR", "Dollar Industries Limited"),
    ("TAKE", "TAKE Limited"), ("TOTAL", "Total Transport Systems Limited"), ("BSE", "BSE Ltd"),
    ("MCX", "Multi Commodity Exchange Of India Ltd"), ("APOLLO", "Apollo Micro Systems Ltd"),
    ("APOLLOHOSP", "Apollo Hospitals Enterprise Ltd"), ("TATASTEEL", "Tata Steel Ltd"),
    ("TATAPOWER", "Tata Power Company Ltd"), ("SBIN", "State Bank of India"), ("RELIANCE", "Reliance Industries Ltd"),
    ("RPOWER", "Reliance Power Ltd"), ("DIXON", "Dixon Technologies (India) Ltd"), ("TCS", "Tata Consultancy Services Ltd"),
    ("M&M", "Mahindra & Mahindra Ltd"), ("SOUTHBANK", "South Indian Bank"), ("CHALET", "Chalet Hotels Ltd"),
    ("^NSEI", "Nifty 50 Index"),
]
LK = build_lookup(ROWS)
tags = lambda t: [m["symbol"] for m in match_symbols(t, LK)]


def test_generic_words_no_longer_tag_stocks():
    assert tags("HDFC Bank, Reliance among 10 stocks with highest DII holding. Share prices rise") == []
    assert tags("Global markets fall; oil prices and the dollar rise; investors take profits") == []
    assert tags("Markets take heavy drubbing; total turnover on BSE and NSE falls") == []
    assert tags("Manic Monday for Bank Nifty as index crashes over 2000 points") == []


def test_real_company_mentions_still_match():
    assert tags("Tata Steel may rework APL Apollo deal") == ["TATASTEEL"]
    assert tags("South Indian Bank shares in focus as RBI approves SBI Mutual Fund stake") == ["SBIN", "SOUTHBANK"]
    assert tags("Stock to buy today: Chalet Hotels (₹868.75) – BUY") == ["CHALET"]
    assert tags("TCS and Infosys lead IT gains") == ["TCS"]
    assert tags("Dixon shares jump 6% on new order") == ["DIXON"]
    assert tags("Mahindra & Mahindra Q2 profit rises; M&M stock up") == ["M&M"]
    assert tags("Anand Rathi Share and Stock Brokers lists at premium") == ["ARSSBL"]
    assert tags("Reliance Industries hits record; RIL to raise funds") == ["RELIANCE"]


def test_ambiguous_tickers_need_a_stock_word():
    assert tags("BSE shares rally 8% on record volumes") == ["BSE"]
    assert tags("OIL Q2 results: profit up 20%") == ["OIL"]
    assert tags("Sensex on the BSE fell 500 points") == []


def test_shared_first_word_needs_two_words():
    assert tags("Apollo shares surge") == []                          # which Apollo? -> none
    assert tags("Apollo Hospitals posts strong Q2") == ["APOLLOHOSP"]
    assert tags("Reliance shares rise") == []                         # Reliance group: ambiguous
    assert tags("Tata Power commissions new plant") == ["TATAPOWER"]


def test_index_symbols_are_never_tagged():
    assert "^NSEI" not in tags("Nifty 50 Index ends higher; ^NSEI at record")


ROWS2 = ROWS + [
    ("NDTV", "New Delhi Television Limited"), ("TNPL", "Tamil Nadu Newsprint & Papers Ltd"),
    ("BANKINDIA", "Bank of India Ltd"), ("MAHABANK", "Bank of Maharashtra Ltd"), ("SALSTEEL", "S.A.L. Steel Limited"),
    ("WORTHPERI", "Worth Peripherals Limited"), ("CHENNPETRO", "Chennai Petroleum Corporation Ltd"),
    ("ADVENZYMES", "Advanced Enzyme Technologies Limited"),
]
LK2 = build_lookup(ROWS2)
tags2 = lambda t: [m["symbol"] for m in match_symbols(t, LK2)]


def test_places_connectors_initials_and_plain_words():
    assert tags2("Pollution in New Delhi; Tamil Nadu rains; gold rates in Chennai") == []
    assert tags2("Bank of India and Bank of Maharashtra cut lending rates") == ["BANKINDIA", "MAHABANK"]
    assert tags2("S.A.L. Steel shares hit upper circuit") == ["SALSTEEL"]
    assert tags2("Is it worth buying? Advanced AI tools for traders") == []
    assert tags2("Worth shares jump; Chennai Petroleum Q2 profit doubles") == ["CHENNPETRO", "WORTHPERI"]
    assert tags2("Tamil Nadu Newsprint posts loss") == ["TNPL"]


def test_shared_phrase_tech_numbers_and_branding():
    lk = build_lookup(ROWS + [("DALBHARAT", "Dalmia Bharat Ltd"), ("DALMIASUG", "Dalmia Bharat Sugar and Industries Ltd"),
                              ("TECHM", "Tech Mahindra Ltd"), ("FIVESTAR", "Five-Star Business Finance Ltd"),
                              ("IPL", "India Pesticides Limited"), ("NDTV", "New Delhi Television Limited")])
    t = lambda x: [m["symbol"] for m in match_symbols(x, lk)]
    assert t("Dalmia Bharat board approves share buyback") == ["DALBHARAT"]
    assert t("Dalmia Bharat Sugar posts record crush") == ["DALMIASUG"]
    assert t("Tech stocks fall as AI trade cools; five stocks to watch") == []
    assert t("Tech Mahindra shares rise") == ["TECHM"]
    assert t("NDTV Profit: Punjab Kings retain stars for IPL") == []


def test_pub_date_formats_and_junk_titles():
    from news_fetcher import parse_pub_date, is_real_headline
    from datetime import datetime, timezone, timedelta
    iso = parse_pub_date({"published": "2026-11-25T14:37:07+05:30"})
    assert iso == datetime(2026, 11, 25, 9, 7, 7, tzinfo=timezone.utc)
    rfc = parse_pub_date({"published": "Wed, 25 Nov 2026 14:25:41 +0530"})
    assert rfc == datetime(2026, 11, 25, 8, 55, 41, tzinfo=timezone.utc)
    assert abs(parse_pub_date({}) - datetime.now(timezone.utc)) < timedelta(seconds=5)
    assert not is_real_headline("2026-12-06") and not is_real_headline("Live")
    assert is_real_headline("Tata Steel shares fall 3%")


def test_regulators_and_bank_name_nesting():
    lk = build_lookup(ROWS + [("BANKINDIA", "Bank of India Ltd"), ("UNIONBANK", "Union Bank of India Ltd"),
                              ("CENTRALBK", "Central Bank of India Ltd")])
    t = lambda x: [m["symbol"] for m in match_symbols(x, lk)]
    assert t("Reserve Bank of India sets new rules for securitisation notes") == []
    assert t("Union Bank of India and Central Bank of India cut MCLR") == ["CENTRALBK", "UNIONBANK"]
    assert t("Bank of India Q2 profit rises 20%") == ["BANKINDIA"]
