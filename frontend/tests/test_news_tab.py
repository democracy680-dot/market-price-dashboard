"""Tests for news_tab pure helpers. Run: pytest frontend/tests/test_news_tab.py"""
import os, sys
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from news_tab import cards_html, cluster_articles, fmt_age, list_html, title_words

T = {"bg_tag": "#dbeafe", "text_accent": "#1d4ed8", "bd_tag": "#bfdbfe", "bd_card": "#e2e8f0",
     "card_title": "#0f172a", "text_muted": "#64748b"}


def _df(rows):
    return pd.DataFrame(rows, columns=["title", "source_name", "published_at", "url", "summary", "symbols"])


def test_title_words_drops_stop_words():
    assert title_words("Sensex plunges 1,484 points as banks drag; Nifty at 25,349") >= {"sensex", "plunges", "484", "banks", "nifty"}
    assert "the" not in title_words("The market today")


def test_same_story_from_several_outlets_is_merged():
    t0 = pd.Timestamp("2026-11-24 10:00", tz="UTC")
    df = _df([
        ("Markets take heavy drubbing dragged by banks; Sensex plunges 1,484 points", "Business Line", t0, "u1", "", "HDFCBANK"),
        ("Sensex plunges 1,484 points, Nifty dives as banks drag markets", "Livemint", t0 - pd.Timedelta("1h"), "u2", "", "ICICIBANK"),
        ("Tata Steel may rework APL Apollo deal", "CNBC-TV18", t0 - pd.Timedelta("2h"), "u3", "", "APLAPOLLO,TATASTEEL"),
    ])
    c = cluster_articles(df)
    assert len(c) == 2
    assert c[0]["main"]["source_name"] == "Business Line" and len(c[0]["others"]) == 1
    assert c[0]["symbols"] == ["HDFCBANK", "ICICIBANK"]
    assert c[1]["others"] == []


def test_similar_but_different_stories_stay_separate():
    t0 = pd.Timestamp("2026-11-24 10:00", tz="UTC")
    df = _df([
        ("Tata Steel shares fall 3% on weak China demand", "ET", t0, "u1", "", None),
        ("Tata Power shares rise 4% on new order win", "Mint", t0, "u2", "", None),
        ("Tata Steel shares fall 3% on weak China demand", "ET", t0 - pd.Timedelta("3D"), "u3", "", None),  # outside window
    ])
    assert len(cluster_articles(df)) == 3


def test_fmt_age():
    now = pd.Timestamp("2026-11-25 12:00", tz="UTC")
    assert fmt_age(now - pd.Timedelta("5min"), now) == "5m ago"
    assert fmt_age(now - pd.Timedelta("3h"), now) == "3h ago"
    assert fmt_age(now - pd.Timedelta("30h"), now) == "Yesterday"
    assert fmt_age(pd.Timestamp("2026-11-20 10:00", tz="UTC"), now) == "Nov 20"


def test_html_renders_and_escapes():
    t0 = pd.Timestamp("2026-11-24 10:00", tz="UTC")
    df = _df([("TCS <Q2 results>: profit beats street estimates", "ET", t0, "https://x/1", "s", "TCS"),
              ("TCS Q2 results: profit beats estimates", "Mint", t0, "https://x/2", "s", "TCS")])
    c = cluster_articles(df)
    h = cards_html(c, T, dark=False)
    assert "&lt;Q2 results&gt;" in h and "2 sources" in h and "Also covered by" in h
    assert "+1 more" in list_html(c, T)


def test_same_source_template_headlines_are_not_merged():
    t0 = pd.Timestamp("2026-11-24 10:00", tz="UTC")
    df = _df([("Arsenal vs Dortmund Live Streaming: Likely Lineups; How To Watch Champions League Match", "NDTV Profit", t0, "u1", "", None),
              ("Real Madrid vs PSV Live Streaming: Likely Lineups; How To Watch Champions League Match", "NDTV Profit", t0, "u2", "", None)])
    assert len(cluster_articles(df)) == 2
