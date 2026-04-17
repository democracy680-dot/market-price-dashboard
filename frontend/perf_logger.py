"""
perf_logger.py — Lightweight timing instrumentation for Streamlit pages.

Usage:
    from perf_logger import measure, show_perf_panel, reset_timings

    reset_timings()                     # call once at the top of each render
    with measure("load_data"):
        df = load_data()
    show_perf_panel()                   # call at the bottom to display results
"""

import os
import time
import streamlit as st
from contextlib import contextmanager
from collections import defaultdict

_timings: dict[str, list[float]] = defaultdict(list)


@contextmanager
def measure(label: str):
    """Context manager that times a block of code and stores it in _timings."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        _timings[label].append(elapsed)


def show_perf_panel():
    """
    Renders a collapsible debug panel showing per-operation timing for the
    current page render. Gated behind DEBUG=true environment variable.
    """
    if os.getenv("DEBUG", "").lower() != "true":
        return
    with st.expander("🔧 Performance Debug", expanded=False):
        if not _timings:
            st.write("No timing data collected this render.")
            return
        total = 0.0
        rows = []
        for label, times in sorted(_timings.items(), key=lambda x: -x[1][-1]):
            latest = times[-1]
            total += latest
            rows.append({
                "Operation": label,
                "Time (ms)": f"{latest * 1000:.0f}",
                "Calls": len(times),
            })
        st.write(f"**Total measured time: {total * 1000:.0f} ms**")
        st.table(rows)


def reset_timings():
    """Call once at the start of each full-page render to clear stale data."""
    _timings.clear()
