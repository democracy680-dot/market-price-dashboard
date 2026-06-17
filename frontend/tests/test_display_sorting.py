"""
Regression tests for the sorting bug: numeric columns in the dashboard tables
were being converted to formatted strings (e.g. "₹1,234.56", "+12.34%") before
being handed to st.dataframe. Streamlit sorts by the *underlying* data, so string
columns sorted lexicographically ("₹9.99" after "₹1,234.56", "+9%" after "+12%").

The fix keeps the underlying columns numeric and moves formatting into the
pandas Styler.format() layer (display-only). These tests assert the prep
functions return numeric dtypes for numeric columns, which is the contract that
guarantees correct interactive sorting.

app.py cannot be imported directly (it runs Streamlit at module scope and renders
tabs that hit the DB), so we exec only the definitions portion with `streamlit`
stubbed out.
"""
import os
import sys
import types
import pathlib

import numpy as np
import pandas as pd
import pytest
from pandas.api.types import is_numeric_dtype

FRONTEND = pathlib.Path(__file__).resolve().parent.parent
APP_PATH = FRONTEND / "app.py"


def _references_streamlit(node) -> bool:
    """True if the AST subtree reads the module-level `st` name (a Streamlit call)."""
    import ast
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id in ("st", "components"):
            return True
    return False


def _load_app_namespace():
    """Load app.py's pure helpers without running the Streamlit app.

    app.py interleaves module-level rendering code (st.* calls, DB reads) with its
    function definitions, so it can't be imported. We parse it and keep only:
    imports, function/class definitions, and module constants that don't touch
    Streamlit — then exec that filtered module. Functions may *reference* st in
    their bodies; they just aren't executed here.
    """
    import ast

    src = APP_PATH.read_text(encoding="utf-8")
    tree = ast.parse(src)

    kept = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom,
                             ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            kept.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            if not _references_streamlit(node):
                kept.append(node)
        # drop module-level Expr/If/With/For/Try (the rendering code)

    # Stub streamlit so any decorators (@st.cache_data) and stray refs are harmless.
    class _Any:
        def __getattr__(self, _):
            return _Any()

        def __call__(self, *a, **k):
            return _Any()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    sys.modules["streamlit"] = _Any()
    sys.modules["streamlit.components.v1"] = _Any()

    sys.path.insert(0, str(FRONTEND))
    ns: dict = {"__name__": "app_under_test", "__file__": str(APP_PATH)}

    # Exec each kept node independently; skip constants that depend on dropped
    # (Streamlit-derived) statements like the theme dict _T. Function defs never
    # fail here since their bodies aren't evaluated until called.
    for node in kept:
        mod = ast.Module(body=[node], type_ignores=[])
        ast.fix_missing_locations(mod)
        try:
            exec(compile(mod, str(APP_PATH), "exec"), ns)
        except Exception:
            pass
    return ns


APP = _load_app_namespace()


def _sample_df():
    """A small snapshot-shaped frame with the columns the prep functions read."""
    return pd.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC"],
            "name": ["Alpha", "Beta", "Gamma"],
            "sector": ["IT", "Pharma", "Auto"],
            "cmp": [9.99, 1234.56, 50000.0],
            "ret_1d": [0.0934, -0.012, 0.005],
            "ret_1w": [0.12, -0.09, np.nan],
            "ret_30d": [0.2, 0.05, -0.3],
            "ret_60d": [0.1, 0.0, -0.1],
            "ret_180d": [0.5, -0.2, 0.05],
            "ret_365d": [1.2, -0.5, 0.0],
            "market_cap_cr": [500.0, 120000.0, 9.5],
            "pe_ratio": [9.9, 123.4, np.nan],
            "status_50dma": ["Above 50DMA", "Below 50DMA", None],
            "status_200dma": ["Above 200DMA", "Below 200DMA", None],
            "pct_from_52wh": [-0.02, -0.5, -0.21],
            "vol_spike": [3.2, 9.9, 1.1],
        }
    )


NUMERIC_PRETTY_COLS = [
    "CMP", "1D%", "1W%", "30D%", "60D%", "180D%", "365D%",
    "MCap (Cr)", "P/E", "52W High%", "Vol Spike",
]


def test_prepare_display_keeps_numeric_columns_numeric():
    out = APP["prepare_display"](_sample_df())
    for col in NUMERIC_PRETTY_COLS:
        assert col in out.columns, f"missing column {col}"
        assert is_numeric_dtype(out[col]), (
            f"{col} must stay numeric for correct sorting, got {out[col].dtype}"
        )


def test_prepare_display_sorts_like_numbers_not_strings():
    out = APP["prepare_display"](_sample_df())
    # CMP ascending: 9.99 < 1234.56 < 50000  → symbols AAA, BBB, CCC
    order = out.assign(symbol=_sample_df()["symbol"]).sort_values("CMP")["symbol"].tolist()
    assert order == ["AAA", "BBB", "CCC"], f"CMP did not sort numerically: {order}"


def test_theme_display_keeps_numeric_columns_numeric():
    out = APP["_prepare_theme_display"](_sample_df())
    for col in ["CMP", "1D %", "1W %", "Market Cap (₹ Cr)", "P/E"]:
        assert col in out.columns, f"missing column {col}"
        assert is_numeric_dtype(out[col]), (
            f"{col} must stay numeric for correct sorting, got {out[col].dtype}"
        )


def test_color_helpers_accept_numeric_values():
    """Styler.map passes the underlying numeric value, so color fns must not assume str."""
    for fn_name in ["_color_return", "_color_vol_spike", "_color_rsi", "_style_adx",
                    "_color_slope", "_color_52wh", "_style_vol_ratio", "_color_dma"]:
        fn = APP[fn_name]
        for v in (12.5, -3.2, 0.0, np.nan):
            fn(v)  # must not raise
