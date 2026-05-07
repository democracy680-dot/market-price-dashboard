"""Query functions for the Quarterly Results earnings tab."""

import pandas as pd
from sqlalchemy import text


def get_earnings_today(engine, date) -> pd.DataFrame:
    """Companies announcing on `date` with their announcement-day 1D return.

    Returns columns: symbol, name, market_cap_cr, announcement_day_return, cmp
    """
    sql = text("""
        SELECT
            ec.symbol,
            s.name,
            sd.market_cap_cr,
            sd.ret_1d   AS announcement_day_return,
            sd.cmp
        FROM earnings_calendar ec
        JOIN stocks s ON ec.symbol = s.symbol
        LEFT JOIN snapshots_daily sd
            ON ec.symbol = sd.symbol AND sd.date = ec.result_date
        WHERE ec.result_date = :today
        ORDER BY sd.ret_1d DESC NULLS LAST
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"today": date}).fetchall()
    return pd.DataFrame(rows, columns=["symbol", "name", "market_cap_cr",
                                       "announcement_day_return", "cmp"])


def get_earnings_season(engine, today) -> pd.DataFrame:
    """All companies that have announced results up to `today`.

    Returns columns: symbol, name, result_date, market_cap_cr,
                     announcement_day_return, return_since_announcement
    """
    sql = text("""
        SELECT
            ec.symbol,
            s.name,
            ec.result_date,
            sd_ann.market_cap_cr,
            sd_ann.ret_1d AS announcement_day_return,
            CASE
                WHEN sd_ann.cmp > 0 AND latest.cmp > 0
                THEN ROUND(
                    CAST((latest.cmp - sd_ann.cmp) / sd_ann.cmp AS NUMERIC), 6
                )
                ELSE NULL
            END AS return_since_announcement
        FROM earnings_calendar ec
        JOIN stocks s ON ec.symbol = s.symbol
        LEFT JOIN snapshots_daily sd_ann
            ON ec.symbol = sd_ann.symbol AND sd_ann.date = ec.result_date
        LEFT JOIN LATERAL (
            SELECT cmp
            FROM snapshots_daily
            WHERE symbol = ec.symbol
            ORDER BY date DESC
            LIMIT 1
        ) latest ON TRUE
        WHERE ec.result_date <= :today
        ORDER BY ec.result_date DESC, sd_ann.ret_1d DESC NULLS LAST
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"today": today}).fetchall()
    return pd.DataFrame(rows, columns=["symbol", "name", "result_date", "market_cap_cr",
                                       "announcement_day_return", "return_since_announcement"])
