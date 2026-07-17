import sqlite3
from typing import Iterable

from models import SaleRow


def open_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            month      TEXT NOT NULL,
            category   TEXT NOT NULL,
            maker      TEXT NOT NULL,
            units      INTEGER NOT NULL,
            scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (month, category, maker)
        )
        """
    )
    conn.commit()


def upsert_sales(conn: sqlite3.Connection, rows: Iterable[SaleRow]) -> int:
    count = 0
    for r in rows:
        conn.execute(
            """
            INSERT INTO sales (month, category, maker, units)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(month, category, maker)
            DO UPDATE SET units = excluded.units, scraped_at = datetime('now')
            """,
            (r.month, r.category, r.maker, int(r.units)),
        )
        count += 1
    conn.commit()
    return count


def get_rows_for_month(conn: sqlite3.Connection, month: str) -> list[SaleRow]:
    cur = conn.execute(
        "SELECT month, category, maker, units FROM sales WHERE month = ? "
        "ORDER BY category, units DESC, maker",
        (month,),
    )
    return [SaleRow(row["month"], row["category"], row["maker"], row["units"]) for row in cur]


def has_month(conn: sqlite3.Connection, month: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sales WHERE month = ? LIMIT 1", (month,))
    return cur.fetchone() is not None
