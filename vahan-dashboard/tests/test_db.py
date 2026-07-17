from models import SaleRow
from store import db


def _conn(tmp_path):
    conn = db.open_db(str(tmp_path / "t.db"))
    db.init_schema(conn)
    return conn


def test_upsert_and_read_back(tmp_path):
    conn = _conn(tmp_path)
    rows = [
        SaleRow("2026-06", "TWO WHEELER", "HERO", 100),
        SaleRow("2026-06", "TWO WHEELER", "HONDA", 80),
    ]
    written = db.upsert_sales(conn, rows)
    assert written == 2
    got = db.get_rows_for_month(conn, "2026-06")
    assert set(got) == set(rows)


def test_upsert_is_idempotent(tmp_path):
    conn = _conn(tmp_path)
    row = SaleRow("2026-06", "TWO WHEELER", "HERO", 100)
    db.upsert_sales(conn, [row])
    db.upsert_sales(conn, [SaleRow("2026-06", "TWO WHEELER", "HERO", 150)])
    got = db.get_rows_for_month(conn, "2026-06")
    assert got == [SaleRow("2026-06", "TWO WHEELER", "HERO", 150)]


def test_has_month(tmp_path):
    conn = _conn(tmp_path)
    assert db.has_month(conn, "2026-06") is False
    db.upsert_sales(conn, [SaleRow("2026-06", "TWO WHEELER", "HERO", 100)])
    assert db.has_month(conn, "2026-06") is True
