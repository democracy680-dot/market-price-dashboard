import os
import psycopg2
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def get_db_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("SUPABASE_DB_URL environment variable not set.")
    return url


def get_engine():
    return create_engine(get_db_url(), pool_pre_ping=True)


def get_psycopg2_conn():
    """Raw psycopg2 connection — use for bulk inserts with execute_values."""
    return psycopg2.connect(get_db_url())
