import os
import sys
import time
import logging
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Environment validation ────────────────────────────────────────────────────

REQUIRED_ENV_VARS = ["SUPABASE_DB_URL"]


def validate_env():
    missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
    if missing:
        print(f"ERROR: Missing required environment variables: {missing}", file=sys.stderr)
        sys.exit(1)


validate_env()


# ── Connection helpers ────────────────────────────────────────────────────────

def get_db_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("SUPABASE_DB_URL environment variable not set.")
    return url


def get_engine(max_attempts: int = 3):
    """
    Return a SQLAlchemy engine with connection resilience.
    NullPool avoids stale PgBouncer connections across long-running loops.
    Retries with exponential backoff on transient failures.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            engine = create_engine(
                get_db_url(),
                poolclass=NullPool,
                connect_args={
                    "connect_timeout": 10,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 5,
                    "keepalives_count": 3,
                    "options": "-c statement_timeout=30000",  # 30s query timeout
                },
            )
            # Probe the connection on first attempt
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                wait = 2 ** attempt
                logger.warning(f"DB connection attempt {attempt + 1} failed: {exc}. Retrying in {wait}s...")
                time.sleep(wait)

    raise RuntimeError(f"Could not connect to database after {max_attempts} attempts: {last_exc}")


def get_psycopg2_conn():
    """Raw psycopg2 connection — use for bulk inserts with execute_values."""
    return psycopg2.connect(
        get_db_url(),
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=5,
        keepalives_count=3,
    )


# ── Health check ─────────────────────────────────────────────────────────────

def check_db_health() -> bool:
    """Return True if the database is reachable, False otherwise."""
    try:
        engine = create_engine(
            get_db_url(),
            poolclass=NullPool,
            connect_args={"connect_timeout": 5},
        )
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error(f"DB health check failed: {exc}")
        return False
