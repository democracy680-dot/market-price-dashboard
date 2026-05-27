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


def get_tx_url() -> str:
    """Transaction-pooler URL (port 6543) — higher concurrency than session mode (5432)."""
    return get_db_url().replace(":5432/", ":6543/")


def get_engine(max_attempts: int = 3):
    """
    Return a SQLAlchemy engine with connection resilience.
    Tries session-mode pooler (5432) first, then falls back to transaction-mode
    pooler (6543) if the session pool is exhausted.
    NullPool avoids stale PgBouncer connections across long-running loops.
    """
    urls_to_try = [get_tx_url(), get_db_url()]
    last_exc = None
    for url in urls_to_try:
        for attempt in range(max_attempts):
            try:
                engine = create_engine(
                    url,
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
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                return engine
            except Exception as exc:
                last_exc = exc
                if attempt < max_attempts - 1:
                    wait = 2 ** attempt
                    logger.warning(f"DB connection attempt {attempt + 1} failed: {exc}. Retrying in {wait}s...")
                    time.sleep(wait)
        logger.warning("Session pooler exhausted, trying transaction pooler...")

    raise RuntimeError(f"Could not connect to database after all attempts: {last_exc}")


def get_psycopg2_conn():
    """Raw psycopg2 connection — use for bulk inserts with execute_values.
    Falls back to transaction pooler (6543) if session pool is exhausted.
    """
    for url in [get_db_url(), get_tx_url()]:
        try:
            return psycopg2.connect(url, connect_timeout=10, keepalives=1,
                                    keepalives_idle=30, keepalives_interval=5,
                                    keepalives_count=3)
        except psycopg2.OperationalError as exc:
            err = str(exc).lower()
            if "max_client_conn" in err or "remaining connection slots" in err or "too many clients" in err:
                logger.warning("Session pooler full, retrying with transaction pooler...")
                continue
            raise
    raise RuntimeError("Could not open psycopg2 connection on either pooler")


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
