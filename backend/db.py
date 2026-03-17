from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
from typing import Optional

# ---- Direct connection settings (edit to match your setup) ----
PGHOST = "localhost"
PGPORT = 5432
PGDB   = "reddit_warehouse"
PGUSER = "postgres"
PGPASSWORD = "ermal123gashi"
# ---------------------------------------------------------------

_pool: Optional[SimpleConnectionPool] = None

def init_pool(minconn: int = 1, maxconn: int = 10):
    """Initialize the PostgreSQL connection pool."""
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn, maxconn,
            host=PGHOST,
            port=PGPORT,
            dbname=PGDB,
            user=PGUSER,
            password=PGPASSWORD,
            connect_timeout=5
        )
    return _pool

@contextmanager
def get_conn():
    """Context manager to borrow a connection from the pool."""
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call init_pool() first.")
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)
