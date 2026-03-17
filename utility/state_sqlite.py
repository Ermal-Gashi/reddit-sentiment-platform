import os
import sqlite3
import time

# ============================================================
#  ⚙️ Configuration toggle
# ============================================================
# Set to False to skip refreshing threads older than 24 hours
ENABLE_OLD_THREAD_REFRESH = False  # 👈 Turn ON later if needed

# ============================================================
#  🔧 MOD: Global path + helper to open per-thread connections
# ============================================================
_STATE_DB_PATH = None

def get_conn():
    """Return a new SQLite connection (per-thread, safe across threads)."""
    if not _STATE_DB_PATH:
        raise RuntimeError("State DB not initialized — call init_state_db() first.")
    return sqlite3.connect(_STATE_DB_PATH, check_same_thread=False)

# ============================================================
#  INIT: Create / upgrade SQLite state database
# ============================================================
def init_state_db(db_path: str):
    """Create or upgrade local SQLite state database with performance optimizations."""
    global _STATE_DB_PATH
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _STATE_DB_PATH = db_path

    conn = sqlite3.connect(db_path, check_same_thread=False)
    cur = conn.cursor()

    # --- Performance PRAGMAs ---
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")     # safe balance of speed/reliability
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-40000;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # --- Core tables ---
    cur.execute("CREATE TABLE IF NOT EXISTS seen_comments (comment_id TEXT PRIMARY KEY)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_submissions (
            submission_id TEXT PRIMARY KEY,
            last_checked_utc REAL
        )
    """)

    # --- Auto-migrate old schema ---
    cur.execute("PRAGMA table_info(seen_submissions);")
    cols = [r[1] for r in cur.fetchall()]
    if "last_checked_utc" not in cols:
        print("[Migration] Adding missing column 'last_checked_utc' to seen_submissions …")
        cur.execute("ALTER TABLE seen_submissions ADD COLUMN last_checked_utc REAL;")

    # --- Active threads tracking ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_threads (
            submission_id TEXT PRIMARY KEY,
            subreddit TEXT,
            title TEXT,
            created_utc REAL,
            last_checked_utc REAL,
            last_comment_count INTEGER,
            inactive_streak INTEGER DEFAULT 0
        )
    """)

    # --- Indexes ---
    cur.execute("CREATE INDEX IF NOT EXISTS idx_seen_comments_id ON seen_comments(comment_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_seen_submissions_id ON seen_submissions(submission_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_active_threads_checked ON active_threads(last_checked_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_active_threads_sub ON active_threads(submission_id)")

    conn.commit()
    conn.close()
    print("[State DB] Tables verified / migrations applied successfully (optimized).")
    return db_path


# ============================================================
#  SEEN HELPERS
# ============================================================
def is_comment_seen(_, comment_id: str) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM seen_comments WHERE comment_id = ? LIMIT 1", (comment_id,))
        return cur.fetchone() is not None


def mark_comment_seen(_, comment_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO seen_comments(comment_id) VALUES (?)", (comment_id,))
        conn.commit()


# ============================================================
# 🚀 Optimized batch insert for seen_comments
# ============================================================
def mark_comments_seen_batch(_, comment_ids: list[str]):
    """Batch insert multiple comment IDs efficiently with transaction + fast mode."""
    if not comment_ids:
        return
    conn = get_conn()
    cur = conn.cursor()
    try:
        # 🔥 Temporary high-speed mode
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("BEGIN TRANSACTION;")

        cur.executemany(
            "INSERT OR IGNORE INTO seen_comments(comment_id) VALUES (?)",
            [(cid,) for cid in comment_ids]
        )

        conn.commit()
        cur.execute("PRAGMA synchronous=NORMAL;")  # restore safety
    except Exception as e:
        conn.rollback()
        print(f"[SQLite WARN] mark_comments_seen_batch rollback — {e}")
    finally:
        conn.close()


def is_submission_seen_recent(_, submission_id: str, hours: int = 12) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM seen_submissions
            WHERE submission_id = ?
              AND (strftime('%s','now') - last_checked_utc) < ?
            LIMIT 1
        """, (submission_id, hours * 3600))
        return cur.fetchone() is not None


def mark_submission_seen(_, submission_id: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO seen_submissions(submission_id, last_checked_utc)
            VALUES (?, strftime('%s','now'))
            ON CONFLICT(submission_id)
            DO UPDATE SET last_checked_utc = strftime('%s','now');
        """, (submission_id,))
        conn.commit()


# ============================================================
# 🚀 Optimized bulk upsert for active_threads
# ============================================================
def bulk_upsert_active_threads(records: list[tuple]):
    """
    Batch insert/update active_threads in a single transaction (fast mode).
    Each record is a tuple: (submission_id, subreddit, title, created_utc, comment_count)
    """
    if not records:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    now = time.time()
    try:
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute("BEGIN TRANSACTION;")

        cur.executemany("""
            INSERT INTO active_threads (
                submission_id, subreddit, title, created_utc,
                last_checked_utc, last_comment_count, inactive_streak
            )
            VALUES (?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(submission_id) DO UPDATE SET
                subreddit = excluded.subreddit,
                title = excluded.title,
                last_checked_utc = ?,
                inactive_streak = CASE
                    WHEN excluded.last_comment_count = active_threads.last_comment_count
                    THEN active_threads.inactive_streak + 1
                    ELSE 0
                END,
                last_comment_count = excluded.last_comment_count;
        """, [(sid, sub, ttl, cts, now, ccnt, now) for sid, sub, ttl, cts, ccnt in records])

        conn.commit()
        cur.execute("PRAGMA synchronous=NORMAL;")
        return len(records)
    except Exception as e:
        conn.rollback()
        print(f"[SQLite WARN] bulk_upsert_active_threads rollback — {e}")
        return 0
    finally:
        conn.close()


# ============================================================
#  ACTIVE THREAD LOGIC (standard read/update ops)
# ============================================================
def get_threads_to_refresh(_, max_age_hours: int = 12):
    """
    Returns a list of threads to refresh, unless the feature is disabled.
    """
    if not ENABLE_OLD_THREAD_REFRESH:
        print("[Refresh Log] Skipping old-thread refresh (disabled by config).")
        return []

    cutoff = time.time() - max_age_hours * 3600
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT submission_id, subreddit, title, created_utc,
                   last_checked_utc, last_comment_count, inactive_streak
            FROM active_threads
            WHERE last_checked_utc <= ?
        """, (cutoff,))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if rows:
        print(f"[Refresh Log] {len(rows)} thread(s) due for refresh:")
        for t in rows[:10]:
            hrs = round((time.time() - (t['last_checked_utc'] or 0)) / 3600, 2)
            print(f"  - {t['submission_id']} ({t['subreddit']}) last checked {hrs}h ago")
        if len(rows) > 10:
            print(f"  ... and {len(rows)-10} more.")
    else:
        print("[Refresh Log] No threads due for refresh.")
    return rows


def should_refresh_thread(_, submission_id, current_comment_count, max_age_hours=12):
    now = time.time()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT last_comment_count, last_checked_utc
            FROM active_threads
            WHERE submission_id = ?
        """, (submission_id,))
        row = cur.fetchone()

    if not row:
        return True, {"last_comment_count": 0, "last_checked_utc": 0}

    prev_count, last_checked = row
    should = (
        current_comment_count > (prev_count or 0)
        or now - (last_checked or 0) > max_age_hours * 3600
    )
    return should, {"last_comment_count": prev_count, "last_checked_utc": last_checked}


def prune_inactive_threads(_, inactivity_threshold: int = 3, max_age_days: int = 14):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM active_threads
            WHERE inactive_streak >= ? OR
                  (strftime('%s','now') - created_utc) > (? * 86400)
        """, (inactivity_threshold, max_age_days))
        deleted = cur.rowcount
        conn.commit()
        return deleted


def ensure_thread_row(_, submission_id, subreddit, title, created_utc):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO active_threads (
                submission_id, subreddit, title, created_utc,
                last_checked_utc, last_comment_count, inactive_streak
            ) VALUES (?, ?, ?, ?, 0, 0, 0)
        """, (submission_id, subreddit, title, created_utc))
        conn.commit()


def update_thread_after_scan(_, submission_id, latest_comment_count, had_growth: bool):
    now = time.time()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE active_threads
            SET
                last_checked_utc = ?,
                last_comment_count = ?,
                inactive_streak = CASE
                    WHEN ? THEN 0
                    ELSE inactive_streak + 1
                END
            WHERE submission_id = ?
        """, (now, latest_comment_count, had_growth, submission_id))
        conn.commit()
