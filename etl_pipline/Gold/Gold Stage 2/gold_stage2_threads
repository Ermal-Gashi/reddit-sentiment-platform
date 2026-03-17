import os
import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# ============================================================
# 1. SETUP
# ============================================================
load_dotenv()
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")

def get_conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB,
        user=PGUSER, password=PGPASSWORD
    )

# ============================================================
# 2. Fetch recent thread IDs (incremental mode)
# ============================================================
def fetch_recent_thread_ids(days=3, min_comments=2):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    sql = """
        SELECT 
            COALESCE(root_comment_id, comment_id) AS root_id
        FROM silver.comments
        WHERE created_ts >= %s
        GROUP BY COALESCE(root_comment_id, comment_id)
        HAVING COUNT(*) >= %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (cutoff, min_comments))
        rows = cur.fetchall()
        return [r[0] for r in rows if r[0]]

# ============================================================
# 3. Fetch thread-level metrics (SQL-only)
# ============================================================
def fetch_thread_metrics_for(root_ids):
    if not root_ids:
        return []

    sql = """
        WITH base AS (
            SELECT
                COALESCE(root_comment_id, comment_id) AS root_id,
                comment_id,
                source_table,
                depth,
                created_ts,
                is_op
            FROM silver.comments
        )
        SELECT
            b.root_id AS root_comment_id,
            b.source_table,

            COUNT(*) AS n_comments,
            MAX(depth) AS max_depth,
            AVG(depth) AS mean_depth,

            -- Branch factor
            (COUNT(*) - 1)::float /
                NULLIF(SUM(CASE WHEN depth > 0 THEN 1 ELSE 0 END), 0)
                    AS branch_factor,

            -- Time to first response
            EXTRACT(EPOCH FROM(
                MIN(CASE WHEN depth = 1 THEN created_ts END) -
                MIN(CASE WHEN depth = 0 THEN created_ts END)
            )) AS first_response_sec,

            -- Lifetime
            EXTRACT(EPOCH FROM(MAX(created_ts) - MIN(created_ts)))
                AS thread_lifetime_sec,

            -- OP participation
            SUM(CASE WHEN is_op THEN 1 ELSE 0 END)::float / COUNT(*)
                AS op_participation_rate,

            -- Sentiment
            AVG(g.sentiment_score) AS avg_sentiment,
            STDDEV_SAMP(g.sentiment_score) AS sentiment_std,

            SUM(CASE WHEN g.sentiment_score > 0.10 THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(g.sentiment_score), 0)
                AS pos_ratio,

            SUM(CASE WHEN g.sentiment_score < -0.10 THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(g.sentiment_score), 0)
                AS neg_ratio,

            -- Emotion
            MODE() WITHIN GROUP (ORDER BY g.emotion_label)
                AS thread_emotion_label,

            AVG(g.emotion_strength)
                AS thread_emotion_strength,

            NOW() AT TIME ZONE 'UTC' AS last_updated

        FROM base b
        LEFT JOIN gold.comment_features g
            ON g.comment_id = b.comment_id
        WHERE b.root_id = ANY(%s)
        GROUP BY b.root_id, b.source_table;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, (root_ids,))
        return cur.fetchall()

# ============================================================
# 4. UPSERT
# ============================================================
def upsert_thread_metrics(records):
    if not records:
        return 0

    cols = list(records[0].keys())
    values = [[rec[col] for col in cols] for rec in records]

    sql = f"""
        INSERT INTO gold.thread_metrics ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (root_comment_id, source_table)
        DO UPDATE SET
            {", ".join([
                f"{c}=EXCLUDED.{c}"
                for c in cols if c not in ("root_comment_id","source_table")
            ])};
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
    return len(records)


def fetch_all_thread_ids(min_comments=2):
    """
    Return ALL root threads in the dataset,
    except single-comment threads.
    """
    sql = """
        SELECT
            COALESCE(root_comment_id, comment_id) AS root_id
        FROM silver.comments
        GROUP BY COALESCE(root_comment_id, comment_id)
        HAVING COUNT(*) >= %s;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (min_comments,))
        rows = cur.fetchall()
        return [r[0] for r in rows if r[0]]




# ============================================================
# 5. RUNNER (Incremental + Full Backfill Mode)
# ============================================================
if __name__ == "__main__":
    print("🚀 Gold Stage 2 v5.2 — Minimalist Thread Metrics")

    import sys
    full_mode = len(sys.argv) > 1 and sys.argv[1] == "--full"

    # --------------------------------------------------------
    # Select mode
    # --------------------------------------------------------
    if full_mode:
        print(" FULL BACKFILL MODE — Rebuilding ALL historical threads…")
        root_ids = fetch_all_thread_ids(min_comments=2)
    else:
        print("⚡ INCREMENTAL MODE — Last 3 days only…")
        root_ids = fetch_recent_thread_ids(days=14, min_comments=2)

    print(f"[INFO] Found {len(root_ids):,} multi-comment threads.")

    # --------------------------------------------------------
    # Processing threads in batches
    # --------------------------------------------------------
    total = 0
    batch_size = 500

    for i in range(0, len(root_ids), batch_size):
        batch = root_ids[i:i+batch_size]

        rows = fetch_thread_metrics_for(batch)
        rows = [dict(r) for r in rows]

        # Add polarization + defaults
        for r in rows:
            avg = r["avg_sentiment"] or 0.0
            std = r["sentiment_std"] or 0.0
            r["polarization"] = std / (abs(avg) + 0.05)

            if r["first_response_sec"] is None:
                r["first_response_sec"] = 999999.0

            r["z_score"] = 0  # will be replaced later

        inserted = upsert_thread_metrics(rows)
        total += inserted

        print(
            f"[Batch {i//batch_size+1}] → {inserted} threads updated "
            f"({total:,} total)"
        )

    # --------------------------------------------------------
    # Recompute global z-scores on branch_factor
    # --------------------------------------------------------
    print("\n[INFO] Recomputing z-scores for branch_factor …")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT AVG(branch_factor), STDDEV_SAMP(branch_factor)
            FROM gold.thread_metrics;
        """)
        mu, sd = cur.fetchone()
        mu = mu or 0
        sd = sd or 1

        cur.execute("""
            UPDATE gold.thread_metrics
            SET z_score =
                CASE
                    WHEN %s > 0 THEN (branch_factor - %s) / %s
                    ELSE 0
                END;
        """, (sd, mu, sd))
        conn.commit()

    print("====================================================")
    print(f"✔ DONE — Updated {total:,} threads total")
    print(f"✔ Z-score μ={mu:.3f}, σ={sd:.3f}")
    print("====================================================")
