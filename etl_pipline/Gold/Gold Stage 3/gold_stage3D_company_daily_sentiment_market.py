
import os
import argparse
import logging
import numpy as np
import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv
from datetime import datetime, timedelta

# -------------------------------------------------------------------
# 1. Setup
# -------------------------------------------------------------------
load_dotenv()

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def get_conn():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDB,
        user=PGUSER,
        password=PGPASSWORD
    )


# -------------------------------------------------------------------
# 2. Ensure gold.sentiment_daily exists
# -------------------------------------------------------------------
TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.sentiment_daily (
    date_utc             DATE NOT NULL,
    scope                TEXT NOT NULL,          
    subreddit            TEXT,                   
    n_comments           INTEGER,
    mean_sentiment       DOUBLE PRECISION,
    median_sentiment     DOUBLE PRECISION,
    pct_positive         DOUBLE PRECISION,
    pct_negative         DOUBLE PRECISION,
    sentiment_volatility DOUBLE PRECISION,
    sentiment_delta      DOUBLE PRECISION,
    zscore_anomaly       DOUBLE PRECISION,
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date_utc, scope, subreddit)
);

CREATE INDEX IF NOT EXISTS idx_sent_daily_date
    ON gold.sentiment_daily (date_utc);

CREATE INDEX IF NOT EXISTS idx_sent_daily_scope
    ON gold.sentiment_daily (scope);

CREATE INDEX IF NOT EXISTS idx_sent_daily_subreddit
    ON gold.sentiment_daily (subreddit);
"""


def ensure_table_exists():
    with get_conn() as conn, conn.cursor() as cur:
        for stmt in TABLE_DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s + ";")
        conn.commit()
    logging.info(" Ensured gold.sentiment_daily exists.")


# -------------------------------------------------------------------
# 3. Fetch base sentiment rows
# -------------------------------------------------------------------
def fetch_comment_sentiments(full_mode=False, days=14, start_date=None):
    """
    Pulls ALL comments with sentiment, joined to silver.comments
    to get created_ts and subreddit.

    Modes:
      • full_mode=True   → all available history
      • start_date set   → only rows from that date onward
      • default          → last N days (days)
    """
    if full_mode:
        logging.info("Running FULL backfill for sentiment_daily.")
        date_filter = ""
        params = ()
    elif start_date:
        logging.info(f"Running incremental mode from {start_date}.")
        date_filter = "WHERE COALESCE(c.created_ts, g.processed_at) >= %s"
        params = (start_date,)
    else:
        logging.info(f"Running limited mode (last {days} days).")
        date_filter = """
            WHERE COALESCE(c.created_ts, g.processed_at)
                  >= CURRENT_DATE - (%s || ' days')::interval
        """
        params = (str(days),)

    sql = f"""
        SELECT
            DATE_TRUNC('day', COALESCE(c.created_ts, g.processed_at))::date AS date_utc,
            c.subreddit AS subreddit,
            g.sentiment_score::float AS sentiment_score
        FROM gold.comment_features g
        LEFT JOIN silver.comments c
          ON c.comment_id = g.comment_id
        {date_filter}
        ORDER BY date_utc ASC;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    logging.info(f"Fetched {len(rows)} comment sentiment rows.")
    return rows


# -------------------------------------------------------------------
# 4. Aggregate: global + per-subreddit
# -------------------------------------------------------------------
def aggregate_sentiment_daily(rows):
    """
    Builds two sets of aggregates:
      • per (date_utc, subreddit)  → scope='subreddit'
      • per date_utc (all subs)    → scope='global'
    """
    if not rows:
        logging.warning("No rows fetched to aggregate.")
        return []

    from collections import defaultdict

    per_sub = defaultdict(list)
    per_global = defaultdict(list)

    for r in rows:
        date_utc = r["date_utc"]
        subreddit = r["subreddit"]
        score = r["sentiment_score"]

        if score is None:
            continue

        # per subreddit (scope='subreddit') — only if we actually know the subreddit
        if subreddit is not None:
            per_sub[(date_utc, subreddit)].append(score)

        # global (scope='global') — always includes ALL comments
        per_global[date_utc].append(score)

    results = []

    # ---- helper to compute metrics ----
    def compute_metrics(scores):
        scores = [float(s) for s in scores]
        if not scores:
            return None

        n = len(scores)
        mean_sent = float(np.mean(scores))
        median_sent = float(np.median(scores))
        std_sent = float(np.std(scores)) if n > 1 else 0.0
        pct_pos = sum(1 for s in scores if s > 0.05) / n
        pct_neg = sum(1 for s in scores if s < -0.05) / n

        return {
            "n_comments": n,
            "mean_sentiment": mean_sent,
            "median_sentiment": median_sent,
            "pct_positive": pct_pos,
            "pct_negative": pct_neg,
            "sentiment_volatility": std_sent,  # initial; rolling will overwrite
            "sentiment_delta": None,
            "zscore_anomaly": None,
        }

    # ---- per subreddit ----
    for (date_utc, subreddit), scores in per_sub.items():
        metrics = compute_metrics(scores)
        if not metrics:
            continue

        rec = {
            "date_utc": date_utc,
            "scope": "subreddit",
            "subreddit": subreddit,
        }
        rec.update(metrics)
        results.append(rec)

    # ---- global ----
    for date_utc, scores in per_global.items():
        metrics = compute_metrics(scores)
        if not metrics:
            continue

        rec = {
            "date_utc": date_utc,
            "scope": "global",
            "subreddit": "global",  # NULL for global aggregates
        }
        rec.update(metrics)
        results.append(rec)

    logging.info(f"Aggregated {len(results)} sentiment_daily rows.")
    return results


# -------------------------------------------------------------------
# 5. Upsert into gold.sentiment_daily
# -------------------------------------------------------------------
def upsert_sentiment_daily(records):
    if not records:
        logging.warning("No sentiment_daily records to upsert.")
        return 0

    cols = list(records[0].keys())
    values = [[r[c] for c in cols] for r in records]

    sql = f"""
        INSERT INTO gold.sentiment_daily ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (date_utc, scope, subreddit)
        DO UPDATE SET
            n_comments           = EXCLUDED.n_comments,
            mean_sentiment       = EXCLUDED.mean_sentiment,
            median_sentiment     = EXCLUDED.median_sentiment,
            pct_positive         = EXCLUDED.pct_positive,
            pct_negative         = EXCLUDED.pct_negative,
            sentiment_volatility = EXCLUDED.sentiment_volatility,
            sentiment_delta      = EXCLUDED.sentiment_delta,
            zscore_anomaly       = EXCLUDED.zscore_anomaly,
            created_at           = NOW();
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        conn.commit()

    logging.info(f" Upserted {len(records)} rows into gold.sentiment_daily.")
    return len(records)


# -------------------------------------------------------------------
# 6. Rolling volatility + z-score
# -------------------------------------------------------------------
def update_rolling_stats():
    """
    Computes:
      • sentiment_delta (day-over-day)
      • sentiment_volatility (3-day rolling stddev)
      • zscore_anomaly ((today - roll_mean) / roll_std)
    Partitioned by (scope, subreddit).
    """
    sql = """
    WITH diffs AS (
        SELECT
            date_utc,
            scope,
            subreddit,
            mean_sentiment,
            n_comments,
            LAG(mean_sentiment) OVER (
                PARTITION BY scope, subreddit
                ORDER BY date_utc
            ) AS prev_sent,
            AVG(mean_sentiment) OVER (
                PARTITION BY scope, subreddit
                ORDER BY date_utc
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS roll_mean,
            STDDEV_SAMP(mean_sentiment) OVER (
                PARTITION BY scope, subreddit
                ORDER BY date_utc
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS roll_std
        FROM gold.sentiment_daily
    )
    UPDATE gold.sentiment_daily AS t
    SET
        sentiment_delta = diffs.mean_sentiment - diffs.prev_sent,
        sentiment_volatility = COALESCE(diffs.roll_std, 0),
        zscore_anomaly = COALESCE(
            CASE
                WHEN diffs.roll_std IS NULL OR diffs.roll_std = 0 THEN 0
                ELSE (diffs.mean_sentiment - diffs.roll_mean) / diffs.roll_std
            END,
            0
        )
    FROM diffs
    WHERE t.date_utc = diffs.date_utc
      AND t.scope = diffs.scope
      AND COALESCE(t.subreddit, '') = COALESCE(diffs.subreddit, '');
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

    logging.info(" Updated rolling sentiment_delta, volatility, and z-score in gold.sentiment_daily.")


# -------------------------------------------------------------------
# 7. Helper: get last date for incremental mode
# -------------------------------------------------------------------
def get_last_date():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT MAX(date_utc) FROM gold.sentiment_daily;")
        row = cur.fetchone()
        return row[0]


# -------------------------------------------------------------------
# 8. Runner
# -------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()

    # NEW: full mode must be explicitly requested
    ap.add_argument(
        "--full",
        action="store_true",
        help="Run FULL historical backfill (manual use only)"
    )

    args = ap.parse_args()

    logging.info("🚀 Starting Gold Stage 3C — Global + Subreddit Daily Sentiment")

    ensure_table_exists()

    # ---------------------------------------------------------------
    # DEFAULT = INCREMENTAL MODE
    # ---------------------------------------------------------------
    if args.full:
        # FULL BACKFILL
        logging.info("🟣 FULL mode explicitly requested → rebuilding entire history.")
        rows = fetch_comment_sentiments(full_mode=True)

    else:
        # INCREMENTAL MODE (default)
        last_date = get_last_date()

        if last_date is None:
            logging.info("No existing sentiment_daily data found → running FULL backfill once.")
            rows = fetch_comment_sentiments(full_mode=True)

        else:
            # Incremental start = last_date + 1 day
            start_date = last_date + timedelta(days=1)
            logging.info(f"🟢 Incremental mode → processing dates after {last_date} (start={start_date})")

            rows = fetch_comment_sentiments(
                full_mode=False,
                start_date=start_date
            )

    # ---------------------------------------------------------------
    # Process + Upsert
    # ---------------------------------------------------------------
    daily_records = aggregate_sentiment_daily(rows)
    upsert_sentiment_daily(daily_records)
    update_rolling_stats()

    # Summary
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(date_utc), MAX(date_utc) FROM gold.sentiment_daily;")
        count, mind, maxd = cur.fetchone()

    logging.info(f"🏁 Stage 3C complete — {count} rows from {mind} → {maxd}")
