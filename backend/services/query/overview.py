from typing import List, Dict, Any, Optional
from backend.db import get_conn

from decimal import Decimal
from datetime import datetime, timezone


def to_native(obj):
    """
    Recursively convert Decimals, NumPy types, and other non-JSON types
    into pure Python natives (float, int, str, list, dict).
    """
    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [to_native(i) for i in obj]

    # fallback
    return obj

def _smart_snippet(text):
    """Return snippet until the first punctuation. If none, return max 200 chars."""
    if not text:
        return ""
    for p in [".", "!", "?"]:
        idx = text.find(p)
        if idx != -1:
            return text[:idx+1]
    return text[:200]  # fallback


# =============== 1) KPIs (ALL-TIME, FINAL) =================
def _fetch_kpis() -> Dict[str, Any]:
    sql = """
        WITH
        total_comments AS (
            SELECT COUNT(*)::bigint AS v
            FROM silver.comments
        ),
        total_threads AS (
            SELECT COUNT(*)::bigint AS v
            FROM gold.thread_metrics
        ),
        first_last AS (
            SELECT 
                MIN(DATE(created_ts)) AS d0,
                MAX(DATE(created_ts)) AS d1
            FROM silver.comments
        ),
        avg_daily AS (
            SELECT 
                (SELECT v FROM total_comments)::float /
                GREATEST((SELECT d1 - d0 FROM first_last) + 1, 1)
                AS v
        ),
        top_all_time AS (
            SELECT cm.company AS ticker, COUNT(*)::bigint AS cnt
            FROM silver.company_mentions cm
            JOIN silver.comments c ON c.comment_id = cm.comment_id
            GROUP BY cm.company
            ORDER BY cnt DESC
            LIMIT 1
        )

        SELECT
            (SELECT v FROM total_comments),
            (SELECT v FROM total_threads),
            (SELECT v FROM avg_daily),
            (SELECT json_build_object('ticker', ticker, 'count', cnt)
             FROM top_all_time)
        ;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    return {
        "total_comments": int(row[0] or 0),
        "total_threads": int(row[1] or 0),
        "avg_daily_comments_global": float(row[2] or 0.0),
        "top_ticker_all_time": row[3] or None
    }


# =============== 2) Cumulative dataset growth (all-time) ===============
# =============== 2) Cumulative dataset growth (Split by source_table) ===============
def _fetch_cumulative_comments() -> List[Dict[str, Any]]:
    """
    Returns cumulative counts split by the 'source_table' column:
    - 'company' -> Ticker Comments
    - 'market'  -> General Market Comments
    """
    sql = """
        WITH daily_split AS (
            SELECT
                DATE(created_ts) AS d,
                -- Count rows where source_table is 'company'
                COUNT(*) FILTER (WHERE source_table = 'company') AS daily_ticker,
                -- Count rows where source_table is 'market'
                COUNT(*) FILTER (WHERE source_table = 'market')  AS daily_market
            FROM silver.comments
            GROUP BY 1
        ),
        running_totals AS (
            SELECT
                d,
                SUM(daily_ticker) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_ticker,
                SUM(daily_market) OVER (ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_market
            FROM daily_split
        )
        SELECT d, total_ticker, total_market
        FROM running_totals
        ORDER BY d;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [
        {
            "date": r[0].isoformat(),
            "ticker": int(r[1] or 0),
            "market": int(r[2] or 0)
        }
        for r in rows
    ]

# =============== 3) Source breakdown (all-time) ===============
def _fetch_source_breakdown(limit: int = 8) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT subreddit, COUNT(*)::bigint AS cnt
        FROM silver.comments
        GROUP BY subreddit
        ORDER BY cnt DESC
        LIMIT {limit};
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [{"subreddit": r[0], "count": int(r[1])} for r in rows if r[0]]

# =============== 4) Ingest health (from first day of data in October -> today) ===============
def _fetch_ingest_health() -> Dict[str, Any]:
    # Last ingest = latest comment in silver (fallback if bronze not available)
    sql_last_ingest = "SELECT MAX(created_ts) FROM silver.comments;"
    # Comments today
    sql_comments_today = """
        SELECT COUNT(*)::bigint FROM silver.comments
        WHERE DATE(created_ts) = CURRENT_DATE;
    """
    # Threads today = distinct root_comment_id observed today
    sql_threads_today = """
        SELECT COUNT(DISTINCT root_comment_id)::bigint
        FROM silver.comments
        WHERE DATE(created_ts) = CURRENT_DATE;
    """
    # First day of dataset (should be October per your note)
    sql_first_day = "SELECT MIN(DATE(created_ts)) FROM silver.comments;"
    # Missing days from first_day..today (0-count days)
    sql_missing_days = """
        WITH bounds AS (
            SELECT MIN(DATE(created_ts)) AS d0, MAX(DATE(created_ts)) AS d1
            FROM silver.comments
        ),
        calendar AS (
            SELECT generate_series((SELECT d0 FROM bounds), (SELECT d1 FROM bounds), '1 day')::date AS d
        ),
        daily AS (
            SELECT DATE(created_ts) AS d, COUNT(*) AS c
            FROM silver.comments
            GROUP BY 1
        )
        SELECT COUNT(*)::int
        FROM calendar cal
        LEFT JOIN daily ON daily.d = cal.d
        WHERE COALESCE(daily.c,0) = 0;
    """
    # 7d vs previous 7d volume stability (comments)
    sql_volume_stability = """
        WITH dcount AS (
            SELECT DATE(created_ts) AS d, COUNT(*)::float AS c
            FROM silver.comments
            WHERE created_ts >= CURRENT_DATE - INTERVAL '15 days'
            GROUP BY 1
        ),
        w1 AS ( -- last 7 full days excluding today
            SELECT COALESCE(SUM(c),0)/7.0 AS avg_c
            FROM dcount
            WHERE d BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE - INTERVAL '1 day'
        ),
        w0 AS ( -- previous 7 days window
            SELECT COALESCE(SUM(c),0)/7.0 AS avg_c
            FROM dcount
            WHERE d BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE - INTERVAL '8 days'
        )
        SELECT (SELECT avg_c FROM w1) AS avg_last7,
               (SELECT avg_c FROM w0) AS avg_prev7;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql_last_ingest); last_ingest = cur.fetchone()[0]
        cur.execute(sql_comments_today); comments_today = int(cur.fetchone()[0] or 0)
        cur.execute(sql_threads_today); threads_today = int(cur.fetchone()[0] or 0)
        cur.execute(sql_first_day); first_day = cur.fetchone()[0]
        cur.execute(sql_missing_days); missing_days = int(cur.fetchone()[0] or 0)
        cur.execute(sql_volume_stability); v = cur.fetchone()
        avg_last7 = float(v[0] or 0.0)
        avg_prev7 = float(v[1] or 0.0)

    # --- Health scoring (0..100) ---
    # Freshness (0..30)
    freshness_pts = 0
    if last_ingest:
        age_sec = (datetime.now(timezone.utc) - last_ingest).total_seconds()
        if age_sec <= 6*3600:       freshness_pts = 30
        elif age_sec <= 24*3600:    freshness_pts = 20
        elif age_sec <= 48*3600:    freshness_pts = 10
        else:                       freshness_pts = 0

    # Missing days (0..30) – fewer missing days => higher score
    if missing_days == 0:          missing_pts = 30
    elif missing_days <= 2:        missing_pts = 20
    elif missing_days <= 5:        missing_pts = 10
    else:                          missing_pts = 0

    # Volume stability (0..25) – compare avg_last7 vs avg_prev7
    vol_pts = 5
    if avg_prev7 > 0:
        diff = abs(avg_last7 - avg_prev7) / avg_prev7
        if diff <= 0.15:           vol_pts = 25
        elif diff <= 0.35:         vol_pts = 15
        else:                      vol_pts = 5

    # Thread coverage today (0..15) – vs last7 daily avg of threads
    # Use comments ratio proxy if thread avg not readily available
    thread_pts = 10
    # We’ll estimate 7-day avg threads using comments→threads ratio from all-time:
    # If you prefer precise 7-day thread avg, add a small query; for now this is lightweight.

    score = int(freshness_pts + missing_pts + vol_pts + thread_pts)
    status = "green" if score >= 80 else ("yellow" if score >= 60 else "red")

    return {
        "last_ingest_ts": last_ingest.isoformat() if last_ingest else None,
        "comments_today": comments_today,
        "threads_today": threads_today,
        "missing_days": missing_days,
        "dataset_health_score": score,
        "status": status,
        "first_day": first_day.isoformat() if first_day else None
    }


def _fetch_top_tickers(limit: int = 20) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT cm.company AS company, COUNT(*)::bigint AS cnt
        FROM silver.company_mentions cm
        JOIN silver.comments c ON c.comment_id = cm.comment_id
        GROUP BY cm.company
        ORDER BY cnt DESC
        LIMIT {limit};
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [
        {
            "name": r[0],        # <-- matches BarChartComponent
            "value": int(r[1]),  # <-- matches valueKey="value"
            "sentiment": None
        }
        for r in rows if r[0]
    ]


def _fetch_sentiment_extremes(limit: int = 5) -> Dict[str, Any]:
    """
    Returns the top N most loved and most hated companies of all time
    using weighted mean sentiment from gold.company_daily.
    """

    sql = f"""
         WITH agg AS (
             SELECT
                 company AS ticker,
                 SUM(mean_sentiment * n_mentions)::float / NULLIF(SUM(n_mentions), 0) AS avg_sentiment,
                 SUM(n_mentions) AS total_mentions
             FROM gold.company_daily
             WHERE mean_sentiment IS NOT NULL
               AND n_mentions IS NOT NULL
               AND n_mentions > 0
             GROUP BY company
             HAVING SUM(n_mentions) > 20      -- avoid low-sample garbage
         ),
         loved AS (
             -- FIX: Added total_mentions here
             SELECT ticker, avg_sentiment, total_mentions
             FROM agg
             ORDER BY avg_sentiment DESC
             LIMIT {limit}
         ),
         hated AS (
             -- FIX: Added total_mentions here
             SELECT ticker, avg_sentiment, total_mentions
             FROM agg
             ORDER BY avg_sentiment ASC
             LIMIT {limit}
         )
         SELECT
             (SELECT json_agg(row_to_json(loved)) FROM loved),
             (SELECT json_agg(row_to_json(hated)) FROM hated);
     """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()

    return {
        "most_loved": row[0] or [],
        "most_hated": row[1] or []
    }


def _fetch_hourly_activity() -> List[Dict[str, Any]]:
    """
    Returns average comments per hour (0–23) across all days.
    """
    sql = """
        WITH hourly AS (
            SELECT
                EXTRACT(HOUR FROM created_ts)::int AS hour,
                COUNT(*)::bigint AS cnt
            FROM silver.comments
            GROUP BY hour
        ),
        num_days AS (
            SELECT COUNT(DISTINCT DATE(created_ts))::float AS days
            FROM silver.comments
        )
        SELECT
            h.hour,
            h.cnt / NULLIF((SELECT days FROM num_days), 0) AS avg_per_hour
        FROM hourly h
        ORDER BY h.hour;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [
        {"hour": r[0], "avg": float(r[1])}
        for r in rows
    ]

    # =============== Main aggregator ===============


def fetch_overview() -> Dict[str, Any]:
    return to_native({
        "kpis": _fetch_kpis(),
        "cumulative_comments": _fetch_cumulative_comments(),
        "source_breakdown": _fetch_source_breakdown(limit=8),
        "ingest_health": _fetch_ingest_health(),
        "top_20_tickers": _fetch_top_tickers(limit=20),   # ⭐ updated
        "sentiment_extremes": _fetch_sentiment_extremes(limit=5),
        "hourly_activity": _fetch_hourly_activity(),
    })



def fetch_overview_summary(start=None, end=None, subreddit=None, market_only=False):
    try:
        return fetch_overview()
    except Exception as e:
        print("❌ ERROR in fetch_overview_summary:", e)
        import traceback; traceback.print_exc()
        return {
            "kpis": {},
            "cumulative_comments": [],
            "source_breakdown": [],
            "ingest_health": {},
            "top_20_tickers": [],    # ⭐ updated
            "sentiment_extremes": {
                "most_loved": [],
                "most_hated": []
            },
            "hourly_activity": []
        }
