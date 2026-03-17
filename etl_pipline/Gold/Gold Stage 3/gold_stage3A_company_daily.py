
import os
import argparse
import logging
import numpy as np
import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv

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
        host=PGHOST, port=PGPORT, dbname=PGDB,
        user=PGUSER, password=PGPASSWORD
    )

# -------------------------------------------------------------------
# 2. Fetch unified comments
# -------------------------------------------------------------------
def fetch_company_sentiments(full_mode=False, days=14, start_date=None):
    """
    Pull company mentions joined to unified silver.comments + gold.comment_features.
    Uses unified sentiment_score (VADER + FinLex emoji-aware).

    Modes:
      • full_mode=True   → fetch all available data (no date limit)
      • start_date set   → incremental mode (fetch only data since start_date)
      • default          → fetch last N days (default = 14)
    """
    if full_mode:
        logging.info("Running FULL backfill (all available data).")
        date_filter = ""
        params = ()
    elif start_date:
        logging.info(f"Running incremental mode from {start_date}.")
        date_filter = "WHERE COALESCE(c.created_ts, g.processed_at) >= %s"
        params = (start_date,)
    else:
        logging.info(f"Running limited mode (last {days} days).")
        date_filter = "WHERE COALESCE(c.created_ts, g.processed_at) >= CURRENT_DATE - (%s || ' days')::interval"
        params = (str(days),)

    sql = f"""
        SELECT
            m.company,
            c.source_table,
            DATE_TRUNC('day', COALESCE(c.created_ts, g.processed_at))::date AS date_utc,
            g.sentiment_score::float AS sentiment_score,
            COALESCE(c.score, c.ups, 1)::float AS comment_score,
            info.sector,
            info.industry
        FROM silver.company_mentions m
        JOIN gold.comment_features g
          ON m.comment_id = g.comment_id
        LEFT JOIN silver.comments c
          ON c.comment_id = m.comment_id
        LEFT JOIN silver.company_info info
          ON info.company = m.company
        {date_filter}
        ORDER BY date_utc ASC;
    """

    with get_conn() as conn, conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    logging.info(f"Fetched {len(rows)} sentiment rows{' (full history)' if full_mode else ''}.")
    return rows


# -------------------------------------------------------------------
# 3. Aggregate daily sentiment
# -------------------------------------------------------------------
def aggregate_daily(rows):
    """Aggregate daily sentiment per (company, date, source_table)."""
    if not rows:
        logging.warning("No rows fetched to aggregate.")
        return []

    grouped = {}
    for row in rows:
        key = (row["company"], row["date_utc"], row["source_table"])
        grouped.setdefault(key, []).append(row)

    results = []
    for (company, date_utc, source_table), entries in grouped.items():
        scores = [float(e["sentiment_score"]) for e in entries if e["sentiment_score"] is not None]
        if not scores:
            continue

        sector   = next((e.get("sector") for e in entries if e.get("sector")), None)
        industry = next((e.get("industry") for e in entries if e.get("industry")), None)
        weights  = [max(1.0, (e.get("comment_score") or 1.0)) for e in entries]

        mean_sentiment   = float(np.mean(scores))
        median_sentiment = float(np.median(scores))
        sentiment_std    = float(np.std(scores)) if len(scores) > 1 else 0.0
        pct_positive     = sum(1 for s in scores if s > 0.05) / len(scores)
        pct_negative     = sum(1 for s in scores if s < -0.05) / len(scores)
        weighted_mean    = float(np.average(scores, weights=weights))
        polarity_strength = abs(mean_sentiment) * (1 + sentiment_std)

        results.append({
            "company": company,
            "source_table": source_table,
            "date_utc": date_utc,
            "sector": sector,
            "industry": industry,
            "n_mentions": len(scores),
            "mean_sentiment": mean_sentiment,
            "median_sentiment": median_sentiment,
            "pct_positive": pct_positive,
            "pct_negative": pct_negative,
            "weighted_mean_sent": weighted_mean,
            "sentiment_std": sentiment_std,
            "sentiment_delta": None,
            "mention_growth": None,
            "sentiment_volatility": sentiment_std,
            "polarity_strength": polarity_strength,
            "zscore_anomaly": None,
            "method": "DailyAggregateVADER+FinLex(emoji)",
            "version": "3.1"
        })

    logging.info(f"Aggregated {len(results)} company-day-source records.")
    return results

# -------------------------------------------------------------------
# 4. Upsert into gold.company_daily
# -------------------------------------------------------------------
def upsert_company_daily(records):
    if not records:
        logging.warning("No records to upsert.")
        return 0

    cols = list(records[0].keys())
    values = [[rec[c] for c in cols] for rec in records]
    sql = f"""
        INSERT INTO gold.company_daily ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (company, date_utc, source_table)
        DO UPDATE SET
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            n_mentions = EXCLUDED.n_mentions,
            mean_sentiment = EXCLUDED.mean_sentiment,
            median_sentiment = EXCLUDED.median_sentiment,
            pct_positive = EXCLUDED.pct_positive,
            pct_negative = EXCLUDED.pct_negative,
            weighted_mean_sent = EXCLUDED.weighted_mean_sent,
            sentiment_std = EXCLUDED.sentiment_std,
            sentiment_delta = EXCLUDED.sentiment_delta,
            mention_growth = EXCLUDED.mention_growth,
            sentiment_volatility = EXCLUDED.sentiment_volatility,
            polarity_strength = EXCLUDED.polarity_strength,
            zscore_anomaly = EXCLUDED.zscore_anomaly,
            method = EXCLUDED.method,
            version = EXCLUDED.version,
            created_at = NOW();
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
        conn.commit()
    logging.info(f"✅ Upserted {len(records)} rows into gold.company_daily.")
    return len(records)

# -------------------------------------------------------------------
# 5. Rolling updates
# -------------------------------------------------------------------
def update_deltas_and_growth():
    sql = """
    WITH diffs AS (
        SELECT
            company,
            source_table,
            date_utc,
            mean_sentiment,
            n_mentions,
            LAG(mean_sentiment) OVER (PARTITION BY company, source_table ORDER BY date_utc) AS prev_sent,
            LAG(n_mentions) OVER (PARTITION BY company, source_table ORDER BY date_utc) AS prev_mentions,
            AVG(mean_sentiment) OVER (PARTITION BY company, source_table ORDER BY date_utc ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS roll_mean,
            STDDEV_SAMP(mean_sentiment) OVER (PARTITION BY company, source_table ORDER BY date_utc ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS roll_std
        FROM gold.company_daily
    )
    UPDATE gold.company_daily AS t
    SET
        sentiment_delta = d.mean_sentiment - d.prev_sent,
        mention_growth = CASE
            WHEN d.prev_mentions IS NULL OR d.prev_mentions = 0 THEN NULL
            ELSE (d.n_mentions - d.prev_mentions)::float / d.prev_mentions END,
        sentiment_volatility = COALESCE(d.roll_std, 0),
        zscore_anomaly = COALESCE(
            CASE
                WHEN d.roll_std IS NULL OR d.roll_std = 0 THEN 0
                ELSE (d.mean_sentiment - d.roll_mean) / d.roll_std
            END, 0)
    FROM diffs d
    WHERE t.company = d.company 
      AND t.date_utc = d.date_utc
      AND t.source_table = d.source_table;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    logging.info("🌀 Updated rolling deltas, growth, volatility, and z-scores (null-safe).")


# -------------------------------------------------------------------
# 7. Runner
# -------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--incremental", action="store_true", help="Run incremental mode (only new data)")
    ap.add_argument("--days", type=int, default=14, help="Fallback number of past days (if incremental has no start date)")
    args = ap.parse_args()

    logging.info("🚀 Starting Gold Stage 3 — Company Daily Sentiment Aggregation")

    # ---------------------------------------------------------------
    # Default behavior → FULL backfill
    # ---------------------------------------------------------------
    if not args.incremental:
        logging.info("🟢 Default mode: FULL historical backfill.")
        rows = fetch_company_sentiments(full_mode=True)
    else:
        # Incremental mode (only new data after latest date)
        start_date = get_last_date()
        if start_date is None:
            logging.info("No existing data found; switching to full backfill.")
            rows = fetch_company_sentiments(full_mode=True)
        else:
            logging.info(f"Incremental mode from {start_date}.")
            rows = fetch_company_sentiments(full_mode=False, start_date=start_date)

    daily = aggregate_daily(rows)
    upsert_company_daily(daily)
    update_deltas_and_growth()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MIN(date_utc), MAX(date_utc) FROM gold.company_daily;")
        count, mind, maxd = cur.fetchone()
    logging.info(f"🏁 Stage 3 complete — {count} rows from {mind} → {maxd}")