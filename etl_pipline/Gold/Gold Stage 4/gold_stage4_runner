
import os
import argparse
import logging
import pandas as pd
from datetime import datetime, timezone

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv

# ============================================================
# 1. SETUP
# ============================================================
load_dotenv()

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGDB = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")

ENGINE = create_engine(
    f"postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDB}",
    poolclass=QueuePool,
    pool_size=8,
    max_overflow=8,
    pool_pre_ping=True,
    future=True,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

now_utc = lambda: datetime.now(timezone.utc)

# ============================================================
# 1B. OUTPUT TABLE DDL
# ============================================================

TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.company_sentiment_vs_price (
    company               TEXT NOT NULL,
    date_utc              DATE NOT NULL,

    price_close           DOUBLE PRECISION,
    daily_return          DOUBLE PRECISION,
    price_return_next_day DOUBLE PRECISION,
    volume                BIGINT,
    price_volatility_7d   DOUBLE PRECISION,

    mean_sentiment        DOUBLE PRECISION,
    weighted_mean_sent    DOUBLE PRECISION,
    sentiment_delta       DOUBLE PRECISION,
    n_mentions            INTEGER,
    sentiment_volatility  DOUBLE PRECISION,

    corr_7d               DOUBLE PRECISION,
    corr_30d              DOUBLE PRECISION,
    divergence_score      DOUBLE PRECISION,

    fetched_at            TIMESTAMPTZ DEFAULT now(),

    sector                TEXT,
    industry              TEXT,
    source_table          TEXT NOT NULL,

    CONSTRAINT pk_company_sentiment_vs_price
        PRIMARY KEY (company, date_utc, source_table)
);

CREATE INDEX IF NOT EXISTS idx_csvp_date
    ON gold.company_sentiment_vs_price (date_utc);

CREATE INDEX IF NOT EXISTS idx_csvp_company
    ON gold.company_sentiment_vs_price (company);
"""


def ensure_table_exists(engine):
    with engine.begin() as con:
        for stmt in TABLE_DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                con.execute(text(s))


# ============================================================
# 2. LOAD DAILY — UNION ANCHOR GRID
# ============================================================

LOAD_SQL = """
WITH
price_daily AS (
    SELECT company, date_utc, price_close, volume
    FROM gold.company_daily_market
),

sentiment_daily AS (
    SELECT
        company,
        date_utc,
        mean_sentiment,
        weighted_mean_sent,
        sentiment_delta,
        n_mentions,
        sentiment_volatility,
        sector,
        industry
    FROM gold.company_daily
),

company_dates AS (
    SELECT company, date_utc FROM price_daily
    UNION
    SELECT company, date_utc FROM sentiment_daily
),

merged AS (
    SELECT
        cd.company,
        cd.date_utc,
        p.price_close,
        p.volume,
        s.mean_sentiment,
        s.weighted_mean_sent,
        s.sentiment_delta,
        s.n_mentions,
        s.sentiment_volatility,
        s.sector,
        s.industry
    FROM company_dates cd
    LEFT JOIN price_daily p
      ON p.company = cd.company AND p.date_utc = cd.date_utc
    LEFT JOIN sentiment_daily s
      ON s.company = cd.company AND s.date_utc = cd.date_utc
)

SELECT
    company,
    date_utc,
    price_close,
    volume,
    mean_sentiment,
    weighted_mean_sent,
    sentiment_delta,
    n_mentions,
    sentiment_volatility,
    sector,
    industry
FROM merged
ORDER BY company, date_utc;
"""


def load_daily(engine):
    with engine.connect() as con:
        df = pd.read_sql(LOAD_SQL, con)
    logging.info(f"📥 Loaded {len(df)} merged rows (price + sentiment union).")
    return df


# ============================================================
# 3. DAILY METRICS (NO SENTIMENT FORWARD-FILL)
# ============================================================

def compute_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = []

    for company, g in df.groupby("company", sort=False):
        g = g.sort_values("date_utc").copy()

        # 1️ forward-fill price per company (to compute returns cleanly)
        g["price_close"] = g["price_close"].ffill()



        # 2⃣ daily return
        g["daily_return"] = g["price_close"].pct_change(fill_method=None)

        # 3️ next-day return
        g["price_return_next_day"] = g["daily_return"].shift(-1)


        g["volume"] = g["volume"].fillna(0).astype("int64")


        diff = g["mean_sentiment"].fillna(0) - g["daily_return"].fillna(0)
        std = diff.std(ddof=0)
        g["divergence_score"] = (
            0 if std == 0 or pd.isna(std)
            else (diff - diff.mean()) / std
        )


        g["price_volatility_7d"] = g["daily_return"].rolling(7).std()


        g["corr_7d"] = (
            g["mean_sentiment"]
            .rolling(7, min_periods=3)
            .corr(g["price_return_next_day"])
        )
        g["corr_30d"] = (
            g["mean_sentiment"]
            .rolling(30, min_periods=5)
            .corr(g["price_return_next_day"])
        )

        # 8 metadata
        g["fetched_at"] = now_utc()
        # NOTE: keeping legacy name so existing API queries keep working
        g["source_table"] = "daily_union_ffill"

        out.append(g)

    df_out = pd.concat(out, ignore_index=True)
    logging.info(f"🧮 Computed metrics for {df_out['company'].nunique()} companies.")
    return df_out


# ============================================================
# 4. UPSERT
# ============================================================

UPSERT_SQL = """
INSERT INTO gold.company_sentiment_vs_price (
    company, date_utc,
    price_close, daily_return, price_return_next_day,
    volume, price_volatility_7d,
    mean_sentiment, weighted_mean_sent, sentiment_delta,
    n_mentions, sentiment_volatility,
    corr_7d, corr_30d, divergence_score,
    fetched_at, sector, industry, source_table
)
VALUES (
    :company, :date_utc,
    :price_close, :daily_return, :price_return_next_day,
    :volume, :price_volatility_7d,
    :mean_sentiment, :weighted_mean_sent, :sentiment_delta,
    :n_mentions, :sentiment_volatility,
    :corr_7d, :corr_30d, :divergence_score,
    :fetched_at, :sector, :industry, :source_table
)
ON CONFLICT (company, date_utc, source_table)
DO UPDATE SET
    price_close           = EXCLUDED.price_close,
    daily_return          = EXCLUDED.daily_return,
    price_return_next_day = EXCLUDED.price_return_next_day,
    volume                = EXCLUDED.volume,
    price_volatility_7d   = EXCLUDED.price_volatility_7d,
    mean_sentiment        = EXCLUDED.mean_sentiment,
    weighted_mean_sent    = EXCLUDED.weighted_mean_sent,
    sentiment_delta       = EXCLUDED.sentiment_delta,
    n_mentions            = EXCLUDED.n_mentions,
    sentiment_volatility  = EXCLUDED.sentiment_volatility,
    corr_7d               = EXCLUDED.corr_7d,
    corr_30d              = EXCLUDED.corr_30d,
    divergence_score      = EXCLUDED.divergence_score,
    fetched_at            = EXCLUDED.fetched_at,
    sector                = EXCLUDED.sector,
    industry              = EXCLUDED.industry;
"""


def upsert(engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df["volume"] = df["volume"].fillna(0).astype("int64")
    df["n_mentions"] = df["n_mentions"].fillna(0).astype("int32")

    # allow NULLs for everything else
    df = df.where(pd.notnull(df), None)

    cols = [
        "company", "date_utc",
        "price_close", "daily_return", "price_return_next_day",
        "volume", "price_volatility_7d",
        "mean_sentiment", "weighted_mean_sent", "sentiment_delta",
        "n_mentions", "sentiment_volatility",
        "corr_7d", "corr_30d", "divergence_score",
        "fetched_at", "sector", "industry", "source_table",
    ]

    with engine.begin() as con:
        con.execute(text(UPSERT_SQL), df[cols].to_dict("records"))

    logging.info(f" Upserted {len(df)} rows → gold.company_sentiment_vs_price")
    return len(df)


# ============================================================
# 5. RUNNER
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--incremental", action="store_true")
    args = ap.parse_args()

    logging.info(" Gold Stage 4 v13.3 — START")
    ensure_table_exists(ENGINE)

    df = load_daily(ENGINE)

    if args.incremental:
        logging.info(" Incremental mode enabled")
        with ENGINE.connect() as con:
            existing = pd.read_sql(
                "SELECT DISTINCT date_utc FROM gold.company_sentiment_vs_price;",
                con,
            )["date_utc"].tolist()

        before = len(df)
        df = df[~df["date_utc"].isin(existing)]
        logging.info(f" Incremental filter: {before} → {len(df)} rows")

        if df.empty:
            logging.info(" Already up to date.")
            return

    enriched = compute_daily(df)
    upsert(ENGINE, enriched)

    logging.info(" Stage 4 completed.")


if __name__ == "__main__":
    main()
