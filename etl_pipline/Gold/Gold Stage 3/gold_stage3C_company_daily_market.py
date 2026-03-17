
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
# 2. TABLE DDL
# ============================================================

TABLE_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.company_daily_market (
    company        TEXT NOT NULL,
    date_utc       DATE NOT NULL,
    price_close    DOUBLE PRECISION,
    volume         BIGINT,
    fetched_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (company, date_utc)
);

CREATE INDEX IF NOT EXISTS idx_cdm_date
    ON gold.company_daily_market (date_utc);

CREATE INDEX IF NOT EXISTS idx_cdm_company
    ON gold.company_daily_market (company);
"""


def ensure_table_exists(engine):
    with engine.begin() as con:
        for stmt in TABLE_DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                con.execute(text(stmt))


# ============================================================
# 3. SQL — Extract daily OHLC
# ============================================================

DAILY_SQL = """
WITH intraday AS (
    SELECT
        ticker AS company,
        ts_utc,
        (ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::date AS date_utc,
        close,
        volume
    FROM gold.market_prices_intraday
),

first_bar AS (
    SELECT DISTINCT ON (company, date_utc)
        company, date_utc, ts_utc AS first_ts
    FROM intraday
    ORDER BY company, date_utc, ts_utc ASC
),

last_bar AS (
    SELECT DISTINCT ON (company, date_utc)
        company, date_utc, ts_utc AS last_ts
    FROM intraday
    ORDER BY company, date_utc, ts_utc DESC
),

joined AS (
    SELECT
        f.company,
        f.date_utc,
        SUM(i.volume) AS total_volume,
        ic.close AS close_price
    FROM first_bar f
    JOIN last_bar l
      ON f.company = l.company
     AND f.date_utc = l.date_utc
    JOIN intraday i
      ON i.company = f.company
     AND i.date_utc = f.date_utc
    JOIN intraday ic
      ON ic.company = f.company
     AND ic.ts_utc = l.last_ts
    GROUP BY f.company, f.date_utc, ic.close
)

SELECT
    company,
    date_utc,
    close_price AS price_close,
    total_volume AS volume
FROM joined
ORDER BY company, date_utc;
"""


def load_prices(engine):
    """Load daily OHLC/volume from intraday."""
    with engine.connect() as con:
        df = pd.read_sql(DAILY_SQL, con)
    logging.info(f" Loaded {len(df)} rows of daily prices from intraday.")
    return df


# ============================================================
# 4. Forward-fill / clean per ticker
# ============================================================

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = []

    for company, g in df.groupby("company"):
        g = g.sort_values("date_utc").copy()

        # Forward fill price_close
        g["price_close"] = g["price_close"].ffill()

        # Ensure volume is int
        g["volume"] = g["volume"].fillna(0).astype("int64")

        g["fetched_at"] = now_utc()

        out.append(g)

    df_out = pd.concat(out, ignore_index=True)
    logging.info(f" Cleaned + forward-filled {df_out['company'].nunique()} tickers.")
    return df_out


# ============================================================
# 5. UPSERT
# ============================================================

UPSERT_SQL = """
INSERT INTO gold.company_daily_market (
    company, date_utc, price_close, volume, fetched_at
)
VALUES (
    :company, :date_utc, :price_close, :volume, :fetched_at
)
ON CONFLICT (company, date_utc)
DO UPDATE SET
    price_close = EXCLUDED.price_close,
    volume      = EXCLUDED.volume,
    fetched_at  = EXCLUDED.fetched_at;
"""


def upsert(engine, df: pd.DataFrame):
    if df.empty:
        return 0

    df = df.where(pd.notnull(df), None)

    cols = ["company", "date_utc", "price_close", "volume", "fetched_at"]

    with engine.begin() as con:
        con.execute(text(UPSERT_SQL), df[cols].to_dict("records"))

    logging.info(f" Upserted {len(df)} rows into gold.company_daily_market")
    return len(df)


# ============================================================
# 6. RUNNER
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, help="Run for a specific date only (YYYY-MM-DD)")
    args = ap.parse_args()

    logging.info(" Gold Stage 3B — Build company_daily_market")

    ensure_table_exists(ENGINE)

    df = load_prices(ENGINE)

    # Specific day mode
    if args.date:
        target = pd.to_datetime(args.date).date()
        df = df[df["date_utc"] == target]
        logging.info(f" Filtered to specific day: {target}")

    cleaned = clean_prices(df)
    upsert(ENGINE, cleaned)

    logging.info(" Stage 3B complete.")


if __name__ == "__main__":
    main()
