import os
import time
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import yfinance as yf

# -------------------------------------------------------------------
# Load environment variables
# -------------------------------------------------------------------
load_dotenv()
PG = dict(
    dbname=os.getenv("PGDB", "reddit_warehouse"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", ""),
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", 5432),
)

# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# -------------------------------------------------------------------
# Database utilities
# -------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(**PG)

def get_tickers(limit=None):
    sql = """
        SELECT DISTINCT company
        FROM ops.company_universe
        WHERE enabled = TRUE
        ORDER BY company
        """ + (f" LIMIT {int(limit)}" if limit else "") + ";"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall() if r[0]]

# -------------------------------------------------------------------
# Fetch company metadata from Yahoo Finance
# -------------------------------------------------------------------
def fetch_meta(ticker: str):
    try:
        info = yf.Ticker(ticker).info  # network call
        return {
            "company": ticker,
            "company_name": info.get("shortName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "market_cap": info.get("marketCap"),
            "country": info.get("country"),
            "currency": info.get("currency"),
        }
    except Exception as e:
        logging.warning(f"{ticker}: metadata fetch failed: {e}")
        return {
            "company": ticker,
            "company_name": None,
            "sector": None,
            "industry": None,
            "market_cap": None,
            "country": None,
            "currency": None,
        }

# -------------------------------------------------------------------
# Upsert metadata into silver.company_info
# -------------------------------------------------------------------
def upsert_meta(rows):
    if not rows:
        return

    sql = """
        INSERT INTO silver.company_info
        (company, company_name, sector, industry, market_cap, country, currency)
        VALUES %s
        ON CONFLICT (company) DO UPDATE SET
          company_name = EXCLUDED.company_name,
          sector       = EXCLUDED.sector,
          industry     = EXCLUDED.industry,
          market_cap   = EXCLUDED.market_cap,
          country      = EXCLUDED.country,
          currency     = EXCLUDED.currency,
          last_updated = NOW();
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, [
            (
                r["company"],
                r["company_name"],
                r["sector"],
                r["industry"],
                r["market_cap"],
                r["country"],
                r["currency"]
            )
            for r in rows
        ], page_size=100)
        conn.commit()

# -------------------------------------------------------------------
# Main execution
# -------------------------------------------------------------------
if __name__ == "__main__":
    tickers = get_tickers()  # or get_tickers(limit=200) to test
    logging.info(f"Enriching {len(tickers)} tickers with sector/industry…")

    batch, BATCH_SIZE = [], 25
    for i, t in enumerate(tickers, 1):
        batch.append(fetch_meta(t))
        if len(batch) >= BATCH_SIZE:
            upsert_meta(batch)
            batch.clear()
            time.sleep(0.4)  # gentle pacing between batches
        if i % 100 == 0:
            logging.info(f"Processed {i}/{len(tickers)}")

    if batch:
        upsert_meta(batch)

    logging.info("✅ Sector enrichment complete.")

    # -------------------------------------------------------------------
    # Manual fixes for tickers Yahoo Finance misses
    # -------------------------------------------------------------------
    manual_rows = [
        {
            "company": "BF.B",
            "company_name": "Brown-Forman Corp. Class B",
            "sector": "Consumer Defensive",
            "industry": "Beverages—Wineries & Distilleries",
            "market_cap": None,
            "country": "United States",
            "currency": "USD"
        },
        {
            "company": "BRK.B",
            "company_name": "Berkshire Hathaway Inc. Class B",
            "sector": "Financial Services",
            "industry": "Insurance—Diversified",
            "market_cap": None,
            "country": "United States",
            "currency": "USD"
        },
    ]

    upsert_meta(manual_rows)
    logging.info("✅ Applied manual sector/industry fixes for known missing tickers.")
