

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------
# Load DB credentials
# ---------------------------------------------------------
load_dotenv()

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")


# ---------------------------------------------------------
# DB Connection
# ---------------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDB,
        user=PGUSER,
        password=PGPASSWORD,
    )


# ---------------------------------------------------------
# Fetch texts + company lists
# ---------------------------------------------------------
def fetch_raw_texts_and_companies(start_dt_utc, end_dt_utc):
    """
    Returns THREE aligned lists:
        ids:        list[str/int] (The comment_id)
        texts:      list[str]     (The text content)
        companies:  list[list[str]] (The list of tickers found)

    Pulls comments from silver.comments (body_clean_bert)
    AND joins silver.company_mentions to extract company lists.
    """

    sql = """
        SELECT
            c.comment_id,
            c.body_clean_bert,
            cm.company
        FROM silver.comments c
        LEFT JOIN silver.company_mentions cm
            ON c.comment_id = cm.comment_id
        WHERE c.body_clean_bert IS NOT NULL
          AND LENGTH(c.body_clean_bert) > 20
          AND c.source_table = 'company'
          AND c.company_set_size > 0
          AND c.created_ts >= %s
          AND c.created_ts <  %s
        ORDER BY c.comment_id, cm.company;
    """

    with get_conn() as conn:
        df = pd.read_sql(sql, conn, params=(start_dt_utc, end_dt_utc))

    if df.empty:
        print("Fetched 0 rows.")
        return [], [], [] # Return 3 empty lists

    # Group by comment_id
    grouped = df.groupby("comment_id")

    ids = []
    texts = []
    company_lists = []

    for cid, group in grouped:
        text = group["body_clean_bert"].iloc[0]
        comps = group["company"].dropna().unique().tolist()

        # Use uppercase tickers
        comps = [c.upper() for c in comps]

        ids.append(cid) # Capture the ID
        texts.append(text)
        company_lists.append(comps)

    print(f"Fetched {len(texts):,} COMPANY docs with company lists")

    return ids, texts, company_lists