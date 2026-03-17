

from etl_pipeline.bronze.load_bronze import open_database_connection
from etl_pipeline.silver.silver_schema import create_silver_schema
from etl_pipeline.silver.silver_transform import (
    transform_comment,
    DROP_STATS,
)

# ============================================================
# SQL STATEMENTS (MATCH WORKING SCRIPT)
# ============================================================

FETCH_NEW_BRONZE_SQL = """
SELECT
    b.comment_id,
    b.raw_json
FROM bronze.comments b
LEFT JOIN silver.comments s
    ON b.comment_id = s.comment_id
LEFT JOIN silver.market_comments m
    ON b.comment_id = m.comment_id
LEFT JOIN silver.filtered_comments f
    ON b.comment_id = f.comment_id
WHERE s.comment_id IS NULL
  AND m.comment_id IS NULL
  AND f.comment_id IS NULL
ORDER BY b.created_utc
LIMIT %s;
"""

INSERT_SILVER_COMMENT_SQL = """
INSERT INTO silver.comments (
    comment_id, submission_id, parent_id, parent_comment_id, root_comment_id,
    depth, is_top_level, is_op,
    author, subreddit, created_ts, permalink,
    body_original, body_clean, body_clean_emoji, body_clean_bert,
    lang, n_chars, n_tokens, has_url,
    score, ups, downs,
    matched_companies, matched_source, match_confidence, matched_where,
    company_set_size, is_reply_to_company_mention,
    source_table
)
VALUES (
    %(comment_id)s, %(submission_id)s, %(parent_id)s, %(parent_comment_id)s, %(root_comment_id)s,
    %(depth)s, %(is_top_level)s, %(is_op)s,
    %(author)s, %(subreddit)s, %(created_ts)s, %(permalink)s,
    %(body_original)s, %(body_clean)s, %(body_clean_emoji)s, %(body_clean_bert)s,
    %(lang)s, %(n_chars)s, %(n_tokens)s, %(has_url)s,
    %(score)s, %(ups)s, %(downs)s,
    %(matched_companies)s, %(matched_source)s, %(match_confidence)s, %(matched_where)s,
    %(company_set_size)s, %(is_reply_to_company_mention)s,
    %(source_table)s
)
ON CONFLICT (comment_id) DO NOTHING;
"""

INSERT_COMPANY_SQL = """
INSERT INTO silver.company_mentions (
    comment_id, company, source, confidence,
    created_ts, subreddit, submission_id,
    depth, is_top_level
)
VALUES (
    %(comment_id)s, %(company)s, %(source)s, %(confidence)s,
    %(created_ts)s, %(subreddit)s, %(submission_id)s,
    %(depth)s, %(is_top_level)s
);
"""

INSERT_FILTERED_SQL = """
INSERT INTO silver.filtered_comments (comment_id, reason)
VALUES (%s, %s)
ON CONFLICT (comment_id) DO NOTHING;
"""

INSERT_MARKET_SQL = """
INSERT INTO silver.market_comments (comment_id)
VALUES (%s)
ON CONFLICT (comment_id) DO NOTHING;
"""


# ============================================================
# MAIN SILVER PROCESS
# ============================================================

def process_new_comments(limit: int = 500):
    print("🚀 SILVER STAGE STARTED")
    print(f"⚙️  Batch size: {limit}")

    conn = open_database_connection()
    create_silver_schema(conn)

    inserted_comments = 0
    inserted_companies = 0
    batch = 0

    try:
        with conn.cursor() as cur:
            while True:
                batch += 1
                print(f" Processing batch #{batch} …")

                cur.execute(FETCH_NEW_BRONZE_SQL, (limit,))
                rows = cur.fetchall()

                if not rows:
                    print(" No more Bronze rows to process.")
                    break

                columns = [d[0] for d in cur.description]

                for row in rows:
                    bronze_row = dict(zip(columns, row))
                    cid = bronze_row["comment_id"]

                    comment, companies, has_company = transform_comment(bronze_row)

                    if comment is None:
                        reason = next(iter(DROP_STATS.keys()), "filtered")
                        cur.execute(INSERT_FILTERED_SQL, (cid, reason))
                        continue

                    comment["source_table"] = "company" if has_company else "market"
                    cur.execute(INSERT_SILVER_COMMENT_SQL, comment)
                    inserted_comments += 1

                    for comp in companies:
                        cur.execute(INSERT_COMPANY_SQL, comp)
                        inserted_companies += 1

                    if not has_company:
                        cur.execute(INSERT_MARKET_SQL, (cid,))

                conn.commit()
                print(
                    f" Batch #{batch} committed "
                    f"({len(rows)} rows)"
                )

    finally:
        conn.close()

    print(
        f" SILVER STAGE COMPLETE — "
        f"{inserted_comments} comments, "
        f"{inserted_companies} company mentions"
    )

    return inserted_comments, inserted_companies
# ============================================================
# CLI ENTRY
# ============================================================

if __name__ == "__main__":
    n_comments, n_companies = process_new_comments(limit=500)
    print(
        f" Silver completed: "
        f"{n_comments} comments, {n_companies} company mentions"
    )
