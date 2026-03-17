

def create_silver_schema(conn):
    with conn.cursor() as cur:

        # ============================================================
        # Schema
        # ============================================================
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS silver;
        """)

        # ============================================================
        # silver.comments
        # ============================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver.comments (
                comment_id TEXT PRIMARY KEY,

                submission_id TEXT,
                parent_id TEXT,
                parent_comment_id TEXT,
                root_comment_id TEXT,

                depth INTEGER,
                is_top_level BOOLEAN,
                is_op BOOLEAN,

                author TEXT,
                subreddit TEXT,
                created_ts TIMESTAMP,
                permalink TEXT,

                body_original TEXT,
                body_clean TEXT,
                body_clean_emoji TEXT,
                body_clean_bert TEXT,

                lang TEXT,
                n_chars INTEGER,
                n_tokens INTEGER,
                has_url BOOLEAN,

                score INTEGER,
                ups INTEGER,
                downs INTEGER,

                matched_companies TEXT,
                matched_source TEXT,
                match_confidence REAL,
                matched_where TEXT,
                company_set_size INTEGER,
                is_reply_to_company_mention BOOLEAN,

                source_table TEXT,

                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_comments_created_ts
            ON silver.comments (created_ts);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_comments_submission_id
            ON silver.comments (submission_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_comments_subreddit
            ON silver.comments (subreddit);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_comments_source_table
            ON silver.comments (source_table);
        """)

        # ============================================================
        # silver.company_mentions
        # ============================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver.company_mentions (
                comment_id TEXT,
                company TEXT,
                source TEXT,
                confidence REAL,

                created_ts TIMESTAMP,
                subreddit TEXT,
                submission_id TEXT,

                depth INTEGER,
                is_top_level BOOLEAN
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_company_mentions_company
            ON silver.company_mentions (company);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_company_mentions_comment_id
            ON silver.company_mentions (comment_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_company_mentions_created_ts
            ON silver.company_mentions (created_ts);
        """)

        # ============================================================
        # silver.filtered_comments
        # ============================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver.filtered_comments (
                comment_id TEXT PRIMARY KEY,
                reason TEXT,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_filtered_comments_reason
            ON silver.filtered_comments (reason);
        """)

        # ============================================================
        # silver.market_comments
        # ============================================================
        # Marker table used ONLY to prevent reprocessing
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver.market_comments (
                comment_id TEXT PRIMARY KEY
            );
        """)

    conn.commit()


# ============================================================
#  Standalone test runner (optional) Doesnt work currently
# ============================================================
if __name__ == "__main__":
    print(" Running Silver schema initializer in standalone mode…")

    # IMPORTANT: reuse existing connection helper
    from etl_pipeline.load_bronze import open_database_connection

    conn = open_database_connection()
    try:
        create_silver_schema(conn)
        print(" Silver schema created / verified successfully.")
    finally:
        conn.close()