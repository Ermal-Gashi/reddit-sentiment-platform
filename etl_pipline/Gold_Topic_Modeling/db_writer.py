

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import json

# ---------------------------------------------------------
# Load Environment
# ---------------------------------------------------------
load_dotenv()
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")


def get_conn():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDB,
        user=PGUSER,
        password=PGPASSWORD
    )


# ---------------------------------------------------------
# 1. Initialize / Migrate Tables
# ---------------------------------------------------------
def init_gold_tables():
    """
    Ensures all Gold layer tables exist and are schema-updated.
    SAFE to run multiple times.
    """
    sql = """
        CREATE SCHEMA IF NOT EXISTS gold;

        -- ---------------------------
        -- DAILY TOPICS (DIMENSION)
        -- ---------------------------
        CREATE TABLE IF NOT EXISTS gold.daily_topics (
            date_utc         DATE NOT NULL,
            topic_id         INTEGER NOT NULL,

            topic_title      TEXT,
            topic_keywords   TEXT[],

            representative_sentences JSONB, -- ⭐ NEW

            doc_count        INTEGER,
            is_junk          BOOLEAN DEFAULT FALSE,

            metrics          JSONB,
            model_version    TEXT,

            inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

            PRIMARY KEY (date_utc, topic_id)
        );

        -- Backward-compatible migration (no-op if exists)
        ALTER TABLE gold.daily_topics
        ADD COLUMN IF NOT EXISTS representative_sentences JSONB;

        -- ---------------------------
        -- COMMENT ↔ TOPIC FACT TABLE
        -- ---------------------------
        CREATE TABLE IF NOT EXISTS gold.comment_topics (
            comment_id       TEXT NOT NULL,
            date_utc         DATE NOT NULL,
            topic_id         INTEGER NOT NULL,
            topic_prob       DOUBLE PRECISION,
            inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (comment_id, date_utc)
        );

        -- ---------------------------
        -- TOPIC EVOLUTION LINKS
        -- ---------------------------
        CREATE TABLE IF NOT EXISTS gold.topic_evolution (
            source_date      DATE NOT NULL,
            source_topic_id  INTEGER NOT NULL,
            target_date      DATE NOT NULL,
            target_topic_id  INTEGER NOT NULL,
            similarity       DOUBLE PRECISION,
            link_type        TEXT,
            inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_date, source_topic_id, target_date, target_topic_id)
        );

        -- ---------------------------
        -- INDEXES
        -- ---------------------------
        CREATE INDEX IF NOT EXISTS idx_daily_topics_date
            ON gold.daily_topics (date_utc);

        CREATE INDEX IF NOT EXISTS idx_comment_topics_lookup
            ON gold.comment_topics (date_utc, topic_id);

        CREATE INDEX IF NOT EXISTS idx_topic_evolution_lookup
            ON gold.topic_evolution (source_date, target_date);
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

    print("🛠️ Gold tables ensured + representative_sentences ready.")


# ---------------------------------------------------------
# 2. Insert Topic Summaries (UPDATED)
# ---------------------------------------------------------
def insert_topic_summaries(date_utc, topic_data_list, model_version):
    """
    Inserts/Updates gold.daily_topics.

    topic_data_list item must support:
    {
        'topic_id': int,
        'title': str,
        'keywords': list[str],
        'doc_count': int,
        'metrics': dict,
        'is_junk': bool,
        'representatives': [
            { 'text': str, 'score': float }
        ]
    }
    """
    if not topic_data_list:
        return 0

    init_gold_tables()

    records = []
    for t in topic_data_list:
        records.append((
            date_utc,
            t["topic_id"],
            t["title"],
            t["keywords"],
            json.dumps(t.get("representatives", [])),  # ⭐ NEW
            t["doc_count"],
            json.dumps(t.get("metrics", {})),
            t["is_junk"],
            model_version
        ))

    sql = """
        INSERT INTO gold.daily_topics
        (
            date_utc,
            topic_id,
            topic_title,
            topic_keywords,
            representative_sentences,
            doc_count,
            metrics,
            is_junk,
            model_version
        )
        VALUES %s
        ON CONFLICT (date_utc, topic_id) DO UPDATE
        SET
            topic_title = EXCLUDED.topic_title,
            topic_keywords = EXCLUDED.topic_keywords,
            representative_sentences = EXCLUDED.representative_sentences,
            doc_count = EXCLUDED.doc_count,
            metrics = EXCLUDED.metrics,
            is_junk = EXCLUDED.is_junk,
            model_version = EXCLUDED.model_version,
            inserted_at = NOW();
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records)
        conn.commit()

    print(f"📊 Saved {len(records)} topics with representatives.")


# ---------------------------------------------------------
# 3. Insert Comment Assignments (UNCHANGED)
# ---------------------------------------------------------
def insert_comment_assignments(date_utc, assignments_list):
    if not assignments_list:
        return 0

    records = [
        (str(a["comment_id"]), date_utc, int(a["topic_id"]), float(a["prob"]))
        for a in assignments_list
    ]

    sql = """
        INSERT INTO gold.comment_topics
        (comment_id, date_utc, topic_id, topic_prob)
        VALUES %s
        ON CONFLICT (comment_id, date_utc) DO UPDATE
        SET
            topic_id = EXCLUDED.topic_id,
            topic_prob = EXCLUDED.topic_prob,
            inserted_at = NOW();
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records, page_size=2000)
        conn.commit()

    print(f"🔗 Linked {len(records):,} comments to topics.")


# ---------------------------------------------------------
# 4. Insert Topic Evolution Links (UNCHANGED)
# ---------------------------------------------------------
def insert_topic_links(links_list):
    if not links_list:
        return 0

    sql = """
        INSERT INTO gold.topic_evolution
        (source_date, source_topic_id, target_date, target_topic_id, similarity, link_type)
        VALUES %s
        ON CONFLICT (source_date, source_topic_id, target_date, target_topic_id)
        DO UPDATE SET
            similarity = EXCLUDED.similarity,
            link_type = EXCLUDED.link_type,
            inserted_at = NOW();
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, links_list)
        conn.commit()

    print(f"🧬 Evolved/Linked {len(links_list)} topics.")
