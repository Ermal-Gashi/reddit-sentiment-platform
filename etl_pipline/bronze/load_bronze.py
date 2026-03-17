import os, json, re
from datetime import datetime, timezone
from hashlib import md5
import psycopg2
from psycopg2.extras import execute_values, DictCursor
from dotenv import load_dotenv

# =========================================================
#  Environment
# =========================================================
load_dotenv()
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")

# =========================================================
#  Helpers
# =========================================================
COMMENT_ID_RE = re.compile(r"/comments/[a-z0-9]+/[^/]+/([a-z0-9]{3,10})/?", re.I)

def open_database_connection():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB, user=PGUSER, password=PGPASSWORD
    )

def parse_timestamp(val):
    """Convert epoch timestamp to ISO UTC."""
    if val is None:
        return None
    try:
        ts = datetime.fromtimestamp(float(val), tz=timezone.utc)
        return ts.isoformat()
    except Exception:
        return None

def extract_comment_id(rec: dict):
    """Find or synthesize a comment ID."""
    cid = rec.get("comment_id") or rec.get("id")
    if cid:
        return str(cid)
    name = rec.get("name")
    if name and str(name).startswith("t1_"):
        return name.replace("t1_", "")
    permalink = rec.get("permalink") or rec.get("url")
    if permalink:
        match = COMMENT_ID_RE.search(permalink)
        if match:
            return match.group(1)
    basis = f"{permalink}|{rec.get('created_iso') or rec.get('created_utc') or ''}"
    if basis.strip("|"):
        return "surr_" + md5(basis.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return None

# =========================================================
#  Manifest helpers
# =========================================================
def update_manifest(conn, source_name: str, row_count: int, status: str = "done"):
    """Upsert manifest entry to track direct DB loads."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.load_manifest (file_name, row_count, status, loaded_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (file_name)
            DO UPDATE SET
                row_count = EXCLUDED.row_count,
                status    = EXCLUDED.status,
                loaded_at = now();
            """,
            (source_name, row_count, status),
        )
    conn.commit()

# =========================================================
#  Core Inserts (direct ingestion)
# =========================================================
def _insert_comments_batch(conn, rows):
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bronze.comments
              (comment_id, post_id, parent_id, link_id, permalink,
               author, body, created_utc, raw_json, is_context_only)
            VALUES %s
            ON CONFLICT (comment_id) DO NOTHING;
            """,
            rows,
            page_size=2000,
        )

def insert_comments_direct(conn, comment_rows):
    """
    Insert in-memory fetched Reddit comments directly into bronze.comments.
    Keeps full raw_json (35+ fields) and logs the load in ops.load_manifest.
    """
    rows, surrogated, skipped = [], 0, 0

    for rec in comment_rows:
        cid = extract_comment_id(rec)
        if not cid:
            skipped += 1
            continue
        if cid.startswith("surr_"):
            surrogated += 1

        post_id = (
            rec.get("submission_id")
            or (rec.get("link_id") or "").replace("t3_", "")
            or rec.get("post_id")
        )

        created_ts = rec.get("created_iso") or parse_timestamp(
            rec.get("created_utc") or rec.get("created_ts")
        )

        rows.append((
            cid,
            post_id,
            rec.get("parent_id"),
            rec.get("link_id"),
            rec.get("permalink"),
            str(rec.get("author")),
            rec.get("body_raw") or rec.get("body") or rec.get("text"),
            created_ts,
            json.dumps(rec, ensure_ascii=False),
            rec.get("is_context_only", False),
        ))

        if len(rows) >= 2000:
            _insert_comments_batch(conn, rows)
            rows.clear()

    if rows:
        _insert_comments_batch(conn, rows)

    conn.commit()

    # Log ETL manifest entry
    try:
        update_manifest(conn, "bronze_direct_insert", len(comment_rows), "done")
    except Exception as e:
        print(" Warning: Failed to update ops.load_manifest:", e)

    print(f"Inserted {len(comment_rows)} comments "
          f"(surrogated={surrogated}, skipped={skipped}) directly into bronze.")
    return len(comment_rows)

# =========================================================
#  Main (for manual run)
# =========================================================
if __name__ == "__main__":
    print("This module is now used only for direct ingestion from fetch/main.")
    print("To test manually, import and call insert_comments_direct().")
