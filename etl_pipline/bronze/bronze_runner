
import time
from datetime import datetime, timezone

from config import SUBREDDITS, COMMENT_LIMIT_PER_POST, POST_LIMIT, STATE_DB_PATH
from utility.reddit_client import make_client
from utility.state_sqlite import init_state_db

# Local Bronze imports (same directory)
from fetch import (
    fetch_new_posts,
    fetch_comments_from_posts,
    refresh_active_threads,
)
from load_bronze import open_database_connection, insert_comments_direct



def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg, icon="🔹"):
    print(f"{icon} [{now_iso()}] {msg}")


MAX_TEST_POSTS = None


def run_bronze_cycle():
    reddit = make_client()
    log("Fetching new posts…")

    posts = fetch_new_posts(reddit, SUBREDDITS, POST_LIMIT)
    log(f"Posts fetched (raw): {len(posts)}", "")


    if MAX_TEST_POSTS and len(posts) > MAX_TEST_POSTS:
        posts = posts[:MAX_TEST_POSTS]
        log(f"Posts capped to {len(posts)} for test run", "")

    all_comments = []

    # Phase 1 — discovery
    log("Fetching comments for posts …", "")
    discovered = fetch_comments_from_posts(
        posts,
        conn=None,
        comment_limit_per_post=COMMENT_LIMIT_PER_POST,
    )
    if discovered:
        all_comments.extend(discovered)

    # Phase 2 — refresh (also limited by state DB)
    log("Refreshing active threads …", "")
    refreshed = refresh_active_threads(
        reddit,
        conn=None,
        comment_limit_per_post=COMMENT_LIMIT_PER_POST,
        max_age_hours=24,
    )
    if refreshed:
        all_comments.extend(refreshed)

    log(f"Total fetched comments: {len(all_comments)}", "")
    return all_comments



def main():
    log("Starting BRONZE FULL RUN", "")
    start = time.time()

    # Init SQLite state
    init_state_db(STATE_DB_PATH)

    # Fetch
    comments = run_bronze_cycle()

    # Load Bronze
    if comments:
        log(f"Inserting {len(comments)} comments into Bronze …", "")
        pg_conn = open_database_connection()
        insert_comments_direct(pg_conn, comments)
        pg_conn.close()
    else:
        log("No comments fetched — nothing to insert.", "")

    elapsed = int(time.time() - start)
    log(f"Bronze run completed in {elapsed}s", "")


if __name__ == "__main__":
    main()
