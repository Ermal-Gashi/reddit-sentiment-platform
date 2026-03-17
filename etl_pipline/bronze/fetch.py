
import time
from typing import List, Dict, Any, Tuple, Set

from prawcore.exceptions import RequestException, ResponseException, ServerError, Forbidden
from praw.exceptions import RedditAPIException
from concurrent.futures import ThreadPoolExecutor, as_completed

from utility.time_utils import to_iso_utc

from utility.state_sqlite import (
    is_comment_seen, mark_comment_seen, mark_comments_seen_batch,
    ensure_thread_row, should_refresh_thread, update_thread_after_scan,
    get_threads_to_refresh, prune_inactive_threads
)


from utility.matching import match_text_to_companies
from functools import lru_cache



ENABLE_KEYWORD_GUARD = False
FAST_KEYWORDS: Set[str] = set()  # e.g. {"canon","sony","raiffeisen","aztech"}

PER_POST_SLEEP_SECONDS = 0.05    # tiny pacing to be gentle on API

# When a writer callback is provided, flush rows every N items
FLUSH_EVERY = 2000
MAX_PARALLEL_WORKERS = 4


def _fast_guard(text: str) -> bool:

    return True


def _edited_fields(raw_edited) -> Tuple[bool, str]:
    if not raw_edited or raw_edited is False or raw_edited == "False":
        return False, ""
    try:
        return True, to_iso_utc(float(raw_edited))
    except Exception:
        return True, ""



#  Discovery phase (new posts)


def fetch_new_posts(reddit, subreddits, limit) -> List[Any]:
    """Fetch most recent posts from a list of subreddits."""
    posts = []
    for subreddit in subreddits:
        for post in reddit.subreddit(subreddit).new(limit=limit):
            posts.append(post)
    return posts


# >>> Helpers to compute thread context
def _compute_root(parent_map: Dict[str, str], cid: str) -> str:
    """Walk up parent_map until top-level (parent == ''), return the top-level comment id (or cid if already top-level)."""
    seen = set()
    cur = cid
    while True:
        if cur in seen:
            return cid
        seen.add(cur)
        pid = parent_map.get(cur, "")
        if not pid:
            return cur
        cur = pid


def _lazy_match_comment(match_map: Dict[str, Set[str]], body_map: Dict[str, str], cid: str) -> Set[str]:
    s = match_map.get(cid)
    if s is not None:
        return s
    body = body_map.get(cid, "") or ""
    companies, _, _ = match_text_to_companies(body)
    res = set(companies)
    match_map[cid] = res
    return res

@lru_cache(maxsize=50000)
def _match_cached(text: str):
    """Cache wrapper for match_text_to_companies to cut redundant work."""
    return match_text_to_companies(text or "")


#  Core comment extraction


def fetch_comments_from_posts(posts, conn, comment_limit_per_post, writer=None) -> List[Dict[str, Any]]:
    all_comments: List[Dict[str, Any]] = []

    for i, post in enumerate(posts):
        sub_id = getattr(post, "id", "")
        subreddit = str(getattr(post, "subreddit", "") or "")
        title = getattr(post, "title", "") or ""
        created_utc = float(getattr(post, "created_utc", 0) or 0)
        current_comment_count = int(getattr(post, "num_comments", 0) or 0)

        if not sub_id:
            continue

        # 1) Ensure thread row exists
        try:
            ensure_thread_row(conn, sub_id, subreddit, title, created_utc)
        except Exception as e:
            print(f"[{i}] Warning: ensure_thread_row failed for {sub_id}: {e}")

        # 2) Check if refresh is needed
        try:
            refresh_now, prev_state = should_refresh_thread(
                conn,
                submission_id=sub_id,
                current_comment_count=current_comment_count,
                max_age_hours=12,
            )
        except Exception as e:
            print(f"[{i}] Warning: should_refresh_thread failed for {sub_id}: {e}")
            refresh_now, prev_state = True, {"last_comment_count": None}

        if not refresh_now:
            print(f"[{i}] Skip {sub_id}: no growth and not stale (count={current_comment_count})")
            continue

        print(f"[{i}] Processing {sub_id} (count={current_comment_count})")

        # 3) Basic post metadata
        selftext = getattr(post, "selftext", "") or ""
        post_text = f"{title}\n{selftext}"

        # Optional annotation only (kept for later analysis)
        post_companies, post_terms, post_types = _match_cached(post_text)
        post_matched_companies = sorted(post_companies)
        post_matched_terms = post_terms

        # 4) Load full comment forest
        forest_list = []
        try:
            try:
                post.comment_sort = "new"
            except Exception:
                pass
            post.comments.replace_more(limit=24)
            forest_list = list(post.comments.list())

            if isinstance(comment_limit_per_post, int) and comment_limit_per_post > 0:
                forest_list = forest_list[:comment_limit_per_post]
        except (RequestException, ResponseException, ServerError, Forbidden, RedditAPIException) as e:
            print(f"[{i}] replace_more failed on {sub_id}: {e}")
            continue
        except Exception as e:
            print(f"[{i}] unexpected replace_more error on {sub_id}: {e}")
            continue

        # 5) Build maps
        parent_map, body_map, match_map = {}, {}, {}
        for c in forest_list:
            cid0 = str(getattr(c, "id", "") or "")
            if not cid0:
                continue
            pid_full = getattr(c, "parent_id", "") or ""
            parent_comment_id = pid_full[3:] if pid_full.startswith("t1_") else ""
            parent_map[cid0] = parent_comment_id
            body_map[cid0] = getattr(c, "body", "") or ""

        saved_count, processed_new = 0, 0
        batch, batch_cids = [], []

        # 6) Iterate comments
        for comment in forest_list:
            if processed_new >= comment_limit_per_post:
                break

            cid = str(getattr(comment, "id", "") or "")
            if not cid:
                continue
            if is_comment_seen(conn, cid):
                continue

            body = body_map.get(cid, "")
            c_companies, c_terms, c_types = _match_cached(body)
            comment_matched_companies = sorted(c_companies)
            comment_matched_terms = c_terms

            # --- Optional metadata only (no filtering) ---
            if c_companies:
                final_companies = comment_matched_companies
                matched_source = "comment"
                match_confidence = "high"
                matched_where = "comment"
            else:
                final_companies = []
                matched_source = "none"
                match_confidence = "none"
                matched_where = "none"
            # ---------------------------------------------

            parent_id = getattr(comment, "parent_id", "") or ""
            link_id = getattr(comment, "link_id", "") or ""
            parent_comment_id = parent_map.get(cid, "")
            is_top_level = parent_id.startswith("t3_")
            depth = int(getattr(comment, "depth", 0) or 0)
            root_comment_id = _compute_root(parent_map, cid)

            # This remains valid: it tracks structural reply info
            if is_top_level:
                is_reply_to_company_mention = 1 if post_companies else 0
            else:
                parent_companies = _lazy_match_comment(match_map, body_map, parent_comment_id)
                is_reply_to_company_mention = 1 if parent_companies else 0

            comm_created_epoch = getattr(comment, "created_utc", None)
            sub_created_epoch = float(getattr(post, "created_utc", 0) or 0)
            comm_created_iso = to_iso_utc(comm_created_epoch)
            sub_created_iso = to_iso_utc(sub_created_epoch)
            edited_bool, edited_time_iso = _edited_fields(getattr(comment, "edited", False))

            row = {
                "comment_id": cid,
                "parent_id": parent_id,
                "link_id": link_id,
                "submission_id": link_id.replace("t3_", ""),
                "is_top_level": is_top_level,
                "parent_comment_id": parent_comment_id,
                "depth": depth,
                "root_comment_id": root_comment_id,
                "is_reply_to_company_mention": is_reply_to_company_mention,
                "is_context_only": False,
                "submission_title": title,
                "submission_created_utc": sub_created_epoch,
                "submission_created_iso": sub_created_iso,
                "subreddit": subreddit,
                "permalink": f"https://www.reddit.com{getattr(comment, 'permalink', '')}",
                "author": str(getattr(comment, "author", None)) if getattr(comment, "author", None) else "[deleted]",
                "author_is_mod": bool(getattr(comment, "author_is_moderator", False)),
                "is_submitter": bool(getattr(comment, "is_submitter", False)),
                "distinguished": getattr(comment, "distinguished", "") or "",
                "edited_bool": edited_bool,
                "edited_time_iso": edited_time_iso,
                "body_raw": body,
                "score": getattr(comment, "score", 0),
                "ups": getattr(comment, "ups", 0),
                "downs": getattr(comment, "downs", 0),
                "created_utc": comm_created_epoch,
                "created_iso": comm_created_iso,
                "post_matched_companies": ";".join(post_matched_companies),
                "post_matched_terms": ";".join(post_matched_terms),
                "comment_matched_companies": ";".join(comment_matched_companies),
                "comment_matched_terms": ";".join(comment_matched_terms),
                "matched_companies": ";".join(final_companies),
                "matched_source": matched_source,
                "match_confidence": match_confidence,
                "matched_where": matched_where,
                "company_set_size": len(final_companies),
            }

            # write & mark seen (batched)
            if writer is None:
                all_comments.append(row)
                batch_cids.append(cid)
                if len(batch_cids) >= 200:
                    mark_comments_seen_batch(conn, batch_cids)
                    batch_cids.clear()
            else:
                batch.append(row)
                batch_cids.append(cid)
                if len(batch) >= FLUSH_EVERY:
                    written = writer(batch)
                    if written:
                        mark_comments_seen_batch(conn, batch_cids)
                    batch.clear()
                    batch_cids.clear()

            saved_count += 1
            processed_new += 1

        # flush remaining batch
        if writer and batch:
            written = writer(batch)
            if written:
                mark_comments_seen_batch(conn, batch_cids)
            batch.clear()
            batch_cids.clear()
        elif not writer and batch_cids:
            mark_comments_seen_batch(conn, batch_cids)
            batch_cids.clear()

        # 7) Update thread metadata
        try:
            prev_count = (prev_state or {}).get("last_comment_count") or 0
            had_growth = current_comment_count > prev_count
            update_thread_after_scan(
                conn,
                submission_id=sub_id,
                latest_comment_count=current_comment_count,
                had_growth=had_growth,
            )
        except Exception as e:
            print(f"[{i}] Warning: update_thread_after_scan failed for {sub_id}: {e}")

        print(f"[{i}] Saved {saved_count} comments (all-inclusive) | post ID {sub_id}")
        time.sleep(PER_POST_SLEEP_SECONDS)

    return all_comments


def parallel_fetch_comments(
        reddit,
        posts,
        conn,
        comment_limit_per_post,
        writer=None,
        max_workers: int = 4
) -> List[Dict[str, Any]]:
    """
    Fetch comments from multiple posts in parallel.
    Each worker runs fetch_comments_from_posts([post]).
    """
    results: List[Dict[str, Any]] = []
    if not posts:
        print(" No posts provided to parallel_fetch_comments.")
        return results

    print(f" Starting parallel fetch with {max_workers} workers "
          f"for {len(posts)} posts...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_post = {
            executor.submit(fetch_comments_from_posts, [post], conn, comment_limit_per_post, writer): post
            for post in posts
        }
        for i, future in enumerate(as_completed(future_to_post)):
            post = future_to_post[future]
            try:
                res = future.result()
                if res:
                    results.extend(res)
                pid = getattr(post, "id", "?")
                print(f"[Thread {i + 1}]  Finished post {pid[:6]} ({len(res)} comments)")

            except Exception as e:
                print(f"[Thread {i + 1}] ️ Error processing {getattr(post, 'id', '?')}: {e}")

    print(f" Parallel fetch complete: {len(results)} total comments.")
    return results



def refresh_active_threads(
    reddit,
    conn,
    writer=None,
    comment_limit_per_post: int = 10000,
    max_age_hours: int = 48
):
    """
    Refresh active threads that haven't been updated recently.
    Now uses small-scale parallel fetching for better efficiency.
    """
    threads = get_threads_to_refresh(conn, max_age_hours=max_age_hours)
    if not threads:
        print("No active threads need refresh.")
        return []

    print(f"Refreshing {len(threads)} active threads …")
    total_refreshed = 0
    all_refreshed: List[Dict[str, Any]] = []

    for i, t in enumerate(threads):
        sub_id = t.get("submission_id")
        if not sub_id:
            continue

        try:
            post = reddit.submission(id=sub_id)
            refreshed_comments = parallel_fetch_comments(
                reddit,
                [post],
                conn,
                comment_limit_per_post,
                writer=writer,
                max_workers=2
            )

            if refreshed_comments:
                all_refreshed.extend(refreshed_comments)

            total_refreshed += 1
            print(f"[{i}]  Refreshed {sub_id} ({len(refreshed_comments)} new/updated comments)")
        except Exception as e:
            print(f"[{i}]  Error refreshing {sub_id}: {e}")

        # Optional — very short pause
        time.sleep(PER_POST_SLEEP_SECONDS)

    # 8) Prune inactive threads
    try:
        deleted = prune_inactive_threads(conn)
        if deleted:
            print(f" Pruned {deleted} inactive threads from queue.")
    except Exception as e:
        print(f"[Cleanup] Warning: prune_inactive_threads failed: {e}")

    print(f" Refreshed {total_refreshed} threads total. "
          f"Collected {len(all_refreshed)} comments overall.")

    return all_refreshed
