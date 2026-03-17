

import numpy as np


def build_comment_assignments(comment_ids, topics, probs):
    """
    Prepares the assignment records for gold.comment_topics (The Fact Table).

    Args:
        comment_ids (list): List of comment IDs from fetch.
        topics (list): List of topic IDs from BERTopic.
        probs (list/np.array): Probabilities from BERTopic.

    Returns:
        list[dict]: Records ready for db_writer.insert_comment_assignments
    """
    print(f"• Mapping {len(comment_ids):,} comments to topics...")

    records = []

    # Safety Check: Ensure aligned lengths
    if len(comment_ids) != len(topics):
        print(
            f"⚠️ WARNING: Mismatch detected. {len(comment_ids)} IDs vs {len(topics)} Topics. Truncating to minimum length.")
        limit = min(len(comment_ids), len(topics))
        comment_ids = comment_ids[:limit]
        topics = topics[:limit]
        probs = probs[:limit]

    for i, cid in enumerate(comment_ids):
        tid = int(topics[i])

        # Determine probability value robustly
        # HDBSCAN soft clusters return an array of probs, hard clusters usually return a single list
        p = 0.0
        try:
            if isinstance(probs, np.ndarray) and probs.ndim > 1:
                # Soft clustering case: grab prob of the specific assigned topic
                if tid != -1 and tid < probs.shape[1]:
                    p = float(probs[i][tid])
            elif isinstance(probs, (list, np.ndarray)):
                # Hard clustering case (1D array)
                p = float(probs[i])
            else:
                # Fallback
                p = 1.0
        except Exception:
            p = 0.0

        records.append({
            "comment_id": cid,
            "topic_id": tid,
            "prob": round(p, 4)
        })

    print(f"  → Prepared {len(records):,} assignment records.")
    return records