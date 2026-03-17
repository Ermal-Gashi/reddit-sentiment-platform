

import numpy as np
from collections import defaultdict


# ---------------------------------------------------------
# 1. Distinctiveness
# ---------------------------------------------------------
def _compute_distinctiveness(topics, embeddings):
    """
    Calculates how far a topic's centroid is from its nearest neighbor.

    Distinctiveness is defined as the minimum Euclidean distance
    between a topic centroid and any other topic centroid.

    Higher = better topic separation.
    """
    topic_vecs = defaultdict(list)

    # Group embeddings by topic
    for emb, tid in zip(embeddings, topics):
        if tid != -1:
            topic_vecs[tid].append(emb)

    # Compute centroids (require at least 3 documents)
    centroids = {
        t: np.mean(vecs, axis=0)
        for t, vecs in topic_vecs.items()
        if len(vecs) >= 3
    }

    topic_ids = list(centroids.keys())
    distinctiveness = {}

    for tid in topic_ids:
        base = centroids[tid]

        # Distance to all other topic centroids
        dists = [
            np.linalg.norm(base - centroids[other])
            for other in topic_ids
            if other != tid
        ]

        # Closest neighbor distance
        distinctiveness[tid] = float(min(dists)) if dists else 0.0

    return distinctiveness


# ---------------------------------------------------------
# 2. Main Quality Metric Function
# ---------------------------------------------------------
def compute_topic_quality_metrics(
        docs,
        topics,
        keywords,   # dict[int -> list[(word, score)]]
        embeddings,
        top_k=10
):
    """
    Computes topic quality metrics.

    Parameters:
      docs        : list[str]   (unused, kept for interface stability)
      topics      : list[int]   (topic assignment per document)
      keywords    : dict[int -> list[(word, score)]]
      embeddings  : ndarray     (document embeddings)
      top_k       : int         (kept for compatibility)

    Returns:
      dict[int -> dict]:
        {
          topic_id: {
            "distinctiveness": float
          }
        }
    """
    print(f"• Computing quality metrics for {len(keywords)} topics...")

    # Compute embedding-based distinctiveness
    distinctiveness = _compute_distinctiveness(topics, embeddings)

    results = {}

    for tid in keywords.keys():
        if tid == -1:
            continue

        results[tid] = {
            "distinctiveness": round(
                float(distinctiveness.get(tid, 0.0)),
                4
            )
        }

    print("  → Topic quality metrics computed.")
    return results
