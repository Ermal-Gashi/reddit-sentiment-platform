

import os
import numpy as np
from datetime import timedelta
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Tuple, Dict

# Import database functions
from db_writer import get_conn, insert_topic_links

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
# Strict: Almost identical topics (likely the same conversation continuing)
SIMILARITY_THRESHOLD_STRICT = 0.85

# Loose: Related topics (e.g., "Nvidia Earnings" -> "Nvidia Stock Drop")
SIMILARITY_THRESHOLD_LOOSE = 0.65

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"


# ---------------------------------------------------------
# 1. Fetch Helpers
# ---------------------------------------------------------
def fetch_topics_for_date(target_date) -> Dict[int, str]:
    """
    Fetches non-junk topics for a specific date.
    Returns a dict: { topic_id: "keyword1 keyword2 ..." }
    """
    sql = """
        SELECT topic_id, topic_keywords
        FROM gold.daily_topics
        WHERE date_utc = %s 
          AND is_junk = FALSE
    """

    topic_map = {}

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (target_date,))
            rows = cur.fetchall()

        for tid, keywords in rows:
            if keywords:
                # Join top 15 words to form a "topic signature" for embedding
                # This provides enough context for the model to understand the specific nuance
                signature = " ".join(keywords[:15])
                topic_map[tid] = signature

    except Exception as e:
        print(f"   ⚠️ Error fetching topics for {target_date}: {e}")
        return {}

    return topic_map


# ---------------------------------------------------------
# 2. Matching Logic
# ---------------------------------------------------------
def find_topic_links(current_date, prev_date) -> List[Tuple]:
    """
    Compares two dates and returns a list of evolution records.
    """
    print(f"🔍 Analyzing Topic Evolution: {prev_date} -> {current_date}")

    # A. Fetch Data
    curr_topics = fetch_topics_for_date(current_date)
    prev_topics = fetch_topics_for_date(prev_date)

    # Validation
    if not curr_topics:
        print(f"   ⚠️ No valid topics found for Today ({current_date}). Skipping evolution.")
        return []

    if not prev_topics:
        print(f"   ⚠️ No valid topics found for Yesterday ({prev_date}). First run or gap in data.")
        return []

    print(f"   • Comparing {len(prev_topics)} yesterday vs {len(curr_topics)} today.")

    # B. Prepare Embeddings
    # We use a lightweight model specifically for this comparison
    # (Loading inside function to save memory if script is imported but not run)
    model = SentenceTransformer(MODEL_NAME)

    # Sort IDs to ensure alignment between index and ID
    prev_ids = sorted(prev_topics.keys())
    curr_ids = sorted(curr_topics.keys())

    prev_texts = [prev_topics[tid] for tid in prev_ids]
    curr_texts = [curr_topics[tid] for tid in curr_ids]

    # Encode (Batch processing is efficient)
    # convert_to_numpy=True is default in newer versions but explicit here for safety
    prev_embeddings = model.encode(prev_texts, convert_to_numpy=True, show_progress_bar=False)
    curr_embeddings = model.encode(curr_texts, convert_to_numpy=True, show_progress_bar=False)

    # C. Calculate Similarity Matrix
    # Shape: (n_prev_topics, n_curr_topics)
    similarity_matrix = cosine_similarity(prev_embeddings, curr_embeddings)

    # D. Extract Links
    links = []

    # Iterate through Yesterday's topics (Rows)
    for i, p_id in enumerate(prev_ids):
        # Find the column (Today's topic) with the highest similarity
        best_match_idx = np.argmax(similarity_matrix[i])
        best_score = similarity_matrix[i][best_match_idx]
        best_c_id = curr_ids[best_match_idx]

        # Determine Link Type based on thresholds
        link_type = None
        if best_score >= SIMILARITY_THRESHOLD_STRICT:
            link_type = "CONTINUATION"
        elif best_score >= SIMILARITY_THRESHOLD_LOOSE:
            link_type = "RELATED"

        if link_type:
            links.append((
                prev_date,  # source_date
                p_id,  # source_id
                current_date,  # target_date
                best_c_id,  # target_id
                float(best_score),
                link_type
            ))

    return links


# ---------------------------------------------------------
# 3. Main Runner
# ---------------------------------------------------------
def run_topic_evolution(target_date):
    """
    Main entry point to be called by Orchestrator.
    Calculates T-1 -> T evolution.
    """
    # Calculate "Yesterday"
    prev_date = target_date - timedelta(days=1)

    # Find Links
    links = find_topic_links(target_date, prev_date)

    # Write to DB
    if links:
        insert_topic_links(links)
    else:
        print("   -> No significant topic evolution found (or missing data).")


# ---------------------------------------------------------
# CLI Entry Point (For Testing)
# ---------------------------------------------------------
if __name__ == "__main__":
    import argparse
    from datetime import datetime, date

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="YYYY-MM-DD (Target Date)")
    args = parser.parse_args()

    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target = date.today()

    run_topic_evolution(target)