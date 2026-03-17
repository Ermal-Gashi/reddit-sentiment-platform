

import time
import argparse
import numpy as np
from datetime import datetime, timedelta, timezone
from rich.console import Console
from rich.progress import track

# -------------------------------
# CONFIG
# -------------------------------
MAX_TOPIC_KEYWORDS = 50
MAX_REPRESENTATIVES = 3   #  how many sentences to store/display

# -------------------------------
# Imports
# -------------------------------
from fetch import fetch_raw_texts_and_companies
from db_writer import insert_topic_summaries, insert_comment_assignments

console = Console()


# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def run_daily_pipeline(target_date):
    console.rule(f"[bold green]Running Gold Stage 5 for {target_date}[/]")
    t0 = time.time()

    # ---------------------------------------
    # LAZY IMPORTS
    # ---------------------------------------
    console.print("[dim]...Loading AI Libraries...[/]")

    from bertopic_runner import run_bertopic_macro, preprocess_minimal
    from topic_postprocess import postprocess_topics
    from topic_quality_metrics import compute_topic_quality_metrics
    from llm_generator import call_groq_retry
    from topic_assignment import build_comment_assignments
    from topic_evolution import run_topic_evolution

    # Time window (UTC)
    start_dt = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    # -------------------------
    # STEP 1: FETCH
    # -------------------------
    console.print("[yellow]1. Fetching data...[/]")
    ids, docs, companies = fetch_raw_texts_and_companies(start_dt, end_dt)

    if not ids:
        console.print("[red]No data found. Exiting.[/]")
        return

    # -------------------------
    # STEP 2: PREPROCESS
    # -------------------------
    console.print(f"[yellow]2. Preprocessing {len(docs)} documents...[/]")

    clean_ids, clean_docs, clean_companies = [], [], []

    for i, txt in enumerate(docs):
        processed = preprocess_minimal(txt)
        if processed and len(processed.split()) >= 5:
            clean_ids.append(ids[i])
            clean_docs.append(processed)
            clean_companies.append(companies[i])

    if not clean_docs:
        console.print("[red]No documents remained after preprocessing. Exiting.[/]")
        return

    console.print(f"   → {len(clean_docs)} docs ready.")

    # -------------------------
    # STEP 3: BERTOPIC
    # -------------------------
    console.print("[yellow]3. Running BERTopic...[/]")

    model, topics, probs, embeddings, company_stats, tagged_docs = run_bertopic_macro(
        clean_docs, clean_companies
    )

    model_version = f"v7_trigram_{target_date}"

    # -------------------------
    # STEP 4: POST-PROCESS
    # -------------------------
    console.print("[yellow]4. Post-processing topics...[/]")

    post_data = postprocess_topics(model, tagged_docs, topics, embeddings)

    keywords_map = {tid: info["keywords"] for tid, info in post_data.items()}

    quality_metrics = compute_topic_quality_metrics(
        clean_docs,
        topics,
        keywords_map,
        embeddings
    )

    # -------------------------
    # STEP 5: BUILD TOPIC SUMMARIES
    # -------------------------
    console.print("[yellow]5. Generating titles & summaries...[/]")

    topic_summary_records = []
    unique_topics = sorted(post_data.keys())

    for t_id in track(unique_topics, description="Processing Topics"):
        info = post_data[t_id]
        q_info = quality_metrics.get(t_id, {})
        keywords = info["keywords"]          # [(word, score)]
        reps = info["representatives"]       # [(text, idx, score)]
        is_junk = info["is_junk"] or info["is_meme"]

        # ---------------------
        # Representative Sentences ( NEW)
        # ---------------------
        representative_sentences = [
            {
                "text": txt,
                "score": round(float(score), 4)
            }
            for (txt, _, score) in reps[:MAX_REPRESENTATIVES]
        ]

        metrics_blob = {
            "meme_score": info["meme_score"],
            "spam_score": info.get("spam_score", 0.0),
            "structural_score": info["structural_score"],
            "semantic_density": info["semantic_density"],
            "distinctiveness": q_info.get("distinctiveness"),
        }

        if is_junk:
            professional_title = "Skipped (Junk/Noise)"
        else:
            try:
                _, professional_title = call_groq_retry(t_id, keywords, reps)
            except Exception:
                professional_title = "API limit hit"

        real_count = int(np.sum(np.array(topics) == t_id))

        topic_summary_records.append({
            "topic_id": int(t_id),
            "title": professional_title,
            "keywords": [w for w, _ in keywords[:MAX_TOPIC_KEYWORDS]],
            "representatives": representative_sentences,  #  STORED
            "doc_count": real_count,
            "is_junk": bool(is_junk),
            "metrics": metrics_blob,
        })

    # -------------------------
    # STEP 6: COMMENT ASSIGNMENTS
    # -------------------------
    console.print("[yellow]6. Preparing comment assignments...[/]")
    assignment_records = build_comment_assignments(clean_ids, topics, probs)

    # -------------------------
    # STEP 7: DB WRITE
    # -------------------------
    console.print("[yellow]7. Writing Gold layer...[/]")
    insert_topic_summaries(target_date, topic_summary_records, model_version)
    insert_comment_assignments(target_date, assignment_records)

    # -------------------------
    # STEP 8: TOPIC EVOLUTION
    # -------------------------
    console.print("[yellow]8. Running topic evolution...[/]")
    run_topic_evolution(target_date)

    console.print(
        f"[bold green] DONE! {len(clean_docs)} docs → "
        f"{len(topic_summary_records)} topics[/]"
    )
    console.print(f"⏱ Runtime: {(time.time() - t0) / 60:.2f} min")


# ---------------------------------------------------------
# CLI ENTRY POINT
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str)
    parser.add_argument("--backfill-start", type=str)
    parser.add_argument("--backfill-end", type=str)

    args = parser.parse_args()

    if args.backfill_start and args.backfill_end:
        start = datetime.strptime(args.backfill_start, "%Y-%m-%d").date()
        end = datetime.strptime(args.backfill_end, "%Y-%m-%d").date()

        console.print(f"[bold magenta] Backfill {start} → {end}[/]")

        curr = start
        while curr <= end:
            run_daily_pipeline(curr)
            curr += timedelta(days=1)
    else:
        if args.date:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            target = datetime.now(timezone.utc).date() - timedelta(days=1)

        run_daily_pipeline(target)
