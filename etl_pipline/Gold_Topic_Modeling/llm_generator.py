import os
import json
import time
import argparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Rich Console for beautiful output
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich import box

# Groq & Env
from groq import Groq
from dotenv import load_dotenv

# Import your pipeline modules
from bertopic_runner import run_bertopic_macro, preprocess_minimal
from topic_postprocess import postprocess_topics
from fetch import fetch_raw_texts_and_companies

# ============================================================
#  CONFIG & SETUP
# ============================================================

load_dotenv()  # Load credentials from .env file
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

console = Console()

if not GROQ_API_KEY:
    console.print("[bold red]CRITICAL ERROR:[/] GROQ_API_KEY not found in environment variables.")
    console.print("Please ensure you have a .env file with GROQ_API_KEY=your_key")
    # We don't exit here strictly to allow import in diagnose script,
    # but actual calls will fail if key is missing.

# Initialize client if key exists
client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

# UPDATED: Using the correct, supported model
LLM_MODEL = "llama-3.3-70b-versatile"

SYSTEM_PROMPT = """
You are a Senior Financial Analyst for a Hedge Fund. 
Your job is to label clusters of social media comments about the stock market.

Rules for Titles:
1. Be professional, concise, and specific (2-6 words).
2. Use financial terminology (e.g., "Bearish Sentiment", "Short Squeeze", "Technical Reversal", "Call Buying Frenzy").
3. If a specific ticker (like NVDA, TSLA) is dominant in the keywords, INCLUDE IT.
4. If the topic has words like "sold", "bought", "calls", "puts", focus on the TRADING ACTIVITY (e.g., "Heavy Retail Selling").
5. Do NOT use quotes, colons, or generic words like "Topic" or "Discussion".
""".strip()

USER_TEMPLATE = """
TOPIC KEYWORDS: {keywords}

SAMPLE COMMENTS:
{documents}

Based on the above, provide a single, professional title for this topic.
"""


# ============================================================
#  ROBUST GROQ CALLER
# ============================================================

def call_groq_retry(topic_id, keywords, representatives, max_retries=3):
    """
    Calls Groq with retries and backoff logic.
    """
    if not client:
        return topic_id, "API Key Missing"

    # Prepare Prompt Data
    kw_str = ", ".join([w for w, _ in keywords[:12]])  # Give top 12 words for context
    docs_str = "\n".join([f"- {doc[:250]}..." for doc, _, _ in representatives[:4]])  # Give 4 reps

    user_content = USER_TEMPLATE.format(keywords=kw_str, documents=docs_str)

    # Small throttle to prevent immediate rate limit on startup
    time.sleep(0.1)

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=LLM_MODEL,  # Corrected Model Name
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content}
                ],
                temperature=0.3,  # Consistent, analytical tone
                max_tokens=30,  # Titles are short
            )

            raw_title = response.choices[0].message.content.strip()
            # Basic cleaning: remove quotes if the LLM adds them
            clean_title = raw_title.replace('"', '').replace("'", "").strip()

            if clean_title.endswith("."):
                clean_title = clean_title[:-1]

            return topic_id, clean_title

        except Exception as e:
            wait_time = 1.5 * (attempt + 1)
            # console.print(f"[yellow]Retrying Topic {topic_id} (Attempt {attempt+1}/{max_retries})...[/]")
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                return topic_id, "Manual Review Needed (API Error)"

    return topic_id, "Manual Review Needed (Failed)"


# ============================================================
#  MAIN PIPELINE
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    if not client:
        console.print("[bold red]Cannot run pipeline: API Key is missing.[/]")
        return

    # 1. Fetch Data
    start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    console.rule(f"[bold green]Running Pipeline for {start.date()}")

    with console.status("[bold green]Fetching data from Database...") as status:
        raw_docs, raw_companies = fetch_raw_texts_and_companies(start, end)
    console.print(f"Fetched {len(raw_docs):,} raw documents.")

    # 2. Preprocess
    processed_docs = []
    processed_companies = []
    for txt, comps in zip(raw_docs, raw_companies):
        clean = preprocess_minimal(txt)
        if clean and len(clean.split()) >= 5:
            processed_docs.append(clean)
            processed_companies.append(comps)

    # 3. Run BERTopic
    console.print("[yellow]Running BERTopic (Trigrams + Smart Stopwords)...[/]")
    model, topics, probs, embeddings, company_stats, tagged_docs = run_bertopic_macro(
        processed_docs, processed_companies
    )

    # 4. Post-Process
    console.print("[yellow]Running Post-Processing (Junk Detection)...[/]")
    post_data = postprocess_topics(model, tagged_docs, topics, embeddings)

    # 5. Filter Valid Topics
    valid_topics = []
    skipped_count = 0

    for t, info in post_data.items():
        # Check Junk Flags
        if info['is_junk'] or info['is_meme']:
            skipped_count += 1
            continue
        valid_topics.append(t)

    console.print(
        f"\n[bold cyan]Generating Titles for {len(valid_topics)} valid topics (Skipped {skipped_count} junk)...[/]")

    # 6. Parallel LLM Generation
    final_results = []

    # Setup Table for live view
    table = Table(title=f"LLM Generated Titles ({args.date})", box=box.ROUNDED)
    table.add_column("ID", style="dim", width=4)
    table.add_column("Keywords", style="magenta", width=40)
    table.add_column("Professional Title", style="bold green")

    # Parallel Execution
    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    ) as progress:

        task = progress.add_task("[cyan]Processing with Groq...", total=len(valid_topics))

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_topic = {
                executor.submit(
                    call_groq_retry,
                    t_id,
                    post_data[t_id]['keywords'],
                    post_data[t_id]['representatives']
                ): t_id for t_id in valid_topics
            }

            for future in as_completed(future_to_topic):
                t_id, llm_title = future.result()

                info = post_data[t_id]
                kw_preview = ", ".join([w for w, _ in info['keywords'][:4]])

                # Add to Table
                table.add_row(str(t_id), kw_preview, llm_title)

                # Build Final JSON Object
                topic_obj = {
                    "topic_id": int(t_id),
                    "title": llm_title,
                    "doc_count": len(info['representatives']),  # Using rep count as proxy for now
                    "keywords": [w for w, _ in info['keywords'][:10]],
                    "representative_docs": [d for d, _, _ in info['representatives'][:3]],
                    "metrics": {
                        "meme_score": info['meme_score'],
                        "spam_score": info.get('spam_score', 0.0),
                        "semantic_density": info['semantic_density']
                    }
                }
                final_results.append(topic_obj)
                progress.advance(task)

    # 7. Show Summary Table
    console.print(table)

    # 8. Save
    filename = f"topics_{args.date}.json"
    with open(filename, "w") as f:
        json.dump(final_results, f, indent=2)

    console.print(f"\n[bold green]Success! Saved {len(final_results)} clean topics to {filename}[/]")


if __name__ == "__main__":
    main()