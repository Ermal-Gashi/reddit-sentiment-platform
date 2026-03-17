import re
from typing import List, Optional

import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from bertopic.representation import MaximalMarginalRelevance
from collections import defaultdict, Counter


# ============================================================
#  MINIMAL PREPROCESSOR (preserves letters, but not tags)
# ============================================================
def preprocess_minimal(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r"[^a-zA-Z\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ============================================================
#  TAG INJECTION (used only for embeddings)
# ============================================================
def inject_company_tag(text: str, companies: Optional[List[str]]) -> str:
    if not companies:
        return text
    return "[COMPANY=" + ",".join(c.upper() for c in companies) + "] " + text


# ============================================================
#  REMOVE TAG BEFORE VECTORIZER
# ============================================================
def strip_company_tag(text: str) -> str:
    return re.sub(r"\[COMPANY=[A-Z0-9,]+\]\s*", "", text)


# ============================================================
#  COMPANY STATS
# ============================================================
def analyze_company_distribution(topics, companies):
    dist = defaultdict(list)

    for t, comps in zip(topics, companies):
        if t == -1:
            continue
        if comps:
            dist[t].extend(c.lower() for c in comps)

    cluster_stats = {}
    for t, comp_list in dist.items():
        if not comp_list:
            cluster_stats[t] = {
                "dominant": None,
                "dominance_ratio": 0.0,
                "unique_companies": 0,
                "counts": {}
            }
            continue

        counts = Counter(comp_list)
        dominant_company, dom_count = counts.most_common(1)[0]
        cluster_stats[t] = {
            "dominant": dominant_company,
            "dominance_ratio": dom_count / len(comp_list),
            "unique_companies": len(counts),
            "counts": dict(counts)
        }

    return cluster_stats



# ============================================================
#  CUSTOM STOP WORDS DEFINITION (Smart-Tuned)
# ============================================================
CUSTOM_STOCK_STOP_WORDS = {
    # --- Conversation / Fillers ---
    'bro', 'ppl', 'just', 'got', 'shit', 'lol', 'lmao', 'fuck', 'fucking',
    'today', 'tomorrow', 'yesterday', 'impressed', 'want', 'think', 'thinking',
    'know', 'good', 'bad', 'great', 'small', 'big', 'amp', 'yeah', 'like',
    'did', 'can', 'cant', 'would', 'could', 'should', 'say', 'said', 'way',
    'make', 'made', 'people', 'guy', 'guys', 'man', 'men', 'woman', 'women',
    'thing', 'things', 'stuff', 'lot', 'lots', 'bit', 'little', 'actually',
    'literally', 'basically', 'pretty', 'quite', 'really', 'definitely',
    'probably', 'maybe', 'perhaps', 'guess', 'guessing', 'looking', 'look',
    'looks', 'use', 'using', 'used', 'try', 'trying', 'tried', 'start',
    'started', 'end', 'ended', 'happen', 'happened', 'happening', 'getting',
    'going', 'gone', 'went', 'come', 'came', 'coming', 'tell', 'told',
    'ask', 'asked', 'asking', 'let', 'lets', 'doing', 'does', 'didnt',
    'isnt', 'wasnt', 'arent', 'werent', 'havent', 'hasnt', 'wont', 'dont',
    'doesnt', 'gotta', 'gonna', 'wanna', 'idk', 'tbh', 'imo', 'imho',
    'point', 'points', 'idea', 'ideas', 'reason', 'reasons', 'case',
    'cases', 'fact', 'facts', 'sure', 'unsure', 'believe', 'believing',
    'understand', 'understanding', 'mean', 'meaning', 'means', 'hope',
    'hoping', 'wish', 'wishing', 'wait', 'waiting', 'seen', 'saw', 'see',
    'read', 'reading', 'hear', 'hearing', 'heard', 'listen', 'listening',
    'post', 'posts', 'posting', 'comment', 'comments', 'thread', 'sub',
    'subreddit', 'reddit', 'edit', 'update', 'link', 'source', 'video',
    'image', 'pic', 'picture', 'graph', 'chart', 'charts',

    # --- Generic Financial/Market (Still Vague) ---
    'market', 'stock', 'stocks', 'money', 'cash',
    'price', 'prices', 'value', 'values', 'cost', 'costs', 'worth',
    'company', 'companies', 'business', 'businesses', 'industry',
    'sector', 'sectors', 'economy', 'economic', 'finance', 'financial',
    'invest', 'investing', 'investment', 'investments', 'investor',
    'investors', 'trade', 'trading', 'trader', 'traders', 'portfolio',
    'portfolios', 'position', 'positions', 'holding', 'holdings',
    'pay', 'paying', 'paid', 'new', 'old', 'high', 'low', 'higher', 'lower',
    'increase', 'decrease', 'up', 'down', 'drop', 'rise', 'huge',
    'massive', 'tiny', 'total', 'amount', 'number', 'numbers',
    'level', 'levels', 'rate', 'rates', 'percentage', 'percent',
    'avg', 'average', 'max', 'min', 'term', 'terms', 'mid',
    'week', 'weeks', 'month', 'months', 'year', 'years', 'daily',
    'weekly', 'monthly', 'yearly', 'annual', 'quarter', 'quarters',
    'report', 'reports', 'news', 'breaking', 'update', 'updates'
}

EXTENDED_STOP_WORDS = list(ENGLISH_STOP_WORDS.union(CUSTOM_STOCK_STOP_WORDS))

# ============================================================
#  MAIN RUNNER
# ============================================================
def run_bertopic_macro(docs: List[str], companies: List[List[str]]):
    MIN_TOKENS = 5
    processed_docs = []
    processed_companies = []

    # 1. Minimal preprocessing
    for raw_text, comp_list in zip(docs, companies):
        clean = preprocess_minimal(raw_text)
        if clean and len(clean.split()) >= MIN_TOKENS:
            processed_docs.append(clean)
            processed_companies.append(comp_list)

    if not processed_docs:
        raise ValueError("No documents left after minimal preprocessing.")

    print(f" After minimal preprocessing: {len(processed_docs)} docs remain")

    # 2. Inject tag ONLY for embeddings
    tagged_for_embedding = [
        inject_company_tag(txt, comp_list)
        for txt, comp_list in zip(processed_docs, processed_companies)
    ]

    # 3. Encode embeddings
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(" Using:", device.upper())

    embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    embedder = embedder.to(device)
    embedder.max_seq_length = 256

    embeddings = embedder.encode(
        tagged_for_embedding,
        batch_size=64,
        show_progress_bar=True,
        convert_to_numpy=True,
        device=device
    )

    # 4. Strip tags before topic modeling
    docs_for_topic_model = [strip_company_tag(t) for t in tagged_for_embedding]

    # 5. UMAP
    umap_model = UMAP(
        n_components=4,
        n_neighbors=18,
        min_dist=0.08,
        metric="cosine",
        random_state=42
    )

    # 6. HDBSCAN
    hdbscan_model = HDBSCAN(
        min_cluster_size=20,
        min_samples=10,
        metric="euclidean",
        prediction_data=True,
        cluster_selection_method="leaf",
        cluster_selection_epsilon=0.05
    )

    # 7. Vectorizer
    vectorizer_model = CountVectorizer(
        stop_words=EXTENDED_STOP_WORDS,
        lowercase=True,
        min_df=3,
        ngram_range=(1, 3),
        token_pattern=r"(?u)\b[a-zA-Z]{2,}\b"
    )

    # 7.5 Representation (MMR)
    representation_model = MaximalMarginalRelevance(diversity=0.7)

    # 8. Train BERTopic   ONLY CHANGE IS HERE
    print(f" Training BERTopic Macro on {len(docs_for_topic_model):,} docs…")

    model = BERTopic(
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        min_topic_size=20,
        calculate_probabilities=True,
        top_n_words=40,          #  FIX: ranked keywords increased
        verbose=True,
    )

    topics, probs = model.fit_transform(docs_for_topic_model, embeddings)

    print(" Micro Topics discovered:", len(set(int(t) for t in topics if t != -1)))

    # 9. Company distribution
    company_stats = analyze_company_distribution(topics, processed_companies)

    if device == "cuda":
        torch.cuda.empty_cache()

    print(" BERTopic Macro DONE.\n")

    return model, topics, probs, embeddings, company_stats, tagged_for_embedding
