

from typing import Dict, List, Tuple
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import re


# ============================================================
#  TAG STRIPPING
# ============================================================

def strip_company_tag(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"\[COMPANY=[A-Z0-9,]+\]\s*", "", text).strip()


# ============================================================
#  GLOBAL LISTS (Expanded)
# ============================================================

# WSB / Reddit Slang
MEME_WORDS = {
    "haha", "lmao", "lol", "cope", "based", "cringe", "npc",
    "sigma", "bruh", "bro", "skibidi", "moon", "retard", "faggot",
    "autist", "wsb", "diamond", "hands", "tendies", "yolo",
    "bagholder", "regarded", "regards", "stonk", "stonks", "rocket",
    "ape", "apes", "boyfriend", "wife", "casino", "gambling", "sir"
}

# Bot / Spam Indicators
SPAM_TOKENS = {
    "discord", "join", "bet", "wins", "losses", "record", "ban",
    "est", "profile", "created", "server", "group", "chat",
    "alert", "signals", "whatsapp", "telegram"
}

STRUCTURAL_TOKENS = {
    "this", "that", "these", "those", "have", "has", "had", "will",
    "would", "should", "could", "like", "just", "they", "them",
    "their", "there", "here", "one"
}

GLUE_TOKENS = {
    "about", "again", "today", "back", "over", "around", "along",
    "almost", "even", "still", "yet", "ever", "maybe", "really"
}


# ============================================================
#  BASE SCORING
# ============================================================

def meme_score(text: str) -> float:
    t = strip_company_tag(text).lower()
    toks = t.split()
    return sum(1 for tok in toks if tok in MEME_WORDS) / max(len(toks), 1)


def spam_score(text: str) -> float:
    t = strip_company_tag(text).lower()
    toks = t.split()
    return sum(1 for tok in toks if tok in SPAM_TOKENS) / max(len(toks), 1)


def structural_ratio(text: str) -> float:
    t = strip_company_tag(text).lower()
    toks = t.split()
    return sum(1 for tok in toks if tok in STRUCTURAL_TOKENS) / max(len(toks), 1)


# ============================================================
#  NOISE SENTENCE FILTER
# ============================================================

def is_noise_sentence(text: str) -> bool:
    if not text:
        return True

    t = strip_company_tag(text).strip()

    if len(t) < 18:
        return True

    toks = t.split()
    if len(toks) < 5:
        return True

    # High meme or spam density makes a sentence "noise" for representative selection
    if meme_score(t) >= 0.40 or spam_score(t) >= 0.40:
        return True

    return False


# ============================================================
#  SEMANTIC METRICS
# ============================================================

def semantic_density(sentence: str) -> float:
    t = strip_company_tag(sentence)
    toks = t.split()
    if len(toks) < 6:
        return 0.0

    unique_ratio = len(set(toks)) / len(toks)
    # Penalize meme/spam content heavily
    penalty = (meme_score(sentence) + spam_score(sentence)) * 2.0
    return unique_ratio * (1.0 - penalty)


def info_score(sentence: str) -> float:
    t = strip_company_tag(sentence)
    toks = t.split()
    if not toks:
        return 0.0

    ur = len(set(toks)) / len(toks)
    length_factor = min(len(toks) / 20, 1.0)
    return 0.5 * ur + 0.5 * length_factor


def keyword_score(sentence: str, keywords: List[str]) -> int:
    t = strip_company_tag(sentence).lower()
    sentence_tokens = set(t.split())

    count = 0
    for w in keywords[:10]:
        # Handle n-grams (single word or phrase)
        if len(w.split()) > 1:
            if w in t:
                count += 1
        elif len(w) >= 3 and w in sentence_tokens:
            count += 1
    return count


# ============================================================
#  REPRESENTATIVE SENTENCE EXTRACTION
# ============================================================

def extract_representatives(
        processed_docs: List[str],
        embeddings: np.ndarray,
        topics: List[int],
        model,
        top_k: int = 5
) -> Dict[int, List[Tuple[str, int, float]]]:
    groups = {}
    for idx, t in enumerate(topics):
        t = int(t)
        if t != -1:
            groups.setdefault(t, []).append(idx)

    centroids = {
        t: np.mean(
            np.vstack([embeddings[i] for i in idxs]), axis=0, keepdims=True
        )
        for t, idxs in groups.items()
    }

    out = {}
    for t, idxs in groups.items():
        kws_raw = model.get_topic(t) or []
        keywords = [w for (w, _) in kws_raw]
        centroid = centroids[t]

        scored = []
        for idx in idxs:
            sent = processed_docs[idx]

            # NOTE: We do NOT skip noise sentences here if we want to DETECT junk topics.
            # But for "clean" representatives, we usually want to skip.
            # Compromise: We calculate score, but penalize heavily in semantic_density.

            sim = float(cosine_similarity([embeddings[idx]], centroid)[0][0])
            k = keyword_score(sent, keywords)
            info = info_score(sent)
            sem = semantic_density(sent)  # This will be low for spam/memes

            score = (0.45 * sim + 0.22 * k + 0.18 * sem + 0.15 * info)
            scored.append((score, idx))

        scored.sort(key=lambda x: x[0], reverse=True)

        out[t] = [
            (strip_company_tag(processed_docs[idx]), idx, score)
            for score, idx in scored[:top_k]
        ]

    return out


# ============================================================
#  TOPIC FLAGS
# ============================================================

def is_meme_topic(keywords, reps) -> bool:
    """ Flags topics where a high percentage of representative text is meme-based. """
    meme_rep = np.mean([meme_score(txt) for (txt, _, _) in reps]) if reps else 0.0
    return meme_rep > 0.15  # Lowered threshold for strictness


def is_junk_topic(reps, keywords) -> bool:
    """
    Flags topics that are either:
    1. Spam (high spam_score)
    2. Low info (low semantic density + low keyword diversity)
    """
    if not reps or not keywords:
        return True

    spam_rep = np.mean([spam_score(txt) for (txt, _, _) in reps])
    if spam_rep > 0.15:  # Strict spam threshold
        return True

    sem = np.mean([semantic_density(txt) for (txt, _, _) in reps])
    kws = [w for (w, _) in keywords[:7]]

    # Low semantic density AND very low unique keyword count
    return (sem < 0.10 and len(set(kws)) <= 2)


def is_structural_topic(reps) -> bool:
    if not reps:
        return True
    struct = np.mean([structural_ratio(txt) for (txt, _, _) in reps])
    return struct > 0.55


def is_shell_topic(reps, keywords) -> bool:
    if not keywords or not reps:
        return True
    sem = np.mean([semantic_density(txt) for (txt, _, _) in reps])
    return sem < 0.08


# ============================================================
#  MAIN POSTPROCESSOR
# ============================================================

def postprocess_topics(model, processed_docs, topics, embeddings) -> Dict:
    topic_ids = sorted({int(t) for t in topics if int(t) != -1})

    reps = extract_representatives(
        processed_docs, embeddings, topics, model, top_k=5
    )

    out = {}
    for t in topic_ids:
        keywords = model.get_topic(t) or []
        r = reps.get(t, [])

        meme_r = float(np.mean([meme_score(txt) for (txt, _, _) in r])) if r else 0.0
        spam_r = float(np.mean([spam_score(txt) for (txt, _, _) in r])) if r else 0.0
        struct_r = float(np.mean([structural_ratio(txt) for (txt, _, _) in r])) if r else 0.0
        sem_d = float(np.mean([semantic_density(txt) for (txt, _, _) in r])) if r else 0.0

        # Pass spam check into junk flag
        is_junk = is_junk_topic(r, keywords)
        if spam_r > 0.15:
            is_junk = True

        out[t] = {
            "keywords": keywords,
            "representatives": r,
            "is_meme": is_meme_topic(keywords, r),
            "is_structural": is_structural_topic(r),
            "is_shell": is_shell_topic(r, keywords),
            "is_junk": is_junk,
            "meme_score": round(meme_r, 4),
            "spam_score": round(spam_r, 4),  # Added to output stats
            "structural_score": round(struct_r, 4),
            "semantic_density": round(sem_d, 4),
        }

    return out