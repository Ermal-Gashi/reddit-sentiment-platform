# gold_stage1_comments.py — VADER+FINLEX v6 with sarcasm, scaling, emoji boosts, emotions
import os, sys, re, json, signal, psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv
from datetime import datetime, timezone
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from load_finlex import load_finlex


# -------------------
# 1. Setup
# -------------------
load_dotenv()
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD")

def get_conn():
    return psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB,
        user=PGUSER, password=PGPASSWORD
    )


# -------------------
# 2. Analyzer (+ FINLEX lexicon expansions)
# -------------------
analyzer = SentimentIntensityAnalyzer()


FINLEX_PATH = os.path.join(os.path.dirname(__file__), "finlex.json")
finlex = load_finlex(FINLEX_PATH)


analyzer.lexicon.update(finlex)


# -------------------
# 3. Fetch new comments (Unified Silver)
# -------------------
def fetch_new_comments(limit=500):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:

            cur.execute("SELECT COUNT(*) FROM silver.comments;")
            silver_total = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM gold.comment_features;")
            gold_total = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM silver.comments s
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.comment_features g 
                    WHERE g.comment_id = s.comment_id
                );
            """)
            missing_total = cur.fetchone()[0]

            print(f"[DBG] Silver={silver_total} | Gold={gold_total} | To-process={missing_total}")

            cur.execute("""
                SELECT s.comment_id, s.body_clean, s.body_clean_emoji,
                       s.body_original, s.source_table
                FROM silver.comments s
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.comment_features g 
                    WHERE g.comment_id = s.comment_id
                )
                ORDER BY s.created_ts
                LIMIT %s;
            """, (limit,))

            rows = cur.fetchall()
            print(f"Fetched {len(rows)} comments for enrichment.")
            return rows


# -------------------
# 4. Regex + keyword sets
# -------------------
QUESTION_RE = re.compile(r"\?")
EXCLAIM_RE  = re.compile(r"!")
ALLCAPS_RE  = re.compile(r"\b[A-Z]{2,}\b")

UNCERTAINTY_WORDS = {
    "maybe","might","could","possibly","uncertain","unsure","guess",
    "idk","not sure","kinda","seems like","appears","perhaps"
}

MACRO_TERMS = {
    "fed","federal reserve","inflation","rates","interest","rate hike","rate cut",
    "cpi","gdp","unemployment","jobs report","treasury","yield curve","pivot",
    "qt","qe","core cpi","pce"
}

TRADE_TERMS = {
    "buy","sell","puts","calls","put","call","short","long","leverage",
    "margin","bagholder","call flow","buy the dip","btd","selloff",
    "sell-off","scalp","swing"
}

SARC_MARKERS = {
    "yeah right","sure buddy","as if","totally","great job","nice work",
    "what a genius","what a great idea"
}

LAUGH_PATTERNS = re.compile(r"\b(lol|lmao|lmfao|rofl|haha+|hehe+|kek)\b", re.I)
SARC_EMOJIS = {"🙄","😒","😑","🤡"}

NEG_WORDS = {
    "crash","down","lost","bagholder","bagholding","bankruptcy","dump",
    "scam","fraud","overvalued","selloff","sell-off","panic","rugpull","rug-pull"
}


def detect_sarcasm(text_with_caps_punct_emoji: str) -> dict:
    t = text_with_caps_punct_emoji
    tl = t.lower()
    score = 0

    if any(p in tl for p in SARC_MARKERS):
        score += 2
    if LAUGH_PATTERNS.search(tl):
        score += 1
    if any(e in t for e in SARC_EMOJIS):
        score += 2
    if "😂" in t and any(w in tl for w in NEG_WORDS):
        score += 2
    if "😭" in t and any(w in tl for w in NEG_WORDS):
        score += 1
    if "great job" in tl and any(w in tl for w in NEG_WORDS):
        score += 1
    if EXCLAIM_RE.search(t) and len(ALLCAPS_RE.findall(t)) >= 2 and any(w in tl for w in NEG_WORDS):
        score += 1

    return {"sarcasm_score": score, "sarcasm_flag": score >= 2}


# -------------------
# 5. Enrichment
# -------------------
def enrich_batch(rows):
    enriched = []

    for r in rows:
        cid = r["comment_id"]

        # Prefer emoji/caps/punct-preserved text; fallback to body_clean
        text = (r.get("body_clean_emoji") or "").strip()
        if not text:
            text = (r.get("body_clean") or "").strip()

        text_orig = r.get("body_original") or ""
        src_table = r.get("source_table", "unknown")

        # ----- VADER -----
        scores = analyzer.polarity_scores(text)
        vader_compound = scores["compound"]
        intensity = max(scores["pos"], scores["neg"])

        # ----- Heuristics -----
        tl = text.lower()
        uncertainty_flag = any(w in tl for w in UNCERTAINTY_WORDS)
        macro_hits = sum(bool(re.search(rf"\b{re.escape(w)}\b", tl)) for w in MACRO_TERMS)
        trade_hits = sum(bool(re.search(rf"\b{re.escape(w)}\b", tl)) for w in TRADE_TERMS)

        # Original punctuation / CAPS
        has_q = bool(QUESTION_RE.search(text_orig))
        has_exc = bool(EXCLAIM_RE.search(text_orig))
        allcaps = len(ALLCAPS_RE.findall(text_orig))

        # ----- Sarcasm detection -----
        sarc = detect_sarcasm(text_orig if text_orig else text)

        import math

        # ---- 1. Sarcasm-adjusted base ----
        adjusted = vader_compound
        if sarc["sarcasm_flag"]:
            if vader_compound > 0:
                adjusted = -abs(vader_compound) * 0.5
            elif vader_compound < 0:
                adjusted = vader_compound * 1.2

        # ---- 2. Power scaling ----
        if adjusted != 0:
            scaled = math.copysign(abs(adjusted) ** 0.75, adjusted)
        else:
            scaled = 0.0

        # ---- 3. Emoji boosts ----
        emoji_boost = 0.0
        if "🚀" in text: emoji_boost += 0.05
        if any(e in text for e in ["😂","🤣"]): emoji_boost += 0.03
        if "😉" in text: emoji_boost += 0.01
        if "😭" in text: emoji_boost -= 0.04
        if "🤡" in text: emoji_boost -= 0.05
        if any(e in text for e in ["🙄","😒","😑"]): emoji_boost -= 0.02

        # ---- 4. Final score ----
        final_score = max(-1.0, min(1.0, scaled + emoji_boost))

        # ---- 5. Emotion classification ----
        def classify_emotion(score, sarcasm_flag, allcaps, has_exc, uncertainty_flag, tl):
            if sarcasm_flag and score < 0:
                return "sarcastic_negative"
            if score < -0.4 and (allcaps >= 2 or has_exc):
                return "angry"
            NEG_FEAR = {
                "crash","collapse","bankruptcy","recession",
                "crashing","tanking","panic","scam","fraud"
            }
            if any(w in tl for w in NEG_FEAR) and score < -0.2:
                return "fearful"
            if score < -0.3:
                return "negative"
            if uncertainty_flag and -0.2 < score < 0.2:
                return "uncertain"
            POS_HYPE = {"moon","pump","rip","breakout","run","rocket"}
            if score > 0.5 or any(w in tl for w in POS_HYPE):
                return "bullish"
            if score > 0.3 and has_exc:
                return "excited"
            if score > 0.2:
                return "positive"
            if -0.2 <= score <= 0.2:
                return "neutral"
            return "neutral"

        emotion_label = classify_emotion(
            final_score,
            sarc["sarcasm_flag"],
            allcaps,
            has_exc,
            uncertainty_flag,
            tl
        )

        # --------------------------------------------------
        # 6. EMOTION STRENGTH (NEW)
        # --------------------------------------------------
        # Definition:
        # - base strength = abs(final_score)
        # - plus a tiny boost from intensity (not too big)
        base_strength = abs(final_score)
        emotion_strength = min(1.0, base_strength + (intensity * 0.1))

        # ----- sentiment_raw for debugging -----
        sentiment_raw = {
            "vader": scores,
            "vader_compound": vader_compound,
            "pre_scaled_adjusted": adjusted,
            "scaled_after_power": scaled,
            "emoji_boost": emoji_boost,
            "final_score": final_score,
            "emotion_label": emotion_label,
            "emotion_strength": emotion_strength,
            "intensity": intensity,
            "uncertainty": uncertainty_flag,
            "macro_terms": macro_hits,
            "trade_terms": trade_hits,
            "sarcasm_score": sarc["sarcasm_score"],
            "sarcasm_flag": sarc["sarcasm_flag"],
            "signals": {
                "has_question": has_q,
                "has_exclamation": has_exc,
                "all_caps_count": allcaps
            }
        }

        enriched.append({
            "comment_id": cid,
            "sentiment_score": final_score,
            "sentiment_raw": json.dumps(sentiment_raw),
            "has_question": has_q,
            "has_exclamation": has_exc,
            "all_caps_count": allcaps,
            "emotion_label": emotion_label,
            "emotion_strength": emotion_strength,   # <-- NEW COLUMN
            "method": "VADER+FINLEX(v5)+sarcasm+scaling+emoji+emotion",
            "version": "5.2",
            "processed_at": datetime.now(timezone.utc),
            "source_table": src_table,
        })

    return enriched



# -------------------
# 6. Insert into Gold (supports emotion_label + emotion_strength)
# -------------------
def insert_features(conn, records):
    if not records:
        return 0

    # We assume records already contain:
    #   sentiment_score
    #   sentiment_raw
    #   has_question
    #   has_exclamation
    #   all_caps_count
    #   emotion_label        <-- new
    #   emotion_strength     <-- new
    #   method
    #   version
    #   processed_at
    #   source_table

    cols = list(records[0].keys())
    values = [[r[c] for c in cols] for r in records]

    # build update clause for all fields except PK
    update_clause = ", ".join([
        f"{col} = EXCLUDED.{col}"
        for col in cols
        if col != "comment_id"
    ])

    sql = f"""
        INSERT INTO gold.comment_features ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (comment_id) DO UPDATE
        SET {update_clause};
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=250)

    return len(records)



# -------------------
# 7. Runner
# -------------------
stop_requested = False

def handle_interrupt(sig, frame):
    global stop_requested
    stop_requested = True
    print("\n Stop signal received — finishing current batch gracefully...")

signal.signal(signal.SIGINT, handle_interrupt)
signal.signal(signal.SIGTERM, handle_interrupt)

if __name__ == "__main__":
    print(" Starting Gold Stage 1 enrichment (VADER+FINLEX v5 + emotions + intensity)...")
    total = 0
    batch = 500

    try:
        while not stop_requested:
            rows = fetch_new_comments(limit=batch)
            if not rows:
                break

            feats = enrich_batch(rows)

            with get_conn() as conn:
                n = insert_features(conn, feats)
                conn.commit()

            total += n
            print(f"Inserted batch: {n} comment features")

            if stop_requested:
                print(" Graceful stop — committed everything.")
                break

    except KeyboardInterrupt:
        print("\n Interrupted manually, committing remaining...")

    finally:
        print(f" Done. Total enriched: {total} comments")
        sys.exit(0)
