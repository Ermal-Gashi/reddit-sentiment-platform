import re
import json
from datetime import datetime
from collections import defaultdict



DEBUG_MODE = False
DROP_STATS = defaultdict(lambda: {"n": 0, "samples": []})


def _bump(reason, sample=None, max_samples=3):
    DROP_STATS[reason]["n"] += 1
    if sample and len(DROP_STATS[reason]["samples"]) < max_samples:
        DROP_STATS[reason]["samples"].append(str(sample)[:80])


# ============================================================
# Regex & cleaning helpers
# ============================================================

URL_RE = re.compile(r"https?://\S+", re.I)

REMOVED_RE = re.compile(
    r"^\s*\[?\s*(removed by reddit|removed|deleted)\s*\]?\s*$",
    re.I
)


def clean_text(text: str) -> str:
    if not text:
        return ""

    s = str(text)

    s = re.sub(
        r"\[([^\]]*)\]\(\s*https?://[^\s)]+\s*\)",
        lambda m: (m.group(1) or "link").strip(),
        s,
        flags=re.I,
    )

    s = re.sub(r"https?://\S+", " link ", s, flags=re.I)
    s = re.sub(r"&(?:amp|lt|gt|quot|nbsp|#x?[0-9a-f]+);", " ", s, flags=re.I)

    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002700-\U000027BF"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    s = emoji_pattern.sub(" ", s)

    s = re.sub(r"[*_`>#~^|\\]", " ", s)
    s = re.sub(r"[\[\]\(\){}<>]+", " ", s)
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()

    return s if re.search(r"[a-z0-9]", s) else ""


def clean_text_with_emojis(text: str) -> str:
    if not text:
        return ""

    s = str(text)

    s = re.sub(
        r"\[([^\]]*)\]\(\s*https?://[^\s)]+\s*\)",
        lambda m: (m.group(1) or "link").strip(),
        s,
        flags=re.I,
    )
    s = re.sub(r"https?://\S+", " link ", s, flags=re.I)
    s = re.sub(r"&(?:amp|lt|gt|quot|nbsp|#x?[0-9a-f]+);", " ", s, flags=re.I)
    s = re.sub(r"[*_`>#~^|\\]", " ", s)
    s = re.sub(r"[\[\]\(\){}<>]+", " ", s)

    s = re.sub(
        r"[^A-Za-z0-9\s\.\,\!\?\:\;\'\"\-\–\—\…"
        r"\u2600-\u26FF\u2700-\u27BF"
        r"\U0001F300-\U0001FAFF]",
        " ",
        s,
        flags=re.UNICODE,
    )

    s = re.sub(r"\s+", " ", s).strip()
    return s if re.search(r"[A-Za-z0-9\U0001F300-\U0001FAFF]", s) else ""


def clean_text_for_bert(text: str) -> str:
    if not text:
        return ""

    s = str(text)

    s = re.sub(
        r"\[([^\]]*)\]\(\s*https?://[^\s)]+\s*\)",
        lambda m: (m.group(1) or "").strip(),
        s,
        flags=re.I,
    )
    s = re.sub(r"https?://\S+", " ", s, flags=re.I)
    s = re.sub(r"&(?:amp|lt|gt|quot|nbsp|#x?[0-9a-f]+);", " ", s, flags=re.I)

    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F700-\U0001F77F"
        "\U0001F780-\U0001F7FF"
        "\U0001F800-\U0001F8FF"
        "\U0001F900-\U0001F9FF"
        "\U0001FA00-\U0001FA6F"
        "\U0001FA70-\U0001FAFF"
        "\U00002700-\U000027BF"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE,
    )
    s = emoji_pattern.sub(" ", s)

    s = re.sub(r"[*_`>#~^|\\]", " ", s)
    s = re.sub(r"[\[\]{}<>]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s if re.search(r"[A-Za-z0-9]", s) else ""


# ============================================================
# Safe parsers
# ============================================================

def safe_int(val):
    try:
        return int(val)
    except Exception:
        return None


def safe_bool(val):
    if val in ("True", "true", True, 1, "1"):
        return True
    if val in ("False", "false", False, 0, "0"):
        return False
    return None


def parse_ts(val):
    if not val:
        return None
    try:
        if str(val).isdigit():
            return datetime.utcfromtimestamp(int(val))
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except Exception:
        return None


# ============================================================
# Core transformation
# ============================================================

def transform_comment(bronze_row):
    try:
        raw_val = bronze_row["raw_json"]
        raw = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
    except Exception:
        _bump("json_parse_error")
        return None, [], False

    cid = bronze_row["comment_id"]
    author = (raw.get("author") or "").lower().strip()
    body_original = (raw.get("body_raw") or raw.get("body") or "").strip()
    lo = body_original.lower().strip()

    if not body_original:
        _bump("empty_body")
        return None, [], False

    if author in ("automoderator", "auto-moderator"):
        _bump("automoderator")
        return None, [], False

    if lo in ("[deleted]", "[removed]"):
        _bump("deleted_removed")
        return None, [], False

    if REMOVED_RE.match(lo):
        _bump("system_removed")
        return None, [], False

    body_clean = clean_text(body_original)
    body_clean_emoji = clean_text_with_emojis(body_original)
    body_clean_bert = clean_text_for_bert(body_original)

    if not body_clean or body_clean in {"link", "link link"}:
        _bump("link_only_comment")
        return None, [], False

    matched_companies = raw.get("matched_companies") or ""
    companies = [c for c in matched_companies.split(";") if c]

    comment_record = {
        "comment_id": cid,
        "submission_id": raw.get("submission_id"),
        "parent_id": raw.get("parent_id"),
        "parent_comment_id": raw.get("parent_comment_id"),
        "root_comment_id": raw.get("root_comment_id"),
        "depth": safe_int(raw.get("depth")),
        "is_top_level": safe_bool(raw.get("is_top_level")),
        "is_op": safe_bool(raw.get("is_submitter")),
        "author": raw.get("author"),
        "subreddit": raw.get("subreddit"),
        "created_ts": parse_ts(raw.get("created_iso")),
        "permalink": raw.get("permalink"),
        "body_original": body_original,
        "body_clean": body_clean,
        "body_clean_emoji": body_clean_emoji,
        "body_clean_bert": body_clean_bert,
        "lang": raw.get("lang"),
        "n_chars": len(body_clean),
        "n_tokens": len(body_clean.split()),
        "has_url": bool(URL_RE.search(body_original)),
        "score": safe_int(raw.get("score")),
        "ups": safe_int(raw.get("ups")),
        "downs": safe_int(raw.get("downs")),
        "matched_companies": ";".join(companies),
        "matched_source": raw.get("matched_source"),
        "match_confidence": raw.get("match_confidence"),
        "matched_where": raw.get("matched_where"),
        "company_set_size": safe_int(raw.get("company_set_size")),
        "is_reply_to_company_mention": safe_bool(raw.get("is_reply_to_company_mention")),
    }

    company_records = [
        {
            "comment_id": cid,
            "company": comp,
            "source": raw.get("matched_source"),
            "confidence": raw.get("match_confidence"),
            "created_ts": parse_ts(raw.get("created_iso")),
            "subreddit": raw.get("subreddit"),
            "submission_id": raw.get("submission_id"),
            "depth": safe_int(raw.get("depth")),
            "is_top_level": safe_bool(raw.get("is_top_level")),
        }
        for comp in companies
    ]

    return comment_record, company_records, bool(companies)
