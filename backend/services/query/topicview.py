from typing import Dict, Any, List, Optional
from backend.db import get_conn
from decimal import Decimal
from datetime import date as date_cls


# -------------------------------
# JSON-safe conversion
# -------------------------------
def to_native(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_native(i) for i in obj]
    return obj


# -------------------------------
# Internal helpers
# -------------------------------
def _resolve_focus_date(start: Optional[str], end: Optional[str], focus_date: Optional[str]) -> Optional[str]:
    """
    Resolve a single 'focus day' used for the TopicGrid and TopicSummary.
    """
    if focus_date:
        return focus_date

    with get_conn() as conn, conn.cursor() as cur:
        if start and end:
            cur.execute(
                """
                SELECT MAX(date_utc)
                FROM gold.daily_topics
                WHERE date_utc BETWEEN %s AND %s;
                """,
                (start, end),
            )
        else:
            cur.execute("SELECT MAX(date_utc) FROM gold.daily_topics;")

        d = cur.fetchone()[0]

    if not d:
        return None
    if isinstance(d, date_cls):
        return d.isoformat()
    return str(d)


def _extract_metric(metrics: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    """
    Pull numeric metric from metrics JSONB safely.
    """
    if not metrics or key not in metrics:
        return None
    try:
        v = metrics.get(key)
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


# =========================================================
# 1) TopicGrid payload (Daily Top Topics)
# =========================================================
def fetch_topicgrid_summary(
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 15,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns the top topics for ONE day (focus_date).
    """
    focus = _resolve_focus_date(start, end, focus_date)
    if not focus:
        return to_native({
            "date_range": {"start": start, "end": end},
            "focus_date": None,
            "topics": []
        })

    sql = """
        SELECT
            date_utc,
            topic_id,
            topic_title,
            topic_keywords[1:40] AS topic_keywords,
            doc_count,
            is_junk,
            metrics,
            model_version
        FROM gold.daily_topics
        WHERE date_utc = %s
        ORDER BY doc_count DESC NULLS LAST
        LIMIT %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (focus, limit))
        rows = cur.fetchall()

    topics: List[Dict[str, Any]] = []
    for r in rows:
        metrics = r[6] or {}
        topics.append({
            "date_utc": r[0].isoformat() if r[0] else None,
            "topic_id": int(r[1]),
            "topic_title": r[2],
            "topic_keywords": r[3] or [],
            "doc_count": int(r[4] or 0),
            "is_junk": bool(r[5] or False),
            "metrics": metrics,
            "model_version": r[7],

            # Convenience metrics
            "coherence": _extract_metric(metrics, "coherence"),
            "spam_score": _extract_metric(metrics, "spam_score"),
            "meme_score": _extract_metric(metrics, "meme_score"),
        })

    return to_native({
        "date_range": {"start": start, "end": end},
        "focus_date": focus,
        "topics": topics
    })


# =========================================================
# 2) TopicDetails summary payload
# =========================================================
def fetch_topic_summary(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns one topic row for the selected day.
    """
    focus = _resolve_focus_date(start, end, focus_date)
    if not focus:
        return to_native({
            "topic": None,
            "focus_date": None,
            "date_range": {"start": start, "end": end}
        })

    sql = """
        SELECT
            date_utc,
            topic_id,
            topic_title,
            topic_keywords[1:40] AS topic_keywords,
            doc_count,
            is_junk,
            metrics,
            model_version
        FROM gold.daily_topics
        WHERE date_utc = %s
          AND topic_id = %s
        LIMIT 1;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (focus, int(topic_id)))
        row = cur.fetchone()

    if not row:
        return to_native({
            "topic": None,
            "focus_date": focus,
            "date_range": {"start": start, "end": end}
        })

    metrics = row[6] or {}
    topic = {
        "date_utc": row[0].isoformat() if row[0] else None,
        "topic_id": int(row[1]),
        "topic_title": row[2],
        "topic_keywords": row[3] or [],
        "doc_count": int(row[4] or 0),
        "is_junk": bool(row[5] or False),
        "metrics": metrics,
        "model_version": row[7],

        # Convenience metrics
        "coherence": _extract_metric(metrics, "coherence"),
        "spam_score": _extract_metric(metrics, "spam_score"),
        "meme_score": _extract_metric(metrics, "meme_score"),
    }

    return to_native({
        "date_range": {"start": start, "end": end},
        "focus_date": focus,
        "topic": topic
    })


# =========================================================
# Placeholders (unchanged)
# =========================================================
def fetch_topic_evolution_series(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    return to_native({
        "topic_id": int(topic_id),
        "date_range": {"start": start, "end": end},
        "focus_date": focus_date,
        "series": []
    })


# def fetch_topic_heatmap(
#     topic_id: int,
#     start: Optional[str] = None,
#     end: Optional[str] = None,
#     focus_date: Optional[str] = None,
#     k: int = 12
# ) -> Dict[str, Any]:
#     return to_native({
#         "topic_id": int(topic_id),
#         "date_range": {"start": start, "end": end},
#         "focus_date": focus_date,
#         "k": int(k),
#         "days": [],
#         "keywords": [],
#         "matrix": []
#     })
#
#
# def fetch_topic_sankey(
#     topic_id: int,
#     start: Optional[str] = None,
#     end: Optional[str] = None,
#     focus_date: Optional[str] = None,
#     max_links: int = 30
# ) -> Dict[str, Any]:
#     return to_native({
#         "topic_id": int(topic_id),
#         "date_range": {"start": start, "end": end},
#         "focus_date": focus_date,
#         "nodes": [],
#         "links": [],
#         "max_links": int(max_links)
#     })




def fetch_topic_representatives(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
    limit: int = 3,
) -> Dict[str, Any]:
    """
    Returns representative sentences for a topic on the focus day.
    """

    focus = _resolve_focus_date(start, end, focus_date)
    print("🧠 DEBUG | focus:", focus, type(focus))

    if not focus:
        print("🧠 DEBUG | focus is falsy → returning empty")
        return {
            "topic_id": topic_id,
            "focus_date": None,
            "sentences": []
        }

    sql = """
        SELECT representative_sentences
        FROM gold.daily_topics
        WHERE date_utc = %s
          AND topic_id = %s
        LIMIT 1;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (focus, int(topic_id)))
        row = cur.fetchone()

    print("🧠 DEBUG | raw row:", row)

    if not row or row[0] is None:
        print("🧠 DEBUG | row missing or representative_sentences is NULL")
        return {
            "topic_id": topic_id,
            "focus_date": focus,
            "sentences": []
        }

    reps = row[0]
    print("🧠 DEBUG | reps:", reps)
    print("🧠 DEBUG | reps type:", type(reps))

    # 🔒 Defensive: ensure list
    if not isinstance(reps, list):
        print("🧠 DEBUG | reps is NOT list → returning empty")
        return {
            "topic_id": topic_id,
            "focus_date": focus,
            "sentences": []
        }

    print("🧠 DEBUG | returning", len(reps[:limit]), "sentences")

    return {
        "topic_id": topic_id,
        "focus_date": focus,
        "sentences": reps[:limit]
    }
