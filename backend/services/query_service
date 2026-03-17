# services/query_service.py

from typing import List, Dict, Any, Optional
from decimal import Decimal
import traceback


from backend.services.query.overview import fetch_overview_summary
from backend.services.query.marketview import fetch_marketview_summary
from backend.services.query.marketview import fetch_marketview_kpis
from backend.services.query.marketview import fetch_marketview_metadata
from backend.services.query.marketview import fetch_market_correlations_summary
from backend.services.query.marketview import fetch_sentiment_distribution_summary
from backend.services.query.marketview import fetch_volume_summary
from backend.services.query.marketview import fetch_marketview_landscape_summary

from backend.services.query.topicview import (
    fetch_topicgrid_summary,
    fetch_topic_summary,
    fetch_topic_evolution_series,
    # fetch_topic_heatmap,
    # fetch_topic_sankey,
    fetch_topic_representatives
)

def to_native(obj: Any) -> Any:
    """
    Recursively convert Decimals and other non-JSON-serializable
    types into pure Python primitives (float, int, str, list, dict).

    This should be used on any payload that is returned from
    service-layer functions before sending it as a FastAPI response.
    """
    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [to_native(i) for i in obj]

    # Fallback: return as-is if it is already JSON-friendly
    return obj


def get_overview_summary(
    start: Optional[str] = None,
    end: Optional[str] = None,
    subreddit: Optional[str] = None,
    market_only: bool = False,
) -> Dict[str, Any]:
    """
    High-level controller for the Overview dashboard.
    """
    raw_summary = fetch_overview_summary()
    return to_native(raw_summary)


def get_marketview(
        start: Optional[str] = None,
        end: Optional[str] = None,
        sort: str = "price",

        # filters
        company: Optional[str] = None,
        sector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Unified Market View endpoint.
    Supports:
      - date range
      - sort:
           price / sentiment / mentions / corr7 / corr30
      - company filter
      - sector filter
    """

    try:
        # -----------------------------------------------------
        # NORMALIZE AND VALIDATE SORT MODE
        # -----------------------------------------------------
        if sort is None:
            sort = "price"

        sort = sort.lower()

        allowed_modes = {
            "price",
            "sentiment",
            "mentions",
            "corr7",
            "corr30",
        }

        if sort not in allowed_modes:
            print(f"⚠️ WARNING: Invalid sort='{sort}', defaulting to 'price'")
            sort = "price"

        # -----------------------------------------------------
        # DELEGATE TO SUMMARY FUNCTION (now supports corr7/30)
        # -----------------------------------------------------
        raw = fetch_marketview_summary(
            start=start,
            end=end,
            sort_mode=sort,
            limit=None,     # always full list

            company=company,
            sector=sector,
        )

        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_marketview():", e)
        import traceback; traceback.print_exc()

        return {
            "start_date": None,
            "end_date": None,
            "heatmap": [],
            "errors": [str(e)],
        }




def get_marketview_kpis(
        start: Optional[str] = None,
        end: Optional[str] = None,

        # OPTIONAL FILTERS (same as heatmap)
        company: Optional[str] = None,
        sector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Controller for MarketView KPIs.
    Computes:
      - most mentioned ticker
      - most mentioned sector
      - highest sentiment (min_mentions=20)
      - lowest sentiment (min_mentions=20)
      - top gainer
      - top loser
    """

    try:
        raw = fetch_marketview_kpis(
            start=start,
            end=end,
            company=company,
            sector=sector,
        )
        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_marketview_kpis():", e)
        import traceback; traceback.print_exc()

        return {
            "start_date": None,
            "end_date": None,
            "kpis": {},
            "errors": [str(e)],
        }

def get_marketview_metadata() -> Dict[str, Any]:
        try:
            raw = fetch_marketview_metadata()
            return to_native(raw)
        except Exception as e:
            print("❌ ERROR in get_marketview_metadata():", e)
            return {"companies": [], "sectors": [], "errors": [str(e)]}



def get_market_correlations(
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        return fetch_market_correlations_summary(start=start, end=end)

    except Exception as e:
        print("❌ ERROR in get_market_correlations():", e)
        traceback.print_exc()
        return {
            "start_date": None,
            "end_date": None,
            "correlations": [],
            "errors": [str(e)],
        }


def get_sentiment_modal(
        company: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Controller for Sentiment Modal (Emotion Distribution).
    """

    try:
        raw = fetch_sentiment_distribution_summary(
            company=company,
            start=start,
            end=end,
        )
        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_sentiment_modal():", e)
        import traceback; traceback.print_exc()

        return {
            "company": company,
            "start_date": None,
            "end_date": None,
            "emotion_daily": [],
            "errors": [str(e)],
        }


def get_volume_modal(
        company: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        raw = fetch_volume_summary(
            company=company,
            start=start,
            end=end,
        )
        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_volume_modal():", e)
        import traceback;
        traceback.print_exc()

        return {
            "company": company,
            "start_date": None,
            "end_date": None,
            "volume_daily": [],
            "errors": [str(e)],
        }



def get_marketview_landscape(
    start: Optional[str] = None,
    end: Optional[str] = None,
    sort: str = "price",
    company: Optional[str] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Controller for the Market Landscape Treemap and comparison leaderboards.
    Calls the landscape summary, which handles merging heatmap and correlation data.
    """

    try:
        # -----------------------------------------------------
        # NORMALIZE AND VALIDATE SORT MODE
        # -----------------------------------------------------
        if sort is None:
            sort = "price"

        sort = sort.lower()

        # We only support price and sentiment for the Treemap color
        allowed_modes = {
            "price",
            "sentiment",
        }

        if sort not in allowed_modes:
            print(f"⚠️ WARNING: Invalid sort='{sort}', defaulting to 'price' for landscape.")
            sort = "price"

        # -----------------------------------------------------
        # DELEGATE TO THE NEW LANDSCAPE SUMMARY FUNCTION
        # -----------------------------------------------------
        # Note: This assumes fetch_marketview_landscape_summary is accessible/imported
        # (you'll need to update your imports in query_service.py)
        raw = fetch_marketview_landscape_summary(
            start=start,
            end=end,
            sort_mode=sort,
            limit=None,     # Always full market for the landscape view
            company=company,
            sector=sector,
        )

        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_marketview_landscape():", e)
        import traceback; traceback.print_exc()

        # Returning clean error payload
        return {
            "start_date": None,
            "end_date": None,
            "landscape": [],
            "errors": [str(e)],
        }


# backend/services/query_service.py

# Import the wrapper (Function #2 from above), NOT the core SQL
from .query.marketview import fetch_marketview_candles_summary


def get_marketview_candles(
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Controller for the Hero Chart.
    Delegates to the summary function in marketview.py.
    """
    try:
        if not ticker:
            return {"error": "Ticker is required"}

        # Delegate to the new summary function
        raw = fetch_marketview_candles_summary(
            ticker=ticker,
            start=start,
            end=end
        )

        return to_native(raw)

    except Exception as e:
        print("❌ ERROR in get_marketview_candles():", e)
        return {
            "ticker": ticker,
            "candles": [],
            "errors": [str(e)],
        }




def get_topics_daily(
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 15,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Daily Top Topics (single focus day).
    Keyword expansion is handled inside fetch_topicgrid_summary.
    """
    raw = fetch_topicgrid_summary(
        start=start,
        end=end,
        limit=limit,
        focus_date=focus_date
    )
    return to_native(raw)


def get_topic_summary(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Topic summary for selected topic on focus day.
    Keyword expansion is handled inside fetch_topic_summary.
    """
    raw = fetch_topic_summary(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date
    )
    return to_native(raw)



def get_topic_evolution(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
) -> Dict[str, Any]:
    raw = fetch_topic_evolution_series(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date
    )
    return to_native(raw)





def get_topic_representatives(
    topic_id: int,
    start: Optional[str] = None,
    end: Optional[str] = None,
    focus_date: Optional[str] = None,
    limit: int = 3,
) -> Dict[str, Any]:
    """
    Representative sentences for selected topic on focus day.
    """
    raw = fetch_topic_representatives(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date,
        limit=limit,
    )
    return to_native(raw)
