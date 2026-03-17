from fastapi import APIRouter, Query
from typing import Optional

# Import controllers
from backend.services.query_service import (
    get_overview_summary,
    get_marketview,
    get_marketview_kpis,
    get_market_correlations,
    get_marketview_landscape,
    get_marketview_candles

)

from .json_safe import SafeJSONResponse

router = APIRouter(prefix="/api", tags=["api"])



@router.get("/health")
def health():
    return {"status": "ok"}



@router.get("/overview")
def overview(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    subreddit: Optional[str] = Query(None, description="Subreddit filter"),
    marketOnly: Optional[int] = Query(0, description="1 = only market comments"),
):

    market_only = bool(marketOnly)

    data = get_overview_summary(
        start=start,
        end=end,
        subreddit=subreddit,
        market_only=market_only,
    )

    return SafeJSONResponse(content=data)

@router.get("/marketview")
def marketview(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),

    # UPDATED — now includes corr7, corr30
    sort: Optional[str] = Query(
        "price",
        description="Color mode: price | sentiment | mentions | corr7 | corr30",
    ),

    # 🔥 NEW OPTIONAL FILTERS
    company: Optional[str] = Query(None, description="Optional company filter"),
    sector: Optional[str] = Query(None, description="Optional sector filter"),
):


    data = get_marketview(
        start=start,
        end=end,
        sort=sort,

        # 🔥 Forward filters to Query Service
        company=company,
        sector=sector,
    )

    return SafeJSONResponse(content=data)



@router.get("/marketview/kpis")
def marketview_kpis(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),

    company: Optional[str] = Query(None, description="Optional company filter"),
    sector: Optional[str] = Query(None, description="Optional sector filter"),
):

    data = get_marketview_kpis(
        start=start,
        end=end,
        company=company,
        sector=sector,
    )

    return SafeJSONResponse(content=data)

@router.get("/marketview/metadata")
def marketview_metadata():
    from backend.services.query_service import get_marketview_metadata
    data = get_marketview_metadata()
    return SafeJSONResponse(content=data)


@router.get("/market/correlations")
def route_market_correlations(
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
):
    return get_market_correlations(start=start, end=end)


@router.get("/market/sentimentmodal/{ticker}")
def market_sentiment_modal(
    ticker: str,
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
):

    from backend.services.query_service import get_sentiment_modal

    data = get_sentiment_modal(
        company=ticker,
        start=start,
        end=end,
    )

    return SafeJSONResponse(content=data)


@router.get("/market/volumemodal/{ticker}")
def volume_modal(
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
):
    from backend.services.query_service import get_volume_modal
    data = get_volume_modal(ticker, start, end)
    return SafeJSONResponse(content=data)


@router.get("/market/landscape")
def market_landscape(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),

    # Only supports price and sentiment for Treemap color
    sort: Optional[str] = Query(
        "price",
        description="Color mode: price | sentiment",
    ),

    # Filters
    company: Optional[str] = Query(None, description="Optional company filter"),
    sector: Optional[str] = Query(None, description="Optional sector filter"),
):
    """
    Returns data for the Market Landscape Treemap and comparison leaderboards.
    Data is merged (Heatmap stats + Correlation scores).
    """

    data = get_marketview_landscape(
        start=start,
        end=end,
        sort=sort,
        company=company,
        sector=sector,
    )

    return SafeJSONResponse(content=data)

@router.get("/marketview/candles/{ticker}")
def market_candles(
        ticker: str,
        start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
        end: Optional[str] = Query(None, description="End date YYYY-MM-DD")
):
    """
    Returns daily time-series data for the Hero Chart.
    """
    # Call the controller
    data = get_marketview_candles(
        ticker=ticker,
        start=start,
        end=end
    )

    return SafeJSONResponse(content=data)



@router.get("/topics/daily")
def topics_daily(
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(15, description="Max number of topics to return"),
    focus_date: Optional[str] = Query(None, description="Optional focus date YYYY-MM-DD (defaults to max date in range)"),
):
    """
    Returns the Daily Top Topics list for TopicGrid (one focus day).
    """
    from backend.services.query_service import get_topics_daily

    data = get_topics_daily(
        start=start,
        end=end,
        limit=limit,
        focus_date=focus_date
    )

    return SafeJSONResponse(content=data)



@router.get("/topics/{topic_id}/summary")
def topic_summary(
    topic_id: int,
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    focus_date: Optional[str] = Query(None, description="Optional focus date YYYY-MM-DD (defaults to max date in range)"),
):
    """
    Returns one selected topic row for TopicSummary (same focus day as TopicGrid).
    """
    from backend.services.query_service import get_topic_summary

    data = get_topic_summary(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date
    )

    return SafeJSONResponse(content=data)


@router.get("/topics/{topic_id}/evolution")
def topic_evolution(
    topic_id: int,
    start: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    focus_date: Optional[str] = Query(None, description="Optional focus date YYYY-MM-DD"),
):
    from backend.services.query_service import get_topic_evolution

    data = get_topic_evolution(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date
    )

    return SafeJSONResponse(content=data)



@router.get(
    "/topics/{topic_id}/representatives",
    summary="Topic Representative Sentences",
    description="Returns representative sentences for the selected topic on the focus day.",
)
def topic_representatives(
        topic_id: int,
        start: Optional[str] = Query(
            None, description="Start date YYYY-MM-DD"
        ),
        end: Optional[str] = Query(
            None, description="End date YYYY-MM-DD"
        ),
        focus_date: Optional[str] = Query(
            None, description="Optional focus date YYYY-MM-DD (defaults to max date in range)"
        ),
        limit: int = Query(
            3, ge=1, le=10, description="Number of representative sentences"
        ),
):
    """
    API endpoint for Topic Representative Sentences.
    """

    from backend.services.query_service import get_topic_representatives

    data = get_topic_representatives(
        topic_id=topic_id,
        start=start,
        end=end,
        focus_date=focus_date,
        limit=limit,
    )

    return SafeJSONResponse(content=data)
