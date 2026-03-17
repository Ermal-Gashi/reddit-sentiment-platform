
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, List

from backend.db import get_conn
import pandas as pd
import traceback




def to_native(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_native(i) for i in obj]
    return obj


def _determine_date_range(
    start_str: Optional[str],
    end_str: Optional[str],
) -> Tuple[date, date]:

    start_date: Optional[date] = None
    end_date: Optional[date] = None

    if start_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        except Exception:
            start_date = None

    if end_str:
        try:
            end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
        except Exception:
            end_date = None

    if start_date and not end_date:
        end_date = start_date
    if end_date and not start_date:
        start_date = end_date

    if not start_date or not end_date:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT MAX(date_utc) FROM gold.company_sentiment_vs_price;")
            latest = cur.fetchone()[0]
        if latest is None:
            from datetime import date as _date
            latest = _date.today()
        start_date = latest
        end_date = latest

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    return start_date, end_date

def fetch_market_heatmap(
    start_date: date,
    end_date: date,
    sort_mode: str = "price",
    limit: Optional[int] = None,
    sector: Optional[str] = None,   # frontend-only
    company: Optional[str] = None   # frontend-only
) -> List[Dict[str, Any]]:

    one_day = (start_date == end_date)

    print("\n============================")
    print("🔥 MARKETVIEW QUERY (Hybrid v4 + Filters — FIXED NON-SQL)")
    print("start_date:", start_date, " end_date:", end_date)
    print("one_day:", one_day)
    print("NOTE: company & sector are NOT used in SQL filtering")
    print("============================\n")

    if one_day:
        sql = f"""
            SELECT
                company,
                daily_return AS price_change,
                mean_sentiment AS avg_sentiment,
                n_mentions    AS mentions,
                sector
            FROM gold.company_sentiment_vs_price
            WHERE date_utc = %s
            ORDER BY company ASC
        """

        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (start_date,))
                rows = cur.fetchall()
        except Exception as e:
            print("❌ SQL ERROR (single-day):", e)
            traceback.print_exc()
            return []

        out = []
        for c, p, s, m, sector_val in rows:
            out.append({
                "ticker": c,
                "price_change": float(p) if p is not None else None,
                "avg_sentiment": float(s) if s is not None else None,
                "mentions": int(m or 0),
                "sector": sector_val,
            })

        if limit:
            out = out[:limit]

        return out

    # ======================================================
    # 2) RANGE MODE (Hybrid window logic)
    # ======================================================
    sql = f"""
        SELECT
            company,
            date_utc,
            price_close,
            mean_sentiment,
            n_mentions,
            sector
        FROM gold.company_sentiment_vs_price
        WHERE date_utc BETWEEN %s AND %s
        ORDER BY company ASC, date_utc ASC
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            rows = cur.fetchall()
            df = pd.DataFrame(
                rows,
                columns=[
                    "company",
                    "date_utc",
                    "price_close",
                    "mean_sentiment",
                    "n_mentions",
                    "sector",
                ],
            )
    except Exception as e:
        print("❌ SQL ERROR (range):", e)
        traceback.print_exc()
        return []

    if df.empty:
        return []

    df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.date
    df = df.sort_values(["company", "date_utc"], ascending=True)

    companies = sorted(df["company"].unique())

    out_rows: List[Dict[str, Any]] = []

    for comp in companies:
        g = df[df["company"] == comp].copy()
        if g.empty:
            continue

        g = g.sort_values("date_utc")

        g["price_close"] = g["price_close"].ffill()
        g["mean_sentiment"] = g["mean_sentiment"].ffill()

        first_price = g["price_close"].iloc[0]
        last_price = g["price_close"].iloc[-1]

        if first_price is None or pd.isna(first_price) or first_price == 0:
            price_change = None
        else:
            price_change = (last_price - first_price) / first_price

        mentions_total = g["n_mentions"].fillna(0).sum()
        sentiment_last = g["mean_sentiment"].iloc[-1]
        if pd.isna(sentiment_last):
            sentiment_last = None

        sector_val = g["sector"].iloc[-1]

        out_rows.append({
            "ticker": comp,
            "price_change": float(price_change) if price_change is not None else None,
            "avg_sentiment": float(sentiment_last) if sentiment_last is not None else None,
            "mentions": int(mentions_total),
            "sector": sector_val,
        })

    if limit:
        out_rows = out_rows[:limit]

    return out_rows




#-----------------------------------------fetch_market_corr_snapshot()

def fetch_market_corr_snapshot(end_date: date) -> List[Dict[str, Any]]:
    """
    Snapshot of per-company latest non-null corr7 / corr30 before end_date.
    Each company gets ITS OWN last valid correlation day.
    """

    print("\n============================")
    print("📘 CORR SNAPSHOT FETCH — PER-COMPANY LAST VALID")
    print("end_date:", end_date)
    print("============================\n")

    sql = """
        SELECT
            company,
            date_utc,
            corr_7d,
            corr_30d,
            sector
        FROM gold.company_sentiment_vs_price
        WHERE date_utc <= %s
          AND (corr_7d IS NOT NULL OR corr_30d IS NOT NULL)
        ORDER BY company ASC, date_utc ASC
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (end_date,))
            rows = cur.fetchall()

        df = pd.DataFrame(rows, columns=[
            "company", "date_utc", "corr_7d", "corr_30d", "sector"
        ])

    except Exception as e:
        print("❌ SQL ERROR (corr snapshot):", e)
        traceback.print_exc()
        return []

    if df.empty:
        print("⚠️ No valid corr rows available")
        return []

    df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.date

    # ---------------------------------------------------------
    # 🔥 FIXED:
    # Only consider rows where corr7 or corr30 is NOT NULL.
    # Then take ONLY the LAST valid row per company.
    # ---------------------------------------------------------
    df = df.dropna(subset=["corr_7d", "corr_30d"], how="all")

    last_valid = (
        df.sort_values(["company", "date_utc"])
          .groupby("company")
          .tail(1)
          .reset_index(drop=True)
    )

    out = []
    for _, row in last_valid.iterrows():
        out.append({
            "ticker": row["company"],
            "sector": row["sector"],
            "corr7": float(row["corr_7d"]) if not pd.isna(row["corr_7d"]) else None,
            "corr30": float(row["corr_30d"]) if not pd.isna(row["corr_30d"]) else None,
        })

    print(f"📈 Final correlation snapshot count: {len(out)}")
    return out



def _compute_marketview_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {
            "most_mentioned_ticker": None,
            "most_mentioned_sector": None,
            "highest_sentiment": None,
            "lowest_sentiment": None,
            "top_gainer": None,
            "top_loser": None,
        }

    # Ensure correct types
    df = df.copy()
    df["mentions"] = df["mentions"].fillna(0).astype(int)
    df["avg_sentiment"] = df["avg_sentiment"].astype(float)
    df["price_change"] = df["price_change"].astype(float)

    MIN_MENTIONS = 20

    # ---------------------------------------------
    # 1. MOST MENTIONED TICKER
    # ---------------------------------------------
    most_mentioned = df.sort_values("mentions", ascending=False).iloc[0]

    most_mentioned_ticker = {
        "ticker": most_mentioned["ticker"],
        "mentions": int(most_mentioned["mentions"])
    }

    # ---------------------------------------------
    # 2. MOST MENTIONED SECTOR
    # ---------------------------------------------
    sector_df = df.dropna(subset=["sector"])
    if not sector_df.empty:
        sector_group = sector_df.groupby("sector")["mentions"].sum().sort_values(ascending=False)
        top_sector = sector_group.index[0]
        most_mentioned_sector = {
            "sector": top_sector,
            "mentions": int(sector_group.iloc[0])
        }
    else:
        most_mentioned_sector = None

    # ---------------------------------------------
    # 3 + 4. SENTIMENT LEADERS (min mentions = 20)
    # ---------------------------------------------
    sent_df = df[df["mentions"] >= MIN_MENTIONS]

    if sent_df.empty:
        highest_sentiment = None
        lowest_sentiment = None
    else:
        highest_row = sent_df.sort_values("avg_sentiment", ascending=False).iloc[0]
        lowest_row = sent_df.sort_values("avg_sentiment", ascending=True).iloc[0]

        highest_sentiment = {
            "ticker": highest_row["ticker"],
            "sentiment": float(highest_row["avg_sentiment"]),
            "mentions": int(highest_row["mentions"])
        }

        lowest_sentiment = {
            "ticker": lowest_row["ticker"],
            "sentiment": float(lowest_row["avg_sentiment"]),
            "mentions": int(lowest_row["mentions"])
        }

    # ---------------------------------------------
    # 5. TOP GAINER
    # ---------------------------------------------
    gain_df = df.dropna(subset=["price_change"])
    if gain_df.empty:
        top_gainer = None
        top_loser = None
    else:
        gain_sorted = gain_df.sort_values("price_change", ascending=False)
        loss_sorted = gain_df.sort_values("price_change", ascending=True)

        gr = gain_sorted.iloc[0]
        lr = loss_sorted.iloc[0]

        top_gainer = {
            "ticker": gr["ticker"],
            "change": float(gr["price_change"])
        }

        top_loser = {
            "ticker": lr["ticker"],
            "change": float(lr["price_change"])
        }

    return {
        "most_mentioned_ticker": most_mentioned_ticker,
        "most_mentioned_sector": most_mentioned_sector,
        "highest_sentiment": highest_sentiment,
        "lowest_sentiment": lowest_sentiment,
        "top_gainer": top_gainer,
        "top_loser": top_loser,
    }



def fetch_marketview_summary(
        start: Optional[str] = None,
        end: Optional[str] = None,
        sort_mode: str = "price",
        limit: Optional[int] = None,
        company: Optional[str] = None,
        sector: Optional[str] = None,
) -> Dict[str, Any]:

    summary = {
        "start_date": None,
        "end_date": None,
        "heatmap": [],
        "scatter_data": [],
        "sort_mode": sort_mode,
        "errors": [],
    }

    try:
        # ---------------------------------------
        # DATE RANGE
        # ---------------------------------------
        start_date, end_date = _determine_date_range(start, end)
        summary["start_date"] = start_date.isoformat()
        summary["end_date"] = end_date.isoformat()

        # ---------------------------------------
        # VALID SORT MODES (NOW ONLY 3)
        # ---------------------------------------
        valid_modes = ["price", "sentiment", "mentions"]

        if sort_mode not in valid_modes:
            base_sort = "price"
        else:
            base_sort = sort_mode

        # ---------------------------------------
        # BASE HEATMAP (Hybrid v4)
        # ---------------------------------------
        summary["heatmap"] = fetch_market_heatmap(
            start_date=start_date,
            end_date=end_date,
            sort_mode=base_sort,
            limit=limit,
            company=company,
            sector=sector,
        )

        # ---------------------------------------
        # SCATTER (ALWAYS PRICE)
        # ---------------------------------------
        summary["scatter_data"] = fetch_market_heatmap(
            start_date=start_date,
            end_date=end_date,
            sort_mode="price",
            limit=None,
            company=company,
            sector=sector,
        )

        # Note:
        # - No correlation logic anymore
        # - No corr_data field
        # - No merging of corr values

    except Exception as e:
        print("❌ ERROR in fetch_marketview_summary():", e)
        traceback.print_exc()
        summary["errors"].append(str(e))

    return to_native(summary)


def fetch_marketview_kpis(
    start: Optional[str] = None,
    end: Optional[str] = None,
    company: Optional[str] = None,
    sector: Optional[str] = None,
) -> Dict[str, Any]:

    result = {
        "start_date": None,
        "end_date": None,
        "kpis": {},
        "errors": [],
    }

    try:
        # -------------------------------------------------
        # 1) Determine final date range
        # -------------------------------------------------
        start_date, end_date = _determine_date_range(start, end)
        result["start_date"] = start_date.isoformat()
        result["end_date"] = end_date.isoformat()

        # -------------------------------------------------
        # 2) Load heatmap data FIRST (this generates df)
        #    We reuse the SAME logic as the heatmap endpoint.
        # -------------------------------------------------
        heatmap = fetch_market_heatmap(
            start_date=start_date,
            end_date=end_date,
            sort_mode="price",   # irrelevant here
            limit=None,          # do NOT limit — KPIs require full dataset
            company=company,
            sector=sector,       # ignored in SQL but used in dim logic
        )

        # -------------------------------------------------
        # 3) Convert to DataFrame
        # -------------------------------------------------
        df = pd.DataFrame(heatmap)

        # -------------------------------------------------
        # 4) Compute KPIs
        # -------------------------------------------------
        result["kpis"] = _compute_marketview_kpis(df)

    except Exception as e:
        print("❌ ERROR in fetch_marketview_kpis():", e)
        traceback.print_exc()
        result["errors"].append(str(e))

    return to_native(result)


def fetch_marketview_metadata():
    sql = """
        SELECT DISTINCT company, sector
        FROM gold.company_sentiment_vs_price
        ORDER BY company ASC;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    companies = sorted({row[0] for row in rows})
    sectors = sorted({row[1] for row in rows if row[1]})

    return {
        "companies": companies,
        "sectors": sectors,
        "errors": []
    }



def fetch_market_correlations(end_date: date) -> List[Dict[str, Any]]:
    """
    Clean correlation snapshot:
    - corr7 / corr30 from last available row <= end_date
    - mentions7  = sum of last 7 days n_mentions
    - mentions30 = sum of last 30 days n_mentions
    """

    print("\n============================")
    print("📘 FETCH MARKET CORRELATIONS (Corrected)")
    print("end_date:", end_date)
    print("============================\n")

    # -----------------------------------------
    # 1) Load ALL rows up to end_date
    # -----------------------------------------
    sql = """
        SELECT
            company,
            date_utc,
            corr_7d,
            corr_30d,
            mean_sentiment,
            n_mentions,
            sector
        FROM gold.company_sentiment_vs_price
        WHERE date_utc <= %s
        ORDER BY company ASC, date_utc ASC;
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (end_date,))
            rows = cur.fetchall()

        df = pd.DataFrame(
            rows,
            columns=[
                "company", "date_utc",
                "corr_7d", "corr_30d",
                "mean_sentiment", "n_mentions",
                "sector"
            ]
        )

    except Exception as e:
        print("❌ SQL ERROR (fetch_market_correlations):", e)
        traceback.print_exc()
        return []

    if df.empty:
        return []

    # Convert date
    df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.date

    # -----------------------------------------
    # 2) Determine last-day corr7/corr30 values
    # -----------------------------------------
    last = (
        df.sort_values(["company", "date_utc"])
          .groupby("company")
          .tail(1)
          .reset_index(drop=True)
    )

    # -----------------------------------------
    # 3) Compute rolling 7-day / 30-day mentions
    # -----------------------------------------
    df = df.sort_values(["company", "date_utc"])

    df["mentions7"] = (
        df.groupby("company")["n_mentions"]
          .rolling(window=7, min_periods=1)
          .sum()
          .reset_index(level=0, drop=True)
    )

    df["mentions30"] = (
        df.groupby("company")["n_mentions"]
          .rolling(window=30, min_periods=1)
          .sum()
          .reset_index(level=0, drop=True)
    )

    # Keep last windowed rows
    win_last = (
        df.groupby("company")
          .tail(1)
          .reset_index(drop=True)
    )

    # -----------------------------------------
    # 4) Merge corr & sentiment from last rows
    # -----------------------------------------
    merged = last.merge(
        win_last[["company", "mentions7", "mentions30"]],
        on="company",
        how="left"
    )

    out = []
    for _, row in merged.iterrows():
        out.append({
            "ticker": row["company"],
            "sector": row["sector"],
            "corr7": float(row["corr_7d"]) if pd.notna(row["corr_7d"]) else None,
            "corr30": float(row["corr_30d"]) if pd.notna(row["corr_30d"]) else None,
            "sentiment": float(row["mean_sentiment"]) if pd.notna(row["mean_sentiment"]) else None,
            "mentions7": int(row["mentions7"]) if pd.notna(row["mentions7"]) else 0,
            "mentions30": int(row["mentions30"]) if pd.notna(row["mentions30"]) else 0,
        })

    return out


def fetch_market_correlations_summary(
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:

    out = {
        "start_date": None,
        "end_date": None,
        "correlations": [],
        "errors": [],
    }

    try:
        start_date, end_date = _determine_date_range(start, end)
        out["start_date"] = start_date.isoformat()
        out["end_date"] = end_date.isoformat()

        out["correlations"] = fetch_market_correlations(end_date)

    except Exception as e:
        print("❌ ERROR in fetch_market_correlations_summary:", e)
        traceback.print_exc()
        out["errors"].append(str(e))

    return to_native(out)


def fetch_sentiment_distribution(
        company: str,
        start_date: date,
        end_date: date
) -> List[Dict[str, Any]]:
    """
    Fetch daily emotion_label counts for a given company
    (Gold Stage 1 sentiment + Silver timestamps + Silver company mentions).
    Used for Sentiment Modal (stacked bar chart).
    """

    print("\n============================")
    print("📘 FETCH SENTIMENT DISTRIBUTION")
    print("company:", company)
    print("range:", start_date, "→", end_date)
    print("============================\n")

    sql = """
        SELECT
            g.emotion_label,
            s.created_ts::date AS d,
            COUNT(*) AS n
        FROM gold.comment_features g
        JOIN silver.comments s
            ON g.comment_id = s.comment_id
        JOIN silver.company_mentions m
            ON g.comment_id = m.comment_id
        WHERE m.company = %s
          AND s.created_ts::date >= %s
          AND s.created_ts::date <= %s
        GROUP BY d, g.emotion_label
        ORDER BY d ASC;
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (company, start_date, end_date))
            rows = cur.fetchall()

        df = pd.DataFrame(
            rows,
            columns=["emotion_label", "date", "count"]
        )

    except Exception as e:
        print("❌ SQL ERROR (fetch_sentiment_distribution):", e)
        traceback.print_exc()
        return []

    if df.empty:
        return []

    # Convert date column (datetime → ISO string)
    df["date"] = (
        pd.to_datetime(df["date"])
        .dt.date
        .apply(lambda d: d.isoformat())
    )

    # Pivot: one row per date, columns per emotion
    pivot = (
        df.pivot_table(
            index="date",
            columns="emotion_label",
            values="count",
            aggfunc="sum",
            fill_value=0
        )
        .reset_index()
    )

    # Convert to list-of-dicts for JSON
    out = pivot.to_dict(orient="records")
    return out


def fetch_sentiment_distribution_summary(
        company: str,
        start: Optional[str] = None,
        end: Optional[str] = None
) -> Dict[str, Any]:

    out = {
        "company": company,
        "start_date": None,
        "end_date": None,
        "emotion_daily": [],
        "errors": [],
    }

    try:
        start_date, end_date = _determine_date_range(start, end)
        out["start_date"] = start_date.isoformat()
        out["end_date"] = end_date.isoformat()

        out["emotion_daily"] = fetch_sentiment_distribution(
            company,
            start_date,
            end_date
        )

    except Exception as e:
        print("❌ ERROR in fetch_sentiment_distribution_summary:", e)
        traceback.print_exc()
        out["errors"].append(str(e))

    return to_native(out)



def fetch_comment_volume(
        company: str,
        start_date: date,
        end_date: date,
):
    sql = """
        SELECT 
            DATE(created_ts) AS day,
            COUNT(*) AS volume
        FROM silver.company_mentions
        WHERE company = %s
          AND created_ts >= %s
          AND created_ts <= %s
        GROUP BY DATE(created_ts)
        ORDER BY DATE(created_ts)
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (company.upper(), start_date, end_date))
            rows = cur.fetchall()

        return [
            {"date": r[0].isoformat(), "volume": r[1]}
            for r in rows
        ]

    except Exception as e:
        print("❌ SQL ERROR in fetch_comment_volume:", e)
        traceback.print_exc()
        return []

def fetch_volume_summary(
            company: str,
            start: Optional[str] = None,
            end: Optional[str] = None,
    ):
        out = {
            "company": company,
            "start_date": None,
            "end_date": None,
            "volume_daily": [],
            "errors": [],
        }

        try:
            start_date, end_date = _determine_date_range(start, end)
            out["start_date"] = start_date.isoformat()
            out["end_date"] = end_date.isoformat()

            out["volume_daily"] = fetch_comment_volume(
                company,
                start_date,
                end_date
            )

        except Exception as e:
            print("❌ ERROR in fetch_volume_summary:", e)
            traceback.print_exc()
            out["errors"].append(str(e))

        return to_native(out)



def fetch_market_landscape(
        start_date: date,
        end_date: date,
        sort_mode: str = "price",
        limit: Optional[int] = None,
        sector: Optional[str] = None,
        company: Optional[str] = None
) -> List[Dict[str, Any]]:
    one_day = (start_date == end_date)

    print("\n============================")
    print("🔥 MARKET LANDSCAPE QUERY (Hybrid v4)")
    print("start_date:", start_date, " end_date:", end_date)
    print("============================\n")

    # ---------------------------------------
    # VALIDATION: Check for allowed sort modes
    # ---------------------------------------
    allowed_modes = ["price", "sentiment"]  # REMOVED "mentions"
    if sort_mode not in allowed_modes:
        sort_mode = "price"

    # ... (SQL and Pandas logic for aggregation remains the same) ...
    # (The existing logic for Single-Day Mode and Range Mode Pandas calculation is preserved below)
    # ...

    # [Start of existing range mode logic]
    if one_day:
        # 1) SINGLE-DAY MODE (simple fetch - REMAINS THE SAME)
        sql = f"""
            SELECT company, daily_return AS price_change, mean_sentiment AS avg_sentiment, n_mentions AS mentions, sector
            FROM gold.company_sentiment_vs_price
            WHERE date_utc = %s
            ORDER BY company ASC
        """
        # ... (Execution and return logic for single day) ...

        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (start_date,))
                rows = cur.fetchall()
        except Exception as e:
            print("❌ SQL ERROR (single-day):", e)
            traceback.print_exc()
            return []

        out = []
        for c, p, s, m, sector_val in rows:
            out.append({
                "ticker": c,
                "price_change": float(p) if p is not None else None,
                "avg_sentiment": float(s) if s is not None else None,
                "mentions": int(m or 0),
                "sector": sector_val,
            })

        if limit:
            out = out[:limit]

        return out

    else:
        # 2) RANGE MODE (Hybrid window logic - REMAINS THE SAME)
        sql = f"""
            SELECT company, date_utc, price_close, mean_sentiment, n_mentions, sector
            FROM gold.company_sentiment_vs_price
            WHERE date_utc BETWEEN %s AND %s
            ORDER BY company ASC, date_utc ASC
        """
        # ... (Execution and Pandas processing code for range aggregation, including the volume-weighted sentiment fix) ...

        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (start_date, end_date))
                rows = cur.fetchall()
                df = pd.DataFrame(
                    rows,
                    columns=[
                        "company", "date_utc", "price_close", "mean_sentiment", "n_mentions", "sector",
                    ],
                )
        except Exception as e:
            print("❌ SQL ERROR (range):", e)
            traceback.print_exc()
            return []

        if df.empty:
            return []

        df["date_utc"] = pd.to_datetime(df["date_utc"]).dt.date
        df = df.sort_values(["company", "date_utc"], ascending=True)

        companies = sorted(df["company"].unique())

        out_rows: List[Dict[str, Any]] = []

        for comp in companies:
            g = df[df["company"] == comp].copy()
            if g.empty:
                continue

            g = g.sort_values("date_utc")

            # Fill price/sentiment forward
            g["price_close"] = g["price_close"].ffill()
            g["mean_sentiment"] = g["mean_sentiment"].ffill()

            first_price = g["price_close"].iloc[0]
            last_price = g["price_close"].iloc[-1]

            # Calculation of Price Change
            if first_price is None or pd.isna(first_price) or first_price == 0:
                price_change = None
            else:
                price_change = (last_price - first_price) / first_price

            # Calculation of Volume-Weighted Avg Sentiment (FIXED)
            mentions_total = g["n_mentions"].fillna(0).sum()
            if mentions_total > 0:
                weighted_sum = (g["mean_sentiment"].fillna(0) * g["n_mentions"].fillna(0)).sum()
                avg_sentiment_weighted = weighted_sum / mentions_total
            else:
                avg_sentiment_weighted = None

            sector_val = g["sector"].iloc[-1]

            out_rows.append({
                "ticker": comp,
                "price_change": float(price_change) if price_change is not None else None,
                "avg_sentiment": float(avg_sentiment_weighted) if avg_sentiment_weighted is not None else None,
                "mentions": int(mentions_total),
                "sector": sector_val,
            })

        if limit:
            out_rows = out_rows[:limit]

        return out_rows

# ... (The rest of the file: fetch_market_corr_snapshot, fetch_marketview_candles, etc. remain the same) ...


# backend/services/query/marketview.py

# ======================================================
# WRAPPER — LANDSCAPE SUMMARY (NEW)
# ======================================================
def fetch_marketview_landscape_summary(
        start: Optional[str] = None,
        end: Optional[str] = None,
        sort_mode: str = "price",
        limit: Optional[int] = None,
        company: Optional[str] = None,
        sector: Optional[str] = None,
) -> Dict[str, Any]:
    # Renamed output key from 'heatmap' to 'landscape'
    summary = {
        "start_date": None,
        "end_date": None,
        "landscape": [],
        "sort_mode": sort_mode,
        "errors": [],
    }

    try:
        # ---------------------------------------
        # DATE RANGE
        # ---------------------------------------
        start_date, end_date = _determine_date_range(start, end)
        summary["start_date"] = start_date.isoformat()
        summary["end_date"] = end_date.isoformat()

        # ---------------------------------------
        # VALID SORT MODES
        # ---------------------------------------
        valid_modes = ["price", "sentiment"]  # REMOVED "mentions"

        if sort_mode not in valid_modes:
            base_sort = "price"
        else:
            base_sort = sort_mode

        # ---------------------------------------
        # FETCH CORE LANDSCAPE DATA (REPLACED fetch_market_heatmap)
        # ---------------------------------------
        landscape_data = fetch_market_landscape(
            start_date=start_date,
            end_date=end_date,
            sort_mode=base_sort,
            limit=limit,
            company=company,
            sector=sector,
        )

        # ---------------------------------------
        # MERGE CORRELATION DATA (For Leaderboard/Deep Dive)
        # ---------------------------------------
        corr_data = fetch_market_corr_snapshot(end_date=end_date)

        if not landscape_data:
            summary["landscape"] = []
        else:
            df_heat = pd.DataFrame(landscape_data)
            df_corr = pd.DataFrame(corr_data)

            # Merge: Left join correlation scores onto the heatmap base
            merged_df = df_heat.merge(
                df_corr[['ticker', 'corr7', 'corr30']],
                on='ticker',
                how='left'
            )
            summary["landscape"] = merged_df.to_dict(orient="records")

        # SCATTER DATA REMOVED: It's redundant since the Treemap serves the main data.

    except Exception as e:
        print("❌ ERROR in fetch_marketview_landscape_summary():", e)
        traceback.print_exc()
        summary["errors"].append(str(e))

    return to_native(summary)


# backend/services/query/marketview.py

# ======================================================
# 1. CORE LOGIC — SINGLE TICKER CANDLES
# ======================================================
def fetch_ticker_timeseries(
        ticker: str,
        start_date: date,
        end_date: date
) -> List[Dict[str, Any]]:
    """
    Raw SQL fetcher for daily price/sentiment rows.
    """
    sql = """
        SELECT date_utc, price_close, mean_sentiment, n_mentions
        FROM gold.company_sentiment_vs_price
        WHERE company = %s
          AND date_utc BETWEEN %s AND %s
        ORDER BY date_utc ASC
    """

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (ticker, start_date, end_date))
            rows = cur.fetchall()

        candles = []
        for r in rows:
            # Basic cleaning
            price = float(r[1]) if r[1] is not None else None
            sentiment = float(r[2]) if r[2] is not None else 0.0
            mentions = int(r[3] or 0)

            if price is None and mentions == 0:
                continue

            candles.append({
                "date_utc": r[0].isoformat() if r[0] else "",
                "price_close": price,
                "mean_sentiment": sentiment,
                "n_mentions": mentions
            })
        return candles

    except Exception as e:
        print(f"❌ SQL ERROR (fetch_ticker_timeseries): {e}")
        import traceback;
        traceback.print_exc()
        return []


# ======================================================
# 2. WRAPPER — CANDLES SUMMARY (Mirrors landscape_summary)
# ======================================================
def fetch_marketview_candles_summary(
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Wrapper that handles Date Range determination and formatting
    before calling the core SQL function.
    """
    summary = {
        "ticker": ticker,
        "start_date": None,
        "end_date": None,
        "candles": [],
        "errors": [],
    }

    try:
        # 1. DATE RANGE (Reusing your internal logic if available, or manual)
        # Assuming _determine_date_range is available in this scope like in landscape
        start_date, end_date = _determine_date_range(start, end)

        summary["start_date"] = start_date.isoformat()
        summary["end_date"] = end_date.isoformat()

        # 2. FETCH CORE DATA
        candles_data = fetch_ticker_timeseries(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date
        )

        summary["candles"] = candles_data

    except Exception as e:
        print("❌ ERROR in fetch_marketview_candles_summary():", e)
        import traceback;
        traceback.print_exc()
        summary["errors"].append(str(e))

    return to_native(summary)

