import os
import time
import logging
import argparse
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
import yfinance as yf
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import YFINANCE

# ============================================================
# ENV + LOGGING
# ============================================================
load_dotenv()

PG_CONN = {
    "dbname": os.getenv("PGDB", "reddit_warehouse"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", 5432),
}

VALID_INTERVALS = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]

MAX_WORKERS = 6
SEGMENT_SIZE = 100

DEBUG_TICKERS = {"AMZN", "AAPL", "MSFT", "NVDA"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

GLOBAL_TOTAL = 0
GLOBAL_SKIPPED = 0

# ============================================================
# HELPERS
# ============================================================

def normalize_for_yf(symbol: str) -> str:
    return symbol.replace(".", "-")


def get_connection():
    return psycopg2.connect(**PG_CONN)


def ensure_table_exists(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE SCHEMA IF NOT EXISTS gold;

        CREATE TABLE IF NOT EXISTS gold.market_prices_intraday (
            ticker      TEXT NOT NULL,
            ts_utc      TIMESTAMPTZ NOT NULL,
            open        DOUBLE PRECISION,
            high        DOUBLE PRECISION,
            low         DOUBLE PRECISION,
            close       DOUBLE PRECISION,
            volume      BIGINT,
            fetched_at  TIMESTAMPTZ,
            PRIMARY KEY (ticker, ts_utc)
        );
        """
    )
    conn.commit()
    cur.close()


# ============================================================
# FILTER MULTI-TICKER FRAMES
# ============================================================

def filter_single_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    mask = df.columns.get_level_values(1) == ticker
    df = df.loc[:, mask]
    df.columns = df.columns.droplevel(1)
    return df


# ============================================================
# FLATTENER
# ============================================================

def flatten_yf(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            chosen = None
            for part in col:
                if str(part).lower() in {"open", "high", "low", "close", "adj close", "price", "volume"}:
                    chosen = part
                    break
            if chosen is None:
                chosen = col[0]
            new_cols.append(str(chosen))
        df.columns = new_cols

    # normalize names
    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "adj close": "Close",
        "price": "Close",
        "volume": "Volume",
    }
    df.columns = [rename_map.get(str(c).lower(), str(c)) for c in df.columns]
    return df


# ============================================================
# TIMESTAMP DETECTOR
# ============================================================

def detect_timestamp_column(df: pd.DataFrame, ticker: str) -> str:
    preferred = ["Datetime", "Date", "timestamp", "time"]
    for p in preferred:
        if p in df.columns:
            return p

    for c in df.columns:
        if np.issubdtype(df[c].dtype, np.datetime64):
            return c

    first = df.columns[0]
    pd.to_datetime(df[first].iloc[0])
    return first


# ============================================================
# FETCH CLEAN DATA (MINIMAL LOGS)
# ============================================================

def fetch_intraday_data(ticker: str, days_back: int, interval: str):
    global GLOBAL_TOTAL, GLOBAL_SKIPPED

    yf_symbol = normalize_for_yf(ticker)
    period = f"{days_back}d"
    debug_enabled = ticker in DEBUG_TICKERS

    for attempt in range(1, 3):
        try:
            logging.info(f"📥 [{ticker}] {attempt}/3")

            df = yf.download(
                yf_symbol,
                period=period,
                interval=interval,
                progress=False,
                auto_adjust=True,
                threads=False,
            )

            if df is None or df.empty:
                logging.warning(f" [{ticker}] empty frame")
                time.sleep(1)
                continue

            # key logs
            logging.info(f"   raw={df.shape}")

            df = filter_single_ticker(df, ticker)
            df = flatten_yf(df)
            df = df.reset_index()

            try:
                ts_col = detect_timestamp_column(df, ticker)
            except:
                logging.error(f" [{ticker}] no timestamp col")
                return []

            rows = []
            now_ts = datetime.now(timezone.utc)

            local_total = local_skipped = 0
            skip_ts = skip_ohlc = skip_vol = skip_nan = skip_flat = 0
            dbg = 0

            for idx, row in df.iterrows():
                local_total += 1

                try:
                    ts = pd.to_datetime(row[ts_col], utc=True)
                except:
                    skip_ts += 1
                    local_skipped += 1
                    continue

                try:
                    o = float(row["Open"])
                    h = float(row["High"])
                    l = float(row["Low"])
                    c = float(row["Close"])
                    v_raw = row["Volume"]
                except:
                    skip_ohlc += 1
                    local_skipped += 1
                    if debug_enabled and dbg < 3:
                        logging.warning(f" [{ticker}] ohlc err idx={idx}")
                        dbg += 1
                    continue

                if pd.isna(v_raw):
                    skip_vol += 1
                    local_skipped += 1
                    continue

                try:
                    v = int(v_raw)
                except:
                    skip_vol += 1
                    local_skipped += 1
                    continue

                if any(np.isnan(x) for x in [o, h, l, c]):
                    skip_nan += 1
                    local_skipped += 1
                    continue

                if o == h == l == c and v == 0:
                    skip_flat += 1
                    local_skipped += 1
                    continue

                rows.append((ticker, ts.to_pydatetime(), o, h, l, c, v, now_ts))

            GLOBAL_TOTAL += local_total
            GLOBAL_SKIPPED += local_skipped

            logging.info(
                f" [{ticker}] kept={len(rows)} skipped={local_skipped}"
            )

            return rows

        except Exception as e:
            logging.error(f" [{ticker}] err: {e}")
            time.sleep(1)

    logging.error(f" [{ticker}] failed")
    return []


# ============================================================
# UPSERT
# ============================================================

def upsert_prices_batch(conn, all_rows):
    if not all_rows:
        logging.info("️ No rows to upsert")
        return

    df = pd.DataFrame(
        all_rows,
        columns=["ticker", "ts_utc", "open", "high", "low", "close", "volume", "fetched_at"]
    )

    df.drop_duplicates(subset=["ticker", "ts_utc"], inplace=True)

    cur = conn.cursor()
    sql = """
        INSERT INTO gold.market_prices_intraday
        (ticker, ts_utc, open, high, low, close, volume, fetched_at)
        VALUES %s
        ON CONFLICT (ticker, ts_utc)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            fetched_at = EXCLUDED.fetched_at;
    """

    execute_values(cur, sql, df.values.tolist(), page_size=2000)
    conn.commit()
    cur.close()

    logging.info(f" Upserted {len(df):,}")


# ============================================================
# RUNNER
# ============================================================

def run_batch_segmented(tickers, days_back, interval):
    global GLOBAL_TOTAL, GLOBAL_SKIPPED

    conn = get_connection()
    ensure_table_exists(conn)

    start = time.time()
    all_rows = []

    segments = [
        tickers[i:i + SEGMENT_SIZE]
        for i in range(0, len(tickers), SEGMENT_SIZE)
    ]

    logging.info(f" Running {len(segments)} segments")

    for seg_i, segment in enumerate(segments, start=1):
        logging.info(f" Segment {seg_i} ({len(segment)} tickers)")

        def worker(t):
            return t, fetch_intraday_data(t, days_back, interval)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(worker, t): t for t in segment}

            for j, fut in enumerate(as_completed(futures), start=1):
                t = futures[fut]
                try:
                    _, rows = fut.result()
                    if rows:
                        all_rows.extend(rows)
                        logging.info(f"   [{j}/{len(segment)}] {t}: {len(rows)} rows")
                    else:
                        logging.info(f"   [{j}/{len(segment)}] {t}: 0")
                except Exception as e:
                    logging.error(f" worker {t}: {e}")

        time.sleep(1)

    upsert_prices_batch(conn, all_rows)
    conn.close()

    dur = (time.time() - start) / 60
    logging.info("============== SUMMARY ==============")
    logging.info(f"⏱ {dur:.2f} min")
    logging.info(f"Total parsed: {GLOBAL_TOTAL:,}")
    logging.info(f"Total skipped: {GLOBAL_SKIPPED:,}")
    if GLOBAL_TOTAL:
        logging.info(f"Skip rate: {GLOBAL_SKIPPED / GLOBAL_TOTAL:.2%}")
    logging.info("=====================================")


# ============================================================
# TICKER SOURCE
# ============================================================

def get_all_tickers(limit=None):
    conn = get_connection()
    cur = conn.cursor()

    if limit:
        cur.execute(
            "SELECT company FROM ops.company_universe WHERE enabled = TRUE ORDER BY company ASC LIMIT %s;",
            (limit,)
        )
    else:
        cur.execute(
            "SELECT company FROM ops.company_universe WHERE enabled = TRUE ORDER BY company ASC;"
        )

    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--interval", type=str, default="5m")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)

    args = parser.parse_args()

    if args.interval not in VALID_INTERVALS:
        logging.error(" Invalid interval")
        raise SystemExit(1)

    tickers = get_all_tickers(limit=args.limit)

    run_batch_segmented(
        tickers,
        days_back=args.days,
        interval=args.interval,
    )
