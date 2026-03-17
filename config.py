
import os


# Central knobs & paths
SUBREDDITS = [
    "stocks",
    "investing",
    "wallstreetbets",
    "StockMarket",
    "technology"
]
COMMENT_LIMIT_PER_POST = 10000
POST_LIMIT = 100

# Output files (unchanged)
OUTPUT_PATH = r"C:\Users\ermal\Desktop\thesis\post_comments.csv"
STATE_DB_PATH = r"C:\Users\korab\PycharmProjects\Thesis\data\state\state_seen.sqlite"


REDDIT_CLIENT = {
    "client_id": "X8ZJy6rAeVkpiIae1zfKuQ",
    "client_secret": "A6fbVAoAurq0QMV_XN4oeRpudbqgMA",
    "username": "Salty-Cow-9618",
    "password": "/[12.e.1[2412.[p12@",
    "user_agent": "thesis by u/ETL_Data_Bot_01",
}



PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDB   = os.getenv("PGDB", "reddit_warehouse")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")

# ============================================================
FINNHUB = {
    "api_key": "d3l5jqpr01qq28em1lk0d3l5jqpr01qq28em1lkg",
    "base_url": "https://finnhub.io/api/v1",
    "default_interval": "5",
    "default_days_back": 50,
    "default_delay": 1.0,
    "rate_limit_per_min": 60,
    "max_websocket_symbols": 10
}


YFINANCE = {
    "default_interval": "5m",
    "default_days_back": 50,
    "default_delay": 1.0,
}

# Paste your actual key inside the quotes
GROQ_API_KEY = "gsk_your_actual_key_here"
