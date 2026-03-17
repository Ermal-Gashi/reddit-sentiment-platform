
import os
import re
import csv


CSV_PATH = r"C:\Users\korab\Downloads\Thesis folder\Thesis Main\sp500_companies.csv"

# ------------------------------------------------------------
# 1) Core heuristics
# ------------------------------------------------------------


CANONICAL_MAP = {
    "GOOG": "GOOGL",
    "META": "META",
    "BRK.B": "BRK.B",
    "BRK.A": "BRK.A",
    # Add others
}

# Hard block: ALL 1–2 letter tickers (user’s requirement)
def _is_short_ticker(t: str) -> bool:
    return len(t) <= 2

# Stoplist: 3–5 letter tickers that are common English words and created tons of false positives
STOP_TICKERS = {
    # classic offenders
    "ARE", "ALL", "HAS", "DAY", "NOW", "TECH", "WELL", "FAST", "LOW", "KEY",

    "ON", "IT", "SO",
}

# Expanded aliases for high-recall (names-first)
EXPANDED_ALIASES = {
    "AAPL": ["apple", "iphone", "ipad", "macbook", "apple inc"],
    "MSFT": ["microsoft", "windows", "xbox", "office", "microsoft corp"],
    "GOOGL": ["google", "alphabet", "youtube", "gmail", "alphabet inc"],
    "GOOG": ["google", "alphabet"],
    "AMZN": ["amazon", "aws", "prime", "amazon.com"],
    "META": ["meta", "facebook", "instagram", "whatsapp", "oculus", "meta platforms"],
    "NVDA": ["nvidia", "geforce", "rtx", "cuda", "nvidia corp"],
    "TSLA": ["tesla", "tesla inc", "elon musk"],
    "NFLX": ["netflix", "streaming", "netflix inc"],
    "AMD": ["amd", "radeon", "epyc", "ryzen", "advanced micro devices"],
    "INTC": ["intel", "pentium", "xeon", "intel corp"],
    "DIS": ["disney", "pixar", "marvel", "lucasfilm", "espn", "walt disney"],
    "IBM": ["ibm", "international business machines"],
    "JPM": ["jpmorgan", "jp morgan", "chase", "jpmorgan chase"],
    "GS": ["goldman sachs"],
    "BAC": ["bank of america"],
    "WFC": ["wells fargo"],
    "C": ["citigroup", "citi"],
    "MS": ["morgan stanley"],
    "SCHW": ["charles schwab", "schwab"],
    "V": ["visa"],
    "MA": ["mastercard"],
    "PYPL": ["paypal", "venmo"],
    "SQ": ["block", "square", "cash app", "cashapp"],
    "COIN": ["coinbase"],
    "PLTR": ["palantir"],
    "SNAP": ["snapchat"],
    "UBER": ["uber"],
    "LYFT": ["lyft"],
    "ABNB": ["airbnb"],
    "SHOP": ["shopify"],
    "CRM": ["salesforce"],
    "ORCL": ["oracle"],
    "ADBE": ["adobe"],
    "INTU": ["intuit", "turbotax", "mint"],
    "XOM": ["exxon", "exxonmobil"],
    "CVX": ["chevron"],
    "BP": ["bp"],
    "OXY": ["oxy", "occidental"],
    "COP": ["conocophillips"],
    "SLB": ["schlumberger"],
    "GM": ["general motors", "chevy", "cadillac"],
    "F": ["ford"],
    "TM": ["toyota"],
    "HMC": ["honda"],
    "BA": ["boeing"],
    "CAT": ["caterpillar"],
    "GE": ["general electric"],
    "LMT": ["lockheed martin"],
    "RTX": ["raytheon", "rtx corp"],
    "NOC": ["northrop grumman"],
    "MMM": ["3m"],
    "HON": ["honeywell"],
    "DE": ["john deere"],
    "PEP": ["pepsico", "pepsi"],
    "KO": ["coca cola", "coke"],
    "MCD": ["mcdonalds", "mcdonald's", "big mac"],
    "SBUX": ["starbucks"],
    "TGT": ["target"],
    "WMT": ["walmart"],
    "COST": ["costco"],
    "HD": ["home depot"],
    "LOW": ["lowe's", "lowes"],
    "PG": ["procter gamble", "p&g", "procter & gamble"],
    "JNJ": ["johnson johnson", "johnson & johnson"],
    "MRK": ["merck"],
    "PFE": ["pfizer"],
    "LLY": ["eli lilly"],
    "BMY": ["bristol myers"],
    "AMGN": ["amgen"],
    "UNH": ["unitedhealth", "united health"],
    "CI": ["cigna"],
    "ANTM": ["anthem", "elevance health"],
    "CVS": ["cvs", "cvs health"],
    "WBA": ["walgreens", "boots alliance"],
    "DAL": ["delta airlines"],
    "AAL": ["american airlines"],
    "UAL": ["united airlines"],
    "LUV": ["southwest airlines"],
    "TSM": ["tsmc", "taiwan semiconductor"],
    "ASML": ["asml"],
    "QCOM": ["qualcomm"],
    "AVGO": ["broadcom"],
    "ADSK": ["autodesk"],
    "ZM": ["zoom"],
    "DOCU": ["docusign"],
    "ROKU": ["roku"],
    "SPOT": ["spotify"],
    "SONY": ["sony", "playstation"],
    "NOK": ["nokia"],
    "ERIC": ["ericsson"],
    "BYND": ["beyond meat"],
    "NIO": ["nio"],
    "LI": ["li auto"],
    "XPEV": ["xpeng"],
    "RIVN": ["rivian"],
    "T": ["at&t"],
    "VZ": ["verizon"],
    "CMCSA": ["comcast", "nbc", "universal"],
    "CHTR": ["charter", "spectrum"],
    "DISCA": ["discovery", "discovery communications"],
    "FOX": ["fox corporation"],
    "FOXA": ["fox news"],
    "NWSA": ["news corp", "wall street journal"],
    "NWS": ["news corp"],
    "PARA": ["paramount", "viacom", "cbs"],

    "ABBV": ["abbvie"],
    "GILD": ["gilead", "gilead sciences"],
    "REGN": ["regeneron"],
    "VRTX": ["vertex"],
    "BIIB": ["biogen"],

    "ZTS": ["zoetis"],
    "MDT": ["medtronic"],
    "SYK": ["stryker"],
    "BSX": ["boston scientific"],

    "APD": ["air products", "air products and chemicals"],
    "LIN": ["linde"],
    "ECL": ["ecolab"],
    "DOW": ["dow chemical"],
    "DD": ["dupont"],

    "FDX": ["fedex"],
    "UPS": ["ups", "united parcel service"],
    "CSX": ["csx"],
    "NSC": ["norfolk southern"],
    "UNP": ["union pacific"],

    "BKNG": ["booking.com", "booking holdings", "priceline", "kayak"],
    "EXPE": ["expedia"],
    "MAR": ["marriott"],
    "HLT": ["hilton"],

    "AAP": ["advance auto parts"],
    "AZO": ["autozone"],
    "ORLY": ["oreilly auto parts"],

    "MO": ["altria", "philip morris usa"],
    "PM": ["philip morris international"],
    "BTI": ["british american tobacco"],

    "RCL": ["royal caribbean"],
    "CCL": ["carnival cruise"],
    "NCLH": ["norwegian cruise line"],

    "HCA": ["hca healthcare"],
    "ISRG": ["intuitive surgical", "da vinci robot"],

    "CMI": ["cummins"],
    "PCAR": ["paccar", "kenworth", "peterbilt"],

    "AIG": ["american international group"],
    "TRV": ["travelers"],
    "PRU": ["prudential"],
    "MET": ["metlife"],
    "ALL": ["allstate"],

    "STT": ["state street"],
    "BK": ["bny mellon"],
    "ICE": ["intercontinental exchange"],
    "CME": ["cme group"]
}


# ------------------------------------------------------------
# Product-only aliases (unique product-to-company links)
# ------------------------------------------------------------
PRODUCT_ALIASES = {
    "TSLA": ["model 3", "model y", "cybertruck", "roadster", "powerwall"],
    "AAPL": ["iphone", "ipad", "macbook", "vision pro", "airpods pro", "imac"],
    "NVDA": ["rtx 4090", "rtx 5090", "geforce rtx", "cuda core", "tensor core"],
    "AMD":  ["ryzen 9", "ryzen 7", "radeon rx", "epyc chip"],
    "MSFT": ["surface laptop", "surface pro", "xbox series x", "xbox controller", "copilot+"],
    "META": ["quest 3", "quest pro", "oculus quest", "ray-ban meta"],
    "SONY": ["playstation 5", "ps5", "dualsense controller"],
    "GOOGL": ["pixel 8", "pixel fold", "chromebook", "gmail app", "android 15"],
    "SAMSUNG": ["galaxy s24 ultra", "galaxy z fold", "galaxy buds pro"],
    "INTC": ["core i9", "xeon platinum", "arc a770"],
    "AMZN": ["alexa echo", "prime video", "fire tv stick", "kindle paperwhite"],
    "RIVN": ["rivian r1t", "rivian r1s"],
    "NIO": ["nio et5", "nio es6"],
}




# ------------------------------------------------------------
# 2) Regex helpers & name normalization
# ------------------------------------------------------------

def _word_boundary(term: str) -> str:
    return r"\b" + re.escape(term) + r"\b"

def _ticker_token(term: str) -> str:
    t = re.escape(term)
    return rf"(?<![A-Za-z0-9]){t}(?![A-Za-z0-9])"

def _cashtag(term: str) -> str:
    core = re.escape(term.lstrip("$"))
    return rf"(?<!\w)\${core}(?![A-Za-z0-9])"

SUFFIX_RE = re.compile(
    r"(,?\s+Inc\.?|,?\s+Incorporated|,?\s+Corporation|,?\s+Corp\.?|,?\s+Company|,?\s+Co\.?|,?\s+PLC|,?\s+LLC|"
    r",?\s+Ltd\.?|,?\s+Limited|,?\s+Holdings?|,?\s+Group)$",
    re.I
)
PAREN_TAIL_RE = re.compile(r"\s*\(.*?\)\s*$")

def _short_company(name: str) -> str:
    if not name:
        return ""
    s = name.strip()
    s = PAREN_TAIL_RE.sub("", s)
    s = SUFFIX_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _split_aliases(raw_aliases: str) -> list:
    if not raw_aliases or str(raw_aliases).lower() in {"nan", "none"}:
        return []
    parts = [a.strip() for a in str(raw_aliases).split(";")]
    return [p for p in parts if p]

# ------------------------------------------------------------
# 3) Build patterns from CSV + hardcoded rules
# ------------------------------------------------------------

def _load_company_patterns(csv_path: str):
    """
    Build pattern spec per ticker:
      - Names: shortened company names + CSV aliases + expanded aliases (names are high-priority)
      - Tickers: include only if len>=3 AND not in STOP_TICKERS
      - Cashtags: allowed for any len>=3 ticker (we also ignore <=2)
    """
    patterns = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker  = (row.get("Ticker") or "").strip().upper()
            company = (row.get("Company") or "").strip()
            aliases_raw = row.get("Aliases", "")

            if not (ticker or company):
                continue

            # ---- Build names (priority channel) ----
            names = []
            short = _short_company(company)
            if short:
                names.append(short.lower())

            # CSV-provided aliases
            for a in _split_aliases(aliases_raw):
                names.append(a.lower())

            # Expanded aliases (script dictionary)
            for a in EXPANDED_ALIASES.get(ticker, []):
                names.append(a.lower())

            # Dedup names
            names = sorted(set(n for n in names if n))

            # ---- Build ticker/cashtag with hard rules ----
            tickers, cashtags = [], []
            if ticker:
                if not _is_short_ticker(ticker) and ticker not in STOP_TICKERS:
                    tickers.append(ticker)
                    cashtags.append(f"${ticker}")
                # else: ignore short and stoplisted tickers completely

            key = ticker if ticker else company
            patterns[key] = {
                "names": names,               # high-priority, no stock-context required
                "tickers": tickers,           # only 3+ letters & not stoplisted
                "cashtags": cashtags,         # only for 3+ letters
            }
    return patterns


def _compile_company_regex(company_patterns):
    compiled = {}
    for key, spec in company_patterns.items():
        name_re = re.compile("|".join(_word_boundary(n) for n in spec["names"]) or r"(?!x)x", re.I)
        ticker_re = re.compile("|".join(_ticker_token(t) for t in spec["tickers"]) or r"(?!x)x", re.I)
        cashtag_re = re.compile("|".join(_cashtag(c) for c in spec["cashtags"]) or r"(?!x)x", re.I)
        compiled[key] = {
            "name_re": name_re,
            "ticker_re": ticker_re,
            "cashtag_re": cashtag_re,
            "ambiguous": spec.get("ambiguous", False),
        }
    return compiled


# ------------------------------------------------------------
# 4) Matching API
# ------------------------------------------------------------
def match_text_to_companies(text: str):
    """
    Returns:
      companies: set of canonical tickers
      terms:     list of matched terms
      types:     list of 'name' | 'ticker' | 'product'

    Strategy:
      - Names (company names & aliases) ALWAYS count.
      - Tickers only if not ambiguous (short tickers filtered out).
      - Product-only mentions (from PRODUCT_ALIASES) also count, no stock context required.
      - Cashtags remain ignored.
    """
    if not text:
        return set(), [], []

    companies, terms, types = set(), [], []

    # --- Core matching: names and tickers ---
    for key, rx in COMPILED.items():
        # --- Names: strongest signal ---
        n_hits = rx["name_re"].findall(text)
        if n_hits:
            companies.add(key)
            terms.extend(n_hits)
            types.extend(["name"] * len(n_hits))

        # --- Tickers: allow only if not ambiguous or 3+ chars ---
        t_hits = rx["ticker_re"].findall(text)
        if t_hits and (not rx["ambiguous"]):
            companies.add(key)
            terms.extend(t_hits)
            types.extend(["ticker"] * len(t_hits))

        # ⚠️ Cashtags still ignored

    # --- Product-only detection layer (no context required) ---
    lower_text = text.lower()
    for ticker, plist in PRODUCT_ALIASES.items():
        for product in plist:
            # match whole phrase, case-insensitive
            if re.search(rf"\b{re.escape(product)}\b", lower_text):
                companies.add(ticker)
                terms.append(product)
                types.append("product")

    # --- Canonical normalization ---
    normalized = set()
    for c in companies:
        normalized.add(CANONICAL_MAP.get(c, c))

    return normalized, terms, types

def filter_company_posts_by_regex(posts):
    kept = []
    for p in posts:
        post_text = f"{getattr(p,'title','')}\n{getattr(p,'selftext','')}"
        post_companies, _, _ = match_text_to_companies(post_text)
        if post_companies:
            kept.append(p)
    return kept


# ------------------------------------------------------------
# 5) Initialize patterns at import time
# ------------------------------------------------------------

COMPANY_PATTERNS = _load_company_patterns(CSV_PATH)
COMPILED = _compile_company_regex(COMPANY_PATTERNS)