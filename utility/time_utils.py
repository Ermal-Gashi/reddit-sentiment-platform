from datetime import datetime, timezone
import pandas as pd

def to_iso_utc(ts):
    try:
        if ts is None or pd.isna(ts):
            return ""
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""
