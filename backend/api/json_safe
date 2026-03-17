import json
from fastapi.responses import JSONResponse
from typing import Any


class SafeJSONResponse(JSONResponse):

    def render(self, content: Any) -> bytes:
        def clean_nans(obj):
            if isinstance(obj, float) and (obj != obj or obj in (float("inf"), float("-inf"))):
                return None
            elif isinstance(obj, dict):
                return {k: clean_nans(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nans(i) for i in obj]
            return obj

        cleaned = clean_nans(content)
        return json.dumps(cleaned, ensure_ascii=False).encode("utf-8")
