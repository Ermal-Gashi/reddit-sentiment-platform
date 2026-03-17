import json
import os

def load_finlex(path="finlex.json"):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Lexicon file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    flat = {}

    for group, words in data.items():
        for w, score in words.items():
            key = w.strip().lower()

            # If duplicates across categories → keep the strongest weight
            if key in flat:
                existing = abs(flat[key])
                new_val = abs(score)
                if new_val > existing:
                    flat[key] = score
            else:
                flat[key] = score

    return flat
