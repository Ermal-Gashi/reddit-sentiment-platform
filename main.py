

import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone



def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg, icon="🔹"):
    print(f"{icon} [{now_iso()}] {msg}")


def run_stage(label: str, script_path: Path):
    if not script_path.exists():
        raise FileNotFoundError(f" Missing script: {script_path}")

    log(f"Starting {label}", "")
    log(f"Script: {script_path}", "")

    subprocess.run(
        [sys.executable, str(script_path)],
        check=True
    )

    log(f"Finished {label}", "")



def main():
    BASE_DIR = Path(__file__).parent / "etl_pipeline"

    BRONZE_RUNNER = BASE_DIR / "bronze" / "bronze_runner.py"
    SILVER_RUNNER = BASE_DIR / "silver" / "silver_runner.py"
    GOLD_RUNNER   = BASE_DIR / "Gold"   / "gold_runner.py"

    STEPS = [
        ("Bronze Stage", BRONZE_RUNNER),
        ("Silver Stage", SILVER_RUNNER),
        ("Gold Stage",   GOLD_RUNNER),
    ]

    log("FULL ETL PIPELINE STARTED (Bronze → Silver → Gold)", "")
    log("=" * 55)

    for label, script in STEPS:
        run_stage(label, script)

    log("=" * 55)
    log("FULL ETL PIPELINE COMPLETED SUCCESSFULLY", "")


if __name__ == "__main__":
    main()
