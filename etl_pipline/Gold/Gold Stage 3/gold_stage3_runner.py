# gold_stage3_runner.py
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Gold Stage 3 execution order (explicit & intentional)
STEPS = [
    "gold_stage3A_company_daily.py",
    "gold_stage3B_market_fetch_intraday.py",
    "gold_stage3C_company_daily_market.py",
    "gold_stage3D_company_daily_sentiment_market.py",
]

def run_step(script_name):
    script_path = BASE_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f" Script not found: {script_path}")

    print(f"\n▶ Running {script_name} …")

    subprocess.run(
        [sys.executable, str(script_path)],
        check=True
    )

    print(f"✔ Finished {script_name}")

def main():
    print(" Gold Stage 3 Runner — Daily Company & Market Pipeline")
    print("=======================================================")

    for step in STEPS:
        run_step(step)

    print("\n Gold Stage 3 COMPLETE — all sub-stages executed successfully")

if __name__ == "__main__":
    main()
