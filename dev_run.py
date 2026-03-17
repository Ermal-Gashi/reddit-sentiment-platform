import subprocess
import sys
import signal
from pathlib import Path
import time

BASE_DIR = Path(__file__).parent


BACKEND_CMD = [
    sys.executable,
    "-m",
    "uvicorn",
    "backend.api_main:app",   #  CORRECT ENTRY POINT
    "--host", "127.0.0.1",
    "--port", "8000",
    "--reload"
]


FRONTEND_CMD = [
    "npm",
    "run",
    "dev"
]


def main():
    print(" Starting Backend + Frontend")

    backend = subprocess.Popen(
        BACKEND_CMD,
        cwd=BASE_DIR,  #  IMPORTANT: project root
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )

    frontend = subprocess.Popen(
        FRONTEND_CMD,
        cwd=BASE_DIR / "frontend",
        shell=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n Shutting down services...")

        backend.send_signal(signal.CTRL_BREAK_EVENT)
        frontend.send_signal(signal.CTRL_BREAK_EVENT)

        time.sleep(2)

        backend.terminate()
        frontend.terminate()


if __name__ == "__main__":
    main()
