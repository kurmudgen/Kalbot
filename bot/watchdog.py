"""
Watchdog: monitors bot processes and auto-restarts if they crash.
Run this as a separate process — it watches the main bots.
"""

import os
import subprocess
import sys
import time
from datetime import datetime

PYTHON = sys.executable
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
CHECK_INTERVAL = 60  # Check every 60 seconds

BOTS = [
    {
        "name": "Kalshi",
        "script": os.path.join(BASE_DIR, "bot", "dual_strategy.py"),
        "process_marker": "dual_strategy.py",
    },
    {
        "name": "Stocks/Crypto",
        "script": os.path.join(BASE_DIR, "stock_bot", "run_stock_bot.py"),
        "process_marker": "run_stock_bot.py",
    },
]


def is_running(marker: str) -> bool:
    """Check if a process with the given marker is running."""
    try:
        no_window = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        if os.name == "nt":
            result = subprocess.run(
                ["wmic", "process", "where",
                 "name='python.exe' or name='pythonw.exe'",
                 "get", "commandline"],
                capture_output=True, text=True, timeout=10,
                creationflags=no_window,
            )
        else:
            result = subprocess.run(
                ["ps", "aux"], capture_output=True, text=True, timeout=5,
            )
        return marker in result.stdout
    except Exception:
        return False


def start_bot(bot: dict):
    """Start a bot process."""
    print(f"  [{datetime.now().strftime('%H:%M:%S')}] Starting {bot['name']}...")
    subprocess.Popen(
        [PYTHON, bot["script"]],
        cwd=BASE_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )


def run_watchdog():
    """Monitor bots and restart if crashed."""
    print(f"Watchdog started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Monitoring {len(BOTS)} bots, checking every {CHECK_INTERVAL}s")

    while True:
        for bot in BOTS:
            if not is_running(bot["process_marker"]):
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] {bot['name']} is DOWN — restarting")
                start_bot(bot)
            # else: silently OK

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    run_watchdog()
