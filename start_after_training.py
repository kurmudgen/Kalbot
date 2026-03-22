"""
Auto-chain: waits for training to finish, runs validation, starts 48hr paper trading.
"""

import os
import sqlite3
import subprocess
import sys
import time

PYTHON = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRAIN_DB = os.path.join(BASE_DIR, "logs", "fast_cal_train_local_filter_20260321_165542.sqlite")
TOTAL_TRAIN = 13743


def get_train_count():
    if not os.path.exists(TRAIN_DB):
        return 0
    conn = sqlite3.connect(TRAIN_DB)
    n = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    conn.close()
    return n


def wait_for_training():
    print("=" * 50)
    print("  WAITING FOR TRAINING TO COMPLETE")
    print("=" * 50)
    while True:
        n = get_train_count()
        pct = n / TOTAL_TRAIN * 100
        remaining = (TOTAL_TRAIN - n) / 1.2 / 60
        print(f"  {n:,}/{TOTAL_TRAIN:,} ({pct:.0f}%) — ~{remaining:.0f} min remaining", end="\r")
        if n >= TOTAL_TRAIN:
            print(f"\n  Training complete! {n:,} markets processed.")
            break
        time.sleep(60)


def run_validation():
    print("\n" + "=" * 50)
    print("  RUNNING FULL VALIDATION")
    print("=" * 50)
    subprocess.run(
        [PYTHON, "calibration/run_fast_calibration.py",
         "--per-category", "50000", "--workers", "4", "--split", "val"],
        cwd=BASE_DIR,
        timeout=7200,  # 2 hours max
    )


def start_paper_trading():
    print("\n" + "=" * 50)
    print("  STARTING 48-HOUR PAPER TRADING SESSION")
    print("=" * 50)
    subprocess.run(
        [PYTHON, "bot/run_night.py"],
        cwd=BASE_DIR,
    )


def main():
    wait_for_training()
    run_validation()
    start_paper_trading()


if __name__ == "__main__":
    main()
