"""
Persistent calibration runner — restarts automatically if interrupted.
Keeps running until all markets are processed.
"""

import subprocess
import sys
import time
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "fast_cal_train_local_filter_20260321_165542.sqlite")
TOTAL = 13743
PYTHON = sys.executable


def get_count():
    conn = sqlite3.connect(DB_PATH)
    n = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    conn.close()
    return n


def main():
    while True:
        n = get_count()
        if n >= TOTAL:
            print(f"\nDone! {n}/{TOTAL} markets processed.")
            break

        remaining = TOTAL - n
        print(f"\n[{n}/{TOTAL}] ({n/TOTAL:.0%}) — {remaining} remaining, restarting batch...")

        try:
            subprocess.run(
                [PYTHON, "calibration/run_fast_calibration.py",
                 "--per-category", "50000", "--workers", "4",
                 "--split", "train", "--resume", "train_local_filter_20260321_165542"],
                cwd=os.path.join(os.path.dirname(__file__), ".."),
                timeout=480,  # 8 minutes max per batch
            )
        except subprocess.TimeoutExpired:
            print("Batch timed out (8 min), restarting...")
        except Exception as e:
            print(f"Error: {e}, retrying in 5s...")
            time.sleep(5)

    # Print final results
    print("\nRunning final metrics...")
    subprocess.run(
        [PYTHON, "-c", f"""
import pandas as pd, sqlite3
conn = sqlite3.connect('{DB_PATH}')
df = pd.read_sql_query('SELECT * FROM results', conn)
conn.close()
print(f'Total: {{len(df)}}')
wr = ((df['model_probability'] > 0.5) == (df['actual_outcome'] == 1)).mean()
brier = ((df['model_probability'] - df['actual_outcome']) ** 2).mean()
print(f'Win rate: {{wr:.1%}}')
print(f'Brier: {{brier:.4f}}')
for cat in sorted(df['category'].unique()):
    c = df[df['category']==cat]
    cwr = ((c['model_probability'] > 0.5) == (c['actual_outcome']==1)).mean()
    cbr = ((c['model_probability'] - c['actual_outcome'])**2).mean()
    print(f'  {{cat:<12}} n={{len(c):>5}} WR={{cwr:.1%}} Brier={{cbr:.4f}}')
"""],
    )


if __name__ == "__main__":
    main()
