"""
Run calibration against validation data.
Identical logic to run_calibration.py but uses val.parquet.

WARNING: Do NOT load data/splits/test.parquet from this script.
The test set is held out until calibration tuning is finished.
"""

import argparse
import json
import os
import sqlite3
import sys
import time

import numpy as np
import pandas as pd
import requests

# Import shared functions from run_calibration
sys.path.insert(0, os.path.dirname(__file__))
from run_calibration import (
    compute_metrics,
    init_db,
    load_prompt_template,
    print_metrics,
    query_ollama,
)

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "splits", "val.parquet")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "calibration_val.sqlite")
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "val_results.csv")


def main():
    parser = argparse.ArgumentParser(description="Run calibration on validation data")
    parser.add_argument("--category", type=str, default=None, help="Filter to single category")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of markets to process")
    args = parser.parse_args()

    if not os.path.exists(DATA_PATH):
        print(f"ERROR: Validation data not found at {DATA_PATH}")
        print("Run Phase 2 data splitting first.")
        sys.exit(1)

    template = load_prompt_template()
    df = pd.read_parquet(DATA_PATH)
    print(f"Loaded {len(df)} markets from validation set")

    if args.category:
        df = df[df["category"] == args.category]
        print(f"Filtered to {len(df)} markets in category: {args.category}")

    if args.limit:
        df = df.head(args.limit)
        print(f"Limited to {args.limit} markets")

    conn = init_db(DB_PATH)

    existing = set(
        row[0]
        for row in conn.execute("SELECT market_id FROM calibration_results").fetchall()
    )
    remaining = df[~df["market_id"].isin(existing)]
    print(f"Already processed: {len(existing)}, remaining: {len(remaining)}")

    for i, (_, row) in enumerate(remaining.iterrows()):
        market_id = row["market_id"]
        title = row["title"]
        category = row["category"]
        actual_outcome = row["actual_outcome"]
        market_price = row.get("market_price_at_close", row.get("last_price", 50)) / 100.0

        prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}
Recent relevant headlines: No headlines available for historical data.
"""

        print(f"[{i+1}/{len(remaining)}] {title[:60]}...", end=" ", flush=True)
        result = query_ollama(prompt)

        if result is None:
            print("SKIP (no response)")
            continue

        prob = float(result.get("probability", 0.5))
        conf = float(result.get("confidence", 0.5))
        relevant = bool(result.get("relevant", True))
        reasoning = str(result.get("reasoning", ""))
        price_gap = abs(prob - market_price)

        conn.execute(
            """INSERT OR REPLACE INTO calibration_results
               (market_id, category, title, model_probability, market_price_at_close,
                actual_outcome, confidence, relevant, price_gap, reasoning, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
            (market_id, category, title, prob, market_price, actual_outcome, conf,
             int(relevant), price_gap, reasoning),
        )
        conn.commit()
        print(f"prob={prob:.2f} conf={conf:.2f} gap={price_gap:.2f}")

    conn.close()

    all_conn = sqlite3.connect(DB_PATH)
    all_df = pd.read_sql_query("SELECT * FROM calibration_results", all_conn)
    all_conn.close()

    if all_df.empty:
        print("No results to analyze.")
        return

    all_df.to_csv(CSV_PATH, index=False)
    print(f"\nResults saved to {CSV_PATH}")

    print_metrics(compute_metrics(all_df), "Validation - Overall")

    for cat in all_df["category"].unique():
        cat_df = all_df[all_df["category"] == cat]
        print_metrics(compute_metrics(cat_df), f"Validation - {cat}")


if __name__ == "__main__":
    main()
