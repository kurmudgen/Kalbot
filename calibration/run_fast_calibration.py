"""
Fast calibration runner with concurrent Ollama requests and strategic subsampling.
Optimized for speed while maintaining statistical validity.

WARNING: Do NOT load data/splits/test.parquet from this script.

Usage:
    python run_fast_calibration.py                    # 200 per category, 4 concurrent
    python run_fast_calibration.py --per-category 100 # fewer per category
    python run_fast_calibration.py --workers 2        # fewer concurrent requests
    python run_fast_calibration.py --category tsa     # single category
    python run_fast_calibration.py --split val        # run on validation set
    python run_fast_calibration.py --prompt prompts/v2.txt  # test a different prompt
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import numpy as np
import pandas as pd
import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:14b"
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DEFAULT_PROMPT = os.path.join(BASE_DIR, "prompts", "local_filter.txt")


def load_data(split: str, category: str | None, per_category: int) -> pd.DataFrame:
    path = os.path.join(BASE_DIR, "data", "splits", f"{split}.parquet")
    if not os.path.exists(path):
        print(f"ERROR: {path} not found")
        sys.exit(1)

    df = pd.read_parquet(path)

    if category:
        df = df[df["category"] == category]
        if len(df) == 0:
            print(f"No markets in category '{category}'")
            sys.exit(1)
        # Sample up to per_category
        if len(df) > per_category:
            df = df.sample(n=per_category, random_state=42)
    else:
        # Stratified sample: per_category from each category
        sampled = []
        for cat in df["category"].unique():
            cat_df = df[df["category"] == cat]
            n = min(len(cat_df), per_category)
            sampled.append(cat_df.sample(n=n, random_state=42))
        df = pd.concat(sampled, ignore_index=True)

    return df


def query_ollama(prompt: str, timeout: int = 90) -> dict | None:
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 150},
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "")
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        return json.loads(raw[start:end])
    except Exception:
        return None


def process_market(row: dict, template: str) -> dict | None:
    market_id = row["market_id"]
    title = row["title"]
    category = row["category"]
    actual_outcome = row["actual_outcome"]
    market_price = row.get("market_price_at_close", row.get("last_price", 50))
    if market_price > 1:
        market_price = market_price / 100.0

    # Support both raw templates and templates with {title}/{category}/{price} placeholders
    if "{title}" in template:
        prompt = template.replace("{title}", title).replace("{category}", category).replace("{price}", f"{market_price:.2f}")
    else:
        prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}
Recent relevant headlines: No headlines available for historical data.
"""

    result = query_ollama(prompt)
    if result is None:
        return None

    prob = float(result.get("probability", 0.5))
    conf = float(result.get("confidence", 0.5))
    relevant = bool(result.get("relevant", True))
    reasoning = str(result.get("reasoning", ""))
    price_gap = abs(prob - market_price)

    return {
        "market_id": market_id,
        "category": category,
        "title": title,
        "model_probability": prob,
        "market_price_at_close": market_price,
        "actual_outcome": actual_outcome,
        "confidence": conf,
        "relevant": int(relevant),
        "price_gap": price_gap,
        "reasoning": reasoning,
    }


def compute_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}

    predicted_yes = df["model_probability"] > 0.5
    actual_yes = df["actual_outcome"] == 1
    correct = (predicted_yes == actual_yes).sum()
    win_rate = correct / len(df)

    brier = ((df["model_probability"] - df["actual_outcome"]) ** 2).mean()

    # Calibration buckets
    buckets = {}
    for lo, hi, label in [(0, 0.3, "0.0-0.3"), (0.3, 0.7, "0.3-0.7"), (0.7, 1.01, "0.7-1.0")]:
        bucket = df[(df["model_probability"] >= lo) & (df["model_probability"] < hi)]
        if len(bucket) > 0:
            yes_rate = bucket["actual_outcome"].mean()
            buckets[label] = {"count": len(bucket), "actual_yes_rate": yes_rate}
        else:
            buckets[label] = {"count": 0, "actual_yes_rate": None}

    # Edge detection: where model disagrees with market most
    high_gap = df[df["price_gap"] > 0.15]
    if len(high_gap) > 0:
        gap_correct = (
            (high_gap["model_probability"] > 0.5) == (high_gap["actual_outcome"] == 1)
        ).mean()
    else:
        gap_correct = None

    return {
        "total": len(df),
        "win_rate": win_rate,
        "brier": brier,
        "avg_confidence": df["confidence"].mean(),
        "avg_price_gap": df["price_gap"].mean(),
        "buckets": buckets,
        "high_gap_accuracy": gap_correct,
        "high_gap_count": len(high_gap),
    }


def print_results(all_results: pd.DataFrame, label: str):
    print(f"\n{'='*65}")
    print(f"  {label}")
    print(f"{'='*65}")

    overall = compute_metrics(all_results)
    if not overall:
        print("  No results.")
        return

    print(f"  Markets evaluated:  {overall['total']}")
    print(f"  Win rate:           {overall['win_rate']:.1%}")
    print(f"  Brier score:        {overall['brier']:.4f}")
    print(f"  Avg confidence:     {overall['avg_confidence']:.3f}")
    print(f"  Avg price gap:      {overall['avg_price_gap']:.3f}")

    if overall["high_gap_accuracy"] is not None:
        print(f"  High-gap accuracy:  {overall['high_gap_accuracy']:.1%} ({overall['high_gap_count']} markets with gap > 0.15)")

    print(f"\n  Calibration:")
    for bucket, data in overall["buckets"].items():
        if data["actual_yes_rate"] is not None:
            print(f"    {bucket}: {data['count']} markets, {data['actual_yes_rate']:.1%} actually YES")
        else:
            print(f"    {bucket}: 0 markets")

    # Per category
    print(f"\n  {'Category':<12} {'N':>5} {'WinRate':>8} {'Brier':>8} {'AvgGap':>8}")
    print(f"  {'-'*45}")
    for cat in sorted(all_results["category"].unique()):
        cat_df = all_results[all_results["category"] == cat]
        m = compute_metrics(cat_df)
        print(f"  {cat:<12} {m['total']:>5} {m['win_rate']:>8.1%} {m['brier']:>8.4f} {m['avg_price_gap']:>8.3f}")


def main():
    parser = argparse.ArgumentParser(description="Fast calibration runner")
    parser.add_argument("--split", default="train", choices=["train", "val"],
                        help="Which split to use")
    parser.add_argument("--per-category", type=int, default=200,
                        help="Markets per category to sample")
    parser.add_argument("--category", type=str, default=None,
                        help="Single category to run")
    parser.add_argument("--workers", type=int, default=4,
                        help="Concurrent Ollama requests")
    parser.add_argument("--prompt", type=str, default=None,
                        help="Path to prompt template (default: prompts/local_filter.txt)")
    args = parser.parse_args()

    prompt_path = args.prompt or DEFAULT_PROMPT
    with open(prompt_path) as f:
        template = f.read()

    prompt_name = os.path.basename(prompt_path).replace(".txt", "")
    run_id = f"{args.split}_{prompt_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    df = load_data(args.split, args.category, args.per_category)
    total = len(df)
    print(f"Fast calibration: {total} markets, {args.workers} workers, split={args.split}")
    print(f"Prompt: {prompt_path}")
    print(f"Run ID: {run_id}")

    # Prepare DB
    db_path = os.path.join(BASE_DIR, "logs", f"fast_cal_{run_id}.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS results (
            market_id TEXT PRIMARY KEY,
            category TEXT, title TEXT,
            model_probability REAL, market_price_at_close REAL,
            actual_outcome INTEGER, confidence REAL, relevant INTEGER,
            price_gap REAL, reasoning TEXT
        )
    """)
    conn.commit()

    # Process with thread pool
    rows = df.to_dict("records")
    results = []
    completed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_market, row, template): row
            for row in rows
        }

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            elapsed = time.time() - start_time
            rate = completed / elapsed
            eta = (total - completed) / rate if rate > 0 else 0

            if result:
                results.append(result)
                conn.execute(
                    """INSERT OR REPLACE INTO results VALUES
                       (?,?,?,?,?,?,?,?,?,?)""",
                    (result["market_id"], result["category"], result["title"],
                     result["model_probability"], result["market_price_at_close"],
                     result["actual_outcome"], result["confidence"],
                     result["relevant"], result["price_gap"], result["reasoning"]),
                )
                conn.commit()
                status = f"p={result['model_probability']:.2f}"
            else:
                status = "SKIP"

            print(f"  [{completed}/{total}] {status} | {rate:.1f}/s | ETA {eta:.0f}s", end="\r")

    conn.close()
    print(f"\n\nCompleted {len(results)}/{total} in {time.time()-start_time:.0f}s")

    # Results
    results_df = pd.DataFrame(results)
    csv_path = os.path.join(BASE_DIR, "calibration", f"fast_{run_id}.csv")
    results_df.to_csv(csv_path, index=False)

    print_results(results_df, f"FAST CALIBRATION: {run_id}")

    print(f"\nResults saved to: {csv_path}")
    print(f"SQLite: {db_path}")


if __name__ == "__main__":
    main()
