"""
Run calibration against training data.
Sends each market to Ollama and records predictions vs actual outcomes.

WARNING: Do NOT load data/splits/test.parquet from this script.
The test set is held out until Phase 5.
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

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "qwen2.5:14b"
PROMPT_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "..", "prompts", "local_filter.txt")
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "splits", "train.parquet")
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "logs", "calibration_train.sqlite")
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "train_results.csv")

CATEGORY_MAP = {
    "economics": ["fed", "federal reserve", "interest rate", "fomc", "gdp", "economic"],
    "inflation": ["inflation", "cpi", "pce", "jobless", "unemployment", "nonfarm", "payroll"],
    "tsa": ["tsa", "passenger", "airport", "travel"],
    "weather": ["temperature", "weather", "precipitation", "rain", "snow", "heat", "cold", "hurricane"],
}


def detect_category(title: str, event_ticker: str) -> str | None:
    text = f"{title} {event_ticker}".lower()
    for cat, keywords in CATEGORY_MAP.items():
        if any(kw in text for kw in keywords):
            return cat
    return None


def load_prompt_template() -> str:
    with open(PROMPT_TEMPLATE_PATH, "r") as f:
        return f.read()


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_results (
            market_id TEXT PRIMARY KEY,
            category TEXT,
            title TEXT,
            model_probability REAL,
            market_price_at_close REAL,
            actual_outcome INTEGER,
            confidence REAL,
            relevant INTEGER,
            price_gap REAL,
            reasoning TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    return conn


def query_ollama(prompt: str, retries: int = 2) -> dict | None:
    for attempt in range(retries + 1):
        try:
            resp = requests.post(
                OLLAMA_URL,
                json={
                    "model": MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.3},
                },
                timeout=120,
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "")
            # Extract JSON
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start == -1 or end == 0:
                continue
            return json.loads(raw[start:end])
        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < retries:
                time.sleep(2)
            else:
                print(f"  Error querying Ollama: {e}")
                return None
    return None


def compute_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}

    # Win rate: model prob > 0.5 matched outcome
    predicted_yes = df["model_probability"] > 0.5
    actual_yes = df["actual_outcome"] == 1
    correct = (predicted_yes == actual_yes).sum()
    win_rate = correct / len(df)

    # Brier score
    brier = ((df["model_probability"] - df["actual_outcome"]) ** 2).mean()

    # Calibration buckets
    low = df[df["model_probability"] <= 0.3]
    mid = df[(df["model_probability"] > 0.3) & (df["model_probability"] <= 0.7)]
    high = df[df["model_probability"] > 0.7]

    buckets = {
        "0.0-0.3": {
            "count": len(low),
            "pct_resolved_no": (low["actual_outcome"] == 0).mean() if len(low) > 0 else None,
        },
        "0.3-0.7": {
            "count": len(mid),
            "pct_resolved_yes": (mid["actual_outcome"] == 1).mean() if len(mid) > 0 else None,
        },
        "0.7-1.0": {
            "count": len(high),
            "pct_resolved_yes": (high["actual_outcome"] == 1).mean() if len(high) > 0 else None,
        },
    }

    return {
        "total_markets": len(df),
        "win_rate": win_rate,
        "brier_score": brier,
        "avg_confidence": df["confidence"].mean(),
        "calibration_buckets": buckets,
    }


def print_metrics(metrics: dict, label: str = "Overall"):
    if not metrics:
        print(f"  {label}: No data")
        return
    print(f"\n{'='*50}")
    print(f"  {label}")
    print(f"{'='*50}")
    print(f"  Total markets:    {metrics['total_markets']}")
    print(f"  Win rate:         {metrics['win_rate']:.3f}")
    print(f"  Brier score:      {metrics['brier_score']:.4f}")
    print(f"  Avg confidence:   {metrics['avg_confidence']:.3f}")
    print(f"\n  Calibration buckets:")
    for bucket, data in metrics["calibration_buckets"].items():
        count = data["count"]
        if "pct_resolved_no" in data and data["pct_resolved_no"] is not None:
            print(f"    {bucket}: {count} markets, {data['pct_resolved_no']:.1%} resolved NO")
        elif "pct_resolved_yes" in data and data["pct_resolved_yes"] is not None:
            print(f"    {bucket}: {count} markets, {data['pct_resolved_yes']:.1%} resolved YES")
        else:
            print(f"    {bucket}: {count} markets")


def main():
    parser = argparse.ArgumentParser(description="Run calibration on training data")
    parser.add_argument("--category", type=str, default=None, help="Filter to single category")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of markets to process")
    args = parser.parse_args()

    if not os.path.exists(DATA_PATH):
        print(f"ERROR: Training data not found at {DATA_PATH}")
        print("Run Phase 2 data splitting first.")
        sys.exit(1)

    template = load_prompt_template()
    df = pd.read_parquet(DATA_PATH)
    print(f"Loaded {len(df)} markets from training set")

    if args.category:
        df = df[df["category"] == args.category]
        print(f"Filtered to {len(df)} markets in category: {args.category}")

    if args.limit:
        df = df.head(args.limit)
        print(f"Limited to {args.limit} markets")

    conn = init_db(DB_PATH)

    # Check which markets are already processed
    existing = set(
        row[0]
        for row in conn.execute("SELECT market_id FROM calibration_results").fetchall()
    )
    remaining = df[~df["market_id"].isin(existing)]
    print(f"Already processed: {len(existing)}, remaining: {len(remaining)}")

    results = []
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

        results.append({
            "market_id": market_id,
            "category": category,
            "model_probability": prob,
            "market_price_at_close": market_price,
            "actual_outcome": actual_outcome,
            "confidence": conf,
            "price_gap": price_gap,
        })
        print(f"prob={prob:.2f} conf={conf:.2f} gap={price_gap:.2f}")

    conn.close()

    # Load all results from DB for metrics
    all_conn = sqlite3.connect(DB_PATH)
    all_df = pd.read_sql_query("SELECT * FROM calibration_results", all_conn)
    all_conn.close()

    if all_df.empty:
        print("No results to analyze.")
        return

    # Save CSV
    all_df.to_csv(CSV_PATH, index=False)
    print(f"\nResults saved to {CSV_PATH}")

    # Print overall metrics
    print_metrics(compute_metrics(all_df), "Overall")

    # Per-category metrics
    for cat in all_df["category"].unique():
        cat_df = all_df[all_df["category"] == cat]
        print_metrics(compute_metrics(cat_df), f"Category: {cat}")


if __name__ == "__main__":
    main()
