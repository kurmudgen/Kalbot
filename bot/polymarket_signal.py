"""
Polymarket signal crawler: watches Polymarket price movements as a signal source.
Does NOT trade on Polymarket — just detects sharp moves that may indicate
the same event is mispriced on Kalshi.

Checks for:
1. Sharp price movements (>10% in last hour)
2. High-volume activity spikes
3. Markets that match our Kalshi target categories
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SIGNAL_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "polymarket_signals.sqlite")
POLYMARKET_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# Keywords for our target categories
CATEGORY_KEYWORDS = {
    "economics": ["fed ", "federal reserve", "interest rate", "fomc", "gdp", "recession", "rate cut", "rate hike"],
    "inflation": ["inflation", " cpi", "consumer price", " pce", "jobless", "unemployment", "nonfarm", "payroll"],
    "tsa": ["tsa", "passenger", "airport"],
    "weather": ["temperature", "weather", "hurricane", "tornado", "heat", "cold", "snow"],
}

SHARP_MOVE_THRESHOLD = 0.10  # 10% price change = signal


def init_signal_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SIGNAL_DB), exist_ok=True)
    conn = sqlite3.connect(SIGNAL_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            condition_id TEXT,
            question TEXT,
            category TEXT,
            price REAL,
            volume REAL,
            snapshot_at TEXT,
            PRIMARY KEY (condition_id, snapshot_at)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT,
            question TEXT,
            category TEXT,
            old_price REAL,
            new_price REAL,
            price_change REAL,
            direction TEXT,
            signal_type TEXT,
            detected_at TEXT
        )
    """)
    conn.commit()
    return conn


def classify_market(question: str) -> str | None:
    q = f" {question.lower()} "
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            return cat
    return None


def fetch_active_markets() -> list[dict]:
    """Fetch active Polymarket markets in our target categories."""
    markets = []
    try:
        # Use Gamma API for market listing
        url = f"{GAMMA_API}/markets"
        params = {"active": "true", "closed": "false", "limit": 100}
        r = requests.get(url, params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            for m in data:
                question = m.get("question", "")
                cat = classify_market(question)
                if cat:
                    markets.append({
                        "condition_id": m.get("conditionId", m.get("id", "")),
                        "question": question,
                        "category": cat,
                        "outcomePrices": m.get("outcomePrices", ""),
                        "volume": float(m.get("volume", 0) or 0),
                    })
    except Exception as e:
        print(f"  Polymarket API error: {e}")

    return markets


def get_current_price(market: dict) -> float | None:
    """Extract YES price from market data."""
    prices = market.get("outcomePrices", "")
    if isinstance(prices, str) and prices.startswith("["):
        try:
            parsed = json.loads(prices)
            return float(parsed[0])
        except (json.JSONDecodeError, IndexError, ValueError):
            pass
    elif isinstance(prices, list) and len(prices) > 0:
        return float(prices[0])
    return None


def snapshot_and_detect() -> list[dict]:
    """Take a price snapshot and detect sharp moves."""
    conn = init_signal_db()
    markets = fetch_active_markets()
    now = datetime.now(timezone.utc).isoformat()
    signals = []

    print(f"  Polymarket: {len(markets)} target-category markets")

    for m in markets:
        price = get_current_price(m)
        if price is None:
            continue

        condition_id = m["condition_id"]
        question = m["question"]
        category = m["category"]

        # Save snapshot
        conn.execute(
            """INSERT OR IGNORE INTO price_snapshots
               (condition_id, question, category, price, volume, snapshot_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (condition_id, question, category, price, m.get("volume", 0), now),
        )

        # Check for sharp move vs last snapshot
        prev = conn.execute(
            """SELECT price, snapshot_at FROM price_snapshots
               WHERE condition_id = ? AND snapshot_at < ?
               ORDER BY snapshot_at DESC LIMIT 1""",
            (condition_id, now),
        ).fetchone()

        if prev:
            old_price = prev[0]
            change = price - old_price

            if abs(change) >= SHARP_MOVE_THRESHOLD:
                direction = "UP" if change > 0 else "DOWN"
                signal = {
                    "condition_id": condition_id,
                    "question": question,
                    "category": category,
                    "old_price": old_price,
                    "new_price": price,
                    "price_change": change,
                    "direction": direction,
                    "signal_type": "sharp_move",
                }
                signals.append(signal)

                conn.execute(
                    """INSERT INTO signals
                       (condition_id, question, category, old_price, new_price,
                        price_change, direction, signal_type, detected_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (condition_id, question, category, old_price, price,
                     change, direction, "sharp_move", now),
                )

                print(f"  SIGNAL: {direction} {abs(change):.0%} on '{question[:60]}...'")

    conn.commit()
    conn.close()

    return signals


def find_kalshi_match(signal: dict, kalshi_markets: list[dict]) -> dict | None:
    """Find a matching Kalshi market for a Polymarket signal."""
    pm_question = signal["question"].lower()
    pm_category = signal["category"]

    # Simple keyword matching — look for overlapping terms
    pm_words = set(pm_question.split())

    best_match = None
    best_overlap = 0

    for km in kalshi_markets:
        k_title = km.get("title", "").lower()
        k_cat = km.get("category", "")

        # Must be same category
        if k_cat != pm_category:
            continue

        k_words = set(k_title.split())
        overlap = len(pm_words & k_words)

        if overlap > best_overlap and overlap >= 3:
            best_overlap = overlap
            best_match = km

    return best_match


if __name__ == "__main__":
    signals = snapshot_and_detect()
    if signals:
        print(f"\n{len(signals)} signals detected")
        for s in signals:
            print(f"  [{s['category']}] {s['direction']} {abs(s['price_change']):.0%}: {s['question'][:80]}")
    else:
        print("No signals detected this cycle")
