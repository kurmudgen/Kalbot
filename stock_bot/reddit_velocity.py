"""
Reddit mention velocity detector.
Catches the ACCELERATION of mentions, not the count.
A stock going from 2 to 50 mentions in 4 hours = early signal.
By the time it has 500 mentions, it's too late.
"""

import os
import re
import sqlite3
import requests
from datetime import datetime, timezone, timedelta
from collections import Counter

VELOCITY_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "reddit_velocity.sqlite")

SUBREDDITS = ["wallstreetbets", "stocks", "pennystocks", "cryptocurrency",
              "smallstreetbets", "RobinHoodPennyStocks"]

# Velocity thresholds
MIN_MENTIONS = 5          # Need at least 5 mentions to care
VELOCITY_THRESHOLD = 3.0  # 3x increase from previous check = signal


def init_velocity_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(VELOCITY_DB), exist_ok=True)
    conn = sqlite3.connect(VELOCITY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            symbol TEXT, mentions INTEGER, sentiment REAL,
            checked_at TEXT,
            PRIMARY KEY (symbol, checked_at)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS velocity_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, old_mentions INTEGER, new_mentions INTEGER,
            velocity REAL, subreddits TEXT, alerted_at TEXT
        )
    """)
    conn.commit()
    return conn


def extract_tickers(text: str) -> list[str]:
    """Extract stock tickers from text."""
    # Match $TICKER or standalone 2-5 letter all-caps words
    dollar_tickers = re.findall(r'\$([A-Z]{2,5})\b', text)
    # Common words to exclude
    exclude = {"THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL",
               "CAN", "HAS", "HER", "WAS", "ONE", "OUR", "OUT", "HIS",
               "HOW", "ITS", "MAY", "NEW", "NOW", "OLD", "SEE", "WAY",
               "WHO", "DID", "GET", "HIM", "LET", "SAY", "SHE", "TOO",
               "USE", "CEO", "CFO", "IPO", "ETF", "GDP", "CPI", "IMO",
               "YOLO", "HODL", "FOMO", "LMAO", "EDIT", "JUST", "LIKE",
               "WHAT", "THIS", "THAT", "WITH", "FROM", "HAVE", "BEEN",
               "WILL", "THEN", "THAN", "THEM", "THEY", "SOME", "WHEN",
               "VERY", "MUCH", "LONG", "SHORT", "PUTS", "CALL", "SELL",
               "MOON", "BEAR", "BULL", "PUMP", "DUMP"}
    return [t for t in dollar_tickers if t not in exclude]


def scan_reddit() -> dict[str, int]:
    """Scan Reddit for ticker mentions. Returns {symbol: count}."""
    mentions = Counter()
    headers = {"User-Agent": "KalBot/1.0"}

    for sub in SUBREDDITS:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/hot.json?limit=50",
                headers=headers, timeout=10,
            )
            if r.status_code != 200:
                continue

            posts = r.json().get("data", {}).get("children", [])
            for post in posts:
                data = post.get("data", {})
                title = data.get("title", "")
                selftext = data.get("selftext", "")[:500]
                tickers = extract_tickers(f"{title} {selftext}")
                for t in tickers:
                    mentions[t] += 1
        except Exception:
            continue

    return dict(mentions)


def detect_velocity() -> list[dict]:
    """Detect stocks with accelerating mention velocity."""
    conn = init_velocity_db()
    now = datetime.now(timezone.utc).isoformat()

    # Get current mentions
    current = scan_reddit()

    # Get previous snapshot (last check)
    previous = {}
    rows = conn.execute(
        "SELECT symbol, mentions FROM snapshots WHERE checked_at = (SELECT MAX(checked_at) FROM snapshots)"
    ).fetchall()
    for r in rows:
        previous[r[0]] = r[1]

    # Store current snapshot
    for symbol, count in current.items():
        conn.execute(
            "INSERT OR REPLACE INTO snapshots (symbol, mentions, sentiment, checked_at) VALUES (?, ?, 0, ?)",
            (symbol, count, now),
        )

    # Detect velocity spikes
    alerts = []
    for symbol, new_count in current.items():
        if new_count < MIN_MENTIONS:
            continue

        old_count = previous.get(symbol, 0)
        if old_count == 0:
            old_count = 1  # Avoid division by zero

        velocity = new_count / old_count

        if velocity >= VELOCITY_THRESHOLD and new_count >= MIN_MENTIONS:
            alert = {
                "symbol": symbol,
                "old_mentions": old_count,
                "new_mentions": new_count,
                "velocity": velocity,
            }
            alerts.append(alert)

            conn.execute(
                "INSERT INTO velocity_alerts (symbol, old_mentions, new_mentions, velocity, subreddits, alerted_at) VALUES (?, ?, ?, ?, ?, ?)",
                (symbol, old_count, new_count, velocity, ",".join(SUBREDDITS), now),
            )

            print(f"  VELOCITY: {symbol} {old_count} -> {new_count} mentions ({velocity:.1f}x)")

    conn.commit()
    conn.close()
    return alerts


if __name__ == "__main__":
    print("Reddit Velocity Scanner")
    print("=" * 40)
    mentions = scan_reddit()
    print(f"Top mentioned tickers:")
    for sym, count in sorted(mentions.items(), key=lambda x: -x[1])[:10]:
        print(f"  ${sym}: {count} mentions")

    alerts = detect_velocity()
    print(f"\n{len(alerts)} velocity alerts")
