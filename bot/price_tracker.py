"""
Price movement tracker: monitors how market prices change over time.
Detects significant price movements that may indicate new information.
Prioritizes markets expiring within 24 hours.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
TRACKER_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "price_history.sqlite")

SIGNIFICANT_MOVE = 0.05  # 5 cent move is significant


def init_tracker_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(TRACKER_DB), exist_ok=True)
    conn = sqlite3.connect(TRACKER_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            ticker TEXT,
            last_price INTEGER,
            yes_bid INTEGER,
            yes_ask INTEGER,
            snapshot_at TEXT,
            PRIMARY KEY (ticker, snapshot_at)
        )
    """)
    conn.commit()
    return conn


def snapshot_prices():
    """Record current prices for all tracked markets."""
    if not os.path.exists(MARKETS_DB):
        return 0

    markets_conn = sqlite3.connect(MARKETS_DB)
    markets_conn.row_factory = sqlite3.Row
    markets = markets_conn.execute("SELECT * FROM markets").fetchall()
    markets_conn.close()

    tracker = init_tracker_db()
    now = datetime.now(timezone.utc).isoformat()
    count = 0

    for m in markets:
        tracker.execute(
            """INSERT OR IGNORE INTO price_snapshots
               (ticker, last_price, yes_bid, yes_ask, snapshot_at)
               VALUES (?, ?, ?, ?, ?)""",
            (m["ticker"], m["last_price"], m["yes_bid"], m["yes_ask"], now),
        )
        count += 1

    tracker.commit()
    tracker.close()
    return count


def get_movers(hours: float = 1.0) -> list[dict]:
    """Find markets with significant price movements in the last N hours."""
    if not os.path.exists(TRACKER_DB):
        return []

    conn = sqlite3.connect(TRACKER_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    # Get earliest and latest price for each ticker in the window
    rows = conn.execute("""
        SELECT ticker,
               MIN(last_price) as min_price,
               MAX(last_price) as max_price,
               (SELECT last_price FROM price_snapshots p2
                WHERE p2.ticker = p1.ticker
                ORDER BY snapshot_at ASC LIMIT 1) as first_price,
               (SELECT last_price FROM price_snapshots p2
                WHERE p2.ticker = p1.ticker
                ORDER BY snapshot_at DESC LIMIT 1) as latest_price
        FROM price_snapshots p1
        WHERE snapshot_at > ?
        GROUP BY ticker
        HAVING (MAX(last_price) - MIN(last_price)) >= ?
    """, (cutoff, SIGNIFICANT_MOVE * 100)).fetchall()

    conn.close()

    movers = []
    for r in rows:
        move = (r["latest_price"] - r["first_price"]) / 100.0
        movers.append({
            "ticker": r["ticker"],
            "move": move,
            "direction": "UP" if move > 0 else "DOWN",
            "first_price": r["first_price"] / 100.0,
            "latest_price": r["latest_price"] / 100.0,
        })

    return sorted(movers, key=lambda x: abs(x["move"]), reverse=True)


def get_expiring_soon(hours: int = 24) -> list[dict]:
    """Find markets expiring within N hours — these have the most informational edge."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    cutoff = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    now = datetime.now(timezone.utc).isoformat()

    rows = conn.execute(
        """SELECT * FROM markets
           WHERE close_time > ? AND close_time < ?
           ORDER BY close_time ASC""",
        (now, cutoff),
    ).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def prioritize_markets(markets: list[dict]) -> list[dict]:
    """Sort markets by priority: expiring soon + weather first."""
    now = datetime.now(timezone.utc)

    for m in markets:
        score = 0.0

        # Weather markets get priority
        if m.get("category") == "weather":
            score += 10.0

        # Markets expiring sooner get priority
        close_time = m.get("close_time", "")
        if close_time:
            try:
                close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                hours_until = (close_dt - now).total_seconds() / 3600
                if hours_until < 6:
                    score += 20.0  # Expiring very soon — highest priority
                elif hours_until < 24:
                    score += 10.0
                elif hours_until < 48:
                    score += 5.0
            except (ValueError, TypeError):
                pass

        m["_priority_score"] = score

    return sorted(markets, key=lambda x: x.get("_priority_score", 0), reverse=True)


if __name__ == "__main__":
    n = snapshot_prices()
    print(f"Snapshotted {n} market prices")

    movers = get_movers(hours=1)
    if movers:
        print(f"\nSignificant movers (last 1hr):")
        for m in movers[:10]:
            print(f"  {m['ticker']}: {m['direction']} {abs(m['move']):.2f} ({m['first_price']:.2f} → {m['latest_price']:.2f})")

    expiring = get_expiring_soon(24)
    print(f"\nMarkets expiring within 24hrs: {len(expiring)}")
    for m in expiring[:5]:
        print(f"  {m['title'][:60]}... closes {m['close_time']}")
