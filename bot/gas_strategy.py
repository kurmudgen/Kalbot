"""
Gas price strategy.
Kalshi settles on EIA national average gas price data.
Edge: gas prices are highly correlated with WTI crude oil futures,
which trade 24/7. Crude moves overnight → gas price market is stale.
"""

import os
import re
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
GAS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "gas_strategy.sqlite")

# Historical gas price weekly change std dev: ~$0.05-0.08
GAS_WEEKLY_STD = 0.06


def init_gas_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(GAS_DB), exist_ok=True)
    conn = sqlite3.connect(GAS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gas_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, title TEXT,
            gas_price REAL, crude_price REAL,
            market_price REAL, model_prob REAL, edge REAL,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_gas_and_crude() -> tuple[float | None, float | None]:
    """Get current national avg gas price and WTI crude."""
    gas_price = None
    crude_price = None

    # Gas price from FRED (EIA weekly)
    try:
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=GASREGW", timeout=10)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")
            for line in reversed(lines):
                parts = line.split(",")
                if len(parts) == 2 and parts[1] != ".":
                    try:
                        gas_price = float(parts[1])
                        break
                    except ValueError:
                        continue
    except Exception:
        pass

    # WTI crude from FRED
    try:
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO", timeout=10)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")
            for line in reversed(lines):
                parts = line.split(",")
                if len(parts) == 2 and parts[1] != ".":
                    try:
                        crude_price = float(parts[1])
                        break
                    except ValueError:
                        continue
    except Exception:
        pass

    return gas_price, crude_price


def find_gas_markets() -> list[dict]:
    if not os.path.exists(MARKETS_DB):
        return []
    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM markets WHERE status IN ('open', 'active')
        AND (title LIKE '%gas price%' OR title LIKE '%gasoline%'
             OR title LIKE '%gallon%' OR event_ticker LIKE '%GAS%'
             OR event_ticker LIKE '%FUEL%')
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def analyze_gas_markets() -> list[dict]:
    gas_price, crude_price = get_gas_and_crude()
    if gas_price is None:
        print("  Gas strategy: cannot get gas price")
        return []

    print(f"  Gas strategy: gas=${gas_price:.3f}/gal, crude=${crude_price or '?'}/bbl")

    markets = find_gas_markets()
    if not markets:
        print("  No gas price markets found")
        return []

    conn = init_gas_db()
    signals = []

    for m in markets:
        title = m["title"]
        market_price = (m.get("last_price") or 50) / 100.0

        # Parse threshold from title
        numbers = re.findall(r'\$(\d+\.?\d*)', title)
        if not numbers:
            continue

        threshold = float(numbers[0])

        from scipy.stats import norm
        z = (threshold - gas_price) / GAS_WEEKLY_STD
        if "above" in title.lower() or "exceed" in title.lower() or "more than" in title.lower():
            model_prob = 1.0 - norm.cdf(z)
        elif "below" in title.lower() or "less than" in title.lower():
            model_prob = norm.cdf(z)
        else:
            model_prob = norm.cdf(z)

        model_prob = max(0.01, min(0.99, model_prob))
        edge = model_prob - market_price

        conn.execute(
            "INSERT INTO gas_signals VALUES (NULL,?,?,?,?,?,?,?,?)",
            (m["ticker"], title, gas_price, crude_price or 0,
             market_price, model_prob, edge, datetime.now(timezone.utc).isoformat()),
        )

        if abs(edge) > 0.08:
            side = "YES" if edge > 0 else "NO"
            print(f"  SIGNAL: {side} edge={edge:+.2f} on {title[:60]}...")
            signals.append({
                "ticker": m["ticker"], "title": title, "category": "gas",
                "model_probability": model_prob, "confidence": min(0.80, 0.55 + abs(edge)),
                "market_price": market_price, "price_gap": abs(edge),
                "reasoning": f"Gas=${gas_price:.3f}, crude=${crude_price or '?'}, prob={model_prob:.2f}",
            })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    analyze_gas_markets()
