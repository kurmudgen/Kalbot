"""
Treasury yield bracket strategy.
Uses Econforecasting.com consensus + FRED yield data to price
Kalshi Treasury yield brackets (10Y daily/weekly, spreads).

Newer Kalshi category = likely less efficient = more edge.
"""

import os
import re
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
TREASURY_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "treasury_strategy.sqlite")

# Historical 10Y yield daily change stats (computed from FRED data)
# Mean absolute daily change: ~4.5 basis points
# Std dev of daily change: ~6 basis points
YIELD_DAILY_STD_BPS = 6.0


def init_treasury_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(TREASURY_DB), exist_ok=True)
    conn = sqlite3.connect(TREASURY_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS treasury_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, title TEXT, yield_type TEXT,
            current_yield REAL, bracket_low REAL, bracket_high REAL,
            market_price REAL, model_prob REAL, edge REAL, created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_current_yields() -> dict:
    """Get current Treasury yields from FRED."""
    yields = {}
    for series, name in [("DGS10", "10Y"), ("DGS2", "2Y"), ("DGS5", "5Y"), ("DGS30", "30Y")]:
        try:
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                lines = r.text.strip().split("\n")
                for line in reversed(lines):
                    parts = line.split(",")
                    if len(parts) == 2 and parts[1] != ".":
                        try:
                            yields[name] = float(parts[1])
                            break
                        except ValueError:
                            continue
        except Exception:
            pass
    return yields


def find_treasury_markets() -> list[dict]:
    if not os.path.exists(MARKETS_DB):
        return []
    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM markets WHERE status = 'open'
        AND (title LIKE '%Treasury%' OR title LIKE '%10-year%' OR title LIKE '%10Y%'
             OR title LIKE '%yield%' OR event_ticker LIKE '%TREAS%'
             OR event_ticker LIKE '%10Y%' OR event_ticker LIKE '%BOND%')
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def parse_yield_bracket(title: str) -> tuple[float, float, str] | None:
    """Extract yield bracket and type from title."""
    numbers = re.findall(r'(\d+\.?\d*)%', title)
    numbers = [float(n) for n in numbers if 0.5 < float(n) < 15]

    yield_type = "10Y"
    if "2-year" in title.lower() or "2y" in title.lower():
        yield_type = "2Y"
    elif "30-year" in title.lower() or "30y" in title.lower():
        yield_type = "30Y"
    elif "spread" in title.lower():
        yield_type = "spread"

    if len(numbers) >= 2:
        return (min(numbers), max(numbers), yield_type)
    elif len(numbers) == 1:
        n = numbers[0]
        if "above" in title.lower():
            return (n, n + 0.50, yield_type)
        elif "below" in title.lower():
            return (n - 0.50, n, yield_type)
    return None


def analyze_treasury_markets() -> list[dict]:
    yields = get_current_yields()
    if not yields:
        print("  Treasury strategy: cannot get yield data")
        return []

    print(f"  Treasury strategy: 10Y={yields.get('10Y', '?')}%, 2Y={yields.get('2Y', '?')}%")

    markets = find_treasury_markets()
    if not markets:
        print("  No Treasury bracket markets found")
        return []

    conn = init_treasury_db()
    signals = []

    for m in markets:
        parsed = parse_yield_bracket(m["title"])
        if parsed is None:
            continue

        low, high, yield_type = parsed
        current = yields.get(yield_type)
        if current is None:
            continue

        market_price = (m.get("last_price") or 50) / 100.0

        # Normal CDF with yield daily std dev
        from scipy.stats import norm
        std_pct = YIELD_DAILY_STD_BPS / 100.0  # Convert bps to percentage points
        z_low = (low - current) / std_pct
        z_high = (high - current) / std_pct
        model_prob = norm.cdf(z_high) - norm.cdf(z_low)
        model_prob = max(0.001, min(0.999, model_prob))

        edge = model_prob - market_price

        conn.execute(
            "INSERT INTO treasury_signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?)",
            (m["ticker"], m["title"], yield_type, current, low, high,
             market_price, model_prob, edge, datetime.now(timezone.utc).isoformat()),
        )

        if abs(edge) > 0.08:
            side = "YES" if edge > 0 else "NO"
            print(f"  SIGNAL: {side} edge={edge:+.2f} on {m['title'][:60]}...")
            signals.append({
                "ticker": m["ticker"], "title": m["title"], "category": "treasury",
                "model_probability": model_prob, "confidence": min(0.85, 0.6 + abs(edge)),
                "market_price": market_price, "price_gap": abs(edge),
                "reasoning": f"{yield_type}={current:.2f}%, prob={model_prob:.2f}",
            })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    analyze_treasury_markets()
