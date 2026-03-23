"""
Cross-market correlation detector.
When CPI moves, Fed rate should too. When they diverge = opportunity.
"""

import os
import sqlite3
from datetime import datetime, timezone

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")

# Markets that should be correlated
CORRELATION_PAIRS = [
    {"a_pattern": "CPI", "b_pattern": "fed", "relationship": "CPI up → Fed less likely to cut"},
    {"a_pattern": "CPI", "b_pattern": "PCE", "relationship": "CPI and PCE move together"},
    {"a_pattern": "unemployment", "b_pattern": "fed", "relationship": "High unemployment → Fed more likely to cut"},
    {"a_pattern": "GDP", "b_pattern": "recession", "relationship": "Low GDP → recession more likely"},
    {"a_pattern": "nonfarm", "b_pattern": "unemployment", "relationship": "Strong payrolls → low unemployment"},
]


def find_divergences() -> list[dict]:
    """Find markets that should be correlated but have diverged."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row
    markets = conn.execute(
        "SELECT * FROM markets WHERE last_price > 0 AND status IN ('open', 'active')"
    ).fetchall()
    conn.close()

    divergences = []

    for pair in CORRELATION_PAIRS:
        a_markets = [m for m in markets if pair["a_pattern"].lower() in (m["title"] or "").lower()]
        b_markets = [m for m in markets if pair["b_pattern"].lower() in (m["title"] or "").lower()]

        if not a_markets or not b_markets:
            continue

        # Check if prices moved in unexpected directions
        for a in a_markets[:3]:
            for b in b_markets[:3]:
                a_price = a["last_price"] / 100.0
                b_price = b["last_price"] / 100.0

                # Flag if both are extreme in the same direction (potential divergence)
                if (a_price > 0.7 and b_price < 0.3) or (a_price < 0.3 and b_price > 0.7):
                    divergences.append({
                        "market_a": a["title"][:60],
                        "market_b": b["title"][:60],
                        "price_a": a_price,
                        "price_b": b_price,
                        "relationship": pair["relationship"],
                        "ticker_a": a["ticker"],
                        "ticker_b": b["ticker"],
                    })

    return divergences


if __name__ == "__main__":
    divs = find_divergences()
    print(f"{len(divs)} divergences found")
    for d in divs[:5]:
        print(f"  {d['market_a'][:40]} ({d['price_a']:.0%}) vs {d['market_b'][:40]} ({d['price_b']:.0%})")
        print(f"    Expected: {d['relationship']}")
