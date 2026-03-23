"""
Order book depth analyzer.
Thin order books on Kalshi = manipulation risk.
Detects when liquidity is too thin to trade safely.
"""

import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")

MIN_SPREAD_SAFE = 5     # Spread > 5 cents = thin
MIN_VOLUME_SAFE = 100   # Volume < 100 = thin


def analyze_depth(ticker: str = None) -> list[dict]:
    """Analyze order book depth for markets we might trade."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    if ticker:
        rows = conn.execute("SELECT * FROM markets WHERE ticker = ?", (ticker,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM markets WHERE last_price > 0 AND status IN ('open', 'active')"
        ).fetchall()
    conn.close()

    warnings = []

    for m in rows:
        bid = m["yes_bid"] or 0
        ask = m["yes_ask"] or 0
        volume = m["volume"] or 0
        spread = ask - bid

        issues = []
        if spread > MIN_SPREAD_SAFE:
            issues.append(f"wide spread ({spread}c)")
        if volume < MIN_VOLUME_SAFE:
            issues.append(f"low volume ({volume})")
        if bid == 0 and ask == 0:
            issues.append("no order book")

        if issues:
            warnings.append({
                "ticker": m["ticker"],
                "title": m["title"][:60] if m["title"] else "",
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "volume": volume,
                "issues": issues,
                "safe_to_trade": len(issues) == 0,
            })

    return warnings


def is_safe_to_trade(ticker: str) -> bool:
    """Quick check if a market has enough liquidity."""
    warnings = analyze_depth(ticker)
    if not warnings:
        return True  # No data = assume safe
    return warnings[0].get("safe_to_trade", True)


if __name__ == "__main__":
    warnings = analyze_depth()
    thin = [w for w in warnings if not w["safe_to_trade"]]
    print(f"{len(thin)} thin markets out of {len(warnings)} checked")
    for w in thin[:5]:
        print(f"  {w['ticker'][:30]}: {', '.join(w['issues'])}")
