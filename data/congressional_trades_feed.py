"""
Congressional trades feed — fetches recent Congress member stock trades
from House Stock Watcher (free, no auth).

Kalshi has congressional trading markets. This gives the local model
the actual filing data to score against.

Source: housestockwatcher.com (free JSON API)
Backup: quiverquant.com (free tier)
"""

import os
import requests
from datetime import datetime, timedelta

HOUSE_URL = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
SENATE_URL = "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json"

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 1800  # 30 minutes


def fetch_congressional_trades(days: int = 7) -> list[dict]:
    """Fetch recent congressional stock trades."""
    now = datetime.utcnow()

    # Cache check
    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["data"]

    trades = []
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")

    # House trades
    try:
        r = requests.get(HOUSE_URL, timeout=15)
        if r.status_code == 200:
            for t in r.json():
                tx_date = t.get("transaction_date", "")
                if tx_date >= cutoff:
                    trades.append({
                        "chamber": "House",
                        "representative": t.get("representative", ""),
                        "ticker": t.get("ticker", ""),
                        "type": t.get("type", ""),  # purchase or sale
                        "amount": t.get("amount", ""),
                        "date": tx_date,
                        "disclosure_date": t.get("disclosure_date", ""),
                    })
    except Exception:
        pass

    # Senate trades
    try:
        r = requests.get(SENATE_URL, timeout=15)
        if r.status_code == 200:
            for t in r.json():
                tx_date = t.get("transaction_date", "")
                if tx_date >= cutoff:
                    trades.append({
                        "chamber": "Senate",
                        "representative": t.get("senator", ""),
                        "ticker": t.get("ticker", ""),
                        "type": t.get("type", ""),
                        "amount": t.get("amount", ""),
                        "date": tx_date,
                        "disclosure_date": t.get("disclosure_date", ""),
                    })
    except Exception:
        pass

    # Sort by date descending
    trades.sort(key=lambda x: x.get("date", ""), reverse=True)

    _cache["data"] = trades
    _cache["fetched_at"] = now
    return trades


def get_congressional_context(market_title: str) -> str | None:
    """Build context string for a congressional trading market."""
    trades = fetch_congressional_trades(days=14)
    if not trades:
        return None

    # Count buys vs sells in the last week
    buys = [t for t in trades if "purchase" in (t.get("type") or "").lower()]
    sells = [t for t in trades if "sale" in (t.get("type") or "").lower()]

    # Top traded tickers
    from collections import Counter
    ticker_counts = Counter(t["ticker"] for t in trades if t.get("ticker"))
    top_tickers = ticker_counts.most_common(5)

    # Notable large trades
    large_trades = [t for t in trades if "$1,000,001" in (t.get("amount") or "")
                    or "$500,001" in (t.get("amount") or "")]

    context = (
        f"CONGRESSIONAL TRADING DATA (last 14 days):\n"
        f"Total filings: {len(trades)} ({len(buys)} purchases, {len(sells)} sales)\n"
        f"Top traded tickers: {', '.join(f'{t[0]}({t[1]})' for t in top_tickers)}\n"
    )

    if large_trades:
        context += f"Large trades (>$500K): {len(large_trades)} filed\n"
        for lt in large_trades[:3]:
            context += f"  {lt['representative']}: {lt['type']} {lt['ticker']} {lt['amount']} on {lt['date']}\n"

    return context


if __name__ == "__main__":
    print("=== Congressional Trades Feed ===")
    trades = fetch_congressional_trades(days=7)
    print(f"Trades in last 7 days: {len(trades)}")
    for t in trades[:5]:
        print(f"  {t['date']} {t['chamber']} {t['representative']}: {t['type']} {t['ticker']} {t['amount']}")
    ctx = get_congressional_context("")
    if ctx:
        print(f"\n{ctx}")
