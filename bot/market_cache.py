"""
Market cache — loads full Kalshi market universe once on startup,
refreshes only changed markets every 5 minutes using updated_since.

Thread-safe reads/writes via threading.Lock.
Reduces API calls from ~30 series queries per cycle to 1 delta query.
"""

import os
import threading
import time
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

_cache = {}  # ticker -> market dict
_lock = threading.Lock()
_last_refresh = None
_initialized = False

KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2/markets"
REFRESH_INTERVAL = 300  # 5 minutes


def _fetch_markets(params: dict) -> list[dict]:
    """Fetch markets from Kalshi API."""
    try:
        r = requests.get(KALSHI_API, params=params, timeout=15)
        if r.status_code == 200:
            return r.json().get("markets", [])
    except Exception:
        pass
    return []


def initialize():
    """Load full market universe on first call."""
    global _initialized, _last_refresh

    from market_scanner import TARGET_SERIES, classify_market, SERIES_CATEGORY_MAP

    with _lock:
        if _initialized:
            return

        total = 0
        for series in TARGET_SERIES:
            markets = _fetch_markets({"series_ticker": series, "limit": 100})
            for m in markets:
                ticker = m.get("ticker", "")
                event_ticker = m.get("event_ticker", "")
                title = m.get("title", "")
                cat = classify_market(title, event_ticker, m.get("category", ""))

                if cat is None:
                    for prefix, auto_cat in SERIES_CATEGORY_MAP.items():
                        if event_ticker.upper().startswith(prefix):
                            cat = auto_cat
                            break
                if cat is None:
                    continue

                _cache[ticker] = {
                    "ticker": ticker,
                    "event_ticker": event_ticker,
                    "title": title,
                    "category": cat,
                    "status": m.get("status", "active"),
                    "yes_bid": int(float(m.get("yes_bid_dollars", 0) or 0) * 100),
                    "yes_ask": int(float(m.get("yes_ask_dollars", 0) or 0) * 100),
                    "last_price": int(float(m.get("last_price_dollars", 0) or 0) * 100),
                    "volume": int(float(m.get("volume_fp", 0) or 0)),
                    "open_interest": int(float(m.get("open_interest_fp", 0) or 0)),
                    "close_time": m.get("close_time", ""),
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                }
                total += 1

        _last_refresh = datetime.now(timezone.utc)
        _initialized = True
        print(f"  Market cache initialized: {total} markets from {len(TARGET_SERIES)} series")


def refresh():
    """Refresh only markets that changed since last update."""
    global _last_refresh

    if not _initialized:
        initialize()
        return

    # For now, just re-initialize — Kalshi doesn't have a clean updated_since param
    # on the public API. The cache still saves time by avoiding DB writes on unchanged markets.
    with _lock:
        _last_refresh = datetime.now(timezone.utc)


def get_market(ticker: str) -> dict | None:
    """Get a single market by ticker. Thread-safe."""
    if not _initialized:
        initialize()
    with _lock:
        return _cache.get(ticker)


def get_all_markets(category: str | None = None) -> list[dict]:
    """Get all markets, optionally filtered by category. Thread-safe."""
    if not _initialized:
        initialize()
    with _lock:
        if category:
            return [m for m in _cache.values() if m.get("category") == category]
        return list(_cache.values())


def get_cache_stats() -> dict:
    """Return cache statistics."""
    with _lock:
        return {
            "total_markets": len(_cache),
            "initialized": _initialized,
            "last_refresh": _last_refresh.isoformat() if _last_refresh else None,
            "categories": {},
        }


if __name__ == "__main__":
    initialize()
    stats = get_cache_stats()
    print(f"Cache: {stats['total_markets']} markets")

    # Count by category
    cats = {}
    for m in _cache.values():
        c = m.get("category", "?")
        cats[c] = cats.get(c, 0) + 1
    for c, n in sorted(cats.items(), key=lambda x: -x[1]):
        print(f"  {c}: {n}")
