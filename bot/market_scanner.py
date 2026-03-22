"""
Market scanner: polls Kalshi API for open markets in whitelisted categories.
Writes snapshots to data/live/markets.sqlite every 5 minutes.
"""

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SCAN_INTERVAL = 300  # 5 minutes
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")

CATEGORY_KEYWORDS = {
    "economics": ["fed ", "federal reserve", "interest rate", "fomc", "gdp",
                   "economic growth", "rate cut", "rate hike", "fed funds",
                   "monetary policy", "treasury", "recession"],
    "inflation": ["inflation", " cpi", "consumer price", " pce", "personal consumption",
                  "jobless claim", "unemployment", "nonfarm", "payroll",
                  "jobs report", "initial claims", "labor market"],
    "tsa": ["tsa", "passenger volume", "airport checkpoint", "air travel",
            "tsa checkpoint"],
    "weather": ["temperature", "weather", "precipitation", "rain", "snow",
                "heat", "cold", "hurricane", "tornado", "degree",
                "fahrenheit", "high of", "low of"],
}

# Kalshi API categories that map to our target categories
KALSHI_CATEGORY_MAP = {
    "Economics": "economics",
    "Financials": "economics",
    "Climate and Weather": "weather",
}


def get_whitelisted_categories() -> list[str]:
    raw = os.getenv("WHITELISTED_CATEGORIES", "economics,tsa,weather,inflation")
    return [c.strip().lower() for c in raw.split(",")]


SERIES_CATEGORY_MAP = {
    "KXHIGH": "weather", "KXLOW": "weather",
    "KXCPI": "inflation", "KXPCE": "inflation",
    "KXINX": "economics", "KXINXD": "economics",
    "KXBTC": "economics", "KXBTCD": "economics",
    "KXTSA": "tsa", "TSA": "tsa",
    "KXFED": "economics", "KXFOMC": "economics",
    "KXGDP": "economics",
    "KXJOBLESS": "inflation", "KXNFP": "inflation",
    "KXGAS": "economics",
    "KXEURUSD": "economics", "KXUSDJPY": "economics",
    "KXTREAS": "economics", "KX10Y": "economics",
}


def classify_market(title: str, event_ticker: str, kalshi_category: str = "") -> str | None:
    text = f" {title} {event_ticker} ".lower()
    whitelist = get_whitelisted_categories()
    ticker_upper = (event_ticker or "").upper()

    # Check series ticker prefix first (most reliable)
    for prefix, cat in SERIES_CATEGORY_MAP.items():
        if ticker_upper.startswith(prefix) and cat in whitelist:
            return cat

    # Check Kalshi's own category
    if kalshi_category and kalshi_category in KALSHI_CATEGORY_MAP:
        mapped = KALSHI_CATEGORY_MAP[kalshi_category]
        if mapped in whitelist:
            return mapped

    # Fall back to keyword matching (but skip sports multi-game titles)
    if "kxmve" in text or "multigame" in text or "crosscategory" in text:
        return None  # These are sports bundles, not our markets

    for cat, keywords in CATEGORY_KEYWORDS.items():
        if cat in whitelist and any(kw in text for kw in keywords):
            return cat
    return None


def init_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            ticker TEXT PRIMARY KEY,
            event_ticker TEXT,
            title TEXT,
            category TEXT,
            status TEXT,
            yes_bid INTEGER,
            yes_ask INTEGER,
            last_price INTEGER,
            volume INTEGER,
            open_interest INTEGER,
            close_time TEXT,
            fetched_at TEXT
        )
    """)
    conn.commit()
    return conn


def scan_markets(conn: sqlite3.Connection) -> int:
    try:
        from pykalshi import KalshiClient, MarketStatus

        api_key = os.getenv("KALSHI_API_KEY", "")
        private_key_path = os.getenv("KALSHI_PRIVATE_KEY", "")

        if not api_key or not private_key_path:
            return scan_markets_public(conn)

        # Resolve relative path from project root
        if not os.path.isabs(private_key_path):
            private_key_path = os.path.join(
                os.path.dirname(__file__), "..", private_key_path
            )

        client = KalshiClient(
            api_key_id=api_key,
            private_key_path=private_key_path,
        )

        total = 0

        # Scan by series ticker for our target categories
        # Much faster than fetch_all (which pulls 6K+ junk sports markets)
        # Targeted series scan — ONLY pull our categories
        TARGET_SERIES = ["KXCPI", "KXPCE", "KXINX", "KXINXD", "KXBTC",
                         "KXFED", "KXFOMC", "KXGDP", "KXJOBLESS", "KXNFP",
                         "KXGAS", "KXTREAS", "KX10Y"]

        all_markets = []
        for series in TARGET_SERIES:
            try:
                batch = client.get_markets(series_ticker=series, limit=50)
                if batch:
                    all_markets.extend(batch)
                    print(f"  {series}: {len(batch)} markets")
            except Exception:
                pass

        # Also scan events for weather (different ticker pattern)
        try:
            events = client.get_events(status=MarketStatus.OPEN, limit=100)
            for e in events:
                et = (e.event_ticker or "").upper()
                if any(kw in et for kw in ["HIGH", "LOW", "TEMP", "WEATHER", "TSA"]):
                    try:
                        evt_markets = client.get_markets(event_ticker=e.event_ticker, limit=50)
                        all_markets.extend(evt_markets)
                        print(f"  Event {e.event_ticker}: {len(evt_markets)} markets")
                    except Exception:
                        pass
        except Exception:
            pass

        print(f"  Scanner total: {len(all_markets)} target markets")

        for m in all_markets:
            cat = classify_market(m.title or "", m.event_ticker or "", "")
            if cat is None:
                continue

            # Convert dollars to cents for consistency
            yes_bid = int(float(m.yes_bid_dollars or 0) * 100)
            yes_ask = int(float(m.yes_ask_dollars or 0) * 100)
            last_price = int(float(m.last_price_dollars or 0) * 100)

            conn.execute(
                """INSERT OR REPLACE INTO markets
                   (ticker, event_ticker, title, category, status,
                    yes_bid, yes_ask, last_price, volume, open_interest,
                    close_time, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    m.ticker,
                    m.event_ticker,
                    m.title,
                    cat,
                    m.status.value if m.status else "open",
                    yes_bid,
                    yes_ask,
                    last_price,
                    int(m.volume_fp or 0),
                    int(m.open_interest_fp or 0),
                    str(m.close_time) if m.close_time else None,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            total += 1

        conn.commit()
        client.close()
        return total

    except ImportError:
        print("pykalshi not installed")
        return scan_markets_public(conn)
    except Exception as e:
        print(f"Kalshi API error: {e}")
        return scan_markets_public(conn)


def scan_markets_public(conn: sqlite3.Connection) -> int:
    """Fallback: use Kalshi public API (no auth required for market listing)."""
    import requests

    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    total = 0
    cursor = None

    while True:
        params = {"status": "open", "limit": 200}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print("Rate limited, waiting 60s...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"Public API error: {e}")
            break

        markets = data.get("markets", [])
        cursor = data.get("cursor")

        for m in markets:
            cat = classify_market(m.get("title", ""), m.get("event_ticker", ""), m.get("category", ""))
            if cat is None:
                continue

            conn.execute(
                """INSERT OR REPLACE INTO markets
                   (ticker, event_ticker, title, category, status,
                    yes_bid, yes_ask, last_price, volume, open_interest,
                    close_time, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    m.get("ticker"),
                    m.get("event_ticker"),
                    m.get("title"),
                    cat,
                    m.get("status"),
                    m.get("yes_bid"),
                    m.get("yes_ask"),
                    m.get("last_price"),
                    m.get("volume"),
                    m.get("open_interest"),
                    m.get("close_time"),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            total += 1

        conn.commit()

        if not cursor or not markets:
            break

    return total


def main():
    print(f"Market scanner started at {datetime.now(timezone.utc).isoformat()}")
    print(f"Whitelisted categories: {get_whitelisted_categories()}")
    print(f"Scan interval: {SCAN_INTERVAL}s")

    conn = init_db()

    try:
        while True:
            ts = datetime.now(timezone.utc).isoformat()
            count = scan_markets(conn)
            print(f"[{ts}] Scanned {count} whitelisted markets")

            # Check if we should stop (e.g., running for just one scan in test mode)
            if os.getenv("SCAN_ONCE", "").lower() == "true":
                break

            time.sleep(SCAN_INTERVAL)

    except KeyboardInterrupt:
        print("\nScanner stopped by user")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
