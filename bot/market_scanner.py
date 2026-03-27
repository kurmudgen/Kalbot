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

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

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
    "congressional": ["congress", "congressional", "senator", "representative",
                      "stock act", "insider trading", "disclosure"],
    "energy": ["oil", "petroleum", "crude", "gasoline", "natural gas",
               "eia", "opec", "barrel", "energy price"],
    "entertainment": ["box office", "movie", "opening weekend", "gross",
                      "streaming", "netflix", "viewership"],
}

# Kalshi API categories that map to our target categories
KALSHI_CATEGORY_MAP = {
    "Economics": "economics",
    "Financials": "economics",
    "Climate and Weather": "weather",
    "Politics": "congressional",
    "Entertainment": "entertainment",
    "Energy": "energy",
}


def get_whitelisted_categories() -> list[str]:
    raw = os.getenv("WHITELISTED_CATEGORIES", "economics,tsa,weather,inflation,congressional,energy,entertainment")
    return [c.strip().lower() for c in raw.split(",")]


SERIES_CATEGORY_MAP = {
    # Weather
    "KXHIGH": "weather", "KXLOW": "weather",
    # Inflation / Labor
    "KXCPI": "inflation", "KXPCE": "inflation",
    "KXJOBLESS": "inflation", "KXNFP": "inflation",
    # Economics / Financial
    "KXINX": "economics", "KXINXD": "economics",
    "KXBTC": "economics", "KXBTCD": "economics",
    "KXFED": "economics", "KXFOMC": "economics",
    "KXGDP": "economics",
    # FOREX DISABLED — 1W/8L, bracket flooding, no calibrated edge
    # "KXEURUSD": "economics", "KXUSDJPY": "economics",
    "KXTREAS": "economics", "KX10Y": "economics",
    # TSA
    "KXTSA": "tsa", "TSA": "tsa",
    # Energy
    "KXGAS": "energy", "KXOIL": "energy", "KXWTI": "energy",
    # Congressional / Politics
    "KXCONG": "congressional", "KXSTOCK": "congressional",
    # Entertainment
    "KXMOVIE": "entertainment", "KXBOX": "entertainment",
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
    """Scan using raw REST API — pykalshi SDK hangs on some calls."""
    import requests as req

    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    total = 0

    TARGET_SERIES = [
        # Economics / Fed / GDP
        "KXCPI", "KXPCE", "KXFED", "KXFOMC", "KXGDP",
        # Labor market
        "KXJOBLESS", "KXNFP",
        # Financial / Rates
        "KXTREAS", "KX10Y",
        # Energy
        "KXGAS", "KXOIL", "KXWTI",
        # Crypto
        "KXBTC",
        # Weather — city-specific (full city codes, verified working)
        "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHHOU",
        "KXHIGHDEN", "KXHIGHAUS", "KXHIGHLA", "KXHIGHPHX",
        "KXHIGHSEA", "KXHIGHDFW", "KXHIGHPHI",
        "KXLOWNY", "KXLOWCHI", "KXLOWMIA", "KXLOWHOU",
        "KXLOWDEN", "KXLOWAUS",
        # TSA
        "TSA", "KXTSA",
        # Congressional / Politics
        "KXCONG", "KXSTOCK",
        # Entertainment
        "KXMOVIE", "KXBOX",
        # Forex — DISABLED (1W/8L, -$18.80, bracket flooding)
        # "KXEURUSD", "KXUSDJPY",
    ]

    for series in TARGET_SERIES:
        try:
            r = req.get(url, params={"series_ticker": series, "limit": 100},
                        timeout=10)
            if r.status_code != 200:
                continue
            markets = r.json().get("markets", [])
            if not markets:
                continue

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

                # API returns dollars — convert to cents for DB
                yes_bid = int(float(m.get("yes_bid_dollars", 0) or 0) * 100)
                yes_ask = int(float(m.get("yes_ask_dollars", 0) or 0) * 100)
                last_price = int(float(m.get("last_price_dollars", 0) or 0) * 100)
                volume = int(float(m.get("volume_fp", 0) or 0))
                oi = int(float(m.get("open_interest_fp", 0) or 0))

                conn.execute(
                    """INSERT OR REPLACE INTO markets
                       (ticker, event_ticker, title, category, status,
                        yes_bid, yes_ask, last_price, volume, open_interest,
                        close_time, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ticker, event_ticker, title, cat,
                     m.get("status", "active"),
                     yes_bid, yes_ask, last_price, volume, oi,
                     m.get("close_time", ""),
                     datetime.now(timezone.utc).isoformat()),
                )
                total += 1

            print(f"  {series}: {len(markets)} markets")
        except req.Timeout:
            print(f"  {series}: timeout")
        except Exception as e:
            print(f"  {series}: {e}")

    conn.commit()
    return total


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
