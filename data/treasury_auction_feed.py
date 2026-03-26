"""
Treasury auction feed — fetches upcoming and recent auction results.

Kalshi markets: KXTREAS, KX10Y series — brackets on yield levels.
This gives the model actual auction data + yield trends.

Source: Treasury Direct API (free, no auth)
Backup: FRED for yield data
"""

import os
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
TREASURY_API = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 1800  # 30 minutes


def fetch_recent_auctions(days: int = 30) -> list[dict]:
    """Fetch recent Treasury auction results from TreasuryDirect."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            f"{TREASURY_API}/v1/accounting/od/auctions_query",
            params={
                "filter": f"auction_date:gte:{cutoff}",
                "sort": "-auction_date",
                "page[size]": 20,
                "fields": "cusip,security_type,security_term,auction_date,high_yield,bid_to_cover_ratio,total_accepted",
            },
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get("data", [])
    except Exception:
        pass
    return []


def fetch_yield_data() -> dict:
    """Fetch current Treasury yields from FRED."""
    if not FRED_API_KEY:
        return {}

    yields = {}
    series_map = {
        "DGS1MO": "1mo",
        "DGS3MO": "3mo",
        "DGS1": "1y",
        "DGS2": "2y",
        "DGS5": "5y",
        "DGS10": "10y",
        "DGS30": "30y",
    }

    for series_id, label in series_map.items():
        try:
            r = requests.get(FRED_BASE, params={
                "series_id": series_id,
                "api_key": FRED_API_KEY,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 5,
            }, timeout=10)
            if r.status_code == 200:
                obs = r.json().get("observations", [])
                for o in obs:
                    if o.get("value", ".") != ".":
                        yields[label] = {
                            "value": float(o["value"]),
                            "date": o["date"],
                        }
                        break
        except Exception:
            continue

    return yields


def get_treasury_context(market_title: str) -> str | None:
    """Build context string for a Treasury/yield market."""
    now = datetime.utcnow()

    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            data = _cache["data"]
        else:
            data = None
    else:
        data = None

    if data is None:
        auctions = fetch_recent_auctions()
        yields = fetch_yield_data()
        data = {"auctions": auctions, "yields": yields}
        _cache["data"] = data
        _cache["fetched_at"] = now

    if not data.get("yields") and not data.get("auctions"):
        return None

    context = "TREASURY MARKET DATA:\n"

    # Current yield curve
    if data.get("yields"):
        context += "Current yields:\n"
        for tenor in ["1mo", "3mo", "1y", "2y", "5y", "10y", "30y"]:
            if tenor in data["yields"]:
                context += f"  {tenor}: {data['yields'][tenor]['value']:.2f}%\n"

        # 2s/10s spread
        if "2y" in data["yields"] and "10y" in data["yields"]:
            spread = data["yields"]["10y"]["value"] - data["yields"]["2y"]["value"]
            status = "INVERTED" if spread < 0 else "normal"
            context += f"  2s/10s spread: {spread:+.2f}% ({status})\n"

    # Recent auctions
    if data.get("auctions"):
        context += "\nRecent auctions:\n"
        for a in data["auctions"][:5]:
            high_yield = a.get("high_yield", "N/A")
            btc = a.get("bid_to_cover_ratio", "N/A")
            context += f"  {a.get('auction_date')}: {a.get('security_term')} "
            context += f"yield={high_yield}% b2c={btc}\n"

    # Extract yield threshold from market title
    import re
    match = re.search(r'(\d+\.?\d*)%', market_title)
    if match and data.get("yields"):
        threshold = float(match.group(1))
        # Try to match against 10y yield by default
        current_10y = data["yields"].get("10y", {}).get("value")
        if current_10y:
            context += f"\nMarket threshold: {threshold:.2f}%\n"
            context += f"Current 10Y: {current_10y:.2f}% "
            context += f"({'ABOVE' if current_10y > threshold else 'BELOW'} threshold)\n"

    return context


if __name__ == "__main__":
    print("=== Treasury Auction Feed ===")
    auctions = fetch_recent_auctions()
    print(f"Recent auctions: {len(auctions)}")
    for a in auctions[:3]:
        print(f"  {a.get('auction_date')}: {a.get('security_term')} yield={a.get('high_yield')}%")

    yields = fetch_yield_data()
    if yields:
        print("\nYield curve:")
        for tenor, data in sorted(yields.items()):
            print(f"  {tenor}: {data['value']:.2f}%")
