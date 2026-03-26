"""
Jobless claims feed — fetches initial/continuing claims data from FRED (free).

Kalshi markets: KXJOBLESS series — brackets on weekly initial claims numbers.
This feed gives the local model actual DOL data + FRED forecasts.

Source: FRED API (free, 120 requests/min)
Series: ICSA (initial claims), CCSA (continuing claims)
"""

import os
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 3600  # 1 hour (claims are weekly)


def fetch_fred_series(series_id: str, limit: int = 10) -> list[dict]:
    """Fetch recent observations from FRED."""
    if not FRED_API_KEY:
        return []
    try:
        r = requests.get(FRED_BASE, params={
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": limit,
        }, timeout=10)
        if r.status_code == 200:
            return r.json().get("observations", [])
    except Exception:
        pass
    return []


def fetch_claims_data() -> dict:
    """Fetch initial and continuing claims data."""
    now = datetime.utcnow()

    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["data"]

    initial = fetch_fred_series("ICSA", limit=8)
    continuing = fetch_fred_series("CCSA", limit=8)

    # Parse into clean format
    def parse_obs(obs_list):
        results = []
        for o in obs_list:
            val = o.get("value", ".")
            if val == ".":
                continue
            results.append({
                "date": o.get("date", ""),
                "value": int(float(val)),
            })
        return results

    data = {
        "initial_claims": parse_obs(initial),
        "continuing_claims": parse_obs(continuing),
    }

    # Compute trend
    if len(data["initial_claims"]) >= 4:
        recent_avg = sum(d["value"] for d in data["initial_claims"][:4]) / 4
        data["4wk_avg"] = int(recent_avg)
        if len(data["initial_claims"]) >= 8:
            prior_avg = sum(d["value"] for d in data["initial_claims"][4:8]) / 4
            data["trend"] = "rising" if recent_avg > prior_avg * 1.02 else \
                           "falling" if recent_avg < prior_avg * 0.98 else "stable"
        else:
            data["trend"] = "unknown"
    else:
        data["4wk_avg"] = None
        data["trend"] = "unknown"

    _cache["data"] = data
    _cache["fetched_at"] = now
    return data


def get_jobless_context(market_title: str) -> str | None:
    """Build context string for a jobless claims market."""
    data = fetch_claims_data()
    if not data.get("initial_claims"):
        return None

    latest = data["initial_claims"][0]
    prev = data["initial_claims"][1] if len(data["initial_claims"]) > 1 else None

    context = (
        f"DOL JOBLESS CLAIMS DATA:\n"
        f"Latest initial claims: {latest['value']:,} (week of {latest['date']})\n"
    )
    if prev:
        change = latest["value"] - prev["value"]
        context += f"Previous week: {prev['value']:,} (change: {change:+,})\n"
    if data.get("4wk_avg"):
        context += f"4-week average: {data['4wk_avg']:,}\n"
    if data.get("trend"):
        context += f"Trend: {data['trend']}\n"
    if data.get("continuing_claims"):
        cc = data["continuing_claims"][0]
        context += f"Continuing claims: {cc['value']:,} (week of {cc['date']})\n"

    # Extract bracket threshold from market title if possible
    import re
    match = re.search(r'(\d{3})[,.]?(\d{3})', market_title)
    if match:
        threshold = int(match.group(1) + match.group(2))
        if latest["value"] > 0:
            context += f"\nMarket threshold: {threshold:,}\n"
            context += f"Current vs threshold: {latest['value']:,} vs {threshold:,} "
            if latest["value"] > threshold:
                context += "(ABOVE)\n"
            else:
                context += "(BELOW)\n"

    return context


# Fallback: scrape DOL press release if no FRED key
def fetch_dol_fallback() -> dict | None:
    """Scrape DOL weekly claims press release (no API key needed)."""
    try:
        r = requests.get(
            "https://www.dol.gov/ui/data.pdf",
            timeout=10,
            headers={"User-Agent": "KalBot/1.0"},
        )
        # PDF parsing would go here — for now just return None
        return None
    except Exception:
        return None


if __name__ == "__main__":
    print("=== Jobless Claims Feed ===")
    if not FRED_API_KEY:
        print("No FRED_API_KEY set. Get one free at https://fred.stlouisfed.org/docs/api/api_key.html")
    data = fetch_claims_data()
    if data.get("initial_claims"):
        print(f"Latest: {data['initial_claims'][0]['value']:,}")
        print(f"4wk avg: {data.get('4wk_avg', 'N/A')}")
        print(f"Trend: {data.get('trend', 'unknown')}")
    else:
        print("No data available (need FRED_API_KEY)")
    ctx = get_jobless_context("Will initial jobless claims be above 225,000?")
    if ctx:
        print(f"\n{ctx}")
