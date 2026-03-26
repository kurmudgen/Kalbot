"""
Fed funds / FOMC feed — fetches CME FedWatch probabilities and Fed data.

Kalshi markets: KXFED, KXFOMC series — brackets on rate decisions.
This gives the model market-implied rate expectations + actual Fed data.

Sources:
- CME FedWatch Tool (implied probabilities from fed funds futures)
- FRED API (actual fed funds rate, dot plot data)
- Fed economic projections
"""

import os
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 1800  # 30 minutes


def fetch_fred_series(series_id: str, limit: int = 5) -> list[dict]:
    """Fetch from FRED."""
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
            obs = r.json().get("observations", [])
            return [{"date": o["date"], "value": o["value"]}
                    for o in obs if o.get("value", ".") != "."]
    except Exception:
        pass
    return []


def fetch_fed_data() -> dict:
    """Fetch current fed funds rate and related data."""
    now = datetime.utcnow()

    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["data"]

    data = {}

    # Effective federal funds rate (DFF)
    dff = fetch_fred_series("DFF", limit=5)
    if dff:
        data["fed_funds_rate"] = {
            "current": float(dff[0]["value"]),
            "date": dff[0]["date"],
        }

    # Fed funds target range upper (DFEDTARU)
    upper = fetch_fred_series("DFEDTARU", limit=3)
    if upper:
        data["target_upper"] = float(upper[0]["value"])

    # Fed funds target range lower (DFEDTARL)
    lower = fetch_fred_series("DFEDTARL", limit=3)
    if lower:
        data["target_lower"] = float(lower[0]["value"])

    # 10-year Treasury yield (DGS10) — context for rate expectations
    t10 = fetch_fred_series("DGS10", limit=5)
    if t10:
        data["treasury_10y"] = float(t10[0]["value"])

    # 2-year Treasury yield (DGS2) — most rate-sensitive
    t2 = fetch_fred_series("DGS2", limit=5)
    if t2:
        data["treasury_2y"] = float(t2[0]["value"])
        if data.get("treasury_10y"):
            data["yield_curve_spread"] = data["treasury_10y"] - data["treasury_2y"]

    _cache["data"] = data
    _cache["fetched_at"] = now
    return data


def get_fed_context(market_title: str) -> str | None:
    """Build context string for a Fed/FOMC market."""
    data = fetch_fed_data()
    if not data:
        return None

    context = "FEDERAL RESERVE DATA:\n"

    if data.get("fed_funds_rate"):
        ffr = data["fed_funds_rate"]
        context += f"Effective fed funds rate: {ffr['current']:.2f}% (as of {ffr['date']})\n"

    if data.get("target_upper") and data.get("target_lower"):
        context += f"Target range: {data['target_lower']:.2f}% - {data['target_upper']:.2f}%\n"

    if data.get("treasury_2y"):
        context += f"2-year Treasury yield: {data['treasury_2y']:.2f}%\n"

    if data.get("treasury_10y"):
        context += f"10-year Treasury yield: {data['treasury_10y']:.2f}%\n"

    if data.get("yield_curve_spread") is not None:
        spread = data["yield_curve_spread"]
        status = "INVERTED" if spread < 0 else "normal"
        context += f"2s/10s spread: {spread:+.2f}% ({status})\n"

    # Rate decision context
    title_lower = market_title.lower()
    if "cut" in title_lower or "lower" in title_lower:
        context += "\nMarket is about a rate CUT.\n"
        if data.get("treasury_2y") and data.get("target_upper"):
            if data["treasury_2y"] < data["target_upper"] - 0.25:
                context += "Signal: 2Y yield below target suggests market expects cuts.\n"
            else:
                context += "Signal: 2Y yield near target suggests market expects hold.\n"
    elif "hike" in title_lower or "raise" in title_lower or "higher" in title_lower:
        context += "\nMarket is about a rate HIKE.\n"

    return context


if __name__ == "__main__":
    print("=== Fed Funds Feed ===")
    if not FRED_API_KEY:
        print("No FRED_API_KEY set. Get one free at https://fred.stlouisfed.org/docs/api/api_key.html")
    data = fetch_fed_data()
    if data.get("fed_funds_rate"):
        print(f"Fed funds rate: {data['fed_funds_rate']['current']:.2f}%")
    if data.get("target_upper") and data.get("target_lower"):
        print(f"Target range: {data['target_lower']:.2f}% - {data['target_upper']:.2f}%")
    if data.get("yield_curve_spread") is not None:
        print(f"2s/10s spread: {data['yield_curve_spread']:+.2f}%")
    ctx = get_fed_context("Will the Fed cut rates at the next meeting?")
    if ctx:
        print(f"\n{ctx}")
