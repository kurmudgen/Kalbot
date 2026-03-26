"""
EIA petroleum feed — fetches weekly petroleum status report data.

Kalshi markets: KXGAS series — brackets on gas price and inventory levels.
This feed gives the model actual EIA data before Kalshi settlement.

Source: EIA API v2 (free, requires API key)
Fallback: EIA weekly petroleum status report page
"""

import os
import requests
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
EIA_BASE = "https://api.eia.gov/v2"

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 3600  # 1 hour


def fetch_eia_series(series_id: str, frequency: str = "weekly", limit: int = 10) -> list[dict]:
    """Fetch data from EIA API v2."""
    if not EIA_API_KEY:
        return []
    try:
        r = requests.get(f"{EIA_BASE}/petroleum/sum/sndw/data/", params={
            "api_key": EIA_API_KEY,
            "frequency": frequency,
            "data[0]": "value",
            "facets[series][]": series_id,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": limit,
        }, timeout=15)
        if r.status_code == 200:
            return r.json().get("response", {}).get("data", [])
    except Exception:
        pass
    return []


def fetch_gas_prices() -> dict:
    """Fetch current gas price data from EIA."""
    now = datetime.utcnow()

    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["data"]

    data = {}

    # Regular gasoline retail price
    try:
        r = requests.get(f"{EIA_BASE}/petroleum/pri/gdu/data/", params={
            "api_key": EIA_API_KEY,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPM0",
            "facets[duoarea][]": "NUS",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 8,
        }, timeout=15)
        if r.status_code == 200:
            obs = r.json().get("response", {}).get("data", [])
            prices = []
            for o in obs:
                val = o.get("value")
                if val is not None:
                    prices.append({
                        "date": o.get("period", ""),
                        "price": float(val),
                    })
            data["retail_gas"] = prices
    except Exception:
        pass

    # Crude oil inventory (commercial)
    crude_inv = fetch_eia_series("WCESTUS1", limit=8)
    if crude_inv:
        data["crude_inventory"] = [{
            "date": o.get("period", ""),
            "value": float(o.get("value", 0)),
        } for o in crude_inv if o.get("value")]

    # Gasoline inventory
    gas_inv = fetch_eia_series("WGTSTUS1", limit=8)
    if gas_inv:
        data["gasoline_inventory"] = [{
            "date": o.get("period", ""),
            "value": float(o.get("value", 0)),
        } for o in gas_inv if o.get("value")]

    _cache["data"] = data
    _cache["fetched_at"] = now
    return data


def get_gas_context(market_title: str) -> str | None:
    """Build context string for a gas/petroleum market."""
    data = fetch_gas_prices()
    if not data:
        return None

    context = "EIA PETROLEUM DATA:\n"

    if data.get("retail_gas"):
        latest = data["retail_gas"][0]
        context += f"National avg gas price: ${latest['price']:.3f}/gal (week of {latest['date']})\n"
        if len(data["retail_gas"]) >= 2:
            prev = data["retail_gas"][1]
            change = latest["price"] - prev["price"]
            context += f"Weekly change: {change:+.3f}/gal\n"
        if len(data["retail_gas"]) >= 4:
            avg_4wk = sum(p["price"] for p in data["retail_gas"][:4]) / 4
            context += f"4-week avg: ${avg_4wk:.3f}/gal\n"

    if data.get("crude_inventory"):
        latest = data["crude_inventory"][0]
        context += f"Crude oil inventory: {latest['value']:.1f}M barrels ({latest['date']})\n"
        if len(data["crude_inventory"]) >= 2:
            prev = data["crude_inventory"][1]
            change = latest["value"] - prev["value"]
            context += f"Weekly change: {change:+.1f}M barrels\n"

    if data.get("gasoline_inventory"):
        latest = data["gasoline_inventory"][0]
        context += f"Gasoline inventory: {latest['value']:.1f}M barrels ({latest['date']})\n"

    # Try to extract price threshold from market title
    import re
    match = re.search(r'\$(\d+\.?\d*)', market_title)
    if match and data.get("retail_gas"):
        threshold = float(match.group(1))
        current = data["retail_gas"][0]["price"]
        context += f"\nMarket threshold: ${threshold:.2f}\n"
        context += f"Current vs threshold: ${current:.3f} vs ${threshold:.2f} "
        context += f"({'ABOVE' if current > threshold else 'BELOW'})\n"

    return context


# Fallback: AAA gas prices (no API key needed)
def fetch_aaa_fallback() -> float | None:
    """Get national average gas price from AAA (free, no auth)."""
    try:
        r = requests.get(
            "https://gasprices.aaa.com/",
            timeout=10,
            headers={"User-Agent": "KalBot/1.0"},
        )
        if r.status_code == 200:
            import re
            match = re.search(r'\$(\d+\.\d{2,3})', r.text)
            if match:
                return float(match.group(1))
    except Exception:
        pass
    return None


if __name__ == "__main__":
    print("=== EIA Petroleum Feed ===")
    if not EIA_API_KEY:
        print("No EIA_API_KEY set. Get one free at https://www.eia.gov/opendata/register.php")
        print("Trying AAA fallback...")
        price = fetch_aaa_fallback()
        if price:
            print(f"AAA national average: ${price:.3f}/gal")
    else:
        data = fetch_gas_prices()
        if data.get("retail_gas"):
            print(f"Gas price: ${data['retail_gas'][0]['price']:.3f}/gal")
        if data.get("crude_inventory"):
            print(f"Crude inventory: {data['crude_inventory'][0]['value']:.1f}M bbl")
    ctx = get_gas_context("Will national avg gas price be above $3.50?")
    if ctx:
        print(f"\n{ctx}")
