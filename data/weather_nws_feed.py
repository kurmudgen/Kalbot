"""
NWS forecast feed — pulls official forecasts from api.weather.gov.
This is the SAME source Kalshi settles weather markets on.
No auth required. Updates every 30 minutes.
"""

import json
import os
import requests
from datetime import datetime, timezone

FEED_PATH = os.path.join(os.path.dirname(__file__), "live", "nws_forecasts.json")

# City coordinates → NWS grid point lookup
CITIES = {
    "nyc": {"lat": 40.7128, "lon": -74.0060, "name": "New York City"},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "name": "Chicago"},
    "miami": {"lat": 25.7617, "lon": -80.1918, "name": "Miami"},
    "houston": {"lat": 29.7604, "lon": -95.3698, "name": "Houston"},
    "denver": {"lat": 39.7392, "lon": -104.9903, "name": "Denver"},
    "austin": {"lat": 30.2672, "lon": -97.7431, "name": "Austin"},
    "los_angeles": {"lat": 34.0522, "lon": -118.2437, "name": "Los Angeles"},
    "phoenix": {"lat": 33.4484, "lon": -112.0740, "name": "Phoenix"},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "name": "Seattle"},
    "philadelphia": {"lat": 39.9526, "lon": -75.1652, "name": "Philadelphia"},
}

HEADERS = {"User-Agent": "KalBot research@formationlabs.com", "Accept": "application/json"}


def get_nws_forecast(lat: float, lon: float) -> dict | None:
    """Get 7-day forecast from NWS for a lat/lon."""
    try:
        # Step 1: Get grid point
        r = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers=HEADERS, timeout=10,
        )
        if r.status_code != 200:
            return None

        forecast_url = r.json()["properties"]["forecast"]

        # Step 2: Get forecast
        r2 = requests.get(forecast_url, headers=HEADERS, timeout=10)
        if r2.status_code != 200:
            return None

        periods = r2.json()["properties"]["periods"]

        # Find today's daytime period (has the high temp)
        for period in periods:
            if period.get("isDaytime", True) and "today" in period.get("name", "").lower():
                return {
                    "high_temp": period["temperature"],
                    "unit": period["temperatureUnit"],
                    "short_forecast": period["shortForecast"],
                    "detailed": period["detailedForecast"][:200],
                    "name": period["name"],
                }

        # Fallback: first daytime period
        for period in periods:
            if period.get("isDaytime", True):
                return {
                    "high_temp": period["temperature"],
                    "unit": period["temperatureUnit"],
                    "short_forecast": period["shortForecast"],
                    "detailed": period["detailedForecast"][:200],
                    "name": period["name"],
                }
    except Exception as e:
        return None
    return None


def fetch_all_forecasts() -> dict:
    """Fetch NWS forecasts for all cities."""
    forecasts = {}
    for city_key, city_data in CITIES.items():
        forecast = get_nws_forecast(city_data["lat"], city_data["lon"])
        if forecast:
            forecasts[city_key] = {
                "city": city_data["name"],
                **forecast,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            print(f"  NWS {city_data['name']}: {forecast['high_temp']}F {forecast['short_forecast']}")
    return forecasts


def save_forecasts(forecasts: dict):
    """Save forecasts to JSON file."""
    os.makedirs(os.path.dirname(FEED_PATH), exist_ok=True)
    with open(FEED_PATH, "w") as f:
        json.dump(forecasts, f, indent=2)


def load_forecasts() -> dict:
    """Load saved forecasts."""
    if not os.path.exists(FEED_PATH):
        return {}
    try:
        with open(FEED_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def get_city_forecast(title: str) -> str | None:
    """Get NWS forecast for a city mentioned in a market title.
    Returns formatted string for LLM context."""
    forecasts = load_forecasts()
    if not forecasts:
        return None

    title_lower = title.lower()
    for city_key, data in forecasts.items():
        city_name = data.get("city", "").lower()
        if city_name in title_lower or city_key in title_lower:
            return (
                f"NWS official forecast for {data['city']} today: "
                f"High {data['high_temp']}F. {data['short_forecast']}. "
                f"(This is the same source Kalshi settles on.)"
            )
    return None


def update_feed():
    """Fetch and save all forecasts."""
    print("  Fetching NWS forecasts...")
    forecasts = fetch_all_forecasts()
    save_forecasts(forecasts)
    print(f"  NWS: {len(forecasts)} city forecasts updated")
    return forecasts


if __name__ == "__main__":
    forecasts = update_feed()
    print(f"\n{len(forecasts)} cities:")
    for city, data in forecasts.items():
        print(f"  {data['city']}: {data['high_temp']}F — {data['short_forecast']}")
