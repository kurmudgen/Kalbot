"""
Box office feed — fetches weekend box office data for movie markets.

Kalshi sometimes has markets on opening weekend gross for major releases.
This gives the model tracking data + estimates from industry sources.

Source: The Numbers API (free scraping), Box Office Mojo via search
"""

import os
import re
import requests
from datetime import datetime, timedelta

_cache = {"data": None, "fetched_at": None}
CACHE_TTL = 3600  # 1 hour


def fetch_box_office_data() -> dict:
    """Fetch current box office data from free sources."""
    now = datetime.utcnow()

    if _cache["data"] and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL:
            return _cache["data"]

    data = {"weekend": [], "weekly": []}

    # Try The Numbers daily box office
    try:
        r = requests.get(
            "https://www.the-numbers.com/box-office-chart-daily",
            timeout=10,
            headers={"User-Agent": "KalBot/1.0 (educational project)"},
        )
        if r.status_code == 200:
            # Parse simple table data
            # Look for movie titles and gross amounts
            rows = re.findall(
                r'<td>(\d+)</td>.*?<a[^>]*>([^<]+)</a>.*?\$([0-9,]+)',
                r.text, re.DOTALL
            )
            for rank, title, gross in rows[:10]:
                data["weekend"].append({
                    "rank": int(rank),
                    "title": title.strip(),
                    "gross": int(gross.replace(",", "")),
                })
    except Exception:
        pass

    _cache["data"] = data
    _cache["fetched_at"] = now
    return data


def get_box_office_context(market_title: str) -> str | None:
    """Build context string for a box office market."""
    data = fetch_box_office_data()
    if not data.get("weekend"):
        return None

    context = "BOX OFFICE DATA:\n"
    context += "Current top 10:\n"
    for movie in data["weekend"][:10]:
        context += f"  #{movie['rank']}: {movie['title']} - ${movie['gross']:,}\n"

    # Try to match movie name in market title
    title_lower = market_title.lower()
    for movie in data["weekend"]:
        if movie["title"].lower() in title_lower:
            context += f"\nFeatured movie: {movie['title']}\n"
            context += f"Current gross: ${movie['gross']:,}\n"
            context += f"Current rank: #{movie['rank']}\n"
            break

    # Extract dollar threshold from market title
    match = re.search(r'\$(\d+)\s*(?:million|M)', market_title, re.IGNORECASE)
    if match:
        threshold = int(match.group(1)) * 1_000_000
        context += f"Market threshold: ${threshold:,}\n"

    return context


if __name__ == "__main__":
    print("=== Box Office Feed ===")
    data = fetch_box_office_data()
    if data.get("weekend"):
        for m in data["weekend"][:5]:
            print(f"  #{m['rank']}: {m['title']} - ${m['gross']:,}")
    else:
        print("  No data available")
