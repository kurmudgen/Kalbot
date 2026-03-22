"""
Correlation guard: prevents over-exposure to correlated positions.
If you're long on NYC weather and Philly weather on the same day,
that's basically the same bet. This module limits correlated risk.
"""

import os
import sqlite3
from datetime import datetime, timezone

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

# Cities that are weather-correlated (same weather system)
CORRELATED_CITIES = [
    {"group": "northeast", "cities": ["new york", "nyc", "philadelphia", "boston", "newark", "hartford"]},
    {"group": "southeast", "cities": ["miami", "tampa", "orlando", "jacksonville", "atlanta"]},
    {"group": "midwest", "cities": ["chicago", "detroit", "milwaukee", "indianapolis", "minneapolis"]},
    {"group": "texas", "cities": ["houston", "dallas", "san antonio", "austin"]},
    {"group": "southwest", "cities": ["phoenix", "las vegas", "tucson"]},
    {"group": "pacific", "cities": ["los angeles", "san diego", "san francisco"]},
    {"group": "northwest", "cities": ["seattle", "portland"]},
    {"group": "mountain", "cities": ["denver", "salt lake city", "albuquerque"]},
]

MAX_CORRELATED_POSITIONS = 2  # Max positions in same weather group
MAX_SAME_DAY_WEATHER = 5      # Max weather positions expiring same day


def get_city_group(title: str) -> str | None:
    """Find which weather correlation group a market belongs to."""
    title_lower = title.lower()
    for group in CORRELATED_CITIES:
        if any(city in title_lower for city in group["cities"]):
            return group["group"]
    return None


def get_active_positions() -> list[dict]:
    """Get currently active (non-exited) positions."""
    if not os.path.exists(DECISIONS_DB):
        return []

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM decisions
        WHERE executed = 1
        AND side NOT LIKE '%EXIT%'
        ORDER BY decided_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def check_correlation(new_market: dict) -> dict:
    """Check if a new trade would create too much correlated exposure."""
    title = new_market.get("title", "")
    category = new_market.get("category", "")

    result = {
        "allowed": True,
        "reason": "",
        "correlation_group": None,
        "existing_correlated": 0,
    }

    if category != "weather":
        return result

    new_group = get_city_group(title)
    if not new_group:
        return result

    result["correlation_group"] = new_group

    # Count existing positions in the same correlation group
    positions = get_active_positions()
    correlated_count = 0
    same_day_weather = 0

    for pos in positions:
        pos_title = pos.get("title", "")
        pos_cat = pos.get("category", "")

        if pos_cat == "weather":
            same_day_weather += 1

            pos_group = get_city_group(pos_title)
            if pos_group == new_group:
                correlated_count += 1

    result["existing_correlated"] = correlated_count

    if correlated_count >= MAX_CORRELATED_POSITIONS:
        result["allowed"] = False
        result["reason"] = f"Already {correlated_count} positions in {new_group} weather group (max {MAX_CORRELATED_POSITIONS})"

    elif same_day_weather >= MAX_SAME_DAY_WEATHER:
        result["allowed"] = False
        result["reason"] = f"Already {same_day_weather} weather positions today (max {MAX_SAME_DAY_WEATHER})"

    return result


if __name__ == "__main__":
    # Test with a sample market
    test = {"title": "Will NYC temperature exceed 70F tomorrow?", "category": "weather"}
    result = check_correlation(test)
    print(f"Test market: {test['title']}")
    print(f"Allowed: {result['allowed']}")
    print(f"Group: {result['correlation_group']}")
    print(f"Existing correlated: {result['existing_correlated']}")
    if result['reason']:
        print(f"Reason: {result['reason']}")
