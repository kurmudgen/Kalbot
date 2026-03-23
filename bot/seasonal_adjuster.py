"""
Seasonal adjustments for weather markets.
Summer weather is more predictable than spring/fall transition seasons.
Adjusts confidence thresholds based on time of year.
"""

from datetime import datetime

# Monthly forecast accuracy multipliers (based on NOAA verification data)
# Higher = more predictable = more confident
SEASONAL_ACCURACY = {
    1: 0.85,   # January — cold, stable patterns
    2: 0.85,   # February — cold, stable
    3: 0.75,   # March — transition, unpredictable
    4: 0.70,   # April — spring storms, volatile
    5: 0.80,   # May — settling into summer
    6: 0.90,   # June — summer, very predictable
    7: 0.95,   # July — peak summer, most predictable
    8: 0.95,   # August — peak summer
    9: 0.85,   # September — early fall
    10: 0.75,  # October — fall transition
    11: 0.80,  # November — settling into winter
    12: 0.85,  # December — winter, stable
}

# City-specific adjustments
CITY_SEASONAL = {
    "denver": {3: 0.60, 4: 0.55, 10: 0.60},   # Mountain weather is wild in transitions
    "chicago": {3: 0.65, 11: 0.70},              # Lake effect makes fall/spring hard
    "miami": {6: 0.80, 7: 0.80, 8: 0.80, 9: 0.75, 10: 0.75},  # Hurricane season
}


def get_seasonal_multiplier(city: str = "", month: int = None) -> float:
    """Get confidence multiplier based on season and city."""
    if month is None:
        month = datetime.now().month

    base = SEASONAL_ACCURACY.get(month, 0.80)

    # City-specific override
    city_lower = city.lower()
    for city_key, overrides in CITY_SEASONAL.items():
        if city_key in city_lower:
            if month in overrides:
                base = min(base, overrides[month])

    return base


def adjust_confidence(confidence: float, title: str = "") -> float:
    """Adjust confidence based on seasonal accuracy."""
    multiplier = get_seasonal_multiplier(title)
    return confidence * multiplier


if __name__ == "__main__":
    now = datetime.now()
    print(f"Current month: {now.strftime('%B')} (multiplier: {get_seasonal_multiplier():.2f})")
    for city in ["New York", "Denver", "Miami", "Chicago"]:
        m = get_seasonal_multiplier(city)
        print(f"  {city}: {m:.2f}")
