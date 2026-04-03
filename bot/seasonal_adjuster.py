"""
Seasonal adjustments for weather markets.
Summer weather is more predictable than spring/fall transition seasons.
Adjusts confidence thresholds based on time of year.
"""

from datetime import datetime

# Monthly forecast accuracy multipliers (based on NOAA verification data)
# Higher = more predictable = more confident
SEASONAL_ACCURACY = {
    1: 0.95,   # January — cold, stable patterns
    2: 0.95,   # February — cold, stable
    3: 0.90,   # March — transition, moderate haircut
    4: 0.88,   # April — spring storms, slight haircut
    5: 0.92,   # May — settling into summer
    6: 0.98,   # June — summer, very predictable
    7: 1.00,   # July — peak summer, no haircut
    8: 1.00,   # August — peak summer
    9: 0.95,   # September — early fall
    10: 0.90,  # October — fall transition
    11: 0.92,  # November — settling into winter
    12: 0.95,  # December — winter, stable
}

# City-specific adjustments (modest reductions, not confidence-killing)
CITY_SEASONAL = {
    "denver": {3: 0.85, 4: 0.82, 10: 0.85},   # Mountain transitions
    "chicago": {3: 0.87, 11: 0.90},              # Lake effect
    "miami": {6: 0.92, 7: 0.92, 8: 0.92, 9: 0.90, 10: 0.90},  # Hurricane season
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
