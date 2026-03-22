"""
Cascaded calibration pipeline for ensemble probability estimates.
Based on patterns from dylanpersonguy's Polymarket bot and alexandermazza's MCP.

Pipeline:
1. Platt scaling (logit shrinkage) — compress extreme probabilities toward 0.5
2. Edge dampening — edges >18% are increasingly unreliable
3. Low confidence penalty — pull uncertain estimates toward 0.5
4. Ensemble spread penalty — high disagreement = lower confidence
5. Historical bias adjustment — apply Kalshi market-level calibration

Key findings from repos:
- alexandermazza: NWS-only mode recommended (Open-Meteo differs 2-4F, causes losses)
- alexandermazza: Edge window 10-18% (edges >18% correlate with LOWER win rate)
- alexandermazza: Timing multipliers: PRIME 1.3x after NWS model cycles, OFF_PEAK 0.85x
- ryanfrigo: Economic trades had -70% ROI — use very conservative thresholds
- ryanfrigo: NO-side on near-certain outcomes is the only consistently profitable strategy
- nikhilnd: Cauchy distribution better than normal for S&P (fat tails)
"""

import json
import math
import os


# Per-city forecast standard deviations (from kalshi-trading-mcp)
CITY_STD_DEVS = {
    "new york": 5.2, "nyc": 5.2, "manhattan": 5.2,
    "chicago": 6.6,
    "miami": 4.6,
    "austin": 6.2,
    "los angeles": 4.8, "la": 4.8,
    "denver": 8.5,
    "philadelphia": 6.3, "philly": 6.3,
    "houston": 5.5,
    "phoenix": 5.0,
    "seattle": 5.5,
}

# Edge dampening parameters (from kalshi-trading-mcp)
EDGE_DAMPEN_THRESHOLD = 0.18  # 18% edge
EDGE_DAMPEN_FACTOR = 0.50     # Only 50% of excess edge counts

# Platt scaling shrinkage
LOGIT_SHRINKAGE = 0.90  # 10% shrinkage toward center

# NWS model cycle timing (from alexandermazza)
# Markets are most inefficient right after NWS updates
# PRIME windows (CT): 6-8AM, 12-2PM, 6-11PM → multiply edge by 1.3x
# OFF_PEAK: overnight → multiply edge by 0.85x
NWS_PRIME_MULTIPLIER = 1.3
NWS_OFF_PEAK_MULTIPLIER = 0.85

# Kalshi weather station mapping (must match settlement source)
KALSHI_WEATHER_STATIONS = {
    "new york": "KNYC",  # Central Park, NOT JFK
    "nyc": "KNYC",
    "chicago": "KORD",
    "miami": "KMIA",
    "houston": "KIAH",
    "los angeles": "KLAX",
    "denver": "KDEN",
    "phoenix": "KPHX",
    "seattle": "KSEA",
    "philadelphia": "KPHL",
    "austin": "KAUS",
}

# NO-side bias: from both ryanfrigo and alexandermazza
# Buying NO on near-certain outcomes is the most consistently profitable
# NO at 80+ cents has structural edge due to favorite-longshot bias
NO_SIDE_PREFERENCE = True


def platt_scale(prob: float) -> float:
    """Compress extreme probabilities toward 0.5 via logit shrinkage."""
    prob = max(0.01, min(0.99, prob))
    logit = math.log(prob / (1 - prob))
    shrunk = logit * LOGIT_SHRINKAGE
    return 1.0 / (1.0 + math.exp(-shrunk))


def dampen_edge(raw_edge: float) -> float:
    """Dampen large edges — edges >18% are increasingly unreliable.
    The 'edge paradox': very large calculated edges often indicate
    the model is wrong, not that the market is very wrong."""
    if abs(raw_edge) <= EDGE_DAMPEN_THRESHOLD:
        return raw_edge

    sign = 1 if raw_edge > 0 else -1
    excess = abs(raw_edge) - EDGE_DAMPEN_THRESHOLD
    dampened = EDGE_DAMPEN_THRESHOLD + excess * EDGE_DAMPEN_FACTOR
    return sign * dampened


def penalize_low_confidence(prob: float, confidence: float) -> float:
    """Pull estimates toward 0.5 when confidence is low."""
    if confidence >= 0.7:
        return prob
    # Low confidence: blend toward 0.5
    blend = 0.15 * (1.0 - confidence / 0.7)
    return prob * (1 - blend) + 0.5 * blend


def penalize_spread(prob: float, spread: float) -> float:
    """Penalize when ensemble models disagree significantly."""
    if spread <= 0.10:
        return prob
    penalty = min(0.25, spread)
    return prob * (1 - penalty) + 0.5 * penalty


def get_city_std_dev(title: str) -> float | None:
    """Get forecast standard deviation for a city mentioned in the title."""
    title_lower = title.lower()
    for city, std in CITY_STD_DEVS.items():
        if city in title_lower:
            return std
    return None


def calibrate_probability(
    raw_prob: float,
    confidence: float = 0.8,
    market_price: float = 0.5,
    ensemble_spread: float = 0.0,
    title: str = "",
) -> dict:
    """Run the full calibration pipeline on a raw probability estimate.

    Returns calibrated probability, dampened edge, and all adjustments made.
    """
    adjustments = []

    # Step 1: Platt scaling
    calibrated = platt_scale(raw_prob)
    if abs(calibrated - raw_prob) > 0.005:
        adjustments.append(f"Platt: {raw_prob:.3f} → {calibrated:.3f}")

    # Step 2: Low confidence penalty
    prev = calibrated
    calibrated = penalize_low_confidence(calibrated, confidence)
    if abs(calibrated - prev) > 0.005:
        adjustments.append(f"Low-conf: {prev:.3f} → {calibrated:.3f}")

    # Step 3: Ensemble spread penalty
    prev = calibrated
    calibrated = penalize_spread(calibrated, ensemble_spread)
    if abs(calibrated - prev) > 0.005:
        adjustments.append(f"Spread: {prev:.3f} → {calibrated:.3f}")

    # Step 4: Edge dampening
    raw_edge = calibrated - market_price
    dampened = dampen_edge(raw_edge)
    final_prob = market_price + dampened

    # Clamp
    final_prob = max(0.01, min(0.99, final_prob))

    if abs(dampened - raw_edge) > 0.005:
        adjustments.append(f"Edge dampen: {raw_edge:+.3f} → {dampened:+.3f}")

    return {
        "raw_probability": raw_prob,
        "calibrated_probability": final_prob,
        "raw_edge": raw_edge,
        "dampened_edge": dampened,
        "adjustments": adjustments,
        "city_std_dev": get_city_std_dev(title),
    }


if __name__ == "__main__":
    # Test cases
    tests = [
        (0.95, 0.8, 0.45, 0.0, "NYC temperature"),
        (0.30, 0.5, 0.45, 0.15, "CPI inflation"),
        (0.80, 0.9, 0.50, 0.05, "Denver weather"),
    ]

    for prob, conf, price, spread, title in tests:
        result = calibrate_probability(prob, conf, price, spread, title)
        print(f"{title}: raw={prob:.2f} → calibrated={result['calibrated_probability']:.2f} "
              f"(edge: {result['dampened_edge']:+.3f})")
        for adj in result["adjustments"]:
            print(f"  {adj}")
        print()
