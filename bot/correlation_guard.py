"""
Smart correlation guard — bracket mutual exclusivity detection.

Weather: extracts city+date from ticker, checks bracket overlap.
  Max 3 positions per city per resolution date (from .env).
  Combined probability must stay below 0.90.
  Non-overlapping brackets allowed simultaneously.

Economic: event-level dedup for same release date.
  Max 3 brackets per event per release date.

Replaces old hardcoded group limits (mountain=2, weather/day=5).
"""

import os
import re
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

MAX_PER_CITY_PER_DAY = int(os.getenv("MAX_POSITIONS_PER_CITY_PER_DAY", "3"))
MAX_WEATHER_PER_DAY = int(os.getenv("MAX_WEATHER_POSITIONS_PER_DAY", "10"))


def parse_weather_ticker(ticker: str) -> dict | None:
    """Extract city, date, bracket type and value from weather ticker.

    KXHIGHNY-26MAR25-T51   -> city=NY, date=26MAR25, type=T(threshold), value=51
    KXHIGHDEN-26MAR26-B81.5 -> city=DEN, date=26MAR26, type=B(bracket), value=81.5
    """
    match = re.match(
        r'KX(?:HIGH|LOW)([A-Z]{2,3})-(\d{2}[A-Z]{3}\d{2})-([BT])(\d+\.?\d*)',
        ticker
    )
    if not match:
        return None
    return {
        "city": match.group(1),
        "date": match.group(2),
        "bracket_type": match.group(3),
        "value": float(match.group(4)),
        "ticker": ticker,
    }


def get_bracket_range(parsed: dict) -> tuple[float, float]:
    """B81.5 = 81-82. T51 = under 51."""
    if parsed["bracket_type"] == "T":
        return (-999.0, parsed["value"])
    return (parsed["value"] - 0.5, parsed["value"] + 0.5)


def brackets_overlap(a: dict, b: dict) -> bool:
    """True if ranges overlap (NOT mutually exclusive)."""
    ra = get_bracket_range(a)
    rb = get_bracket_range(b)
    return ra[0] < rb[1] and rb[0] < ra[1]


def get_active_positions() -> list[dict]:
    """Get currently active (non-exited) positions."""
    if not os.path.exists(DECISIONS_DB):
        return []
    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT ticker, title, category, side, cloud_probability, amount
        FROM decisions
        WHERE executed = 1 AND side NOT LIKE '%EXIT%'
        ORDER BY decided_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def check_correlation(new_market: dict) -> dict:
    """Check if a new trade would create too much correlated exposure.

    Backwards-compatible signature: accepts full market dict,
    returns {"allowed": bool, "reason": str, ...}.
    """
    ticker = new_market.get("ticker", "")
    title = new_market.get("title", "")
    category = new_market.get("category", "")
    probability = new_market.get("cloud_probability", 0.5)

    result = {
        "allowed": True,
        "reason": "",
        "correlation_group": None,
        "existing_correlated": 0,
    }

    positions = get_active_positions()

    if category == "weather":
        allowed, reason = _check_weather(ticker, probability, positions)
        result["allowed"] = allowed
        result["reason"] = reason
    elif category in ("economics", "inflation"):
        allowed, reason = _check_econ(ticker, positions)
        result["allowed"] = allowed
        result["reason"] = reason

    return result


def _check_weather(ticker: str, probability: float, positions: list[dict]) -> tuple[bool, str]:
    """Weather: bracket mutual exclusivity + city-date limits."""
    parsed = parse_weather_ticker(ticker)
    if not parsed:
        return True, ""

    # Count total weather positions today
    weather_count = sum(1 for p in positions if p.get("category") == "weather")
    if weather_count >= MAX_WEATHER_PER_DAY:
        return False, f"Already {weather_count} weather positions today (max {MAX_WEATHER_PER_DAY})"

    # Find same city + date positions
    same_city_date = []
    for pos in positions:
        pos_parsed = parse_weather_ticker(pos["ticker"])
        if pos_parsed and pos_parsed["city"] == parsed["city"] and pos_parsed["date"] == parsed["date"]:
            pos_parsed["probability"] = pos.get("cloud_probability") or 0.5
            same_city_date.append(pos_parsed)

    if not same_city_date:
        return True, ""

    # Max per city per day
    if len(same_city_date) >= MAX_PER_CITY_PER_DAY:
        return False, f"max {MAX_PER_CITY_PER_DAY} positions for {parsed['city']} {parsed['date']} (have {len(same_city_date)})"

    # Check for overlapping brackets
    for existing in same_city_date:
        if brackets_overlap(parsed, existing):
            return False, f"overlapping bracket with {existing['ticker']}"

    # Combined probability check
    combined_prob = sum(p["probability"] for p in same_city_date) + probability
    if combined_prob > 0.90:
        return False, f"combined probability {combined_prob:.2f} > 0.90 for {parsed['city']} {parsed['date']}"

    return True, ""


def _check_econ(ticker: str, positions: list[dict]) -> tuple[bool, str]:
    """Economic: event-level dedup for same release date."""
    match = re.match(r'(KX\w+)-(\d{2}[A-Z]{3}\d{0,2})', ticker)
    if not match:
        return True, ""

    series = match.group(1)
    date_part = match.group(2)

    same_event = [
        p for p in positions
        if p["ticker"].startswith(series) and date_part in p["ticker"]
    ]

    if len(same_event) >= 3:
        return False, f"max 3 brackets for {series} {date_part} (have {len(same_event)})"

    for existing in same_event:
        if existing["ticker"] == ticker:
            return False, f"duplicate position on {ticker}"

    return True, ""


if __name__ == "__main__":
    # Test parsing
    tests = [
        "KXHIGHNY-26MAR25-T51",
        "KXHIGHDEN-26MAR26-B81.5",
        "KXHIGHDEN-26MAR26-B79.5",
        "KXHIGHCHI-26MAR26-T77",
    ]
    for t in tests:
        p = parse_weather_ticker(t)
        if p:
            r = get_bracket_range(p)
            print(f"{t} -> city={p['city']}, date={p['date']}, range={r}")

    # Test overlap
    den1 = parse_weather_ticker("KXHIGHDEN-26MAR26-B81.5")
    den2 = parse_weather_ticker("KXHIGHDEN-26MAR26-B79.5")
    den3 = parse_weather_ticker("KXHIGHDEN-26MAR26-T77")
    print(f"\nDEN 81-82 vs 79-80: overlap={brackets_overlap(den1, den2)}")
    print(f"DEN 81-82 vs under-77: overlap={brackets_overlap(den1, den3)}")
    print(f"DEN 79-80 vs under-77: overlap={brackets_overlap(den2, den3)}")

    # Test correlation
    test = {"ticker": "KXHIGHDEN-26MAR26-B81.5", "title": "", "category": "weather"}
    r = check_correlation(test)
    print(f"\nCorrelation: allowed={r['allowed']}, reason={r['reason']}")
