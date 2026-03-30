"""
Simulation runner: replays historical Kalshi weather markets through the full pipeline.

Reads from data/splits/train.parquet, fetches historical temps from Open-Meteo,
scores with Ollama qwen2.5:32b, applies executor gates, writes results to
calibration/simulation_decisions.sqlite.

Completely standalone — does not touch live databases or running processes.
"""

import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Setup paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"), override=True)

TRAIN_PATH = os.path.join(ROOT, "data", "splits", "train.parquet")
SIM_DB = os.path.join(ROOT, "calibration", "simulation_decisions.sqlite")
PROMPT_PATH = os.path.join(ROOT, "prompts", "local_filter.txt")
BIAS_PATH = os.path.join(ROOT, "calibration", "kalshi_market_bias.json")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = os.getenv("LOCAL_FILTER_MODEL", "qwen2.5:32b")

# Executor gate thresholds (mirrors bot/executor.py)
CATEGORY_CONFIDENCE = {
    "weather": float(os.getenv("WEATHER_CONFIDENCE", "0.70")),
}
PRICE_GAP_MIN = float(os.getenv("PRICE_GAP_MIN", "0.08"))
KELLY_FRACTION = 0.25
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_NIGHTLY_SPEND = float(os.getenv("MAX_NIGHTLY_SPEND", "50"))

# City coordinates for Open-Meteo historical data
CITIES = {
    "new york": {"lat": 40.7128, "lon": -74.0060, "name": "New York City", "key": "nyc"},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "name": "Chicago", "key": "chicago"},
    "miami": {"lat": 25.7617, "lon": -80.1918, "name": "Miami", "key": "miami"},
    "houston": {"lat": 29.7604, "lon": -95.3698, "name": "Houston", "key": "houston"},
    "denver": {"lat": 39.7392, "lon": -104.9903, "name": "Denver", "key": "denver"},
    "austin": {"lat": 30.2672, "lon": -97.7431, "name": "Austin", "key": "austin"},
    "los angeles": {"lat": 34.0522, "lon": -118.2437, "name": "Los Angeles", "key": "los_angeles"},
    "phoenix": {"lat": 33.4484, "lon": -112.0740, "name": "Phoenix", "key": "phoenix"},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "name": "Seattle", "key": "seattle"},
    "philadelphia": {"lat": 39.9526, "lon": -75.1652, "name": "Philadelphia", "key": "philadelphia"},
}

# Historical bias data
MARKET_BIAS = {}
if os.path.exists(BIAS_PATH):
    with open(BIAS_PATH) as f:
        MARKET_BIAS = json.load(f).get("bins", {})


def get_historical_bias(market_price_cents: int) -> float:
    for bin_key, data in MARKET_BIAS.items():
        lo, hi = bin_key.split("-")
        if int(lo) <= market_price_cents <= int(hi):
            return data.get("bias_pct", 0) / 100.0
    return 0.0


# ── Title Parsing ─────────────────────────────────────────────

def parse_date_from_title(title: str) -> Optional[str]:
    """Extract date from market title, return as YYYY-MM-DD."""
    m = re.search(r'on (\w+ \d{1,2}, \d{4})', title)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%b %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def parse_cities_from_title(title: str) -> list[dict]:
    """Extract city names and temperature thresholds from title.

    Returns list of dicts with: city_key, city_name, threshold_low, threshold_high, threshold_type
    threshold_type: 'range' (X to Y), 'below' (or below), 'above' (or above)
    """
    results = []
    title_lower = title.lower()

    # Match patterns like "Chicago: 26° to 27°" or "Miami: 67° or below" or "Denver: 65° or above"
    # Handle both single-city and multi-city formats
    for city_search, city_info in CITIES.items():
        if city_search not in title_lower:
            continue

        # Try to find threshold for this city
        # Pattern: CityName: XX° to YY° or CityName: XX° or below/above
        # Use strict pattern: city name followed directly by colon (no other city names between)
        city_name_escaped = re.escape(city_search)

        # Range pattern: "City: XX° to YY°" (degree symbol may be mangled)
        range_match = re.search(
            rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*to\s*(\d+)',
            title_lower
        )
        if range_match:
            results.append({
                "city_key": city_info["key"],
                "city_name": city_info["name"],
                "threshold_low": int(range_match.group(1)),
                "threshold_high": int(range_match.group(2)),
                "threshold_type": "range",
            })
            continue

        # "or below" pattern
        below_match = re.search(
            rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*or below',
            title_lower
        )
        if below_match:
            thresh = int(below_match.group(1))
            results.append({
                "city_key": city_info["key"],
                "city_name": city_info["name"],
                "threshold_low": None,
                "threshold_high": thresh,
                "threshold_type": "below",
            })
            continue

        # "or above" pattern
        above_match = re.search(
            rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*or above',
            title_lower
        )
        if above_match:
            thresh = int(above_match.group(1))
            results.append({
                "city_key": city_info["key"],
                "city_name": city_info["name"],
                "threshold_low": thresh,
                "threshold_high": None,
                "threshold_type": "above",
            })
            continue

        # Single-city "over XX°" pattern
        over_match = re.search(
            rf'over\s+(\d+)\s*[°�]?',
            title_lower
        )
        if over_match and len(results) == 0:
            thresh = int(over_match.group(1))
            results.append({
                "city_key": city_info["key"],
                "city_name": city_info["name"],
                "threshold_low": thresh,
                "threshold_high": None,
                "threshold_type": "above",
            })

    return results


# ── Historical Temperature Fetching (Open-Meteo) ────────────

# Cache: {(city_key, date_str): temp_f}
_temp_cache: dict[tuple[str, str], float | None] = {}
# Batch cache: {city_key: {date_str: temp_f}}
_batch_fetched: set[str] = set()


def fetch_historical_temps_batch(city_key: str, lat: float, lon: float,
                                  start_date: str, end_date: str) -> dict[str, float]:
    """Fetch daily TMAX from Open-Meteo for a date range. Returns {date: temp_f}."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f"    Open-Meteo error for {city_key}: HTTP {r.status_code}")
            return {}
        data = r.json()
        dates = data.get("daily", {}).get("time", [])
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        return {d: t for d, t in zip(dates, temps) if t is not None}
    except Exception as e:
        print(f"    Open-Meteo error for {city_key}: {e}")
        return {}


def prefetch_all_temps(dates: list[str]):
    """Batch-fetch all historical temps for all cities across all needed dates."""
    if not dates:
        return

    min_date = min(dates)
    max_date = max(dates)
    print(f"\nPrefetching historical temps from Open-Meteo ({min_date} to {max_date})...")

    for city_search, city_info in CITIES.items():
        key = city_info["key"]
        if key in _batch_fetched:
            continue

        temps = fetch_historical_temps_batch(
            key, city_info["lat"], city_info["lon"], min_date, max_date
        )
        for date_str, temp in temps.items():
            _temp_cache[(key, date_str)] = temp

        _batch_fetched.add(key)
        print(f"  {city_info['name']}: {len(temps)} days fetched")
        time.sleep(0.5)  # Be nice to Open-Meteo

    print(f"  Total cached temps: {len(_temp_cache)}")


def get_historical_temp(city_key: str, date_str: str) -> float | None:
    """Get historical TMAX for a city on a date. Returns temp in °F."""
    return _temp_cache.get((city_key, date_str))


# ── NWS Forecast Simulation ─────────────────────────────────

def build_nws_context(cities_data: list[dict], date_str: str) -> str:
    """Build NWS-style forecast context from historical temp data.

    Mimics what the live system injects via weather_nws_feed.py.
    """
    forecasts = []
    nws_temps = {}  # city_key -> actual_temp

    for city in cities_data:
        actual_temp = get_historical_temp(city["city_key"], date_str)
        if actual_temp is None:
            continue

        actual_temp = round(actual_temp)
        nws_temps[city["city_key"]] = actual_temp
        forecasts.append(
            f"NWS official forecast for {city['city_name']} today: "
            f"High {actual_temp}F. (This is the same source Kalshi settles on.)"
        )

    if not forecasts:
        return "", {}

    nws_text = "\n".join(forecasts)
    context = (
        f"\nCRITICAL — {nws_text}\n"
        f"This is the official settlement source. Base your answer on this forecast.\n"
        f"Calibration guide based on gap between NWS forecast and market threshold:\n"
        f"  >8F gap: probability 0.05-0.15 (or 0.85-0.95), confidence 0.85-0.95\n"
        f"  5-8F gap: probability 0.15-0.30 (or 0.70-0.85), confidence 0.70-0.85\n"
        f"  3-5F gap: probability 0.30-0.40 (or 0.60-0.70), confidence 0.50-0.70\n"
        f"  <3F gap: probability 0.40-0.60, confidence 0.30-0.50\n"
        f"Confidence = how clear the NWS gap is, NOT certainty about the market price.\n"
        f"Do NOT echo the market price. Use the NWS forecast."
    )
    return context, nws_temps


def compute_nws_gap(cities_data: list[dict], nws_temps: dict) -> float | None:
    """Compute minimum NWS gap across all cities in a market.

    The minimum gap is the bottleneck — the city closest to its threshold
    determines the overall uncertainty.
    """
    gaps = []
    for city in cities_data:
        actual = nws_temps.get(city["city_key"])
        if actual is None:
            continue

        if city["threshold_type"] == "range":
            mid = (city["threshold_low"] + city["threshold_high"]) / 2.0
            gap = abs(actual - mid)
        elif city["threshold_type"] == "below":
            gap = abs(actual - city["threshold_high"])
        elif city["threshold_type"] == "above":
            gap = abs(actual - city["threshold_low"])
        else:
            continue
        gaps.append(gap)

    return min(gaps) if gaps else None


# ── Ollama Scoring ───────────────────────────────────────────

def load_prompt_template() -> str:
    with open(PROMPT_PATH) as f:
        return f.read().strip()


def query_ollama(prompt: str, max_retries: int = 2) -> dict | None:
    """Query Ollama and parse JSON response."""
    for attempt in range(max_retries + 1):
        try:
            r = requests.post(OLLAMA_URL, json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3, "num_predict": 256},
            }, timeout=120)
            if r.status_code != 200:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None

            text = r.json().get("response", "")
            # Extract JSON from response
            json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            if attempt < max_retries:
                time.sleep(1)
                continue
            return None
        except Exception as e:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
    return None


# ── Executor Gates (simulation version) ─────────────────────

def apply_gates(score: dict, category: str, title: str) -> tuple[bool, str, dict]:
    """Apply executor gates. Returns (execute, skip_reason, trade_details)."""
    cloud_prob = score["probability"]
    cloud_conf = score["confidence"]
    market_price = score["market_price"]
    reasoning = score.get("reasoning", "")

    # Null guard
    if cloud_prob == 0.0 or cloud_conf == 0.0 or market_price == 0.0:
        return False, "null_signal", {}

    price_gap = abs(cloud_prob - market_price)
    side = "YES" if cloud_prob > market_price else "NO"

    # Historical bias adjustment
    price_cents = int(market_price * 100)
    hist_bias = get_historical_bias(price_cents)
    bias_aligned = (side == "YES" and hist_bias > 0) or (side == "NO" and hist_bias < 0)
    if bias_aligned:
        cloud_conf = min(1.0, cloud_conf * 1.1)

    # Kelly sizing
    if side == "YES":
        cost = market_price
        our_prob = cloud_prob
    else:
        cost = 1.0 - market_price
        our_prob = 1.0 - cloud_prob

    b = (1.0 / cost) - 1 if cost > 0 else 0
    q = 1.0 - our_prob
    kelly_raw = (b * our_prob - q) / b if b > 0 else 0
    kelly_bet = max(0, kelly_raw * KELLY_FRACTION)
    amount = round(min(kelly_bet * MAX_NIGHTLY_SPEND, MAX_TRADE_SIZE), 2)

    trade = {
        "side": side,
        "amount": amount,
        "cloud_probability": cloud_prob,
        "cloud_confidence": cloud_conf,
        "price_gap": price_gap,
        "market_price": market_price,
        "reasoning": reasoning,
    }

    # Gate: Category confidence threshold
    cat_conf_min = CATEGORY_CONFIDENCE.get(category, 0.75)
    if cloud_conf < cat_conf_min - 0.001:
        return False, f"confidence {cloud_conf:.2f} < {cat_conf_min} ({category})", trade

    # Gate: Price gap minimum
    if price_gap < PRICE_GAP_MIN:
        return False, f"price gap {price_gap:.2f} < {PRICE_GAP_MIN}", trade

    # Gate: NWS required for borderline weather
    if category == "weather" and 0.20 < cloud_prob < 0.80:
        has_nws = "NWS" in reasoning or "nws" in reasoning or "official forecast" in reasoning.lower()
        if not has_nws:
            return False, "nws_data_missing", trade

    # Gate: Borderline EV floor
    if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.85 - 0.001:
        return False, f"borderline EV: prob={cloud_prob:.2f} needs conf>0.85", trade

    # Gate: No budget (Kelly says don't bet)
    if amount <= 0:
        return False, "kelly_says_no_edge", trade

    return True, "", trade


# ── Database ─────────────────────────────────────────────────

def init_sim_db() -> sqlite3.Connection:
    conn = sqlite3.connect(SIM_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            cloud_probability REAL,
            cloud_confidence REAL,
            market_price REAL,
            price_gap REAL,
            side TEXT,
            amount REAL,
            reasoning TEXT,
            mode TEXT,
            executed INTEGER,
            error TEXT,
            decided_at TEXT,
            session_id TEXT
        )
    """)
    # Extra table for simulation metadata
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simulation_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            actual_outcome INTEGER,
            result TEXT,
            nws_gap REAL,
            nws_temps TEXT,
            resolution_date TEXT,
            cities_parsed TEXT,
            sim_timestamp TEXT
        )
    """)
    # Resolved trades table (same schema as resolutions.sqlite)
    # so Tier 2 pattern analyzer can read it directly
    conn.execute("""
        CREATE TABLE IF NOT EXISTS resolved_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            category TEXT,
            side TEXT,
            amount REAL,
            entry_price REAL,
            our_probability REAL,
            our_confidence REAL,
            result TEXT,
            pnl REAL,
            pnl_pct REAL,
            strategy TEXT,
            resolved_at TEXT,
            decided_at TEXT
        )
    """)
    # Calibration tables for Tier 2
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calibration_reflections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            tier INTEGER,
            trade_id TEXT,
            observation TEXT,
            action_taken TEXT
        )
    """)
    conn.commit()
    return conn


def already_simulated(conn: sqlite3.Connection, ticker: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM decisions WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row is not None


# ── Simulation Core ──────────────────────────────────────────

def simulate_market(
    market: dict,
    template: str,
    conn: sqlite3.Connection,
    session_id: str,
) -> dict | None:
    """Simulate full pipeline for one market. Returns result dict or None."""
    ticker = market["market_id"]
    title = market["title"]
    category = market["category"]
    actual_outcome = int(market["actual_outcome"])
    actual_result = market.get("result", "yes" if actual_outcome == 1 else "no")
    raw_price = market.get("market_price_at_close", market.get("last_price", 50))
    if raw_price is None:
        raw_price = 50
    # Convert cents to decimal
    market_price = raw_price / 100.0 if raw_price > 1 else raw_price
    # For settled markets (price at 0 or 1), use 0.50 as neutral midpoint.
    # We don't have the actual trading-period price, so a neutral price
    # tests whether the model can determine direction purely from NWS data.
    if market_price < 0.03 or market_price > 0.97:
        market_price = 0.50

    # Skip already simulated
    if already_simulated(conn, ticker):
        return None

    # Parse title
    date_str = parse_date_from_title(title)
    cities_data = parse_cities_from_title(title)

    if not date_str or not cities_data:
        return None  # Can't simulate without date/city info

    # Build NWS context from historical temps
    nws_context, nws_temps = build_nws_context(cities_data, date_str)
    nws_gap = compute_nws_gap(cities_data, nws_temps)

    if not nws_context:
        return None  # No historical temp data available

    # Build prompt (same as live local_filter.py)
    prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}{nws_context}
Recent relevant headlines:
No recent headlines available.
"""

    # Score with Ollama
    result = query_ollama(prompt)
    if result is None:
        return None

    prob = float(result.get("probability", 0.5))
    conf = float(result.get("confidence", 0.5))
    relevant = bool(result.get("relevant", True))
    reasoning = result.get("reasoning", "")

    score = {
        "probability": prob,
        "confidence": conf,
        "relevant": relevant,
        "reasoning": reasoning,
        "market_price": market_price,
    }

    # Apply executor gates
    executed, skip_reason, trade = apply_gates(score, category, title)

    side = trade.get("side", "YES" if prob > market_price else "NO")
    amount = trade.get("amount", 0)
    final_prob = trade.get("cloud_probability", prob)
    final_conf = trade.get("cloud_confidence", conf)
    price_gap = trade.get("price_gap", abs(prob - market_price))

    # Determine P&L for executed trades
    pnl = 0.0
    if executed and amount > 0:
        if side == "YES":
            pnl = (1.0 - market_price) * amount if actual_outcome == 1 else -market_price * amount
        else:
            pnl = (1.0 - (1.0 - market_price)) * amount if actual_outcome == 0 else -(1.0 - market_price) * amount
        pnl = round(pnl, 2)

    # Write decision
    conn.execute(
        """INSERT INTO decisions
           (ticker, title, category, cloud_probability, cloud_confidence,
            market_price, price_gap, side, amount, reasoning,
            mode, executed, error, decided_at, session_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SIM', ?, ?, ?, ?)""",
        (ticker, title, category, final_prob, final_conf,
         market_price, price_gap, side, amount if executed else 0, reasoning,
         int(executed), skip_reason if not executed else None,
         date_str, session_id),
    )

    # Write simulation metadata
    conn.execute(
        """INSERT INTO simulation_meta
           (ticker, actual_outcome, result, nws_gap, nws_temps,
            resolution_date, cities_parsed, sim_timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, actual_outcome, actual_result, nws_gap,
         json.dumps(nws_temps), date_str,
         json.dumps([c["city_name"] for c in cities_data]),
         datetime.now(timezone.utc).isoformat()),
    )

    # Write resolved trade (for Tier 2 compatibility)
    if executed and amount > 0:
        entry_price = market_price if side == "YES" else (1.0 - market_price)
        won = (side == "YES" and actual_outcome == 1) or (side == "NO" and actual_outcome == 0)
        pnl_pct = (pnl / (entry_price * amount) * 100) if entry_price * amount > 0 else 0

        conn.execute(
            """INSERT INTO resolved_trades
               (ticker, title, category, side, amount, entry_price,
                our_probability, our_confidence, result, pnl, pnl_pct,
                strategy, resolved_at, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SIM_WEATHER', ?, ?)""",
            (ticker, title, category, side, amount, entry_price,
             final_prob, final_conf, actual_result, pnl, round(pnl_pct, 2),
             date_str, date_str),
        )

    conn.commit()

    return {
        "ticker": ticker,
        "executed": executed,
        "skip_reason": skip_reason,
        "side": side,
        "prob": final_prob,
        "conf": final_conf,
        "market_price": market_price,
        "price_gap": price_gap,
        "amount": amount if executed else 0,
        "actual_outcome": actual_outcome,
        "nws_gap": nws_gap,
        "pnl": pnl if executed else None,
        "won": (side == "YES" and actual_outcome == 1) or (side == "NO" and actual_outcome == 0) if executed else None,
    }


# ── Analysis ─────────────────────────────────────────────────

def run_gap_analysis(conn: sqlite3.Connection):
    """NWS gap distance breakdown — the most valuable output."""
    print("\n" + "=" * 70)
    print("NWS GAP DISTANCE BREAKDOWN")
    print("=" * 70)

    conn.row_factory = sqlite3.Row

    # Join decisions with simulation_meta for gap data
    rows = conn.execute("""
        SELECT d.ticker, d.executed, d.side, d.cloud_confidence,
               m.actual_outcome, m.nws_gap, m.result
        FROM decisions d
        JOIN simulation_meta m ON d.ticker = m.ticker
        WHERE d.executed = 1 AND d.amount > 0
    """).fetchall()

    if not rows:
        print("No executed trades to analyze.")
        return

    # Bucket by NWS gap
    buckets = {
        "under_3F": {"wins": 0, "losses": 0, "trades": []},
        "3_to_5F": {"wins": 0, "losses": 0, "trades": []},
        "5_to_8F": {"wins": 0, "losses": 0, "trades": []},
        "8_to_15F": {"wins": 0, "losses": 0, "trades": []},
        "above_15F": {"wins": 0, "losses": 0, "trades": []},
        "unknown": {"wins": 0, "losses": 0, "trades": []},
    }

    for row in rows:
        gap = row["nws_gap"]
        outcome = row["actual_outcome"]
        side = row["side"]
        won = (side == "YES" and outcome == 1) or (side == "NO" and outcome == 0)

        if gap is None:
            bucket = "unknown"
        elif gap < 3:
            bucket = "under_3F"
        elif gap < 5:
            bucket = "3_to_5F"
        elif gap < 8:
            bucket = "5_to_8F"
        elif gap < 15:
            bucket = "8_to_15F"
        else:
            bucket = "above_15F"

        if won:
            buckets[bucket]["wins"] += 1
        else:
            buckets[bucket]["losses"] += 1

    print(f"\n{'Gap Bucket':<15} {'Trades':>8} {'Wins':>8} {'Losses':>8} {'Win Rate':>10}")
    print("-" * 55)

    total_wins = 0
    total_losses = 0
    for name, data in buckets.items():
        total = data["wins"] + data["losses"]
        if total == 0:
            continue
        wr = data["wins"] / total * 100
        total_wins += data["wins"]
        total_losses += data["losses"]
        print(f"{name:<15} {total:>8} {data['wins']:>8} {data['losses']:>8} {wr:>9.1f}%")

    total = total_wins + total_losses
    if total > 0:
        print("-" * 55)
        print(f"{'TOTAL':<15} {total:>8} {total_wins:>8} {total_losses:>8} {total_wins/total*100:>9.1f}%")

    # Also show skip reasons breakdown
    print("\n" + "=" * 70)
    print("SKIP REASONS BREAKDOWN")
    print("=" * 70)

    skips = conn.execute("""
        SELECT error, COUNT(*) as cnt
        FROM decisions
        WHERE executed = 0 AND error IS NOT NULL
        GROUP BY error
        ORDER BY cnt DESC
    """).fetchall()

    for row in skips:
        print(f"  {row['cnt']:>5}x  {row['error'][:70]}")


def run_tier2_analysis(conn: sqlite3.Connection):
    """Run Tier 2 pattern analyzer against simulation data."""
    print("\n" + "=" * 70)
    print("TIER 2 PATTERN ANALYSIS")
    print("=" * 70)

    conn.row_factory = sqlite3.Row

    # Get high-confidence losses
    losses = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl, resolved_at
        FROM resolved_trades
        WHERE pnl <= 0 AND our_confidence > 0.70
        ORDER BY pnl ASC
        LIMIT 50
    """).fetchall()

    wins = conn.execute("""
        SELECT ticker, title, category, side, our_confidence, pnl
        FROM resolved_trades
        WHERE pnl > 0 AND our_confidence > 0.70
        ORDER BY pnl DESC
        LIMIT 20
    """).fetchall()

    if len(losses) < 3:
        print("Not enough losses for pattern analysis.")
        return

    loss_lines = []
    for l in losses:
        loss_lines.append(
            f"  LOSS: {l['title'][:60]} | conf={l['our_confidence']:.2f} | cat={l['category']} | pnl=${l['pnl']:.2f}"
        )

    win_lines = []
    for w in wins[:10]:
        win_lines.append(
            f"  WIN: {w['title'][:60]} | conf={w['our_confidence']:.2f} | cat={w['category']}"
        )

    prompt = f"""You are analyzing trading performance for a prediction market bot.

LOSSES with confidence above 0.70 (simulated historical trades):
{chr(10).join(loss_lines)}

WINS with confidence above 0.70 (simulated historical trades):
{chr(10).join(win_lines) if win_lines else '  (none)'}

Answer in JSON format:
{{
  "common_loss_pattern": "<what characteristic do the losses share?>",
  "data_gap": "<what data was available that should have predicted the correct outcome?>",
  "recommended_change": "<one specific threshold or prompt adjustment>",
  "affected_parameter": "<which parameter to change, e.g. WEATHER_CONFIDENCE>",
  "recommended_value": "<new value as string>",
  "confidence": "<low/medium/high>",
  "min_trades_to_validate": <integer>
}}"""

    print("\nSending to Ollama for pattern analysis...")
    result = query_ollama(prompt)

    if result:
        print(f"\n  Pattern: {result.get('common_loss_pattern', 'N/A')}")
        print(f"  Data gap: {result.get('data_gap', 'N/A')}")
        print(f"  Recommendation: {result.get('recommended_change', 'N/A')}")
        print(f"  Parameter: {result.get('affected_parameter', 'N/A')}")
        print(f"  Suggested value: {result.get('recommended_value', 'N/A')}")
        print(f"  Confidence: {result.get('confidence', 'N/A')}")
        print(f"  Min trades to validate: {result.get('min_trades_to_validate', 'N/A')}")

        # Log to calibration_reflections
        conn.execute(
            "INSERT INTO calibration_reflections (timestamp, tier, trade_id, observation, action_taken) VALUES (?, 2, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), "sim_pattern_analysis",
             json.dumps(result)[:500], "hypothesis_generated"),
        )
        conn.commit()
    else:
        print("  Failed to get pattern analysis from Ollama.")


def print_summary(results: list[dict]):
    """Print simulation summary statistics."""
    print("\n" + "=" * 70)
    print("SIMULATION SUMMARY")
    print("=" * 70)

    total = len(results)
    executed = [r for r in results if r["executed"]]
    skipped = [r for r in results if not r["executed"]]
    wins = [r for r in executed if r.get("won")]
    losses = [r for r in executed if r.get("won") is False]

    print(f"\nMarkets processed: {total}")
    print(f"Trades executed:   {len(executed)} ({len(executed)/total*100:.1f}%)")
    print(f"Trades skipped:    {len(skipped)} ({len(skipped)/total*100:.1f}%)")

    if executed:
        print(f"\nWins:   {len(wins)}")
        print(f"Losses: {len(losses)}")
        win_rate = len(wins) / len(executed) * 100 if executed else 0
        print(f"Win Rate: {win_rate:.1f}%")

        total_pnl = sum(r.get("pnl", 0) for r in executed if r.get("pnl") is not None)
        print(f"Total P&L: ${total_pnl:.2f}")

        avg_conf = sum(r["conf"] for r in executed) / len(executed)
        print(f"Avg Confidence: {avg_conf:.2f}")

        avg_gap = sum(r["nws_gap"] for r in executed if r["nws_gap"] is not None)
        gap_count = sum(1 for r in executed if r["nws_gap"] is not None)
        if gap_count:
            print(f"Avg NWS Gap: {avg_gap/gap_count:.1f}F")

    if skipped:
        skip_reasons = {}
        for r in skipped:
            reason = r.get("skip_reason", "unknown")
            # Normalize reasons
            if "confidence" in reason:
                reason = "confidence_too_low"
            elif "price gap" in reason:
                reason = "price_gap_too_small"
            elif "borderline" in reason:
                reason = "borderline_ev_floor"
            elif "nws" in reason:
                reason = "nws_data_missing"
            elif "kelly" in reason:
                reason = "kelly_no_edge"
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

        print(f"\nSkip breakdown:")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            print(f"  {count:>4}x  {reason}")


# ── Main ─────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Simulation runner for self-calibration acceleration")
    parser.add_argument("--limit", type=int, default=250, help="Max markets to simulate (default 250)")
    parser.add_argument("--category", default="weather", help="Category to simulate (default: weather)")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip Tier 2 analysis")
    args = parser.parse_args()

    print("=" * 70)
    print("KALBOT SIMULATION RUNNER")
    print(f"Model: {MODEL}")
    print(f"Category: {args.category}")
    print(f"Target: {args.limit} markets")
    print("=" * 70)

    # Load training data
    print(f"\nLoading {TRAIN_PATH}...")
    df = pd.read_parquet(TRAIN_PATH)
    weather = df[df["category"] == args.category].copy()

    # Filter to markets with parseable titles (temp markets only)
    weather = weather[weather["title"].str.contains("high temp", case=False, na=False)]
    print(f"Weather temp markets: {len(weather)}")

    # Sample up to limit
    if len(weather) > args.limit:
        weather = weather.sample(n=args.limit, random_state=42)
    print(f"Selected for simulation: {len(weather)}")

    # Extract all dates needed for prefetching
    dates = []
    for title in weather["title"]:
        d = parse_date_from_title(title)
        if d:
            dates.append(d)
    dates = list(set(dates))

    # Prefetch all historical temps in batch (much faster than per-market)
    prefetch_all_temps(dates)

    # Initialize database
    conn = init_sim_db()
    template = load_prompt_template()
    session_id = f"sim_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Run simulation
    results = []
    executed_count = 0
    skip_count = 0
    start_time = time.time()

    for i, (_, market) in enumerate(weather.iterrows()):
        elapsed = time.time() - start_time
        rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
        eta = (len(weather) - i - 1) / (rate / 60) / 60 if rate > 0 else 0

        print(f"\n[{i+1}/{len(weather)}] ({rate:.1f}/min, ETA {eta:.0f}min) ", end="")

        result = simulate_market(dict(market), template, conn, session_id)
        if result is None:
            print("SKIP (parse/data failure)")
            continue

        results.append(result)
        if result["executed"]:
            executed_count += 1
            won = "WIN" if result["won"] else "LOSS"
            print(f"{won} {result['side']} gap={result['nws_gap']:.0f}F conf={result['conf']:.2f} pnl=${result['pnl']:.2f}")
        else:
            skip_count += 1
            print(f"GATE: {result['skip_reason'][:50]}")

    # Print summary
    print_summary(results)

    # Run NWS gap analysis
    run_gap_analysis(conn)

    # Run Tier 2 pattern analysis
    if not args.skip_analysis:
        run_tier2_analysis(conn)

    elapsed_total = time.time() - start_time
    print(f"\n\nSimulation complete in {elapsed_total/60:.1f} minutes.")
    print(f"Results saved to: {SIM_DB}")
    conn.close()


if __name__ == "__main__":
    main()
