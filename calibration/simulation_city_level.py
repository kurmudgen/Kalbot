"""
City-level simulation runner: breaks down weather and TSA by city/group.

Weather: runs all available markets per city, records in per-city SQLite DBs.
TSA: groups by week type (holiday, seasonal, day-of-week), records per-group.

Uses existing simulation infrastructure (Ollama 32b, executor gates, Open-Meteo).
Does not touch live databases or running processes.
"""

import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Setup paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"), override=True)

TRAIN_PATH = os.path.join(ROOT, "data", "splits", "train.parquet")
PROMPT_PATH = os.path.join(ROOT, "prompts", "local_filter.txt")
BIAS_PATH = os.path.join(ROOT, "calibration", "kalshi_market_bias.json")
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = os.getenv("LOCAL_FILTER_MODEL", "qwen2.5:32b")

# Executor gate thresholds
CATEGORY_CONFIDENCE = {
    "weather": float(os.getenv("WEATHER_CONFIDENCE", "0.70")),
    "tsa": float(os.getenv("TSA_CONFIDENCE", "0.85")),
}
PRICE_GAP_MIN = float(os.getenv("PRICE_GAP_MIN", "0.08"))
KELLY_FRACTION = 0.25
MAX_TRADE_SIZE = float(os.getenv("MAX_TRADE_SIZE", "10"))
MAX_NIGHTLY_SPEND = float(os.getenv("MAX_NIGHTLY_SPEND", "50"))

# City-specific NWS gap config
_city_gap_raw = os.getenv("CITY_MIN_NWS_GAP", '{"default": 3}')
try:
    CITY_MIN_NWS_GAP = json.loads(_city_gap_raw)
except Exception:
    CITY_MIN_NWS_GAP = {"default": 3}

# Historical bias
MARKET_BIAS = {}
if os.path.exists(BIAS_PATH):
    with open(BIAS_PATH) as f:
        MARKET_BIAS = json.load(f).get("bins", {})

# Weather cities with coordinates
WEATHER_CITIES = {
    "denver": {"lat": 39.7392, "lon": -104.9903, "name": "Denver"},
    "chicago": {"lat": 41.8781, "lon": -87.6298, "name": "Chicago"},
    "new york": {"lat": 40.7128, "lon": -74.0060, "name": "New York City", "key": "nyc"},
    "miami": {"lat": 25.7617, "lon": -80.1918, "name": "Miami"},
    "austin": {"lat": 30.2672, "lon": -97.7431, "name": "Austin"},
    "houston": {"lat": 29.7604, "lon": -95.3698, "name": "Houston"},
    "philadelphia": {"lat": 39.9526, "lon": -75.1652, "name": "Philadelphia"},
    "seattle": {"lat": 47.6062, "lon": -122.3321, "name": "Seattle"},
}

# US holidays for TSA seasonal analysis
US_HOLIDAYS = [
    # 2024 holidays
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-06-19",
    "2024-07-04", "2024-09-02", "2024-10-14", "2024-11-11", "2024-11-28",
    "2024-11-29", "2024-12-24", "2024-12-25", "2024-12-31",
    # 2025 holidays
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-05-26", "2025-06-19",
    "2025-07-04", "2025-09-01", "2025-10-13", "2025-11-11", "2025-11-27",
    "2025-11-28", "2025-12-24", "2025-12-25", "2025-12-31",
    # 2026 holidays
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19",
    "2026-07-03", "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26",
    "2026-11-27", "2026-12-24", "2026-12-25", "2026-12-31",
]

# ── Shared Utilities ──────────────────────────────────────────

def get_historical_bias(market_price_cents: int) -> float:
    for bin_key, data in MARKET_BIAS.items():
        lo, hi = bin_key.split("-")
        if int(lo) <= market_price_cents <= int(hi):
            return data.get("bias_pct", 0) / 100.0
    return 0.0


def load_prompt_template() -> str:
    with open(PROMPT_PATH) as f:
        return f.read().strip()


def check_ollama() -> bool:
    try:
        r = requests.get(f"{OLLAMA_URL.replace('/api/generate', '')}/api/tags", timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def query_ollama(prompt: str, max_retries: int = 2) -> dict | None:
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
            json_match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            if attempt < max_retries:
                time.sleep(1)
                continue
            return None
        except Exception:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
    return None


def parse_date_from_title(title: str) -> Optional[str]:
    m = re.search(r'on (\w+ \d{1,2}, \d{4})', title)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%b %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS simulation_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            actual_outcome INTEGER,
            result TEXT,
            data_gap REAL,
            actual_value TEXT,
            resolution_date TEXT,
            sim_timestamp TEXT
        )
    """)
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS city_breakdown (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            city TEXT,
            nws_gap REAL,
            nws_temp REAL,
            threshold REAL,
            executed INTEGER,
            won INTEGER,
            pnl REAL
        )
    """)
    conn.commit()
    return conn


def already_simulated(conn: sqlite3.Connection, ticker: str) -> bool:
    return conn.execute("SELECT 1 FROM decisions WHERE ticker = ?", (ticker,)).fetchone() is not None


# ── Weather City Simulation ───────────────────────────────────

# Temperature cache
_temp_cache: dict[tuple[str, str], float | None] = {}
_batch_fetched: set[str] = set()


def fetch_historical_temps_batch(city_key: str, lat: float, lon: float,
                                  start_date: str, end_date: str) -> dict[str, float]:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return {}
        data = r.json()
        dates = data.get("daily", {}).get("time", [])
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        return {d: t for d, t in zip(dates, temps) if t is not None}
    except Exception as e:
        print(f"    Open-Meteo error for {city_key}: {e}")
        return {}


def prefetch_temps_for_city(city_key: str, lat: float, lon: float, dates: list[str]):
    if not dates or city_key in _batch_fetched:
        return
    min_date = min(dates)
    max_date = max(dates)
    print(f"  Prefetching {city_key} temps ({min_date} to {max_date})...")
    temps = fetch_historical_temps_batch(city_key, lat, lon, min_date, max_date)
    for date_str, temp in temps.items():
        _temp_cache[(city_key, date_str)] = temp
    _batch_fetched.add(city_key)
    print(f"    {len(temps)} days cached")
    time.sleep(0.5)


def prefetch_all_temps(dates: list[str]):
    if not dates:
        return
    min_date = min(dates)
    max_date = max(dates)
    print(f"\nPrefetching all city temps ({min_date} to {max_date})...")
    for city_search, city_info in WEATHER_CITIES.items():
        key = city_info.get("key", city_search)
        if key in _batch_fetched:
            continue
        temps = fetch_historical_temps_batch(key, city_info["lat"], city_info["lon"], min_date, max_date)
        for date_str, temp in temps.items():
            _temp_cache[(key, date_str)] = temp
        _batch_fetched.add(key)
        print(f"  {city_info['name']}: {len(temps)} days")
        time.sleep(0.5)
    print(f"  Total cached: {len(_temp_cache)}")


def parse_cities_from_title(title: str) -> list[dict]:
    results = []
    title_lower = title.lower()
    for city_search, city_info in WEATHER_CITIES.items():
        if city_search not in title_lower:
            continue
        city_key = city_info.get("key", city_search)
        city_name_escaped = re.escape(city_search)

        # Range: "City: XX° to YY°"
        range_match = re.search(rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*to\s*(\d+)', title_lower)
        if range_match:
            results.append({"city_key": city_key, "city_name": city_info["name"],
                            "threshold_low": int(range_match.group(1)),
                            "threshold_high": int(range_match.group(2)),
                            "threshold_type": "range"})
            continue

        # "or below"
        below_match = re.search(rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*or below', title_lower)
        if below_match:
            thresh = int(below_match.group(1))
            results.append({"city_key": city_key, "city_name": city_info["name"],
                            "threshold_low": None, "threshold_high": thresh,
                            "threshold_type": "below"})
            continue

        # "or above"
        above_match = re.search(rf'{city_name_escaped}\s*:\s*(\d+)\s*[°�]?\s*or above', title_lower)
        if above_match:
            thresh = int(above_match.group(1))
            results.append({"city_key": city_key, "city_name": city_info["name"],
                            "threshold_low": thresh, "threshold_high": None,
                            "threshold_type": "above"})
            continue

        # Single-city patterns: "be XX-YY°" or ">XX°" or "<XX°"
        if len(results) == 0:
            range2 = re.search(r'be\s+(\d+)\s*-\s*(\d+)\s*[°�]', title_lower)
            if range2:
                results.append({"city_key": city_key, "city_name": city_info["name"],
                                "threshold_low": int(range2.group(1)),
                                "threshold_high": int(range2.group(2)),
                                "threshold_type": "range"})
                continue

            gt = re.search(r'>(\d+)\s*[°�]', title_lower)
            if gt:
                results.append({"city_key": city_key, "city_name": city_info["name"],
                                "threshold_low": int(gt.group(1)),
                                "threshold_high": None,
                                "threshold_type": "above"})
                continue

            lt = re.search(r'<(\d+)\s*[°�]', title_lower)
            if lt:
                results.append({"city_key": city_key, "city_name": city_info["name"],
                                "threshold_low": None, "threshold_high": int(lt.group(1)),
                                "threshold_type": "below"})
                continue

    return results


def build_nws_context(cities_data: list[dict], date_str: str):
    forecasts = []
    nws_temps = {}
    for city in cities_data:
        actual_temp = _temp_cache.get((city["city_key"], date_str))
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


def compute_city_gap(city_data: dict, nws_temps: dict) -> Optional[float]:
    actual = nws_temps.get(city_data["city_key"])
    if actual is None:
        return None
    if city_data["threshold_type"] == "range":
        mid = (city_data["threshold_low"] + city_data["threshold_high"]) / 2.0
        return abs(actual - mid)
    elif city_data["threshold_type"] == "below":
        return abs(actual - city_data["threshold_high"])
    elif city_data["threshold_type"] == "above":
        return abs(actual - city_data["threshold_low"])
    return None


def compute_min_nws_gap(cities_data: list[dict], nws_temps: dict) -> Optional[float]:
    gaps = [compute_city_gap(c, nws_temps) for c in cities_data]
    gaps = [g for g in gaps if g is not None]
    return min(gaps) if gaps else None


def apply_weather_gates(score: dict, title: str, cities_data: list[dict],
                        nws_temps: dict) -> tuple[bool, str, dict]:
    cloud_prob = score["probability"]
    cloud_conf = score["confidence"]
    market_price = score["market_price"]
    reasoning = score.get("reasoning", "")

    if cloud_prob == 0.0 or cloud_conf == 0.0 or market_price == 0.0:
        return False, "null_signal", {}

    price_gap = abs(cloud_prob - market_price)
    side = "YES" if cloud_prob > market_price else "NO"

    # Historical bias
    price_cents = int(market_price * 100)
    hist_bias = get_historical_bias(price_cents)
    bias_aligned = (side == "YES" and hist_bias > 0) or (side == "NO" and hist_bias < 0)
    if bias_aligned:
        cloud_conf = min(1.0, cloud_conf * 1.1)

    # Kelly sizing
    cost = market_price if side == "YES" else (1.0 - market_price)
    our_prob = cloud_prob if side == "YES" else (1.0 - cloud_prob)
    b = (1.0 / cost) - 1 if cost > 0 else 0
    q = 1.0 - our_prob
    kelly_raw = (b * our_prob - q) / b if b > 0 else 0
    kelly_bet = max(0, kelly_raw * KELLY_FRACTION)
    amount = round(min(kelly_bet * MAX_NIGHTLY_SPEND, MAX_TRADE_SIZE), 2)

    trade = {
        "side": side, "amount": amount,
        "cloud_probability": cloud_prob, "cloud_confidence": cloud_conf,
        "price_gap": price_gap, "market_price": market_price,
        "reasoning": reasoning,
    }

    # Gate: Category confidence
    cat_conf_min = CATEGORY_CONFIDENCE.get("weather", 0.70)
    if cloud_conf < cat_conf_min - 0.001:
        return False, f"confidence {cloud_conf:.2f} < {cat_conf_min}", trade

    # Gate: Price gap minimum
    if price_gap < PRICE_GAP_MIN:
        return False, f"price_gap {price_gap:.2f} < {PRICE_GAP_MIN}", trade

    # Gate: NWS required for borderline
    if 0.20 < cloud_prob < 0.80:
        has_nws = "NWS" in reasoning or "nws" in reasoning or "official forecast" in reasoning.lower()
        if not has_nws:
            return False, "nws_data_missing", trade

    # Gate: Borderline EV floor
    if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.85 - 0.001:
        return False, f"borderline_EV: prob={cloud_prob:.2f} conf={cloud_conf:.2f}", trade

    # Gate: City-specific NWS gap minimum
    for city in cities_data:
        city_key = city["city_key"]
        city_gap = compute_city_gap(city, nws_temps)
        if city_gap is not None:
            min_gap = CITY_MIN_NWS_GAP.get(city_key, CITY_MIN_NWS_GAP.get("default", 3))
            if city_gap < min_gap:
                return False, f"city_nws_gap: {city_key} gap={city_gap:.1f}F < min={min_gap}F", trade

    # Gate: Kelly says no edge
    if amount <= 0:
        return False, "kelly_no_edge", trade

    return True, "", trade


def simulate_weather_market(market, template, conn, session_id):
    ticker = market["market_id"]
    title = market["title"]
    category = market["category"]
    actual_outcome = int(market["actual_outcome"])
    actual_result = market.get("result", "yes" if actual_outcome == 1 else "no")
    raw_price = market.get("market_price_at_close", market.get("last_price", 50))
    if raw_price is None:
        raw_price = 50
    market_price = raw_price / 100.0 if raw_price > 1 else raw_price
    if market_price < 0.03 or market_price > 0.97:
        market_price = 0.50

    if already_simulated(conn, ticker):
        return None

    date_str = parse_date_from_title(title)
    cities_data = parse_cities_from_title(title)
    if not date_str or not cities_data:
        return None

    nws_context, nws_temps = build_nws_context(cities_data, date_str)
    min_gap = compute_min_nws_gap(cities_data, nws_temps)
    if not nws_context:
        return None

    prompt = f"""{template}

Market question: "{title}"
Category: {category}
Current YES price: {market_price:.2f}{nws_context}
Recent relevant headlines:
No recent headlines available.
"""

    result = query_ollama(prompt)
    if result is None:
        return None

    prob = float(result.get("probability", 0.5))
    conf = float(result.get("confidence", 0.5))
    reasoning = result.get("reasoning", "")

    score = {"probability": prob, "confidence": conf, "reasoning": reasoning, "market_price": market_price}
    executed, skip_reason, trade = apply_weather_gates(score, title, cities_data, nws_temps)

    side = trade.get("side", "YES" if prob > market_price else "NO")
    amount = trade.get("amount", 0)
    final_prob = trade.get("cloud_probability", prob)
    final_conf = trade.get("cloud_confidence", conf)
    price_gap = trade.get("price_gap", abs(prob - market_price))

    pnl = 0.0
    won = None
    if executed and amount > 0:
        if side == "YES":
            pnl = (1.0 - market_price) * amount if actual_outcome == 1 else -market_price * amount
        else:
            pnl = market_price * amount if actual_outcome == 0 else -(1.0 - market_price) * amount
        pnl = round(pnl, 2)
        won = (side == "YES" and actual_outcome == 1) or (side == "NO" and actual_outcome == 0)

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

    # Write simulation meta
    conn.execute(
        """INSERT INTO simulation_meta
           (ticker, actual_outcome, result, data_gap, actual_value,
            resolution_date, sim_timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (ticker, actual_outcome, actual_result, min_gap,
         json.dumps(nws_temps), date_str,
         datetime.now(timezone.utc).isoformat()),
    )

    # Write resolved trade
    if executed and amount > 0:
        entry_price = market_price if side == "YES" else (1.0 - market_price)
        pnl_pct = (pnl / (entry_price * amount) * 100) if entry_price * amount > 0 else 0
        conn.execute(
            """INSERT INTO resolved_trades
               (ticker, title, category, side, amount, entry_price,
                our_probability, our_confidence, result, pnl, pnl_pct,
                strategy, resolved_at, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, category, side, amount, entry_price,
             final_prob, final_conf, actual_result, pnl, round(pnl_pct, 2),
             f"SIM_WEATHER_CITY", date_str, date_str),
        )

    # Write per-city breakdown
    for city in cities_data:
        city_gap = compute_city_gap(city, nws_temps)
        conn.execute(
            """INSERT INTO city_breakdown
               (ticker, city, nws_gap, nws_temp, threshold, executed, won, pnl)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, city["city_key"], city_gap,
             nws_temps.get(city["city_key"]),
             (city.get("threshold_low", 0) or 0 + (city.get("threshold_high", 0) or 0)) / 2.0
             if city["threshold_type"] == "range"
             else (city.get("threshold_high") or city.get("threshold_low")),
             int(executed), int(won) if won is not None else None,
             pnl if executed else None),
        )

    conn.commit()

    return {
        "ticker": ticker, "executed": executed, "won": won,
        "nws_gap": min_gap, "pnl": pnl if executed else None,
        "cities": [c["city_key"] for c in cities_data],
    }


def print_weather_city_summary(conn: sqlite3.Connection, city_key: str, city_name: str):
    """Print the required city summary format."""
    total_markets = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
    executed = conn.execute("SELECT COUNT(*) FROM resolved_trades").fetchone()[0]

    if executed == 0:
        print(f"\nCITY: {city_name}")
        print(f"Markets available: {total_markets}")
        print(f"Executed trades: 0")
        print(f"Win rate: N/A")
        return

    wins = conn.execute(
        "SELECT COUNT(*) FROM resolved_trades WHERE LOWER(result) = LOWER(side)"
    ).fetchone()[0]
    wr = wins / executed * 100

    print(f"\nCITY: {city_name}")
    print(f"Markets available: {total_markets}")
    print(f"Executed trades: {executed}")
    print(f"Win rate: {wr:.1f}%")

    # Gap breakdown from city_breakdown table
    print("Gap breakdown:")
    gap_buckets = [
        ("Under 3F", 0, 3),
        ("3-5F", 3, 5),
        ("5-8F", 5, 8),
        ("8-15F", 8, 15),
        ("Above 15F", 15, 999),
    ]

    for label, lo, hi in gap_buckets:
        row = conn.execute("""
            SELECT COUNT(*) as n,
                   SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as w
            FROM city_breakdown
            WHERE city = ? AND nws_gap >= ? AND nws_gap < ? AND executed = 1
        """, (city_key, lo, hi)).fetchone()
        n, w = row[0], row[1] or 0
        if n > 0:
            print(f"  {label}: {n} trades, {w/n*100:.1f}% WR")
        else:
            print(f"  {label}: 0 trades")

    # Recommend min gap: find lowest bucket with >85% WR and >=5 trades
    best_gap = 3  # default
    for lo in [0, 3, 5, 8]:
        row = conn.execute("""
            SELECT COUNT(*) as n,
                   SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as w
            FROM city_breakdown
            WHERE city = ? AND nws_gap >= ? AND executed = 1
        """, (city_key, lo)).fetchone()
        n, w = row[0], row[1] or 0
        if n >= 5 and w / n >= 0.85:
            best_gap = lo
            break
    print(f"Recommended min gap: {best_gap}F")

    if executed < 20:
        print("Notable patterns: INSUFFICIENT DATA — fewer than 20 executed trades")
    else:
        total_pnl = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM resolved_trades").fetchone()[0]
        print(f"Notable patterns: {wins}W/{executed-wins}L, ${total_pnl:.2f} total PnL")


# ── TSA Simulation ────────────────────────────────────────────

def parse_tsa_threshold(title: str) -> Optional[int]:
    """Extract TSA passenger threshold from title."""
    m = re.search(r'more than (\d+) people', title.lower())
    if m:
        return int(m.group(1))
    return None


def parse_tsa_week_date(title: str) -> Optional[str]:
    """Extract the week-ending date from TSA market title."""
    # Look for "this week" or date patterns
    m = re.search(r'on (\w+ \d{1,2}, \d{4})', title)
    if m:
        try:
            return datetime.strptime(m.group(1), "%b %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try "week" pattern — use ticker date
    return None


def is_holiday_week(date_str: str) -> bool:
    """Check if date falls within 7 days of a US holiday."""
    if not date_str:
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        for h in US_HOLIDAYS:
            hdt = datetime.strptime(h, "%Y-%m-%d")
            if abs((dt - hdt).days) <= 7:
                return True
    except ValueError:
        pass
    return False


def is_summer(date_str: str) -> bool:
    """June-August = summer peak."""
    if not date_str:
        return False
    try:
        month = int(date_str.split("-")[1])
        return 6 <= month <= 8
    except (ValueError, IndexError):
        return False


def get_day_of_week(date_str: str) -> Optional[str]:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")
    except ValueError:
        return None


# Load TSA volumes from local CSV
def load_tsa_volumes() -> dict:
    """Load historical TSA daily volumes. Returns {date_str: volume}."""
    csv_path = os.path.join(ROOT, "data", "raw", "tsa", "tsa_daily_volumes.csv")
    if not os.path.exists(csv_path):
        print(f"  WARNING: TSA volume CSV not found at {csv_path}")
        return {}

    volumes = {}
    try:
        tsa_df = pd.read_csv(csv_path)
        # Try common column name patterns
        date_col = None
        vol_col = None
        for c in tsa_df.columns:
            cl = c.lower()
            if 'date' in cl:
                date_col = c
            if 'number' in cl or 'volume' in cl or 'passenger' in cl or 'throughput' in cl:
                vol_col = c

        if date_col and vol_col:
            for _, row in tsa_df.iterrows():
                try:
                    d = pd.to_datetime(row[date_col]).strftime("%Y-%m-%d")
                    v = int(str(row[vol_col]).replace(",", ""))
                    volumes[d] = v
                except Exception:
                    continue
        print(f"  Loaded {len(volumes)} TSA daily volumes")
    except Exception as e:
        print(f"  Error loading TSA volumes: {e}")
    return volumes


def build_tsa_context(title: str, date_str: str, tsa_volumes: dict, threshold: int) -> tuple[str, float]:
    """Build TSA data context. Returns (context_str, data_gap_pct)."""
    if not date_str or not tsa_volumes:
        return "", 0.0

    # Get weekly average around the date
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        week_dates = [(dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
        week_vols = [tsa_volumes[d] for d in week_dates if d in tsa_volumes]

        if not week_vols:
            return "", 0.0

        avg_vol = sum(week_vols) / len(week_vols)
        gap_pct = (avg_vol - threshold) / threshold * 100

        context = (
            f"\nCRITICAL — TSA checkpoint data for this week:\n"
            f"Average daily passengers: {avg_vol:,.0f} ({len(week_vols)} days of data)\n"
            f"Market threshold: {threshold:,}\n"
            f"Gap: {gap_pct:+.1f}% ({'above' if avg_vol > threshold else 'below'} threshold)\n"
            f"This is official TSA data — the settlement source."
        )
        return context, gap_pct
    except Exception:
        return "", 0.0


def apply_tsa_gates(score: dict) -> tuple[bool, str, dict]:
    cloud_prob = score["probability"]
    cloud_conf = score["confidence"]
    market_price = score["market_price"]
    reasoning = score.get("reasoning", "")

    if cloud_prob == 0.0 or cloud_conf == 0.0 or market_price == 0.0:
        return False, "null_signal", {}

    price_gap = abs(cloud_prob - market_price)
    side = "YES" if cloud_prob > market_price else "NO"

    price_cents = int(market_price * 100)
    hist_bias = get_historical_bias(price_cents)
    bias_aligned = (side == "YES" and hist_bias > 0) or (side == "NO" and hist_bias < 0)
    if bias_aligned:
        cloud_conf = min(1.0, cloud_conf * 1.1)

    cost = market_price if side == "YES" else (1.0 - market_price)
    our_prob = cloud_prob if side == "YES" else (1.0 - cloud_prob)
    b = (1.0 / cost) - 1 if cost > 0 else 0
    q = 1.0 - our_prob
    kelly_raw = (b * our_prob - q) / b if b > 0 else 0
    kelly_bet = max(0, kelly_raw * KELLY_FRACTION)
    amount = round(min(kelly_bet * MAX_NIGHTLY_SPEND, MAX_TRADE_SIZE), 2)

    trade = {
        "side": side, "amount": amount,
        "cloud_probability": cloud_prob, "cloud_confidence": cloud_conf,
        "price_gap": price_gap, "market_price": market_price,
        "reasoning": reasoning,
    }

    cat_conf_min = CATEGORY_CONFIDENCE.get("tsa", 0.85)
    if cloud_conf < cat_conf_min - 0.001:
        return False, f"confidence {cloud_conf:.2f} < {cat_conf_min}", trade
    if price_gap < PRICE_GAP_MIN:
        return False, f"price_gap {price_gap:.2f} < {PRICE_GAP_MIN}", trade
    if 0.20 < cloud_prob < 0.80 and cloud_conf < 0.85 - 0.001:
        return False, f"borderline_EV", trade
    if amount <= 0:
        return False, "kelly_no_edge", trade

    return True, "", trade


def simulate_tsa_market(market, template, conn, session_id, tsa_volumes):
    ticker = market["market_id"]
    title = market["title"]
    category = market["category"]
    actual_outcome = int(market["actual_outcome"])
    actual_result = market.get("result", "yes" if actual_outcome == 1 else "no")
    raw_price = market.get("market_price_at_close", market.get("last_price", 50))
    if raw_price is None:
        raw_price = 50
    market_price = raw_price / 100.0 if raw_price > 1 else raw_price
    if market_price < 0.03 or market_price > 0.97:
        market_price = 0.50

    if already_simulated(conn, ticker):
        return None

    threshold = parse_tsa_threshold(title)
    if threshold is None:
        return None

    date_str = parse_date_from_title(title)
    # Try ticker date if title doesn't have one
    if not date_str:
        m = re.search(r'(\d{2})([A-Z]{3})(\d{2})', ticker)
        if m:
            try:
                year = 2000 + int(m.group(1))
                mon = m.group(2)
                day = int(m.group(3))
                date_str = datetime.strptime(f"{year} {mon} {day}", "%Y %b %d").strftime("%Y-%m-%d")
            except ValueError:
                pass

    tsa_context, gap_pct = build_tsa_context(title, date_str, tsa_volumes, threshold)
    if not tsa_context:
        return None

    prompt = f"""{template}

Market question: "{title}"
Category: tsa
Current YES price: {market_price:.2f}{tsa_context}
Recent relevant headlines:
No recent headlines available.
"""

    result = query_ollama(prompt)
    if result is None:
        return None

    prob = float(result.get("probability", 0.5))
    conf = float(result.get("confidence", 0.5))
    reasoning = result.get("reasoning", "")

    score = {"probability": prob, "confidence": conf, "reasoning": reasoning, "market_price": market_price}
    executed, skip_reason, trade = apply_tsa_gates(score)

    side = trade.get("side", "YES" if prob > market_price else "NO")
    amount = trade.get("amount", 0)
    final_prob = trade.get("cloud_probability", prob)
    final_conf = trade.get("cloud_confidence", conf)
    price_gap = trade.get("price_gap", abs(prob - market_price))

    pnl = 0.0
    won = None
    if executed and amount > 0:
        if side == "YES":
            pnl = (1.0 - market_price) * amount if actual_outcome == 1 else -market_price * amount
        else:
            pnl = market_price * amount if actual_outcome == 0 else -(1.0 - market_price) * amount
        pnl = round(pnl, 2)
        won = (side == "YES" and actual_outcome == 1) or (side == "NO" and actual_outcome == 0)

    # Determine TSA group
    is_hol = is_holiday_week(date_str) if date_str else False
    is_sum = is_summer(date_str) if date_str else False
    dow = get_day_of_week(date_str)

    conn.execute(
        """INSERT INTO decisions
           (ticker, title, category, cloud_probability, cloud_confidence,
            market_price, price_gap, side, amount, reasoning,
            mode, executed, error, decided_at, session_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SIM', ?, ?, ?, ?)""",
        (ticker, title, "tsa", final_prob, final_conf,
         market_price, price_gap, side, amount if executed else 0, reasoning,
         int(executed), skip_reason if not executed else None,
         date_str or "", session_id),
    )

    conn.execute(
        """INSERT INTO simulation_meta
           (ticker, actual_outcome, result, data_gap, actual_value,
            resolution_date, sim_timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (ticker, actual_outcome, actual_result, gap_pct,
         str(threshold), date_str or "",
         datetime.now(timezone.utc).isoformat()),
    )

    if executed and amount > 0:
        entry_price = market_price if side == "YES" else (1.0 - market_price)
        pnl_pct = (pnl / (entry_price * amount) * 100) if entry_price * amount > 0 else 0
        group_tag = "holiday" if is_hol else ("summer" if is_sum else "regular")
        conn.execute(
            """INSERT INTO resolved_trades
               (ticker, title, category, side, amount, entry_price,
                our_probability, our_confidence, result, pnl, pnl_pct,
                strategy, resolved_at, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, title, "tsa", side, amount, entry_price,
             final_prob, final_conf, actual_result, pnl, round(pnl_pct, 2),
             f"SIM_TSA_{group_tag.upper()}", date_str or "", date_str or ""),
        )

    conn.commit()

    return {
        "ticker": ticker, "executed": executed, "won": won,
        "gap_pct": gap_pct, "pnl": pnl if executed else None,
        "is_holiday": is_hol, "is_summer": is_sum, "dow": dow,
    }


def print_tsa_group_summary(conn: sqlite3.Connection, group_name: str, strategy_filter: str = None):
    """Print TSA group summary."""
    if strategy_filter:
        total = conn.execute(
            "SELECT COUNT(*) FROM resolved_trades WHERE strategy = ?", (strategy_filter,)
        ).fetchone()[0]
        wins = conn.execute(
            "SELECT COUNT(*) FROM resolved_trades WHERE strategy = ? AND LOWER(result) = LOWER(side)",
            (strategy_filter,)
        ).fetchone()[0]
    else:
        total = conn.execute("SELECT COUNT(*) FROM resolved_trades").fetchone()[0]
        wins = conn.execute(
            "SELECT COUNT(*) FROM resolved_trades WHERE LOWER(result) = LOWER(side)"
        ).fetchone()[0]

    if total == 0:
        print(f"\nGROUP: {group_name}")
        print("  Executed trades: 0")
        return

    wr = wins / total * 100
    print(f"\nGROUP: {group_name}")
    print(f"  Executed trades: {total}")
    print(f"  Win rate: {wr:.1f}%")

    # Gap breakdown
    gap_buckets = [
        ("Under 3%", 0, 3),
        ("3-5%", 3, 5),
        ("5-10%", 5, 10),
        ("Above 10%", 10, 999),
    ]

    for label, lo, hi in gap_buckets:
        if strategy_filter:
            row = conn.execute("""
                SELECT COUNT(*) as n,
                       SUM(CASE WHEN LOWER(r.result) = LOWER(r.side) THEN 1 ELSE 0 END) as w
                FROM resolved_trades r
                JOIN simulation_meta m ON r.ticker = m.ticker
                WHERE r.strategy = ? AND ABS(m.data_gap) >= ? AND ABS(m.data_gap) < ?
            """, (strategy_filter, lo, hi)).fetchone()
        else:
            row = conn.execute("""
                SELECT COUNT(*) as n,
                       SUM(CASE WHEN LOWER(r.result) = LOWER(r.side) THEN 1 ELSE 0 END) as w
                FROM resolved_trades r
                JOIN simulation_meta m ON r.ticker = m.ticker
                WHERE ABS(m.data_gap) >= ? AND ABS(m.data_gap) < ?
            """, (lo, hi)).fetchone()
        n, w = row[0], row[1] or 0
        if n > 0:
            print(f"  {label}: {n} trades, {w/n*100:.1f}% WR")

    if total < 20:
        print(f"  INSUFFICIENT DATA — only {total} executed trades")


# ── Main Orchestrator ─────────────────────────────────────────

def run_all_weather(df: pd.DataFrame, template: str):
    """Single-pass simulation through all weather markets.
    Each market scored once, results recorded in per-city breakdown."""
    db_path = os.path.join(ROOT, "calibration", "simulation_weather_cities.sqlite")

    print(f"\n{'#' * 70}")
    print(f"# WEATHER — ALL CITIES (SINGLE PASS)")
    print(f"# Output: {db_path}")
    print(f"{'#' * 70}")

    weather_df = df[df['category'] == 'weather'].copy()
    print(f"Total unique weather markets: {len(weather_df)}")

    conn = init_db(db_path)
    session_id = f"sim_weather_cities_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Prefetch temps for ALL cities across all dates
    dates = []
    for _, m in weather_df.iterrows():
        d = parse_date_from_title(m["title"])
        if d:
            dates.append(d)
    prefetch_all_temps(list(set(dates)))

    start_time = time.time()
    executed_count = 0
    win_count = 0
    total_pnl = 0.0
    skipped_existing = 0

    for i, (_, market) in enumerate(weather_df.iterrows()):
        elapsed = time.time() - start_time
        rate = (i + 1) / (elapsed / 60) if elapsed > 0 else 0
        remaining = (len(weather_df) - i - 1) / rate if rate > 0 else 0

        if (i + 1) % 100 == 0 or i == 0:
            wr = win_count / executed_count * 100 if executed_count > 0 else 0
            print(f"  [{i+1}/{len(weather_df)}] ({rate:.1f}/min, ETA {remaining:.0f}min) "
                  f"executed={executed_count} wins={win_count} WR={wr:.1f}%")

        result = simulate_weather_market(market, template, conn, session_id)
        if result is None:
            skipped_existing += 1
            continue
        if result["executed"]:
            executed_count += 1
            if result["won"]:
                win_count += 1
            total_pnl += result.get("pnl", 0) or 0

        # Periodic commit checkpoint & Ollama health
        if (i + 1) % 500 == 0:
            if not check_ollama():
                print("  Ollama unresponsive, waiting 60s...")
                time.sleep(60)

    elapsed = time.time() - start_time
    wr = win_count / executed_count * 100 if executed_count > 0 else 0
    print(f"\n{'=' * 70}")
    print(f"WEATHER SINGLE-PASS COMPLETE")
    print(f"  Time: {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")
    print(f"  Markets: {len(weather_df)}, Skipped (resume): {skipped_existing}")
    print(f"  Executed: {executed_count}, Wins: {win_count}, WR: {wr:.1f}%")
    print(f"  PnL: ${total_pnl:.2f}")

    # Print per-city summaries
    print(f"\n{'=' * 70}")
    print("PER-CITY BREAKDOWN")
    print(f"{'=' * 70}")

    city_results = []
    for city_search, city_info in WEATHER_CITIES.items():
        city_key = city_info.get("key", city_search)
        city_name = city_info["name"]
        print_weather_city_summary(conn, city_key, city_name)

        # Collect for master table
        row = conn.execute("""
            SELECT COUNT(*) as n,
                   SUM(CASE WHEN won=1 THEN 1 ELSE 0 END) as w,
                   COALESCE(SUM(pnl), 0) as p
            FROM city_breakdown WHERE city = ? AND executed = 1
        """, (city_key,)).fetchone()
        n, w, p = row[0], row[1] or 0, row[2] or 0
        total_city = conn.execute(
            "SELECT COUNT(*) FROM city_breakdown WHERE city = ?", (city_key,)
        ).fetchone()[0]
        city_results.append({
            "city": city_name, "city_key": city_key,
            "markets": total_city, "executed": n,
            "wins": w, "wr": w / n * 100 if n > 0 else 0,
            "pnl": p,
        })

    conn.close()
    return city_results


def run_tsa_simulation(df: pd.DataFrame, template: str):
    """Run TSA simulation with group breakdowns."""
    db_path = os.path.join(ROOT, "calibration", "simulation_tsa_groups.sqlite")

    print(f"\n{'#' * 70}")
    print(f"# TSA GROUP SIMULATION")
    print(f"# Output: {db_path}")
    print(f"{'#' * 70}")

    # Filter TSA markets (exclude sports like "UTSA")
    tsa_df = df[df['category'] == 'tsa'].copy()
    # Remove sports contamination
    sports_mask = tsa_df['title'].str.contains('UTSA|Total Points|Spread|Moneyline', case=False, na=False)
    tsa_df = tsa_df[~sports_mask]
    print(f"TSA markets (after sports filter): {len(tsa_df)}")

    if len(tsa_df) == 0:
        print("SKIP: No TSA markets found")
        return None

    conn = init_db(db_path)
    session_id = f"sim_tsa_groups_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Load TSA volumes
    tsa_volumes = load_tsa_volumes()

    start_time = time.time()
    executed_count = 0
    win_count = 0
    total_pnl = 0.0
    results_by_group = {"holiday": [], "summer": [], "regular": []}

    for i, (_, market) in enumerate(tsa_df.iterrows()):
        elapsed = time.time() - start_time
        rate = (i + 1) / (elapsed / 60) if elapsed > 0 else 0
        remaining = (len(tsa_df) - i - 1) / rate if rate > 0 else 0

        if (i + 1) % 50 == 0 or i == 0:
            print(f"  [{i+1}/{len(tsa_df)}] ({rate:.1f}/min, ETA {remaining:.0f}min) "
                  f"executed={executed_count}")

        result = simulate_tsa_market(market, template, conn, session_id, tsa_volumes)
        if result and result["executed"]:
            executed_count += 1
            if result["won"]:
                win_count += 1
            total_pnl += result.get("pnl", 0) or 0

            group = "holiday" if result["is_holiday"] else ("summer" if result["is_summer"] else "regular")
            results_by_group[group].append(result)

    elapsed = time.time() - start_time
    print(f"\n  Completed TSA in {elapsed/60:.1f} minutes")
    print(f"  {executed_count} executed, {win_count} wins, ${total_pnl:.2f} PnL")

    # Print group summaries
    print(f"\n{'=' * 70}")
    print("TSA GROUP BREAKDOWNS")
    print(f"{'=' * 70}")

    print_tsa_group_summary(conn, "ALL TSA")
    print_tsa_group_summary(conn, "Holiday Weeks", "SIM_TSA_HOLIDAY")
    print_tsa_group_summary(conn, "Summer (Jun-Aug)", "SIM_TSA_SUMMER")
    print_tsa_group_summary(conn, "Regular Weeks", "SIM_TSA_REGULAR")

    conn.close()

    return {
        "total": executed_count, "wins": win_count,
        "wr": win_count / executed_count * 100 if executed_count > 0 else 0,
        "pnl": total_pnl,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="City-level simulation runner")
    parser.add_argument("--weather-only", action="store_true", help="Run only weather")
    parser.add_argument("--tsa-only", action="store_true", help="Run only TSA")
    args = parser.parse_args()

    print("=" * 70)
    print("KALBOT CITY-LEVEL SIMULATION RUNNER")
    print(f"Model: {MODEL}")
    print(f"City gap config: {CITY_MIN_NWS_GAP}")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)

    if not check_ollama():
        print("FATAL: Ollama not responding")
        return

    # Load dataset
    print(f"\nLoading {TRAIN_PATH}...")
    df = pd.read_parquet(TRAIN_PATH)
    template = load_prompt_template()

    weather_results = []
    tsa_result = None

    # ── Weather: single pass, all markets ─────────────────────
    if not args.tsa_only:
        weather_results = run_all_weather(df, template)

        # Git commit weather results
        os.system(
            f'cd {ROOT} && git add calibration/simulation_weather_cities.sqlite && '
            f'git commit -m "Weather city sim complete — single pass all markets"'
        )

    # ── TSA Groups ────────────────────────────────────────────
    if not args.weather_only:
        if not check_ollama():
            print(f"\nOllama not responding. Waiting 60s...")
            time.sleep(60)

        tsa_result = run_tsa_simulation(df, template)

        os.system(
            f'cd {ROOT} && git add calibration/simulation_tsa_groups.sqlite && '
            f'git commit -m "TSA group sim complete — '
            f'{tsa_result["wr"]:.1f}% WR on {tsa_result["total"]} trades"'
        )

    # ── Master Summary ────────────────────────────────────────
    print("\n\n" + "=" * 70)
    print("MASTER CITY COMPARISON TABLE")
    print("=" * 70)

    if weather_results:
        print(f"\nWEATHER BY CITY")
        print(f"{'City':<15} {'Markets':>8} {'Executed':>10} {'Win Rate':>10} {'Rec Gap':>10} {'Deploy?':>10}")
        print("-" * 70)

        for r in weather_results:
            deploy = "YES" if r["wr"] >= 85 and r["executed"] >= 20 else (
                "INSUFF" if r["executed"] < 20 else "NO"
            )
            gap = CITY_MIN_NWS_GAP.get(r["city_key"], CITY_MIN_NWS_GAP.get("default", 3))
            print(f"{r['city']:<15} {r['markets']:>8} {r['executed']:>10} "
                  f"{r['wr']:>9.1f}% {gap:>9}F {'  ' + deploy:>10}")

    if tsa_result:
        print(f"\nTSA OVERALL: {tsa_result['total']} trades, "
              f"{tsa_result['wr']:.1f}% WR, ${tsa_result['pnl']:.2f} PnL")

    # Save master summary
    summary_path = os.path.join(ROOT, "calibration", "city_simulation_summary.txt")
    with open(summary_path, "w") as f:
        f.write(f"CITY-LEVEL SIMULATION SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Model: {MODEL}\n")
        f.write(f"City gap config: {CITY_MIN_NWS_GAP}\n")
        f.write("=" * 70 + "\n\n")
        if weather_results:
            f.write("WEATHER BY CITY\n")
            for r in weather_results:
                f.write(f"  {r['city']}: {r['executed']} trades, {r['wr']:.1f}% WR, ${r['pnl']:.2f} PnL\n")
        if tsa_result:
            f.write(f"\nTSA: {tsa_result['total']} trades, {tsa_result['wr']:.1f}% WR, ${tsa_result['pnl']:.2f} PnL\n")

    print(f"\nSummary saved to: {summary_path}")

    # Final commit
    os.system(
        f'cd {ROOT} && git add calibration/ && '
        f'git commit -m "City-level simulations complete — weather and TSA by city/group"'
    )

    print("\nAll city-level simulations complete.")


if __name__ == "__main__":
    main()
