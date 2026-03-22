"""
Forex bracket strategy (EUR/USD, USD/JPY).
Uses options-implied volatility to price Kalshi forex brackets.
Same math as S&P strategy but for currency pairs.
"""

import math
import os
import re
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
FOREX_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "forex_strategy.sqlite")

# Historical daily vol estimates (annualized → daily)
# EUR/USD annualized vol ~8%, daily ~0.50%
# USD/JPY annualized vol ~10%, daily ~0.63%
FOREX_DAILY_VOL = {
    "eurusd": 0.50,
    "usdjpy": 0.63,
    "gbpusd": 0.55,
}


def init_forex_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(FOREX_DB), exist_ok=True)
    conn = sqlite3.connect(FOREX_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forex_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, title TEXT, pair TEXT,
            current_rate REAL, bracket_low REAL, bracket_high REAL,
            market_price REAL, model_prob REAL, edge REAL, created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_forex_rates() -> dict:
    """Get current forex rates."""
    rates = {}
    try:
        import yfinance as yf
        for pair, ticker in [("eurusd", "EURUSD=X"), ("usdjpy", "JPY=X"), ("gbpusd", "GBPUSD=X")]:
            try:
                data = yf.Ticker(ticker)
                hist = data.history(period="1d")
                if not hist.empty:
                    rates[pair] = float(hist["Close"].iloc[-1])
            except Exception:
                continue
    except Exception:
        pass

    # Fallback: try FRED
    fred_map = {"eurusd": "DEXUSEU", "usdjpy": "DEXJPUS"}
    for pair, series in fred_map.items():
        if pair not in rates:
            try:
                r = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}", timeout=10)
                if r.status_code == 200:
                    lines = r.text.strip().split("\n")
                    for line in reversed(lines):
                        parts = line.split(",")
                        if len(parts) == 2 and parts[1] != ".":
                            rates[pair] = float(parts[1])
                            break
            except Exception:
                pass

    return rates


def find_forex_markets() -> list[dict]:
    if not os.path.exists(MARKETS_DB):
        return []
    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM markets WHERE status IN ('open', 'active')
        AND (title LIKE '%EUR/USD%' OR title LIKE '%USD/JPY%'
             OR title LIKE '%GBP/USD%' OR title LIKE '%euro%dollar%'
             OR event_ticker LIKE '%EURUSD%' OR event_ticker LIKE '%USDJPY%'
             OR event_ticker LIKE '%FX%')
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def detect_pair(title: str) -> str | None:
    t = title.upper()
    if "EUR" in t and "USD" in t:
        return "eurusd"
    if "JPY" in t:
        return "usdjpy"
    if "GBP" in t:
        return "gbpusd"
    return None


def analyze_forex_markets() -> list[dict]:
    rates = get_forex_rates()
    if not rates:
        print("  Forex strategy: cannot get rates")
        return []

    for pair, rate in rates.items():
        print(f"  Forex: {pair.upper()}={rate:.4f}")

    markets = find_forex_markets()
    if not markets:
        print("  No forex bracket markets found")
        return []

    conn = init_forex_db()
    signals = []

    for m in markets:
        pair = detect_pair(m["title"])
        if pair is None or pair not in rates:
            continue

        current = rates[pair]
        daily_vol = FOREX_DAILY_VOL.get(pair, 0.50)
        market_price = (m.get("last_price") or 50) / 100.0

        # Parse bracket
        numbers = re.findall(r'(\d+\.?\d+)', m["title"])
        fx_numbers = [float(n) for n in numbers if 0.5 < float(n) < 200]

        if len(fx_numbers) < 1:
            continue

        from scipy.stats import norm
        vol_points = current * daily_vol / 100.0

        if len(fx_numbers) >= 2:
            low, high = min(fx_numbers), max(fx_numbers)
            z_low = (low - current) / vol_points
            z_high = (high - current) / vol_points
            model_prob = norm.cdf(z_high) - norm.cdf(z_low)
        else:
            threshold = fx_numbers[0]
            z = (threshold - current) / vol_points
            if "above" in m["title"].lower():
                model_prob = 1 - norm.cdf(z)
            else:
                model_prob = norm.cdf(z)

        model_prob = max(0.01, min(0.99, model_prob))
        edge = model_prob - market_price

        conn.execute(
            "INSERT INTO forex_signals VALUES (NULL,?,?,?,?,?,?,?,?,?,?)",
            (m["ticker"], m["title"], pair, current, 0, 0,
             market_price, model_prob, edge, datetime.now(timezone.utc).isoformat()),
        )

        if abs(edge) > 0.08:
            side = "YES" if edge > 0 else "NO"
            print(f"  SIGNAL: {side} edge={edge:+.2f} on {m['title'][:60]}...")
            signals.append({
                "ticker": m["ticker"], "title": m["title"], "category": "forex",
                "model_probability": model_prob, "confidence": min(0.80, 0.55 + abs(edge)),
                "market_price": market_price, "price_gap": abs(edge),
                "reasoning": f"{pair}={current:.4f}, prob={model_prob:.2f}",
            })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    analyze_forex_markets()
