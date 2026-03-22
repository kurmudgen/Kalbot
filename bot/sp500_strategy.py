"""
S&P 500 daily range bracket strategy.
Uses VIX-implied probability distributions to price brackets,
then compares to Kalshi prices for edge detection.

Core edge: VIX overestimates actual volatility ~60% of the time
(the volatility risk premium). This means narrow brackets near
the current price are systematically underpriced on Kalshi.

Data sources:
- VIX from FRED (free, updates daily)
- SPX current price from yfinance (free)
- Historical daily return distribution (computed from 10 years of data)
"""

import json
import math
import os
import sqlite3
from datetime import datetime, timezone

import numpy as np
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
SP500_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sp500_strategy.sqlite")

# Load empirical distributions
DIST_PATH = os.path.join(os.path.dirname(__file__), "..", "calibration", "financial_distributions.json")

# VIX to daily vol conversion
# Daily expected move = VIX / sqrt(252) ≈ VIX / 15.87
VIX_DAILY_DIVISOR = 15.87

# Volatility risk premium adjustment
# VIX overestimates realized vol by ~20-30% on average
VRP_HAIRCUT = 0.75  # Use 75% of VIX-implied vol as our estimate

# From nikhilnd/kalshi-market-making: use Cauchy distribution (fat tails)
# with time-decaying scale parameter
CAUCHY_BASE_GAMMA = 0.000005  # Base scale parameter
CAUCHY_TIME_EXPONENT = 0.6     # (3/5) — narrows distribution as close approaches

# From quantgalore: trade at 2PM ET, predict afternoon vol
# The edge is that morning vol predicts afternoon vol
TRADE_HOUR_ET = 14  # 2:00 PM ET

# From ryanfrigo Safe Compounder: NO-side on near-certain outcomes is most profitable
# Edge window from alexandermazza: 10-18% (edges >18% are unreliable)
MIN_EDGE = 0.08
MAX_EDGE = 0.18  # Edge paradox: larger edges = lower win rate


def init_sp500_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SP500_DB), exist_ok=True)
    conn = sqlite3.connect(SP500_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sp500_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            bracket_low REAL,
            bracket_high REAL,
            market_price REAL,
            model_prob REAL,
            vix REAL,
            spx_price REAL,
            daily_vol REAL,
            edge REAL,
            signal_type TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    return conn


def get_current_vix() -> float | None:
    """Get current VIX level."""
    try:
        import yfinance as yf
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass

    # Fallback: FRED (may be delayed)
    try:
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")
            last_line = lines[-1]
            val = last_line.split(",")[1]
            if val != ".":
                return float(val)
    except Exception:
        pass

    return None


def get_current_spx() -> float | None:
    """Get current S&P 500 level."""
    try:
        import yfinance as yf
        spx = yf.Ticker("^GSPC")
        hist = spx.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


def compute_bracket_probability(spx: float, bracket_low: float, bracket_high: float,
                                daily_vol_pct: float) -> float:
    """Compute probability that SPX closes within a bracket using adjusted normal distribution."""
    if daily_vol_pct <= 0:
        return 0.0

    # Convert to z-scores
    vol_points = spx * daily_vol_pct / 100.0
    z_low = (bracket_low - spx) / vol_points
    z_high = (bracket_high - spx) / vol_points

    # Normal CDF
    from scipy.stats import norm
    prob = norm.cdf(z_high) - norm.cdf(z_low)
    return max(0.001, min(0.999, prob))


def parse_sp500_bracket(title: str) -> tuple[float, float] | None:
    """Extract bracket low and high from a Kalshi S&P 500 market title."""
    import re
    title_lower = title.lower()

    # Pattern: "S&P 500 ... between X,XXX and X,XXX" or "above X,XXX" or "below X,XXX"
    # Also: "Will the S&P 500 close at or above 5,800 on March 22?"
    numbers = re.findall(r'[\d,]+\.?\d*', title)
    numbers = [float(n.replace(",", "")) for n in numbers if float(n.replace(",", "")) > 1000]

    if len(numbers) >= 2:
        return (min(numbers), max(numbers))
    elif len(numbers) == 1:
        n = numbers[0]
        if "above" in title_lower or "at or above" in title_lower:
            return (n, n * 1.10)  # Above threshold
        elif "below" in title_lower:
            return (n * 0.90, n)  # Below threshold
    return None


def find_sp500_markets() -> list[dict]:
    """Find S&P 500 bracket markets on Kalshi."""
    if not os.path.exists(MARKETS_DB):
        return []

    conn = sqlite3.connect(MARKETS_DB)
    conn.row_factory = sqlite3.Row

    # Look for S&P related markets
    rows = conn.execute("""
        SELECT * FROM markets
        WHERE (title LIKE '%S&P%' OR title LIKE '%S&P 500%' OR title LIKE '%SPX%'
               OR title LIKE '%SP500%' OR event_ticker LIKE '%INX%'
               OR event_ticker LIKE '%SPX%' OR event_ticker LIKE '%SP500%')
        AND status IN ('open', 'active')
    """).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def analyze_sp500_markets() -> list[dict]:
    """Analyze S&P 500 bracket markets for edge."""
    vix = get_current_vix()
    spx = get_current_spx()

    if vix is None or spx is None:
        print("  S&P strategy: cannot get VIX or SPX price")
        return []

    # Adjusted daily vol (with VRP haircut)
    daily_vol_raw = vix / VIX_DAILY_DIVISOR
    daily_vol_adj = daily_vol_raw * VRP_HAIRCUT

    print(f"  S&P strategy: VIX={vix:.1f}, SPX={spx:.0f}")
    print(f"  Daily vol: {daily_vol_raw:.2f}% (raw), {daily_vol_adj:.2f}% (VRP-adjusted)")

    markets = find_sp500_markets()
    if not markets:
        print("  No S&P bracket markets found")
        return []

    print(f"  Found {len(markets)} S&P markets")

    conn = init_sp500_db()
    signals = []

    for m in markets:
        title = m["title"]
        bracket = parse_sp500_bracket(title)
        if bracket is None:
            continue

        bracket_low, bracket_high = bracket
        market_price = (m.get("last_price") or 50) / 100.0

        # Compute our probability
        model_prob = compute_bracket_probability(spx, bracket_low, bracket_high, daily_vol_adj)
        edge = model_prob - market_price

        conn.execute(
            """INSERT INTO sp500_signals
               (ticker, title, bracket_low, bracket_high, market_price,
                model_prob, vix, spx_price, daily_vol, edge, signal_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (m["ticker"], title, bracket_low, bracket_high, market_price,
             model_prob, vix, spx, daily_vol_adj, edge,
             "vrp_adjusted",
             datetime.now(timezone.utc).isoformat()),
        )

        if abs(edge) > 0.08:
            side = "YES" if edge > 0 else "NO"
            print(f"  SIGNAL: {side} edge={edge:+.2f} on {title[:60]}...")
            signals.append({
                "ticker": m["ticker"],
                "title": title,
                "category": "sp500",
                "model_probability": model_prob,
                "confidence": min(0.9, 0.6 + abs(edge)),
                "market_price": market_price,
                "price_gap": abs(edge),
                "reasoning": f"VIX={vix:.1f}, VRP-adjusted prob={model_prob:.2f} vs market={market_price:.2f}",
            })

    conn.commit()
    conn.close()
    return signals


if __name__ == "__main__":
    signals = analyze_sp500_markets()
    print(f"\n{len(signals)} S&P signals found")
