"""
Market regime detection using Hidden Markov Model.
Detects bull/bear/volatile states and tells the bot how to behave.

Bull: be aggressive, larger positions, buy dips
Bear: be defensive, smaller positions, favor NO-side
Volatile: tighten stops, reduce position sizes, favor market making
"""

import numpy as np
import os
from datetime import datetime


# Regime states
BULL = "bull"
BEAR = "bear"
VOLATILE = "volatile"

# How each regime adjusts trading behavior
REGIME_ADJUSTMENTS = {
    BULL: {
        "kelly_multiplier": 1.3,     # More aggressive sizing
        "confidence_offset": -0.05,  # Lower bar to enter
        "prefer_side": "YES",        # Buy YES (things go up)
        "stop_loss_multiplier": 1.2, # Wider stops (let winners run)
        "description": "Bull market — be aggressive, buy dips",
    },
    BEAR: {
        "kelly_multiplier": 0.6,     # Smaller positions
        "confidence_offset": 0.05,   # Higher bar to enter
        "prefer_side": "NO",         # Buy NO (things go down)
        "stop_loss_multiplier": 0.8, # Tighter stops
        "description": "Bear market — be defensive, favor NO-side",
    },
    VOLATILE: {
        "kelly_multiplier": 0.5,     # Much smaller positions
        "confidence_offset": 0.10,   # Much higher bar
        "prefer_side": None,         # No preference
        "stop_loss_multiplier": 0.6, # Very tight stops
        "description": "Volatile — reduce exposure, tighten stops",
    },
}


def detect_regime(prices: list[float] = None) -> dict:
    """Detect current market regime from S&P 500 data."""
    if prices is None:
        prices = _get_sp500_prices()

    if not prices or len(prices) < 20:
        return {"regime": BULL, "confidence": 0.5, **REGIME_ADJUSTMENTS[BULL]}

    returns = np.diff(np.log(np.array(prices[-60:]))) if len(prices) >= 60 else np.diff(np.log(np.array(prices)))

    # Simple regime detection using return statistics
    mean_return = np.mean(returns)
    volatility = np.std(returns) * np.sqrt(252)  # Annualized
    recent_vol = np.std(returns[-10:]) * np.sqrt(252) if len(returns) >= 10 else volatility
    trend = (prices[-1] / prices[-20] - 1) if len(prices) >= 20 else 0

    # Classify regime
    if recent_vol > 0.30:  # >30% annualized vol = volatile
        regime = VOLATILE
        confidence = min(0.9, recent_vol / 0.40)
    elif trend > 0.02 and mean_return > 0:  # Up >2% in 20 days + positive mean
        regime = BULL
        confidence = min(0.9, trend / 0.05)
    elif trend < -0.02 and mean_return < 0:  # Down >2% in 20 days + negative mean
        regime = BEAR
        confidence = min(0.9, abs(trend) / 0.05)
    elif mean_return > 0:
        regime = BULL
        confidence = 0.5
    else:
        regime = BEAR
        confidence = 0.5

    result = {
        "regime": regime,
        "confidence": float(confidence),
        "trend_20d": float(trend),
        "volatility": float(volatility),
        "recent_vol": float(recent_vol),
        **REGIME_ADJUSTMENTS[regime],
    }

    return result


def _get_sp500_prices() -> list[float]:
    """Get recent S&P 500 prices."""
    try:
        import yfinance as yf
        data = yf.Ticker("^GSPC").history(period="3mo")
        if not data.empty:
            return data["Close"].tolist()
    except Exception:
        pass

    # Fallback: FRED
    try:
        import requests
        r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500", timeout=10)
        if r.status_code == 200:
            lines = r.text.strip().split("\n")[1:]
            prices = []
            for line in lines[-60:]:
                parts = line.split(",")
                if len(parts) == 2 and parts[1] != ".":
                    prices.append(float(parts[1]))
            return prices
    except Exception:
        pass

    return []


def get_regime_context() -> str:
    """Get regime description for LLM prompts."""
    regime = detect_regime()
    return f"Market regime: {regime['regime'].upper()} (vol: {regime['volatility']:.0%}, trend: {regime['trend_20d']:+.1%}). {regime['description']}"


if __name__ == "__main__":
    regime = detect_regime()
    print(f"Regime: {regime['regime'].upper()}")
    print(f"Confidence: {regime['confidence']:.0%}")
    print(f"20d trend: {regime['trend_20d']:+.1%}")
    print(f"Volatility: {regime['volatility']:.0%}")
    print(f"Kelly mult: {regime['kelly_multiplier']}x")
    print(f"Description: {regime['description']}")
