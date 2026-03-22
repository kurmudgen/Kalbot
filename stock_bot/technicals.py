"""
Technical analysis ensemble from ai-hedge-fund repo (49K stars).
Five strategies with calibrated weights, combined into a single signal.

Weights:
- Trend Following (0.25): EMA crossovers + ADX
- Mean Reversion (0.20): Z-score + Bollinger + RSI
- Momentum (0.25): Multi-timeframe momentum + volume
- Volatility (0.15): Vol regime detection
- Statistical Arbitrage (0.15): Hurst exponent + skew/kurtosis
"""

import numpy as np


def compute_ema(prices: list[float], period: int) -> float:
    """Exponential moving average."""
    if len(prices) < period:
        return prices[-1] if prices else 0
    multiplier = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = (p - ema) * multiplier + ema
    return ema


def compute_rsi(prices: list[float], period: int = 14) -> float:
    """Relative Strength Index."""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    recent = deltas[-period:]
    gains = [d for d in recent if d > 0]
    losses = [-d for d in recent if d < 0]
    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.001
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_bollinger(prices: list[float], period: int = 20) -> tuple[float, float, float]:
    """Bollinger Bands (middle, upper, lower)."""
    if len(prices) < period:
        return prices[-1], prices[-1], prices[-1]
    window = prices[-period:]
    mid = np.mean(window)
    std = np.std(window)
    return mid, mid + 2 * std, mid - 2 * std


def compute_hurst(prices: list[float]) -> float:
    """Hurst exponent — <0.5 = mean reverting, >0.5 = trending."""
    if len(prices) < 20:
        return 0.5
    try:
        ts = np.array(prices[-100:]) if len(prices) > 100 else np.array(prices)
        lags = range(2, min(20, len(ts) // 2))
        tau = [np.std(np.subtract(ts[lag:], ts[:-lag])) for lag in lags]
        tau = [t for t in tau if t > 0]
        if len(tau) < 2:
            return 0.5
        log_lags = np.log(list(lags)[:len(tau)])
        log_tau = np.log(tau)
        hurst = np.polyfit(log_lags, log_tau, 1)[0]
        return max(0.0, min(1.0, hurst))
    except Exception:
        return 0.5


def trend_following(prices: list[float], volumes: list[float]) -> float:
    """EMA crossover + ADX trend strength. Returns -1 to +1."""
    if len(prices) < 55:
        return 0.0

    ema8 = compute_ema(prices, 8)
    ema21 = compute_ema(prices, 21)
    ema55 = compute_ema(prices, 55)
    current = prices[-1]

    signal = 0.0
    if ema8 > ema21 > ema55:
        signal = 0.8  # Strong uptrend
    elif ema8 > ema21:
        signal = 0.4  # Moderate uptrend
    elif ema8 < ema21 < ema55:
        signal = -0.8  # Strong downtrend
    elif ema8 < ema21:
        signal = -0.4

    # Volume confirmation
    if len(volumes) >= 20:
        avg_vol = np.mean(volumes[-20:])
        recent_vol = np.mean(volumes[-5:])
        if recent_vol > avg_vol * 1.5:
            signal *= 1.2  # Volume confirms

    return max(-1, min(1, signal))


def mean_reversion(prices: list[float]) -> float:
    """Z-score + Bollinger + RSI. Returns -1 to +1."""
    if len(prices) < 50:
        return 0.0

    # Z-score
    window = prices[-50:]
    z = (prices[-1] - np.mean(window)) / max(np.std(window), 0.001)

    # Bollinger position
    mid, upper, lower = compute_bollinger(prices)
    bb_range = upper - lower if upper > lower else 0.001
    bb_pos = (prices[-1] - lower) / bb_range  # 0 = at lower band, 1 = at upper

    # RSI
    rsi = compute_rsi(prices)

    signal = 0.0
    if z < -2 and rsi < 30 and bb_pos < 0.1:
        signal = 0.8  # Oversold
    elif z < -1 and rsi < 40:
        signal = 0.4
    elif z > 2 and rsi > 70 and bb_pos > 0.9:
        signal = -0.8  # Overbought
    elif z > 1 and rsi > 60:
        signal = -0.4

    return max(-1, min(1, signal))


def momentum(prices: list[float], volumes: list[float]) -> float:
    """Multi-timeframe momentum. Returns -1 to +1."""
    if len(prices) < 130:
        return 0.0

    # 1-month, 3-month, 6-month momentum
    mom_1m = (prices[-1] / prices[-21] - 1) if prices[-21] > 0 else 0
    mom_3m = (prices[-1] / prices[-63] - 1) if prices[-63] > 0 else 0
    mom_6m = (prices[-1] / prices[-126] - 1) if prices[-126] > 0 else 0

    # Weighted combination (40/30/30 from ai-hedge-fund)
    raw = mom_1m * 0.4 + mom_3m * 0.3 + mom_6m * 0.3

    # Volume confirmation
    if len(volumes) >= 20:
        vol_ratio = np.mean(volumes[-5:]) / max(np.mean(volumes[-20:]), 1)
        if vol_ratio > 1.5:
            raw *= 1.2

    # Normalize to -1..1
    signal = np.tanh(raw * 5)
    return float(signal)


def volatility_regime(prices: list[float]) -> float:
    """Volatility regime detection. Returns -1 to +1."""
    if len(prices) < 60:
        return 0.0

    returns = np.diff(np.log(np.array(prices[-60:])))
    current_vol = np.std(returns[-10:]) * np.sqrt(252)
    hist_vol = np.std(returns) * np.sqrt(252)

    vol_z = (current_vol - hist_vol) / max(hist_vol, 0.01)

    # High vol = cautious (negative), low vol = favorable (positive)
    return float(-np.tanh(vol_z))


def stat_arb(prices: list[float]) -> float:
    """Hurst exponent + skewness. Returns -1 to +1."""
    if len(prices) < 30:
        return 0.0

    hurst = compute_hurst(prices)
    returns = np.diff(np.log(np.array(prices[-60:]))) if len(prices) >= 60 else np.diff(np.log(np.array(prices)))

    skew = float(np.mean(returns ** 3) / max(np.std(returns) ** 3, 0.0001)) if len(returns) > 5 else 0

    signal = 0.0
    if hurst < 0.4:
        # Mean reverting — trade reversals
        signal = -np.sign(prices[-1] - np.mean(prices[-20:])) * 0.5
    elif hurst > 0.6:
        # Trending — follow the trend
        signal = np.sign(prices[-1] - np.mean(prices[-20:])) * 0.5

    return float(max(-1, min(1, signal)))


def analyze_technicals(prices: list[float], volumes: list[float]) -> dict:
    """Run all 5 strategies and return weighted ensemble signal.

    Returns:
        signal: -1 (strong sell) to +1 (strong buy)
        confidence: 0-1
        breakdown: individual strategy signals
    """
    strategies = {
        "trend": (trend_following(prices, volumes), 0.25),
        "mean_reversion": (mean_reversion(prices), 0.20),
        "momentum": (momentum(prices, volumes), 0.25),
        "volatility": (volatility_regime(prices), 0.15),
        "stat_arb": (stat_arb(prices), 0.15),
    }

    weighted_sum = sum(signal * weight for signal, weight in strategies.values())
    total_weight = sum(w for _, w in strategies.values())
    ensemble_signal = weighted_sum / total_weight

    # Confidence: higher when strategies agree
    signals = [s for s, _ in strategies.values()]
    agreement = 1 - np.std(signals) * 2  # Low std = high agreement
    confidence = max(0, min(1, abs(ensemble_signal) * agreement))

    return {
        "signal": float(ensemble_signal),
        "direction": "buy" if ensemble_signal > 0.2 else "sell" if ensemble_signal < -0.2 else "hold",
        "confidence": float(confidence),
        "breakdown": {name: float(sig) for name, (sig, _) in strategies.items()},
        "hurst": float(compute_hurst(prices)),
        "rsi": float(compute_rsi(prices)),
    }


if __name__ == "__main__":
    # Test with random data
    import random
    prices = [100 + random.gauss(0, 2) for _ in range(200)]
    volumes = [1000000 + random.randint(-500000, 500000) for _ in range(200)]
    result = analyze_technicals(prices, volumes)
    print(f"Signal: {result['signal']:.3f} ({result['direction']})")
    print(f"Confidence: {result['confidence']:.3f}")
    for name, val in result["breakdown"].items():
        print(f"  {name}: {val:+.3f}")
