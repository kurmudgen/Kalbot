"""
Crypto liquidation cascade detector.
When funding rates are extreme → leverage is high → crash incoming.
When large liquidations happen → cascade selling follows.

Free data from CoinGlass and Binance.
"""

import os
import requests
from datetime import datetime


# Extreme funding rate thresholds
FUNDING_RATE_EXTREME_LONG = 0.05   # >0.05% per 8hr = overleveraged long
FUNDING_RATE_EXTREME_SHORT = -0.03  # <-0.03% per 8hr = overleveraged short


def get_funding_rates() -> dict:
    """Get current funding rates for major crypto."""
    rates = {}
    try:
        # Binance funding rates (free, no key)
        r = requests.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=10)
        if r.status_code == 200:
            for item in r.json():
                symbol = item.get("symbol", "")
                rate = float(item.get("lastFundingRate", 0))
                if symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT"]:
                    rates[symbol] = {
                        "rate": rate,
                        "rate_pct": rate * 100,
                        "extreme": rate > FUNDING_RATE_EXTREME_LONG or rate < FUNDING_RATE_EXTREME_SHORT,
                        "direction": "long" if rate > 0 else "short",
                    }
    except Exception as e:
        print(f"  Funding rate error: {e}")

    return rates


def get_liquidation_data() -> dict:
    """Get recent liquidation data."""
    try:
        # CoinGlass liquidation data (may need API key for full access)
        r = requests.get(
            "https://open-api.coinglass.com/public/v2/liquidation_history?time_type=all&symbol=BTC",
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return {}


def detect_cascade_risk() -> dict:
    """Detect if a liquidation cascade is likely."""
    rates = get_funding_rates()

    if not rates:
        return {"risk": "unknown", "signal": "hold", "details": "No funding data",
                "extreme_pairs": 0, "avg_funding_rate": 0, "rates": {}}

    # Check for extreme funding across multiple pairs
    extreme_count = sum(1 for r in rates.values() if r["extreme"])
    avg_rate = sum(r["rate"] for r in rates.values()) / len(rates) if rates else 0

    if extreme_count >= 3:
        direction = "long" if avg_rate > 0 else "short"
        risk = "high"
        signal = "short" if direction == "long" else "long"
        details = f"{extreme_count} pairs with extreme funding ({direction}-heavy). Cascade {direction}-liquidation likely."
    elif extreme_count >= 1:
        risk = "elevated"
        signal = "cautious"
        details = f"{extreme_count} pair(s) with extreme funding. Elevated risk."
    else:
        risk = "low"
        signal = "normal"
        details = "Funding rates normal. No cascade risk detected."

    return {
        "risk": risk,
        "signal": signal,
        "details": details,
        "extreme_pairs": extreme_count,
        "avg_funding_rate": avg_rate * 100,
        "rates": {k: v["rate_pct"] for k, v in rates.items()},
    }


def get_cascade_context() -> str:
    """Get cascade risk as context for LLM prompts."""
    result = detect_cascade_risk()
    if result["risk"] == "high":
        return f"WARNING: Liquidation cascade risk HIGH. {result['details']}"
    elif result["risk"] == "elevated":
        return f"CAUTION: {result['details']}"
    return ""


if __name__ == "__main__":
    print("=== Liquidation Cascade Monitor ===")
    risk = detect_cascade_risk()
    print(f"Risk: {risk['risk'].upper()}")
    print(f"Signal: {risk['signal']}")
    print(f"Details: {risk['details']}")
    print(f"Avg funding: {risk['avg_funding_rate']:.4f}%")
    if risk.get("rates"):
        print("Funding rates:")
        for pair, rate in risk["rates"].items():
            print(f"  {pair}: {rate:.4f}%")
