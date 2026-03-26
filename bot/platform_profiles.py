"""
Platform-specific calibration profiles.

Each platform has fundamentally different market mechanics and needs
its own confidence floors, position sizing, signal requirements,
and ensemble prompts.

Kalshi: binary outcomes, NWS/FRED settlement sources, information asymmetry edge
Alpaca Stocks: continuous price, momentum + volume, sector regime
Alpaca Crypto: 24/7, BTC correlation dominates, funding rate signals
Coinbase Momentum: micro-cap risk, rug pull detection, skeptical by default
"""

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "prompts")


def _load_prompt(filename: str) -> str:
    """Load a prompt template from prompts/ directory."""
    path = os.path.join(PROMPTS_DIR, filename)
    if os.path.exists(path):
        with open(path) as f:
            return f.read()
    return ""


@dataclass
class PlatformProfile:
    """Base profile — all platforms inherit from this."""
    name: str = "base"
    confidence_floor: float = 0.75
    min_price_gap: float = 0.08
    kelly_multiplier: float = 0.25
    max_position_size: float = 25.0
    hold_time_hours: int = 24
    required_signals: int = 2       # out of total models
    total_models: int = 3
    prompt_file: str = ""
    gates: list = field(default_factory=list)

    def get_prompt(self) -> str:
        if self.prompt_file:
            return _load_prompt(self.prompt_file)
        return ""

    def check_gates(self, context: dict) -> tuple[bool, str]:
        """Run platform-specific gates. Returns (allowed, reason)."""
        for gate_fn in self.gates:
            try:
                allowed, reason = gate_fn(context)
                if not allowed:
                    return False, reason
            except Exception:
                pass
        return True, ""


# ── GATE FUNCTIONS ────────────────────────────────────────────

def _gate_market_hours(ctx: dict) -> tuple[bool, str]:
    """Block outside US market hours."""
    from datetime import datetime
    now = datetime.now()
    if now.weekday() >= 5:
        return False, "market closed (weekend)"
    # Rough ET check (adjust for timezone)
    hour_et = now.hour + 3  # Rough PT->ET
    if hour_et < 10 or hour_et >= 16:
        return False, "market closed (outside 10am-4pm ET)"
    return True, ""


def _gate_btc_regime(ctx: dict) -> tuple[bool, str]:
    """Block all crypto if BTC down >3% in last 4 hours."""
    try:
        import requests
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "bitcoin", "vs_currencies": "usd",
                    "include_24hr_change": "true"},
            timeout=10,
        )
        if r.status_code == 200:
            change = r.json().get("bitcoin", {}).get("usd_24h_change", 0)
            if change and change < -3:
                return False, f"BTC regime gate: down {change:.1f}% in 24hr"
    except Exception:
        pass
    return True, ""


def _gate_min_adv(ctx: dict) -> tuple[bool, str]:
    """Require minimum average daily volume for stocks."""
    volume = ctx.get("volume", 0)
    if volume < 10_000_000:
        return False, f"ADV ${volume:,.0f} below $10M minimum"
    return True, ""


def _gate_nws_data(ctx: dict) -> tuple[bool, str]:
    """Require NWS data for weather markets."""
    category = ctx.get("category", "")
    if category != "weather":
        return True, ""
    nws = ctx.get("nws_forecast")
    if not nws:
        return False, "no NWS forecast data available"
    return True, ""


def _gate_fred_data(ctx: dict) -> tuple[bool, str]:
    """Require FRED data for economic markets."""
    category = ctx.get("category", "")
    if category not in ("economics", "inflation"):
        return True, ""
    fred = ctx.get("fred_data")
    if not fred:
        # Don't hard-block — FRED may be temporarily unavailable
        pass
    return True, ""


def _gate_coinbase_verified(ctx: dict) -> tuple[bool, str]:
    """Strict verification for Coinbase momentum trades."""
    market_cap = ctx.get("market_cap", 0)
    if market_cap < 50_000_000:
        return False, f"market cap ${market_cap:,.0f} below $50M"

    price = ctx.get("price", 0)
    if price < 0.05:
        return False, f"price ${price:.4f} below $0.05 minimum"

    listing_verified = ctx.get("listing_age_verified", False)
    if not listing_verified:
        return False, "listing age not verified from API"

    volume = ctx.get("volume", 0)
    avg_volume = ctx.get("avg_volume_7d", 0)
    if avg_volume > 0 and volume < avg_volume * 3:
        return False, f"volume spike {volume/avg_volume:.1f}x below 3x threshold"

    return True, ""


# ── PROFILE DEFINITIONS ──────────────────────────────────────

class KalshiProfile(PlatformProfile):
    def __init__(self):
        super().__init__(
            name="kalshi",
            confidence_floor=float(os.getenv("KALSHI_CONFIDENCE", "0.75")),
            min_price_gap=float(os.getenv("KALSHI_PRICE_GAP", "0.08")),
            kelly_multiplier=0.25,  # Adjusted by regime detector
            max_position_size=float(os.getenv("MAX_TRADE_SIZE", "25")),
            hold_time_hours=48,
            required_signals=2,
            total_models=3,
            prompt_file="kalshi_binary.txt",
            gates=[_gate_nws_data, _gate_fred_data],
        )


class AlpacaStocksProfile(PlatformProfile):
    def __init__(self):
        super().__init__(
            name="alpaca_stocks",
            confidence_floor=float(os.getenv("STOCKS_CONFIDENCE", "0.80")),
            min_price_gap=0.0,  # Not applicable — use momentum score instead
            kelly_multiplier=0.25,  # Fixed regardless of regime
            max_position_size=float(os.getenv("STOCK_MAX_POSITION", "10")) * 50,  # $500
            hold_time_hours=24,
            required_signals=3,  # Unanimous
            total_models=3,
            prompt_file="alpaca_stocks_momentum.txt",
            gates=[_gate_market_hours, _gate_min_adv],
        )
        self.min_momentum_score = 0.65


class AlpacaCryptoProfile(PlatformProfile):
    def __init__(self):
        super().__init__(
            name="alpaca_crypto",
            confidence_floor=float(os.getenv("CRYPTO_CONFIDENCE", "0.75")),
            min_price_gap=0.05,
            kelly_multiplier=0.15,
            max_position_size=200.0,
            hold_time_hours=8,
            required_signals=2,
            total_models=3,
            prompt_file="alpaca_crypto_momentum.txt",
            gates=[_gate_btc_regime],
        )
        self.min_market_cap = 100_000_000  # $100M for Alpaca crypto


class CoinbaseMomentumProfile(PlatformProfile):
    def __init__(self):
        super().__init__(
            name="coinbase_momentum",
            confidence_floor=float(os.getenv("MOMENTUM_CONFIDENCE", "0.82")),
            min_price_gap=0.0,
            kelly_multiplier=0.10,
            max_position_size=25.0,
            hold_time_hours=2,
            required_signals=3,  # 3 of 4 signal layers
            total_models=4,
            prompt_file="coinbase_momentum_skeptical.txt",
            gates=[_gate_btc_regime, _gate_coinbase_verified],
        )
        self.min_market_cap = 50_000_000
        self.min_listing_days = 180
        self.min_volume_spike = 3.0  # 3x 7-day average
        self.min_price = 0.05


# ── PROFILE REGISTRY ─────────────────────────────────────────

PROFILES = {
    "kalshi": KalshiProfile,
    "alpaca_stocks": AlpacaStocksProfile,
    "alpaca_crypto": AlpacaCryptoProfile,
    "coinbase_momentum": CoinbaseMomentumProfile,
}


def get_profile(platform: str) -> PlatformProfile:
    """Get the calibration profile for a platform."""
    cls = PROFILES.get(platform, PlatformProfile)
    return cls()


if __name__ == "__main__":
    for name, cls in PROFILES.items():
        p = cls()
        print(f"\n=== {p.name.upper()} ===")
        print(f"  Confidence floor: {p.confidence_floor}")
        print(f"  Kelly multiplier: {p.kelly_multiplier}")
        print(f"  Max position: ${p.max_position_size}")
        print(f"  Hold time: {p.hold_time_hours}hr")
        print(f"  Required signals: {p.required_signals}/{p.total_models}")
        print(f"  Gates: {len(p.gates)}")
        prompt = p.get_prompt()
        print(f"  Prompt loaded: {'yes' if prompt else 'no'} ({p.prompt_file})")
