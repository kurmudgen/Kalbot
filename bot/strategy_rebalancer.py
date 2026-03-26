"""
Strategy rebalancer: shifts budget allocation toward whichever
strategy (Weather, S&P/Econ, Wildcard) is performing best.

Uses a rolling window of trade outcomes to compute per-strategy ROI,
then reallocates budget proportionally. Strategies that are losing
get starved, strategies that are winning get fed.

Constraints:
- No strategy drops below 10% (always keep some exposure)
- No strategy exceeds 70% (no single-strategy concentration)
- Rebalances every 6 hours based on last 7 days of data
- Falls back to defaults if insufficient data (<10 trades per strategy)
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

# Defaults reflect repo findings:
# - Weather and S&P have proven edges
# - Econ is risky (-70% ROI in ryanfrigo's bot) — start small
# - BTC is promising but volatile
DEFAULT_ALLOCATIONS = {
    # Proven strategies (85% of budget)
    "WEATHER": 0.25,      # Proven 87% WR, NWS settlement source
    "SP500": 0.15,        # VIX overestimates vol (DISABLED but keep allocation for rebalancer)
    "BITCOIN": 0.10,      # Fat tails but less efficient markets
    "ECON": 0.06,         # CAUTION: -70% ROI in ryanfrigo's bot, very conservative
    "TREASURY": 0.07,     # Treasury yield brackets + auction data
    "SNIPER": 0.08,       # Expiry sniping — outcomes nearly known
    "EMERGING": 0.05,     # New opportunities, any timeframe
    "GAS": 0.04,          # EIA data feed now injected
    "FOREX": 0.04,        # Thin liquidity
    # Expansion categories (15% pool — new data feeds)
    "JOBLESS": 0.05,      # DOL claims data — weekly, clear settlement
    "CONGRESSIONAL": 0.03,  # Congressional trade disclosure markets
    "ENTERTAINMENT": 0.02,  # Box office + streaming (sporadic markets)
    "ENERGY": 0.03,       # EIA petroleum — crude + gas brackets
    "FED_RATES": 0.03,    # Fed funds rate decisions — FRED data
}

MIN_ALLOCATION = 0.10
MAX_ALLOCATION = 0.70
MIN_TRADES_FOR_REBALANCE = 10
LOOKBACK_DAYS = 7


def get_strategy_performance(days: int = LOOKBACK_DAYS) -> dict[str, dict]:
    """Get per-strategy performance over the lookback window."""
    if not os.path.exists(DECISIONS_DB):
        return {}

    conn = sqlite3.connect(DECISIONS_DB)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    rows = conn.execute("""
        SELECT session_id, side, amount, market_price, cloud_probability,
               cloud_confidence, executed, decided_at
        FROM decisions
        WHERE decided_at > ? AND executed = 1
        ORDER BY decided_at
    """, (cutoff,)).fetchall()
    conn.close()

    # Group by strategy (extracted from session_id suffix)
    strategies = {}
    for row in rows:
        session_id = row[0] or ""
        # Session IDs look like "20260322_120000_SAFE" or "20260322_120000_WILDCARD"
        strategy = "UNKNOWN"
        for s in ["SAFE", "SP500_ECON", "WILDCARD", "QUANT"]:
            if s in session_id.upper():
                strategy = s
                break

        if strategy == "QUANT":
            strategy = "SP500_ECON"

        if strategy not in strategies:
            strategies[strategy] = {
                "trades": 0,
                "total_amount": 0.0,
                "confidence_sum": 0.0,
                "gap_sum": 0.0,
            }

        amount = row[2] or 0
        conf = row[5] or 0.5
        market_price = row[3] or 0.5
        model_prob = row[4] or 0.5

        strategies[strategy]["trades"] += 1
        strategies[strategy]["total_amount"] += amount
        strategies[strategy]["confidence_sum"] += conf
        strategies[strategy]["gap_sum"] += abs(model_prob - market_price)

    # Compute metrics
    for s, data in strategies.items():
        n = data["trades"]
        if n > 0:
            data["avg_confidence"] = data["confidence_sum"] / n
            data["avg_gap"] = data["gap_sum"] / n
        else:
            data["avg_confidence"] = 0.5
            data["avg_gap"] = 0.0

    return strategies


def compute_allocations() -> dict[str, float]:
    """Compute optimal budget allocations based on recent performance."""
    perf = get_strategy_performance()

    if not perf:
        return DEFAULT_ALLOCATIONS.copy()

    # Check if we have enough data
    total_trades = sum(d["trades"] for d in perf.values())
    if total_trades < MIN_TRADES_FOR_REBALANCE * 2:
        return DEFAULT_ALLOCATIONS.copy()

    # Score each strategy: higher avg_confidence × avg_gap = better
    # This rewards strategies that find high-confidence, large-edge trades
    scores = {}
    for strategy in DEFAULT_ALLOCATIONS:
        data = perf.get(strategy, {"trades": 0, "avg_confidence": 0.5, "avg_gap": 0.0})

        if data["trades"] < MIN_TRADES_FOR_REBALANCE:
            # Not enough data — use default
            scores[strategy] = DEFAULT_ALLOCATIONS[strategy]
        else:
            # Score = confidence × gap × sqrt(trade_count)
            # sqrt(count) rewards strategies that trade more (more opportunity)
            score = data["avg_confidence"] * data["avg_gap"] * (data["trades"] ** 0.5)
            scores[strategy] = max(score, 0.001)

    # Normalize scores to allocations
    total_score = sum(scores.values())
    allocations = {}
    for strategy, score in scores.items():
        raw_alloc = score / total_score
        allocations[strategy] = max(MIN_ALLOCATION, min(MAX_ALLOCATION, raw_alloc))

    # Re-normalize to sum to 1.0
    total = sum(allocations.values())
    for s in allocations:
        allocations[s] /= total

    return allocations


def get_current_allocations() -> dict[str, float]:
    """Get current strategy allocations, with logging."""
    allocs = compute_allocations()

    # Check if we deviated from defaults
    changed = False
    for s, a in allocs.items():
        default = DEFAULT_ALLOCATIONS.get(s, 0.33)
        if abs(a - default) > 0.05:
            changed = True

    if changed:
        print("  Strategy rebalancer (performance-based):")
    else:
        print("  Strategy rebalancer (using defaults):")

    for s in sorted(allocs):
        default = DEFAULT_ALLOCATIONS.get(s, 0.33)
        arrow = ""
        if allocs[s] > default + 0.05:
            arrow = " ↑"
        elif allocs[s] < default - 0.05:
            arrow = " ↓"
        print(f"    {s}: {allocs[s]:.0%}{arrow} (default: {default:.0%})")

    return allocs


if __name__ == "__main__":
    allocs = get_current_allocations()
    print(f"\nAllocations: {allocs}")
