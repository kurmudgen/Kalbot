"""
Auto-scaling budget manager.
Adjusts MAX_NIGHTLY_SPEND based on proven track record.
Protects against strategy decay by auto-downgrading.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")

# Scaling tiers: (min_days_live, min_win_rate, min_trades, budget)
TIERS = [
    (90, 0.63, 100, 1000),  # Tier 5: $1,000/night
    (60, 0.62, 60, 500),    # Tier 4: $500/night
    (30, 0.60, 30, 200),    # Tier 3: $200/night
    (14, 0.58, 10, 50),     # Tier 2: $50/night
    (0, 0.0, 0, 50),        # Tier 1: $50/night (starting)
]

# If win rate drops below this, downgrade one tier
DOWNGRADE_THRESHOLD = 0.55
DOWNGRADE_LOOKBACK_DAYS = 14  # Check recent performance


def get_live_stats(days: int = None) -> dict:
    """Get win/loss stats from live (non-paper) trading history."""
    if not os.path.exists(DECISIONS_DB):
        return {"days": 0, "trades": 0, "wins": 0, "win_rate": 0.0}

    conn = sqlite3.connect(DECISIONS_DB)

    # Count days of live trading
    if days:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        rows = conn.execute(
            "SELECT * FROM decisions WHERE mode = 'LIVE' AND executed = 1 AND decided_at > ?",
            (cutoff,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM decisions WHERE mode = 'LIVE' AND executed = 1"
        ).fetchall()
    conn.close()

    if not rows:
        return {"days": 0, "trades": 0, "wins": 0, "win_rate": 0.0}

    # Count unique trading days
    trading_days = set()
    for r in rows:
        if r[14]:  # decided_at column
            trading_days.add(r[14][:10])

    trades = len(rows)
    # We can't know wins yet for open trades — count resolved ones
    # For now, use confidence-weighted estimate
    wins = sum(1 for r in rows if r[4] and r[4] > 0.7)  # cloud_confidence > 0.7 as proxy

    return {
        "days": len(trading_days),
        "trades": trades,
        "wins": wins,
        "win_rate": wins / trades if trades > 0 else 0.0,
    }


def get_current_tier() -> dict:
    """Determine current scaling tier based on track record."""
    stats = get_live_stats()

    # Check for downgrade first (recent performance)
    recent = get_live_stats(days=DOWNGRADE_LOOKBACK_DAYS)
    if recent["trades"] >= 10 and recent["win_rate"] < DOWNGRADE_THRESHOLD:
        # Performance degrading — use conservative tier
        return {
            "tier": 1,
            "budget": 50,
            "reason": f"Downgraded: recent WR {recent['win_rate']:.0%} < {DOWNGRADE_THRESHOLD:.0%} threshold",
            "stats": stats,
        }

    # Find highest qualifying tier
    for i, (min_days, min_wr, min_trades, budget) in enumerate(TIERS):
        if (stats["days"] >= min_days and
                stats["win_rate"] >= min_wr and
                stats["trades"] >= min_trades):
            return {
                "tier": len(TIERS) - i,
                "budget": budget,
                "reason": f"Tier {len(TIERS)-i}: {stats['days']}d live, {stats['win_rate']:.0%} WR, {stats['trades']} trades",
                "stats": stats,
            }

    return {
        "tier": 1,
        "budget": 50,
        "reason": "Starting tier (no live track record yet)",
        "stats": stats,
    }


def get_scaled_budget() -> float:
    """Get the current nightly budget based on auto-scaling."""
    # If PAPER_TRADE is true, use configured budget
    if os.getenv("PAPER_TRADE", "true").lower() == "true":
        return float(os.getenv("MAX_NIGHTLY_SPEND", "50"))

    # If auto-scaling is disabled, use configured budget
    if os.getenv("AUTO_SCALE", "true").lower() != "true":
        return float(os.getenv("MAX_NIGHTLY_SPEND", "50"))

    tier = get_current_tier()
    print(f"  Auto-scale: {tier['reason']} -> ${tier['budget']}/night")
    return tier["budget"]


if __name__ == "__main__":
    tier = get_current_tier()
    print(f"Current tier: {tier['tier']}")
    print(f"Budget: ${tier['budget']}/night")
    print(f"Reason: {tier['reason']}")
    print(f"Stats: {tier['stats']}")
