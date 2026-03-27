"""
Hourly dynamic strategy rebalancer.

Rolling 48hr window of resolved trades. Allocates daily budget:
  - 5% floor per active strategy
  - 70% distributed by win_rate * avg_edge
  - 25% volatility reserve (released when conf>0.85 and WR>65%)

Momentum: 3 consecutive wins -> +10% from reserve. 3 losses -> -50%.
New categories (<10 resolved): fixed 8% until enough history.

Stores hourly snapshots in rebalancer_history table.
"""

import os
import sqlite3
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
RESOLUTIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "resolutions.sqlite")

DEFAULT_ALLOCATIONS = {
    "WEATHER": 0.25,
    "SP500": 0.15,
    "BITCOIN": 0.10,
    "ECON": 0.06,
    "TREASURY": 0.07,
    "SNIPER": 0.08,
    "EMERGING": 0.05,
    "GAS": 0.04,
    # "FOREX": 0.00,  # DISABLED — 1W/8L, bracket flooding, no edge
    "JOBLESS": 0.05,
    "CONGRESSIONAL": 0.03,
    "ENTERTAINMENT": 0.02,
    "ENERGY": 0.03,
    "FED_RATES": 0.03,
}

FLOOR_PCT = 0.05
PERFORMANCE_PCT = 0.70
RESERVE_PCT = 0.25
MAX_ALLOCATION = 0.50
MIN_TRADES_FOR_PERFORMANCE = 10
LOOKBACK_HOURS = 48
NEW_CATEGORY_FIXED = 0.08


def _init_history_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rebalancer_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            strategy TEXT,
            allocation_pct REAL,
            win_rate REAL,
            avg_edge REAL,
            streak INTEGER,
            reason TEXT
        )
    """)
    conn.commit()


def get_resolved_performance(hours: int = LOOKBACK_HOURS) -> dict[str, dict]:
    """Get per-strategy performance from resolved trades in rolling window."""
    if not os.path.exists(RESOLUTIONS_DB):
        return {}

    conn = sqlite3.connect(RESOLUTIONS_DB)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    rows = conn.execute("""
        SELECT category, pnl, our_confidence, strategy, resolved_at
        FROM resolved_trades
        WHERE resolved_at > ?
        ORDER BY resolved_at
    """, (cutoff,)).fetchall()
    conn.close()

    strategies = {}
    for row in rows:
        # Map category to strategy name
        cat = (row["category"] or "").upper()
        session = (row["strategy"] or "").upper()

        # Try to extract strategy from session_id
        strat = cat
        for s in DEFAULT_ALLOCATIONS:
            if s in session:
                strat = s
                break

        if strat not in strategies:
            strategies[strat] = {
                "trades": 0, "wins": 0, "losses": 0,
                "total_pnl": 0, "edge_sum": 0,
                "conf_sum": 0, "outcomes": [],
            }

        won = (row["pnl"] or 0) > 0
        strategies[strat]["trades"] += 1
        strategies[strat]["wins" if won else "losses"] += 1
        strategies[strat]["total_pnl"] += row["pnl"] or 0
        strategies[strat]["conf_sum"] += row["our_confidence"] or 0.5
        strategies[strat]["outcomes"].append(1 if won else 0)

    # Compute metrics
    for s, d in strategies.items():
        n = d["trades"]
        d["win_rate"] = d["wins"] / n if n > 0 else 0
        d["avg_conf"] = d["conf_sum"] / n if n > 0 else 0.5
        d["avg_edge"] = d["total_pnl"] / n if n > 0 else 0

        # Streak: count consecutive same results from most recent
        streak = 0
        if d["outcomes"]:
            last = d["outcomes"][-1]
            for o in reversed(d["outcomes"]):
                if o == last:
                    streak += 1
                else:
                    break
            streak = streak if last == 1 else -streak
        d["streak"] = streak

    return strategies


def compute_allocations() -> dict[str, float]:
    """Compute hourly budget allocations with momentum and reserves."""
    perf = get_resolved_performance()
    allocations = {}

    active_strategies = list(DEFAULT_ALLOCATIONS.keys())
    n_active = len(active_strategies)

    # Start with floor allocation for everyone
    for s in active_strategies:
        allocations[s] = FLOOR_PCT

    remaining = 1.0 - (FLOOR_PCT * n_active)
    if remaining <= 0:
        # Too many strategies — just use floor
        total = sum(allocations.values())
        return {s: v / total for s, v in allocations.items()}

    # Performance portion (70% of remaining)
    perf_pool = remaining * PERFORMANCE_PCT
    reserve_pool = remaining * RESERVE_PCT

    # Score each strategy by win_rate * avg_edge
    scores = {}
    for s in active_strategies:
        data = perf.get(s)

        if data is None or data["trades"] < MIN_TRADES_FOR_PERFORMANCE:
            # New category — fixed allocation
            allocations[s] = NEW_CATEGORY_FIXED
            scores[s] = 0  # Don't participate in performance pool
        else:
            # Score = win_rate * abs(avg_edge)
            wr = data["win_rate"]
            edge = abs(data["avg_edge"]) if data["avg_edge"] != 0 else 0.01
            scores[s] = max(wr * edge, 0.001)

    # Distribute performance pool
    total_score = sum(scores.values())
    if total_score > 0:
        for s in active_strategies:
            if scores.get(s, 0) > 0:
                allocations[s] += perf_pool * (scores[s] / total_score)

    # Volatility reserve — released to high-confidence winners
    for s in active_strategies:
        data = perf.get(s)
        if data and data["trades"] >= MIN_TRADES_FOR_PERFORMANCE:
            if data["avg_conf"] > 0.85 and data["win_rate"] > 0.65:
                share = reserve_pool / max(1, sum(
                    1 for st in active_strategies
                    if perf.get(st) and perf[st].get("avg_conf", 0) > 0.85
                    and perf[st].get("win_rate", 0) > 0.65
                ))
                allocations[s] += share

    # Momentum adjustments
    for s in active_strategies:
        data = perf.get(s)
        if data:
            if data["streak"] >= 3:
                # 3 consecutive wins — bonus from reserve
                allocations[s] *= 1.10
            elif data["streak"] <= -3:
                # 3 consecutive losses — cut 50%
                allocations[s] *= 0.50

    # Enforce max and re-normalize
    for s in allocations:
        allocations[s] = min(allocations[s], MAX_ALLOCATION)

    total = sum(allocations.values())
    if total > 0:
        allocations = {s: v / total for s, v in allocations.items()}

    return allocations


def log_rebalance(allocations: dict, perf: dict):
    """Store hourly snapshot in rebalancer_history."""
    if not os.path.exists(DECISIONS_DB):
        return
    conn = sqlite3.connect(DECISIONS_DB)
    _init_history_table(conn)
    now = datetime.now(timezone.utc).isoformat()

    for s, alloc in allocations.items():
        data = perf.get(s, {})
        conn.execute(
            """INSERT INTO rebalancer_history
               (timestamp, strategy, allocation_pct, win_rate, avg_edge, streak, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (now, s, round(alloc, 4),
             data.get("win_rate", 0), data.get("avg_edge", 0),
             data.get("streak", 0),
             "new_category" if data.get("trades", 0) < MIN_TRADES_FOR_PERFORMANCE else "performance"),
        )
    conn.commit()
    conn.close()


def get_current_allocations() -> dict[str, float]:
    """Get current strategy allocations with logging."""
    perf = get_resolved_performance()
    allocs = compute_allocations()

    has_perf = any(d["trades"] >= MIN_TRADES_FOR_PERFORMANCE for d in perf.values()) if perf else False

    if has_perf:
        print("  Strategy rebalancer (hourly, performance-based):")
    else:
        print("  Strategy rebalancer (hourly, defaults + new-category ramp):")

    for s in sorted(allocs, key=lambda x: -allocs[x]):
        data = perf.get(s, {})
        wr = data.get("win_rate", 0)
        streak = data.get("streak", 0)
        trades = data.get("trades", 0)
        streak_str = f" W{streak}" if streak > 0 else f" L{abs(streak)}" if streak < 0 else ""
        print(f"    {s:<15} {allocs[s]:>5.1%} | {trades}t {wr:.0%}WR{streak_str}")

    # Log the rebalance
    try:
        log_rebalance(allocs, perf)
    except Exception:
        pass

    return allocs


if __name__ == "__main__":
    allocs = get_current_allocations()
    print(f"\nTotal: {sum(allocs.values()):.2f}")
