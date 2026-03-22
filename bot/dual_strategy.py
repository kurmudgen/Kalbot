"""
Dual-strategy runner:
  Bot A ("Safe") — weather + data releases, high confidence, consistent edge
  Bot B ("Wildcard") — all categories, breaking news detection, higher risk/reward

Both share the same Kalshi account and respect a combined spending limit.
"""

import os
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

sys.path.insert(0, os.path.dirname(__file__))

SESSIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sessions.sqlite")
LOOP_INTERVAL = 300  # 5 minutes
RUN_DURATION_HOURS = float(os.getenv("RUN_DURATION_HOURS", "0"))

# Budget split: 70% safe, 30% wildcard
SAFE_BUDGET_FRACTION = 0.70
WILDCARD_BUDGET_FRACTION = 0.30

# Strategy configs
SAFE_CONFIG = {
    "name": "SAFE",
    "categories": ["weather"],
    "confidence_min": 0.65,
    "price_gap_min": 0.06,
    "expiry_hours": 48,        # Focus on markets expiring within 48hr
    "kelly_fraction": 0.30,    # Slightly more aggressive — proven edge
}

WILDCARD_CONFIG = {
    "name": "WILDCARD",
    "categories": ["economics", "inflation", "tsa", "weather"],
    "confidence_min": 0.80,    # Higher bar — less certain
    "price_gap_min": 0.12,     # Only trade big mispricings
    "expiry_hours": 168,       # Wider window (1 week)
    "kelly_fraction": 0.15,    # More conservative sizing
}


def run_strategy(strategy: dict, budget: float, session_id: str) -> dict:
    """Run one strategy cycle."""
    from market_scanner import init_db as init_markets_db, scan_markets
    from local_filter import run_filter
    from price_tracker import snapshot_prices, get_expiring_soon, prioritize_markets
    from auto_scaler import get_scaled_budget

    # Use ensemble if available, else single model
    if os.getenv("ANTHROPIC_API_KEY") or os.getenv("DEEPSEEK_API_KEY"):
        from ensemble_analyst import analyze_markets
    else:
        from cloud_analyst import analyze_markets

    from executor import execute_trades

    stats = {
        "strategy": strategy["name"],
        "markets_scanned": 0,
        "markets_filtered": 0,
        "markets_analyzed": 0,
        "trades_placed": 0,
        "errors": 0,
    }

    name = strategy["name"]

    # Step 1: Scan (shared between strategies)
    try:
        markets_conn = init_markets_db()
        stats["markets_scanned"] = scan_markets(markets_conn)
        markets_conn.close()
    except Exception as e:
        print(f"  [{name}] Scanner error: {e}")
        stats["errors"] += 1

    # Step 2: Track prices
    try:
        snapshot_prices()
    except Exception:
        pass

    # Step 3: Local filter
    try:
        passed = run_filter()

        # Filter to this strategy's categories
        cat_set = set(strategy["categories"])
        passed = [m for m in passed if m.get("category") in cat_set]

        # Filter by expiry window
        expiry_hours = strategy.get("expiry_hours", 168)
        if expiry_hours < 168:
            expiring = get_expiring_soon(expiry_hours)
            expiring_tickers = {m["ticker"] for m in expiring}
            # Prioritize expiring markets but don't exclude others entirely
            passed = sorted(passed,
                            key=lambda m: (m.get("ticker") in expiring_tickers, m.get("confidence", 0)),
                            reverse=True)

        passed = prioritize_markets(passed)
        stats["markets_filtered"] = len(passed)
        print(f"  [{name}] {len(passed)} markets passed filter")
    except Exception as e:
        print(f"  [{name}] Filter error: {e}")
        traceback.print_exc()
        stats["errors"] += 1
        passed = []

    if not passed:
        return stats

    # Step 4: Ensemble analyst
    try:
        analyzed = analyze_markets(passed)
        stats["markets_analyzed"] = len(analyzed)
        print(f"  [{name}] {len(analyzed)} reached consensus")
    except Exception as e:
        print(f"  [{name}] Analyst error: {e}")
        traceback.print_exc()
        stats["errors"] += 1
        analyzed = []

    # Step 5: Execute with strategy-specific parameters
    if analyzed:
        try:
            # Temporarily override environment for this strategy
            orig_spend = os.environ.get("MAX_NIGHTLY_SPEND", "50")
            orig_trade = os.environ.get("MAX_TRADE_SIZE", "10")
            os.environ["MAX_NIGHTLY_SPEND"] = str(budget)
            os.environ["MAX_TRADE_SIZE"] = str(min(budget * 0.3, float(orig_trade) * 3))

            trades = execute_trades(analyzed, session_id=f"{session_id}_{name}")
            stats["trades_placed"] = len(trades)

            os.environ["MAX_NIGHTLY_SPEND"] = orig_spend
            os.environ["MAX_TRADE_SIZE"] = orig_trade
        except Exception as e:
            print(f"  [{name}] Executor error: {e}")
            traceback.print_exc()
            stats["errors"] += 1

    return stats


def main():
    from auto_scaler import get_scaled_budget
    from run_night import init_sessions_db, print_checkin

    session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    start_time = datetime.now(timezone.utc)

    total_budget = get_scaled_budget()
    safe_budget = total_budget * SAFE_BUDGET_FRACTION
    wildcard_budget = total_budget * WILDCARD_BUDGET_FRACTION

    print(f"{'='*60}")
    print(f"  KALBOT DUAL STRATEGY")
    print(f"  Session: {session_id}")
    print(f"  Total budget: ${total_budget:.0f}/night")
    print(f"  Safe (weather): ${safe_budget:.0f} | Wildcard: ${wildcard_budget:.0f}")
    if RUN_DURATION_HOURS > 0:
        print(f"  Duration: {RUN_DURATION_HOURS} hours")
    else:
        print(f"  Running continuously (Ctrl+C to stop)")
    print(f"{'='*60}")

    sess_conn = init_sessions_db()
    sess_conn.execute(
        """INSERT INTO sessions
           (session_id, start_time, cycles_completed, markets_scanned,
            markets_filtered, markets_analyzed, trades_placed, errors, status)
           VALUES (?, ?, 0, 0, 0, 0, 0, 0, 'running')""",
        (session_id, start_time.isoformat()),
    )
    sess_conn.commit()

    totals = {
        "cycles": 0,
        "markets_scanned": 0,
        "markets_filtered": 0,
        "markets_analyzed": 0,
        "trades_placed": 0,
        "errors": 0,
    }

    CHECKIN_INTERVAL = 6 * 3600

    try:
        while True:
            if RUN_DURATION_HOURS > 0:
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
                if elapsed >= RUN_DURATION_HOURS:
                    break

            totals["cycles"] += 1
            print(f"\n{'='*40}")
            print(f"  CYCLE {totals['cycles']} — {datetime.now(timezone.utc).isoformat()[:19]}")
            print(f"{'='*40}")

            # Step 0: Check for early exits on open positions
            try:
                from early_exit import check_all_positions
                exits = check_all_positions(session_id)
                if exits:
                    print(f"  Early exits: {len(exits)}")
            except Exception as e:
                print(f"  Early exit check error: {e}")

            # Step 0b: Polymarket signal detection
            try:
                from polymarket_signal import snapshot_and_detect
                pm_signals = snapshot_and_detect()
                if pm_signals:
                    print(f"  Polymarket signals: {len(pm_signals)}")
            except Exception as e:
                print(f"  Polymarket signal error: {e}")

            # Step 0c: Market making on thin weather markets
            try:
                from market_maker import run_market_maker
                mm_results = run_market_maker(session_id)
                if mm_results:
                    print(f"  Market making: {len(mm_results)} positions")
            except Exception as e:
                print(f"  Market making error: {e}")

            # Step 0d: Data release sniping
            try:
                from data_sniper import run_sniper
                import sqlite3 as _sql
                _mdb = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")
                if os.path.exists(_mdb):
                    _conn = _sql.connect(_mdb)
                    _conn.row_factory = _sql.Row
                    _all_markets = [dict(r) for r in _conn.execute("SELECT * FROM markets").fetchall()]
                    _conn.close()
                    sniped = run_sniper(_all_markets, session_id)
                    if sniped:
                        print(f"  Data sniper: {len(sniped)} flagged")
            except Exception as e:
                print(f"  Data sniper error: {e}")

            # Run Safe strategy
            print(f"\n--- BOT A: SAFE (weather, ${safe_budget:.0f}) ---")
            safe_stats = run_strategy(SAFE_CONFIG, safe_budget, session_id)

            # Run Wildcard strategy
            print(f"\n--- BOT B: WILDCARD (all cats, ${wildcard_budget:.0f}) ---")
            wildcard_stats = run_strategy(WILDCARD_CONFIG, wildcard_budget, session_id)

            # Aggregate
            for key in ["markets_scanned", "markets_filtered", "markets_analyzed", "trades_placed", "errors"]:
                totals[key] += safe_stats.get(key, 0) + wildcard_stats.get(key, 0)

            # Update session
            sess_conn.execute(
                """UPDATE sessions SET
                   cycles_completed = ?, markets_scanned = ?,
                   markets_filtered = ?, markets_analyzed = ?,
                   trades_placed = ?, errors = ?
                   WHERE session_id = ?""",
                (totals["cycles"], totals["markets_scanned"],
                 totals["markets_filtered"], totals["markets_analyzed"],
                 totals["trades_placed"], totals["errors"], session_id),
            )
            sess_conn.commit()

            # Periodic check-in
            elapsed_sec = (datetime.now(timezone.utc) - start_time).total_seconds()
            if totals["cycles"] > 1 and elapsed_sec % CHECKIN_INTERVAL < LOOP_INTERVAL:
                print_checkin(totals, start_time.isoformat(), session_id)

            print(f"\nSleeping {LOOP_INTERVAL}s...")
            time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nStopped by user")

    end_time = datetime.now(timezone.utc).isoformat()
    sess_conn.execute(
        "UPDATE sessions SET end_time = ?, status = 'completed' WHERE session_id = ?",
        (end_time, session_id),
    )
    sess_conn.commit()
    sess_conn.close()

    print(f"\n{'='*60}")
    print(f"  SESSION COMPLETE: {session_id}")
    print(f"  Cycles: {totals['cycles']}")
    print(f"  Trades: {totals['trades_placed']}")
    print(f"  Errors: {totals['errors']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
