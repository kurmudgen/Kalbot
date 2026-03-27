"""
Dual-strategy runner:
  Bot A ("Safe") — weather + data releases, high confidence, consistent edge
  Bot B ("Wildcard") — all categories, breaking news detection, higher risk/reward

Both share the same Kalshi account and respect a combined spending limit.
"""

import os
import signal
import sqlite3
import sys
import time
import traceback
import atexit
from datetime import datetime, timezone

# Single-instance lock — prevent duplicate processes
LOCK_FILE = os.path.join(os.path.dirname(__file__), "..", f"{os.path.basename(__file__)}.lock")

if os.path.exists(LOCK_FILE):
    with open(LOCK_FILE) as f:
        existing_pid = f.read().strip()
    print(f"Already running as PID {existing_pid}. Exiting.")
    sys.exit(0)

with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))

def cleanup_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

atexit.register(cleanup_lock)


# Graceful shutdown — close DB connections before exit
def _graceful_shutdown(sig, frame):
    print("\nGraceful shutdown...")
    sys.exit(0)

signal.signal(signal.SIGTERM, _graceful_shutdown)

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

sys.path.insert(0, os.path.dirname(__file__))

SESSIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "sessions.sqlite")
LOOP_INTERVAL = 300  # 5 minutes
RUN_DURATION_HOURS = float(os.getenv("RUN_DURATION_HOURS", "0"))

# All strategy configs — rebalancer shifts budget between these
STRATEGIES = {
    "WEATHER": {
        "name": "WEATHER",
        "categories": ["weather"],
        "confidence_min": 0.65,
        "price_gap_min": 0.06,
        "expiry_hours": 48,
        "kelly_fraction": 0.30,
    },
    "SNIPER": {
        "name": "SNIPER",
        "categories": ["economics", "inflation", "tsa", "weather",
                        "congressional", "energy", "entertainment"],
        "confidence_min": 0.70,     # Lower bar — outcome is nearly known
        "price_gap_min": 0.08,      # Smaller edge ok near expiry
        "expiry_hours": 6,          # ONLY markets expiring within 6 hours
        "kelly_fraction": 0.25,     # More aggressive — high certainty
    },
    "EMERGING": {
        "name": "EMERGING",
        "categories": ["economics", "inflation", "tsa", "weather",
                        "congressional", "energy", "entertainment"],
        "confidence_min": 0.75,     # Standard bar
        "price_gap_min": 0.10,      # Decent edge required
        "expiry_hours": 168,        # Any timeframe
        "kelly_fraction": 0.15,     # Conservative — less certain further out
    },
    "JOBLESS": {
        "name": "JOBLESS",
        "categories": ["inflation"],
        "confidence_min": 0.70,
        "price_gap_min": 0.08,
        "expiry_hours": 72,         # Weekly claims cycle
        "kelly_fraction": 0.20,
    },
    "ENERGY": {
        "name": "ENERGY",
        "categories": ["energy"],
        "confidence_min": 0.70,
        "price_gap_min": 0.10,
        "expiry_hours": 168,
        "kelly_fraction": 0.15,
    },
    "CONGRESSIONAL": {
        "name": "CONGRESSIONAL",
        "categories": ["congressional"],
        "confidence_min": 0.75,     # Higher bar — less certain data
        "price_gap_min": 0.12,
        "expiry_hours": 168,
        "kelly_fraction": 0.10,     # Conservative until proven
    },
}

# Quantitative strategies (these have their own analysis modules)
# SP500 DISABLED — 0/22 win rate on first resolution, confidence not calibrated
# Re-enable only after proving edge on paper with bracket dedup active
QUANT_STRATEGIES = {
    # "SP500": {"module": "sp500_strategy", "func": "analyze_sp500_markets"},
    "ECON": {"module": "econ_strategy", "func": "analyze_econ_markets"},
    "BITCOIN": {"module": "bitcoin_strategy", "func": "analyze_btc_markets"},
    "TREASURY": {"module": "treasury_strategy", "func": "analyze_treasury_markets"},
    "GAS": {"module": "gas_strategy", "func": "analyze_gas_markets"},
    # "FOREX": {"module": "forex_strategy", "func": "analyze_forex_markets"},  # DISABLED — 1W/8L, -$18.80
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
        if expiry_hours <= 6:
            # Sniper mode: ONLY include markets expiring within the window
            expiring = get_expiring_soon(expiry_hours)
            expiring_tickers = {m["ticker"] for m in expiring}
            passed = [m for m in passed if m.get("ticker") in expiring_tickers]
            if passed:
                print(f"  [{name}] Sniper: {len(passed)} markets expiring within {expiry_hours}hr")
        elif expiry_hours < 168:
            # Soft filter: prioritize expiring but keep others
            expiring = get_expiring_soon(expiry_hours)
            expiring_tickers = {m["ticker"] for m in expiring}
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

    # Dynamic allocation based on what's performing best
    try:
        from strategy_rebalancer import get_current_allocations
        allocs = get_current_allocations()
    except Exception:
        allocs = {"SAFE": 0.50, "SP500_ECON": 0.25, "WILDCARD": 0.25}

    safe_budget = total_budget * allocs.get("SAFE", 0.50)
    sp_econ_budget = total_budget * allocs.get("SP500_ECON", 0.25)
    wildcard_budget = total_budget * allocs.get("WILDCARD", 0.25)

    print(f"{'='*60}")
    print(f"  KALBOT MULTI-STRATEGY")
    print(f"  Session: {session_id}")
    print(f"  Total budget: ${total_budget:.0f}/night")
    print(f"  Strategies: {len(STRATEGIES) + len(QUANT_STRATEGIES)}")
    for s in sorted(allocs, key=lambda x: -allocs[x]):
        budget = total_budget * allocs[s]
        print(f"    {s:<12} ${budget:>6.0f} ({allocs[s]:.0%})")
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

            # Kill switch check
            try:
                from kill_switch import should_trade
                ok, reason = should_trade()
                if not ok:
                    print(f"\n  !!! {reason} — skipping this cycle !!!")
                    time.sleep(LOOP_INTERVAL)
                    continue
            except Exception:
                pass

            # Step -1: Detect market regime (affects all strategies)
            regime_info = None
            try:
                from regime_detector import detect_regime
                regime_info = detect_regime()
                if totals["cycles"] % 12 == 1:  # Log regime hourly
                    print(f"  Regime: {regime_info['regime'].upper()} (vol: {regime_info['volatility']:.0%}, trend: {regime_info['trend_20d']:+.1%})")
            except Exception:
                pass

            # Step -0.5: SEC EDGAR filing monitor
            try:
                from edgar_monitor import scan_edgar
                edgar_signals = scan_edgar()
                if edgar_signals:
                    print(f"  EDGAR: {len(edgar_signals)} signals")
                    # Send to Telegram for urgent filings
                    try:
                        from telegram_alerts import system_alert
                        for sig in edgar_signals[:2]:
                            system_alert(f"SEC 8-K: {sig['direction'].upper()} {sig['ticker']} - {sig['trigger']}", "warning")
                    except Exception:
                        pass
            except Exception:
                pass

            # Update NWS weather forecasts (every 6 cycles = ~30 min)
            if totals["cycles"] % 6 == 0:
                try:
                    import sys as _sys2
                    _sys2.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "data"))
                    from weather_nws_feed import update_feed
                    update_feed()
                except Exception:
                    pass

            # Step 0: Resolve completed trades and track P&L
            try:
                from resolution_tracker import resolve_trades
                resolved = resolve_trades()
                if resolved["resolved"] > 0:
                    print(f"  Resolved: {resolved['resolved']} trades, P&L: ${resolved['pnl']:+.2f}")
            except Exception as e:
                print(f"  Resolution tracker error: {e}")

            # Step 0a: Cross-market divergence detection
            try:
                from cross_market import find_divergences
                divs = find_divergences()
                if divs:
                    print(f"  Cross-market: {len(divs)} divergences found")
            except Exception:
                pass

            # Step 0b: Check for early exits on open positions
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

            # Recompute allocations each cycle
            try:
                from strategy_rebalancer import compute_allocations
                allocs = compute_allocations()
            except Exception:
                allocs = {"WEATHER": 0.25, "SP500": 0.15, "ECON": 0.15,
                          "BITCOIN": 0.10, "TREASURY": 0.10, "GAS": 0.05,
                          "WILDCARD": 0.20}

            # Run quantitative strategies (each has its own analysis module)
            all_quant_signals = []
            for strat_name, strat_info in QUANT_STRATEGIES.items():
                budget = total_budget * allocs.get(strat_name, 0.10)
                if budget < 1:
                    continue
                try:
                    import importlib
                    mod = importlib.import_module(strat_info["module"])
                    func = getattr(mod, strat_info["func"])
                    print(f"\n--- {strat_name} (${budget:.0f}, {allocs.get(strat_name, 0):.0%}) ---")
                    signals = func()
                    if signals:
                        all_quant_signals.extend(signals)
                        print(f"  {len(signals)} signals")
                except Exception as e:
                    print(f"  {strat_name} error: {e}")

            # Execute quant signals through the executor (budget-limited)
            if all_quant_signals:
                try:
                    from executor import execute_trades
                    # Cap quant budget to sum of quant strategy allocations
                    quant_budget = sum(
                        total_budget * allocs.get(s, 0.05)
                        for s in QUANT_STRATEGIES
                    )
                    orig_spend = os.environ.get("MAX_NIGHTLY_SPEND", "50")
                    os.environ["MAX_NIGHTLY_SPEND"] = str(quant_budget)
                    quant_trades = execute_trades(all_quant_signals, session_id=f"{session_id}_QUANT")
                    os.environ["MAX_NIGHTLY_SPEND"] = orig_spend
                except Exception as e:
                    print(f"  Quant execution error: {e}")

            # Run ensemble strategies (local filter → cloud analyst → executor)
            all_strategy_stats = []
            for strat_name, config in STRATEGIES.items():
                budget = total_budget * allocs.get(strat_name, 0.15)
                if budget < 1:
                    continue
                print(f"\n--- {strat_name} (${budget:.0f}, {allocs.get(strat_name, 0):.0%}) ---")
                stats = run_strategy(config, budget, session_id)
                all_strategy_stats.append(stats)

            # Aggregate
            for stats in all_strategy_stats:
                for key in ["markets_scanned", "markets_filtered", "markets_analyzed", "trades_placed", "errors"]:
                    totals[key] += stats.get(key, 0)

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

            # Publish dashboard status every cycle
            try:
                from status_publisher import publish
                publish()
            except Exception as e:
                print(f"  Dashboard publish error: {e}")

            # API cost monitor (every 12 cycles = ~1 hour)
            if totals["cycles"] % 12 == 0:
                try:
                    from api_cost_monitor import run_cost_check
                    run_cost_check()
                except Exception:
                    pass

            # Periodic check-in
            elapsed_sec = (datetime.now(timezone.utc) - start_time).total_seconds()
            if totals["cycles"] > 1 and elapsed_sec % CHECKIN_INTERVAL < LOOP_INTERVAL:
                print_checkin(totals, start_time.isoformat(), session_id)

            # Hourly self-diagnostic
            if totals["cycles"] % 12 == 0:  # Every 12 cycles (~1 hour at 5min intervals)
                try:
                    from self_check import run_self_check
                    run_self_check()
                except Exception as e:
                    print(f"  Self-check error: {e}")

            # Self-calibration Tier 1: hourly market movement reflector
            if totals["cycles"] % 12 == 0:
                try:
                    from self_calibrator import run_tier1
                    run_tier1()
                except Exception as e:
                    print(f"  Self-cal T1 error: {e}")

            # Self-calibration Tier 2: 6-hour pattern analyzer
            if totals["cycles"] % 72 == 0:  # Every 72 cycles (~6 hours)
                try:
                    from self_calibrator import run_tier2
                    run_tier2()
                except Exception as e:
                    print(f"  Self-cal T2 error: {e}")

            # Daily performance report (every day at ~8AM)
            now_local = datetime.now()

            # Self-calibration Tier 3: daily adjustment executor (2am)
            if now_local.hour == 2 and totals["cycles"] % 12 == 0:
                try:
                    from self_calibrator import run_tier3
                    run_tier3()
                except Exception as e:
                    print(f"  Self-cal T3 error: {e}")

            # Self-calibration Tier 4: weekly benchmark (Sunday 3am)
            if now_local.weekday() == 6 and now_local.hour == 3 and totals["cycles"] % 12 == 0:
                try:
                    from self_calibrator import run_tier4
                    run_tier4()
                except Exception as e:
                    print(f"  Self-cal T4 error: {e}")

            if 8 <= now_local.hour < 9 and totals["cycles"] % 12 == 1:
                try:
                    from weekly_report import send_weekly_report
                    print("\n--- DAILY REPORT ---")
                    send_weekly_report()
                except Exception as e:
                    print(f"  Daily report error: {e}")

            # Weekly repo scan (Sundays at ~6AM)
            if now_local.weekday() == 6 and 6 <= now_local.hour < 7 and totals["cycles"] % 12 == 1:
                try:
                    from weekly_repo_scan import run_weekly_scan
                    print("\n--- WEEKLY REPO SCAN ---")
                    run_weekly_scan()
                except Exception as e:
                    print(f"  Repo scan error: {e}")

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
