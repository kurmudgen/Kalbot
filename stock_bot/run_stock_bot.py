"""
Stock trading bot runner.
Scans for penny stock momentum, analyzes with ensemble LLMs,
executes via Alpaca with Kelly sizing and stop-losses.

Runs alongside the Kalshi prediction market bot.
Same philosophy: paper trade first, prove edge, then scale.
"""

import os
import sys
import time
import traceback
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

sys.path.insert(0, os.path.dirname(__file__))

SCAN_INTERVAL = 300  # 5 minutes
MAX_POSITION_SIZE = float(os.getenv("STOCK_MAX_POSITION", "10"))  # $10 per trade to start
MAX_DAILY_SPEND = float(os.getenv("STOCK_MAX_DAILY", "50"))
MAX_POSITIONS = int(os.getenv("STOCK_MAX_POSITIONS", "5"))
STOP_LOSS_PCT = 0.10  # 10% stop loss
TAKE_PROFIT_PCT = 0.20  # 20% take profit


def is_market_open() -> bool:
    """Check if US stock market is open."""
    try:
        from alpaca_executor import get_alpaca_client
        api = get_alpaca_client()
        clock = api.get_clock()
        return clock.is_open
    except Exception:
        # Rough check: M-F, 9:30-4:00 ET
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        hour = now.hour
        return 9 <= hour < 16


def check_exits() -> int:
    """Check open positions for stop-loss or take-profit."""
    try:
        from alpaca_executor import get_alpaca_client, init_stock_db
        api = get_alpaca_client()
        positions = api.list_positions()
        exits = 0

        for p in positions:
            pnl_pct = float(p.unrealized_plpc)

            if pnl_pct <= -STOP_LOSS_PCT:
                print(f"  STOP LOSS: {p.symbol} at {pnl_pct*100:.1f}%")
                api.submit_order(p.symbol, qty=abs(float(p.qty)), side="sell", type="market", time_in_force="day")
                exits += 1

            elif pnl_pct >= TAKE_PROFIT_PCT:
                print(f"  TAKE PROFIT: {p.symbol} at {pnl_pct*100:.1f}%")
                api.submit_order(p.symbol, qty=abs(float(p.qty)), side="sell", type="market", time_in_force="day")
                exits += 1

        return exits
    except Exception as e:
        print(f"  Exit check error: {e}")
        return 0


def run_cycle(session_id: str) -> dict:
    """Run one scan → analyze → trade cycle."""
    from penny_scanner import find_momentum_plays
    from stock_analyst import analyze_stock
    from alpaca_executor import execute_stock_trade, get_account_info

    stats = {"scanned": 0, "analyzed": 0, "traded": 0, "errors": 0}

    # Check exits first
    exits = check_exits()
    if exits:
        print(f"  Exited {exits} positions")

    # Crypto runs 24/7 (doesn't need market hours)
    print("  --- Crypto Strategies ---")
    try:
        from crypto_strategy import run_crypto_scan
        crypto_trades = run_crypto_scan(session_id)
        stats["traded"] += len(crypto_trades)
    except Exception as e:
        print(f"  Crypto error: {e}")

    # Get current state
    info = get_account_info()
    current_positions = len(info.get("positions", []))
    held_symbols = {p["symbol"] for p in info.get("positions", [])}

    if current_positions >= MAX_POSITIONS:
        print(f"  Max positions ({MAX_POSITIONS}) reached, skipping scan")
        return stats

    # Run blue chip / ETF strategy first (lower risk, higher data quality)
    print("  --- Blue Chip / ETF Strategy ---")
    try:
        from blue_chip_strategy import scan_and_analyze, check_pdt_safe
        from alpaca_executor import get_account_info

        info = get_account_info()
        account_val = info.get("portfolio_value", 100000)

        blue_signals = scan_and_analyze()
        for sig in blue_signals:
            # Skip if we already hold this stock
            if sig["symbol"] in held_symbols:
                continue

            if sig["action"] == "buy" and sig["confidence"] > 0.65:
                # Position size: max $500 per stock (safe for paper + real)
                max_per_stock = float(os.getenv("STOCK_MAX_POSITION", "10")) * 50  # $500 default
                amount = min(max_per_stock, account_val * 0.02)  # 2% of portfolio or $500
                qty = max(1, int(amount / sig["price"]))

                result = execute_stock_trade(
                    symbol=sig["symbol"],
                    side="buy",
                    qty=qty,
                    strategy=f"bluechip_{sig['category']}",
                    confidence=sig["confidence"],
                    reasoning=sig["reasoning"],
                    session_id=session_id,
                )
                if result:
                    stats["traded"] += 1
    except Exception as e:
        print(f"  Blue chip error: {e}")

    # Then run penny stock momentum scanner
    print("  --- Penny Stock Momentum ---")
    try:
        from enhanced_scanner import run_enhanced_scan
        candidates = run_enhanced_scan()
    except Exception:
        from penny_scanner import find_momentum_plays
        candidates = find_momentum_plays()
    stats["scanned"] = len(candidates)

    if not candidates:
        return stats

    # Analyze top candidates
    for stock in candidates[:5]:
        # Skip if we already have a position
        held_symbols = {p["symbol"] for p in info.get("positions", [])}
        if stock["symbol"] in held_symbols:
            continue

        print(f"  Analyzing {stock['symbol']}...")
        analysis = analyze_stock(stock)
        stats["analyzed"] += 1

        if analysis is None:
            continue

        if analysis["action"] == "buy" and analysis["confidence"] > 0.65:
            # Kelly-ish sizing
            edge = analysis["confidence"] - 0.5
            bet_fraction = edge * 0.25  # Quarter-Kelly
            amount = min(MAX_POSITION_SIZE, MAX_DAILY_SPEND * bet_fraction)
            qty = max(1, int(amount / stock["price"]))

            result = execute_stock_trade(
                symbol=stock["symbol"],
                side="buy",
                qty=qty,
                strategy="penny_momentum",
                confidence=analysis["confidence"],
                reasoning=analysis["reasoning"],
                session_id=session_id,
            )

            if result:
                stats["traded"] += 1
                print(f"  BOUGHT {qty} {stock['symbol']} @ ${stock['price']:.4f}")

    return stats


def main():
    session_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print(f"{'='*50}")
    print(f"  KALBOT STOCK TRADER (Alpaca)")
    print(f"  Session: {session_id}")
    print(f"  Max position: ${MAX_POSITION_SIZE}")
    print(f"  Max daily: ${MAX_DAILY_SPEND}")
    print(f"  Stop loss: {STOP_LOSS_PCT*100:.0f}% | Take profit: {TAKE_PROFIT_PCT*100:.0f}%")
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    print(f"  Mode: {'PAPER' if paper else 'LIVE'}")
    print(f"{'='*50}")

    while True:
        # Kill switch
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bot"))
            from kill_switch import is_killed
            if is_killed():
                print(f"  HALTED: Kill switch active. Delete ~/kalbot/STOP to resume.")
                time.sleep(60)
                continue
        except Exception:
            pass

        try:
            if is_market_open():
                print(f"\n  {datetime.now().strftime('%H:%M')} — Market OPEN, running cycle...")
                stats = run_cycle(session_id)
                print(f"  Scanned: {stats['scanned']} | Analyzed: {stats['analyzed']} | Traded: {stats['traded']}")
            else:
                print(f"  {datetime.now().strftime('%H:%M')} — Market closed, checking exits only...")
                check_exits()
        except Exception as e:
            print(f"  Cycle error: {e}")
            traceback.print_exc()

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
