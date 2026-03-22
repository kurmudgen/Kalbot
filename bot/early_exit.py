"""
Early exit / profit taking: monitors open positions and sells early
when price has moved enough in our favor to lock in profit.

Rules:
- Take profit at 60%+ of max possible gain
- Cut losses if price moves 30%+ against us
- Never hold through a reversal when we're up big
"""

import os
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=True)

DECISIONS_DB = os.path.join(os.path.dirname(__file__), "..", "logs", "decisions.sqlite")
MARKETS_DB = os.path.join(os.path.dirname(__file__), "..", "data", "live", "markets.sqlite")

# Profit/loss thresholds
TAKE_PROFIT_PCT = 0.60   # Sell when we've captured 60% of max possible profit
STOP_LOSS_PCT = 0.30     # Cut losses at 30% of position value
MIN_PROFIT_CENTS = 5     # Don't bother exiting for less than 5 cents per contract


def get_open_positions() -> list[dict]:
    """Get all executed trades that haven't been exited."""
    if not os.path.exists(DECISIONS_DB):
        return []

    conn = sqlite3.connect(DECISIONS_DB)
    conn.row_factory = sqlite3.Row

    # Get trades that were executed and not yet exited
    rows = conn.execute("""
        SELECT * FROM decisions
        WHERE executed = 1
        AND ticker NOT IN (
            SELECT ticker FROM decisions WHERE executed = 1 AND side LIKE '%EXIT%'
        )
        ORDER BY decided_at DESC
    """).fetchall()
    conn.close()

    return [dict(r) for r in rows]


def get_current_price(ticker: str) -> float | None:
    """Get current market price for a ticker."""
    if not os.path.exists(MARKETS_DB):
        return None

    conn = sqlite3.connect(MARKETS_DB)
    row = conn.execute(
        "SELECT last_price FROM markets WHERE ticker = ?", (ticker,)
    ).fetchone()
    conn.close()

    if row and row[0] is not None:
        return row[0] / 100.0
    return None


def evaluate_exit(position: dict) -> dict | None:
    """Determine if a position should be exited early."""
    ticker = position["ticker"]
    entry_price = position["market_price"]
    side = position["side"]
    amount = position["amount"]

    current_price = get_current_price(ticker)
    if current_price is None:
        return None

    # Calculate P&L
    if side == "YES":
        # Bought YES at entry_price, current value is current_price
        # Max profit = 1.0 - entry_price (if resolves YES)
        # Current P&L = current_price - entry_price
        max_profit = 1.0 - entry_price
        current_pnl = current_price - entry_price
        pnl_per_contract = current_pnl
    else:
        # Bought NO at (1 - entry_price), current value is (1 - current_price)
        # Max profit = entry_price (if resolves NO)
        # Current P&L = entry_price - current_price (NO value goes up when price drops)
        max_profit = entry_price
        current_pnl = entry_price - current_price
        pnl_per_contract = current_pnl

    if max_profit <= 0:
        return None

    pnl_pct = current_pnl / max_profit if max_profit > 0 else 0

    # Take profit: we've captured enough of the max gain
    if pnl_pct >= TAKE_PROFIT_PCT and pnl_per_contract * 100 >= MIN_PROFIT_CENTS:
        return {
            "ticker": ticker,
            "action": "TAKE_PROFIT",
            "entry_price": entry_price,
            "current_price": current_price,
            "pnl_per_contract": pnl_per_contract,
            "pnl_pct": pnl_pct,
            "side": side,
            "reason": f"Profit target hit: {pnl_pct:.0%} of max gain captured (${pnl_per_contract:.2f}/contract)",
        }

    # Stop loss: price moved too far against us
    if current_pnl < 0 and abs(current_pnl) >= STOP_LOSS_PCT * (1.0 if side == "YES" else entry_price):
        return {
            "ticker": ticker,
            "action": "STOP_LOSS",
            "entry_price": entry_price,
            "current_price": current_price,
            "pnl_per_contract": pnl_per_contract,
            "pnl_pct": pnl_pct,
            "side": side,
            "reason": f"Stop loss: down ${abs(pnl_per_contract):.2f}/contract ({abs(pnl_pct):.0%})",
        }

    return None


def execute_exit(exit_signal: dict, session_id: str = "") -> bool:
    """Execute an early exit (sell the position)."""
    paper_trade = os.getenv("PAPER_TRADE", "true").lower() == "true"
    ticker = exit_signal["ticker"]
    action = exit_signal["action"]

    conn = sqlite3.connect(DECISIONS_DB)

    if paper_trade:
        print(f"  PAPER EXIT ({action}): {ticker} — {exit_signal['reason']}")
    else:
        try:
            from pykalshi import KalshiClient, Side as KalshiSide, OrderType

            pk_path = os.getenv("KALSHI_PRIVATE_KEY", "")
            if not os.path.isabs(pk_path):
                pk_path = os.path.join(os.path.dirname(__file__), "..", pk_path)

            client = KalshiClient(
                api_key_id=os.getenv("KALSHI_API_KEY", ""),
                private_key_path=pk_path,
            )

            # Sell the opposite side
            sell_side = KalshiSide.NO if exit_signal["side"] == "YES" else KalshiSide.YES
            client.portfolio.create_order(
                ticker=ticker,
                side=sell_side,
                type=OrderType.MARKET,
                count=1,
            )
            client.close()
            print(f"  LIVE EXIT ({action}): {ticker} — {exit_signal['reason']}")
        except Exception as e:
            print(f"  EXIT FAILED: {ticker} — {e}")
            conn.close()
            return False

    # Log the exit
    conn.execute(
        """INSERT INTO decisions
           (ticker, title, category, cloud_probability, cloud_confidence,
            market_price, price_gap, side, amount, reasoning,
            mode, executed, error, decided_at, session_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, ?, ?)""",
        (ticker, "", "", 0, 0,
         exit_signal["current_price"], 0,
         f"{exit_signal['side']}_EXIT_{action}",
         0, exit_signal["reason"],
         "PAPER" if paper_trade else "LIVE",
         datetime.now(timezone.utc).isoformat(),
         session_id),
    )
    conn.commit()
    conn.close()
    return True


def check_all_positions(session_id: str = "") -> list[dict]:
    """Check all open positions for exit signals."""
    positions = get_open_positions()
    exits = []

    if not positions:
        return exits

    print(f"Checking {len(positions)} open positions for exits...")

    for pos in positions:
        signal = evaluate_exit(pos)
        if signal:
            success = execute_exit(signal, session_id)
            if success:
                exits.append(signal)

    return exits


if __name__ == "__main__":
    exits = check_all_positions()
    if exits:
        print(f"\n{len(exits)} positions exited")
    else:
        print("No exit signals")
